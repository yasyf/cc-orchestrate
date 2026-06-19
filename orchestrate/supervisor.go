package orchestrate

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// restartBudget is the number of times the supervisor re-spawns one agent's
// vanished terminal before it gives up and terminally exits the agent. A healthy
// agent's budget is reset to zero by the tailer (see the resetRestart hook), so the
// budget bounds consecutive failures, not the agent's lifetime restart count.
const restartBudget = 3

// supervisorInterval is the cadence at which the keep-alive supervisor enumerates
// each enumerable backend's live agents and re-spawns any active agent whose
// terminal has vanished. It is a var so a test can shorten it race-free before
// constructing the supervisor.
var supervisorInterval = 30 * time.Second

// supervisor is the daemon-lifetime keep-alive sibling of boot reconcile: it polls
// each enumerable backend on a ticker and re-spawns (under a budget) any StatusActive
// agent whose terminal has vanished, routing through the same reconcileVanished
// decision site reconcile uses so a vanished agent is never both pruned and
// restarted.
type supervisor struct {
	interval time.Duration
}

func newSupervisor() *supervisor { return &supervisor{interval: supervisorInterval} }

// run drives the supervisor loop until ctx is cancelled (daemon shutdown), then
// returns. It scans on its own ticker; db and appendFn are the daemon-lifetime
// handles, not a per-request handler context.
func (s *supervisor) run(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.tick(ctx, db, appendFn); err != nil {
				log.Printf("cc-orchestrate: supervisor tick: %v", err)
			}
		}
	}
}

// tick enumerates each active workstream on an enumerable backend, diffs its live
// agent set against the StatusActive rows by terminal handle (the identical shape
// reconcileAgents uses), and routes every vanished active agent through
// reconcileVanished. A backend it cannot resolve, or one that cannot enumerate
// (superset), is skipped explicitly — an empty ListAgents there is "no signal",
// never "all gone".
func (s *supervisor) tick(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
	wss, err := listWorkstreams(ctx, db, "")
	if err != nil {
		return err
	}
	for _, ws := range wss {
		if ws.Status != StatusActive {
			continue
		}
		bk, ok := backend.Get(ws.Backend)
		if !ok || !bk.Caps().Has(backend.CanEnumerate) {
			continue
		}
		live, err := bk.ListAgents(ctx, backend.WorkstreamHandle{
			Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree,
		})
		if err != nil {
			log.Printf("cc-orchestrate: supervisor skips agents of %s: list agents: %v", ws.ID, err)
			continue
		}
		agents, err := listWorkstreamAgents(ctx, db, ws.ID)
		if err != nil {
			return err
		}
		for _, ag := range agents {
			if ag.Status != StatusActive || containsAgentHandle(live, ag.TerminalHandle) {
				continue
			}
			if err := reconcileVanished(ctx, db, appendFn, ag); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileVanished is the single decision site for a StatusActive agent whose
// backend terminal has vanished, shared by boot reconcile and the keep-alive
// supervisor so a vanished agent is never both pruned and restarted. Under
// restartBudget it re-spawns the agent (resume into a fresh terminal) and keeps the
// row active; at or over budget it abandons the agent and terminally exits it.
//
// It holds agentLock(ag.ID) and re-reads the row under the lock before acting:
// handleAgentKill and markSprintKilled take the same lock and flip the row to a
// terminal status, so re-checking StatusActive under the lock means a concurrently
// killed agent is never resurrected.
func reconcileVanished(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow) error {
	mu := agentLock(ag.ID)
	mu.Lock()
	defer mu.Unlock()
	cur, err := getAgent(ctx, db, ag.ID)
	if err != nil {
		return err
	}
	if cur.Status != StatusActive {
		return nil
	}
	if cur.RestartCount >= restartBudget {
		if _, err := appendFn(ctx, &event.Event{
			SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventAbandoned, Payload: abandonedPayload(cur),
		}); err != nil {
			return err
		}
		return softExitAgent(ctx, db, appendFn, cur)
	}
	// Count and persist the restart attempt BEFORE respawning, then mirror it onto the
	// in-memory row, so respawnAgent's tailer snapshot carries the incremented
	// RestartCount. The reset hook is gated on a positive count, so on a first restart
	// (0->1) the tailer must observe 1, not the stale 0, for a healthy recovery to clear
	// the budget. Counting first also means a respawn that fails leaves the attempt
	// recorded, so a persistently-failing agent climbs toward budget and is eventually
	// abandoned rather than retried forever at a stuck count; the agentLock + re-read of
	// cur serialize ticks, so a single vanish detection counts exactly one attempt.
	attempt := cur.RestartCount + 1
	stamp := nowStamp()
	if err := markRestartAttempt(ctx, db, cur.ID, attempt, stamp); err != nil {
		return err
	}
	cur.RestartCount = attempt
	cur.LastRestartAt = stamp
	handle, err := respawnAgent(ctx, db, appendFn, cur)
	if err != nil {
		return err
	}
	_, err = appendFn(ctx, &event.Event{
		SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventRestarted, Payload: restartedPayload(cur, handle.ID, attempt),
	})
	return err
}
