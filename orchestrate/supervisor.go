package orchestrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
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

// stalenessBound is how long an agent's transcript may go unwritten before the
// supervisor treats it as a death candidate and asks the backend to confirm the
// process is gone. Corroboration (AgentProber) makes a false positive impossible, so
// the bound only sets detection latency, not safety. It is a var so a test can
// shorten it.
var stalenessBound = 2 * time.Minute

// startupGrace suppresses the staleness probe for an agent spawned or restarted
// within this window, so a just-resumed agent whose transcript has not caught up yet
// is never mistaken for a dead one. It is belt to confirmDead's suspenders.
var startupGrace = 45 * time.Second

// supervisor is the daemon-lifetime keep-alive sibling of boot reconcile: it polls on
// a ticker and re-spawns (under a budget) any StatusActive agent whose terminal has
// vanished (enumerable backends) or whose process the backend confirms dead under a
// surviving terminal (AgentProber backends), routing through the reconcileVanished /
// reconcileDeadChild decision sites so a dying agent is never both pruned and restarted.
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

// tick reconciles each active workstream's agents on two independent signals: for an
// enumerable backend it diffs the live agent set against the StatusActive rows by
// terminal handle and resumes any whose terminal has vanished (reconcileVanished); for
// a backend that can probe process liveness (AgentProber) it resumes any agent whose
// transcript has gone stale and whose process the backend confirms is gone
// (reconcileDeadChild) — the remain-on-exit case a vanished-handle diff misses because
// a dead pane is still enumerated. The diff runs first: a dead-but-listed pane stays in
// the diff's live set, so the diff skips it and leaves it to the staleness pass, which
// kills the survivor before resuming. A backend it cannot resolve, or one that neither
// enumerates nor probes, is skipped.
func (s *supervisor) tick(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
	wss, err := listWorkstreams(ctx, db, "", "")
	if err != nil {
		return err
	}
	for _, ws := range wss {
		if ws.Status != StatusActive {
			continue
		}
		bk, ok := backend.Get(ws.Backend)
		if !ok {
			continue
		}
		prober, canProbe := bk.(backend.AgentProber)
		if !bk.Caps().Has(backend.CanEnumerate) && !canProbe {
			continue
		}
		agents, err := listWorkstreamAgents(ctx, db, ws.ID)
		if err != nil {
			return err
		}
		if bk.Caps().Has(backend.CanEnumerate) {
			if err := reconcileVanishedAgents(ctx, db, appendFn, ws, bk, agents); err != nil {
				return err
			}
		}
		if canProbe {
			if err := reconcileStaleAgents(ctx, db, appendFn, prober, agents); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileVanishedAgents diffs an enumerable backend's live agents against the
// workstream's StatusActive rows by terminal handle and routes every vanished one
// through reconcileVanished. A failed enumeration is "no signal" and skipped, never
// "all gone".
func reconcileVanishedAgents(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ws workstreamRow, bk backend.Backend, agents []agentRow) error {
	live, err := bk.ListAgents(ctx, backend.WorkstreamHandle{
		Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree,
	})
	if err != nil {
		log.Printf("cc-orchestrate: supervisor skips agents of %s: list agents: %v", ws.ID, err)
		return nil
	}
	for _, ag := range agents {
		if ag.Status != StatusActive || containsAgentHandle(live, ag.TerminalHandle) {
			continue
		}
		if err := reconcileVanished(ctx, db, appendFn, ag); err != nil {
			return err
		}
	}
	return nil
}

// reconcileStaleAgents resumes every active agent whose transcript has gone stale and
// whose backend confirms the process is dead — the terminal-outlived-its-child case.
// recentlySpawned skips a just-(re)spawned agent whose transcript has not caught up;
// proberConfirmDead is the authoritative gate that keeps an idle or rate-limited agent
// whose process is still alive from being resumed.
func reconcileStaleAgents(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, prober backend.AgentProber, agents []agentRow) error {
	for _, ag := range agents {
		if ag.Status != StatusActive || recentlySpawned(ag) || !transcriptStale(ag) {
			continue
		}
		if err := reconcileDeadChild(ctx, db, appendFn, ag, proberConfirmDead(db, prober)); err != nil {
			return err
		}
	}
	return nil
}

// reconcileVanished is the decision site for a StatusActive agent whose backend
// terminal has vanished, shared by boot reconcile and the keep-alive supervisor so a
// vanished agent is never both pruned and restarted. It holds agentLock(ag.ID) and
// re-reads the row under the lock before acting: handleAgentKill and markSprintKilled
// take the same lock and flip the row to a terminal status, so re-checking
// StatusActive under the lock means a concurrently killed agent is never resurrected.
// The vanished observation is about ag's terminal specifically, so a re-read whose
// handle has moved on means a concurrent respawn (a childExited report, a manual
// respawn) already replaced the terminal between the caller's snapshot and this lock —
// acting anyway would spawn a second resume of the same session and leak the fresh
// terminal untracked. The terminal is already gone, so it does not kill before
// respawning.
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
	if cur.TerminalHandle != ag.TerminalHandle {
		return nil
	}
	_, err = respawnUnderBudget(ctx, db, appendFn, cur, false)
	return err
}

// reconcileDeadChild is the supervisor's decision site for a StatusActive agent whose
// terminal is still present but whose process the backend confirms is dead (a tmux
// remain-on-exit pane outliving its claude). Like reconcileVanished it serializes on
// agentLock and re-reads the row, then re-confirms death under the lock via confirmDead
// — so a just-resumed agent whose fresh terminal probes alive is a no-op, defeating the
// spawn/resume race — and kills the surviving dead terminal before resuming into a new one.
func reconcileDeadChild(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow, confirmDead func(context.Context, agentRow) bool) error {
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
	if !confirmDead(ctx, cur) {
		return nil
	}
	_, err = respawnUnderBudget(ctx, db, appendFn, cur, true)
	return err
}

// agentChildExitedRequest is a pty-host's child-exit report, keyed by the child's
// session id so the supervisor resumes the same session. SpawnNonce identifies the
// reporting incarnation: each (re)spawn mints a fresh nonce into the wrapper's argv
// and the agent row, so a report can only ever act on the incarnation that sent it.
type agentChildExitedRequest struct {
	SessionID  string `json:"session_id"`
	SpawnNonce string `json:"spawn_nonce"`
}

// agentChildExitedResult reports whether the report drove a respawn — false when the
// agent was already terminal (a concurrent kill won the race), the report's nonce no
// longer matches the row's incarnation (a stale report), or the restart budget is
// exhausted (the agent was abandoned instead).
type agentChildExitedResult struct {
	Respawned bool `json:"respawned"`
}

// handleChildExited answers cco.agent.childExited: a pty-host's authoritative report
// that its claude child exited (its cmd.Wait returned). It resolves the agent by session
// id and drives the existing respawn path at once, so a wrapped agent whose child dies
// under a still-present terminal is resumed immediately rather than after the
// membership/staleness latency. It is socket-only — an internal child→daemon signal like
// cco.agent.report, off the parent XRPC/MCP surfaces.
func handleChildExited(hc daemon.HandlerCtx, req agentChildExitedRequest) (agentChildExitedResult, error) {
	if req.SpawnNonce == "" {
		return agentChildExitedResult{}, errors.New("child exit report requires spawn nonce")
	}
	ag, err := getAgentBySession(hc.Ctx, hc.DB, req.SessionID)
	if err != nil {
		return agentChildExitedResult{}, err
	}
	respawned, err := reconcileReportedExit(hc.Ctx, hc.DB, hc.Append, ag, req.SpawnNonce)
	if err != nil {
		return agentChildExitedResult{}, err
	}
	return agentChildExitedResult{Respawned: respawned}, nil
}

// reconcileReportedExit is the decision site for a pty-host's authoritative child-exit
// report: the wrapper witnessed its claude child exit, so unlike reconcileDeadChild it
// needs no prober corroboration — the report itself is the death signal, and the
// pty-host wrapper (superset) is not an AgentProber to corroborate against anyway. It
// mirrors reconcileVanished/reconcileDeadChild's idempotency guard: it holds agentLock
// and re-reads the row, so a report that races an agent-kill (or any already-terminal
// state) re-reads a non-active row and is a no-op, never resurrecting a killed agent.
// The session id survives a respawn, so an active row alone does not prove the report
// is about the row's current incarnation: a report delayed past a concurrent
// kill-then-respawn would otherwise kill the healthy fresh incarnation. The spawn
// nonce settles it — each (re)spawn mints a fresh nonce into both the wrapper's argv
// and the row, so a mismatched nonce marks the report as a prior incarnation's and a
// no-op. A still-active, nonce-matched agent is respawned under budget with killFirst,
// tearing down the wrapper's surviving terminal before resuming the SAME session. It
// reports whether it drove a respawn.
func reconcileReportedExit(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow, spawnNonce string) (bool, error) {
	mu := agentLock(ag.ID)
	mu.Lock()
	defer mu.Unlock()
	cur, err := getAgent(ctx, db, ag.ID)
	if err != nil {
		return false, err
	}
	if cur.Status != StatusActive {
		return false, nil
	}
	if cur.SpawnNonce == "" {
		return false, fmt.Errorf("active agent %q has no spawn nonce", cur.ID)
	}
	if cur.SpawnNonce != spawnNonce {
		return false, nil
	}
	return respawnUnderBudget(ctx, db, appendFn, cur, true)
}

// respawnUnderBudget re-spawns cur (resume into a fresh terminal) while it stays under
// restartBudget and abandons it at or over budget, reporting whether it respawned
// (false means abandoned). The caller holds agentLock and has re-read cur as
// StatusActive. When killFirst is set the agent's terminal is still present (a dead
// child under a surviving pane), so it is torn down before the new one is spawned;
// reconcileVanished passes false because its terminal is already gone.
//
// It counts and persists the restart attempt BEFORE respawning, then mirrors it onto
// the in-memory row, so respawnAgent's tailer snapshot carries the incremented
// RestartCount. The reset hook is gated on a positive count, so on a first restart
// (0->1) the tailer must observe 1, not the stale 0, for a healthy recovery to clear
// the budget. Counting first also means a respawn that fails leaves the attempt
// recorded, so a persistently-failing agent climbs toward budget and is eventually
// abandoned rather than retried forever at a stuck count; the agentLock + re-read of
// cur serialize ticks, so a single death detection counts exactly one attempt.
func respawnUnderBudget(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, cur agentRow, killFirst bool) (bool, error) {
	if cur.RestartCount >= restartBudget {
		if _, err := appendFn(ctx, &event.Event{
			SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventAbandoned, Payload: abandonedPayload(cur),
		}); err != nil {
			return false, err
		}
		fleetLog.emit(ctx, abandonedFrame(cur.ID, cur.RestartCount))
		if err := softExitAgent(ctx, db, appendFn, cur); err != nil {
			return false, err
		}
		fleetLog.emit(ctx, exitedFrame(cur.ID, reasonExited))
		return false, nil
	}
	attempt := cur.RestartCount + 1
	stamp := nowStamp()
	if err := markRestartAttempt(ctx, db, cur.ID, attempt, stamp); err != nil {
		return false, err
	}
	cur.RestartCount = attempt
	cur.LastRestartAt = stamp
	if killFirst {
		if err := killAgentTerminal(ctx, db, cur); err != nil {
			return false, err
		}
	}
	handle, err := respawnAgent(ctx, db, appendFn, cur)
	if err != nil {
		return false, err
	}
	if _, err := appendFn(ctx, &event.Event{
		SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventRestarted, Payload: restartedPayload(cur, handle.ID, attempt),
	}); err != nil {
		return false, err
	}
	fleetLog.emit(ctx, restartedFrame(cur.ID, attempt))
	return true, nil
}

// transcriptStale reports whether an agent's transcript file has gone unwritten past
// stalenessBound — the cheap pre-filter that nominates a death candidate before the
// authoritative process probe. A missing transcript (the agent has not written yet) is
// not stale: it is "no signal", left to other paths.
func transcriptStale(ag agentRow) bool {
	path, ok, err := findTranscript(ag.SessionID)
	if err != nil || !ok {
		return false
	}
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return time.Since(info.ModTime()) > stalenessBound
}

// recentlySpawned reports whether an agent was spawned or last restarted within
// startupGrace, so the staleness pass can skip an agent whose resumed transcript has
// not caught up yet. created_at and last_restart_at are RFC3339 UTC stamps, which sort
// chronologically as strings, so the later of the two is the most recent (re)spawn.
func recentlySpawned(ag agentRow) bool {
	ref := ag.CreatedAt
	if ag.LastRestartAt > ref {
		ref = ag.LastRestartAt
	}
	t, err := time.Parse(time.RFC3339, ref)
	if err != nil {
		return false
	}
	return time.Since(t) < startupGrace
}

// proberConfirmDead builds the under-lock liveness re-check for reconcileDeadChild: it
// resolves the agent's current backend handle and asks the backend whether the process
// is alive. Any failure to resolve or probe is read as "not confirmed dead", so an
// ambiguous signal never resumes a possibly-live agent — a vanished terminal is left to
// the ListAgents diff, an idle-but-alive process is left running.
func proberConfirmDead(db *sql.DB, prober backend.AgentProber) func(context.Context, agentRow) bool {
	return func(ctx context.Context, ag agentRow) bool {
		handle, err := backendAgentHandle(ctx, db, ag)
		if err != nil {
			return false
		}
		alive, err := prober.AgentAlive(ctx, handle)
		if err != nil {
			return false
		}
		return !alive
	}
}
