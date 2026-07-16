package orchestrate

import (
	"context"
	"database/sql"
	"log"

	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// reconcileWorkstreams marks active workstreams whose backend workspace has
// vanished out-of-band — a multiplexer session closed or a machine rebooted while
// the daemon was down — as killed, cascading their agents through
// markWorkstreamKilled. A backend it cannot resolve or whose enumeration fails is
// skipped: boot must stay crash-safe and idempotent, and the DB remains the source
// of truth.
func reconcileWorkstreams(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
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
			log.Printf("cc-orchestrate: reconcile skips workstream %s: unknown backend %q", ws.ID, ws.Backend)
			continue
		}
		live, err := bk.ListWorkstreams(ctx)
		if err != nil {
			log.Printf("cc-orchestrate: reconcile skips workstream %s: list workspaces: %v", ws.ID, err)
			continue
		}
		if !containsWorkstreamHandle(live, ws.WorkspaceHandle) {
			if _, _, err := markWorkstreamKilled(ctx, db, appendFn, ws); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileAgents routes every active agent whose backend terminal has vanished
// through reconcileVanished — the shared decision site that re-spawns it under the
// restart budget or terminally exits it — but only for backends that advertise
// CanEnumerate. A non-enumerable backend's empty ListAgents would be "no signal",
// never "all agents gone", so gating on the capability is what protects the fleet
// from a full prune on boot. It reaches a workstream's agents through the sprint
// join. It runs after reconcileWorkstreams, so a killed workstream's agents are
// already exited and its rows are skipped. The keep-alive supervisor calls the
// identical reconcileVanished branch on its ticks, so boot and the supervisor share
// one actor per agent.
func reconcileAgents(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
	wss, err := listWorkstreams(ctx, db, "", "")
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
			log.Printf("cc-orchestrate: reconcile skips agents of %s: list agents: %v", ws.ID, err)
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

func containsWorkstreamHandle(handles []backend.WorkstreamHandle, id string) bool {
	for _, h := range handles {
		if h.ID == id {
			return true
		}
	}
	return false
}

func containsAgentHandle(handles []backend.AgentHandle, id string) bool {
	for _, h := range handles {
		if h.ID == id {
			return true
		}
	}
	return false
}
