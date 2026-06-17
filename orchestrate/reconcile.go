package orchestrate

import (
	"context"
	"database/sql"
	"log"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// reconcileProjects marks active projects whose backend workspace has vanished
// out-of-band — a multiplexer session closed or a machine rebooted while the
// daemon was down — as killed, cascading their agents through markProjectKilled.
// A backend it cannot resolve or whose enumeration fails is skipped: boot must
// stay crash-safe and idempotent, and the DB remains the source of truth.
func reconcileProjects(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
	projects, err := listProjects(ctx, db)
	if err != nil {
		return err
	}
	for _, proj := range projects {
		if proj.Status != StatusActive {
			continue
		}
		bk, ok := backend.Get(proj.Backend)
		if !ok {
			log.Printf("cc-orchestrate: reconcile skips project %s: unknown backend %q", proj.ID, proj.Backend)
			continue
		}
		live, err := bk.ListProjects(ctx)
		if err != nil {
			log.Printf("cc-orchestrate: reconcile skips project %s: list workspaces: %v", proj.ID, err)
			continue
		}
		if !containsProjectHandle(live, proj.WorkspaceHandle) {
			if err := markProjectKilled(ctx, db, appendFn, proj); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileAgents marks active agents whose backend terminal has vanished as
// exited, but only for backends that can enumerate their agents (CanEnumerate).
// superset's ListAgents is empty by design, so an empty result there is "no
// signal", never "all agents gone" — gating on the capability is what protects
// the fleet from a full prune on boot. It runs after reconcileProjects, so a
// killed project's agents are already exited and its rows are skipped.
func reconcileAgents(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc) error {
	projects, err := listProjects(ctx, db)
	if err != nil {
		return err
	}
	for _, proj := range projects {
		if proj.Status != StatusActive {
			continue
		}
		bk, ok := backend.Get(proj.Backend)
		if !ok || !bk.Caps().Has(backend.CanEnumerate) {
			continue
		}
		live, err := bk.ListAgents(ctx, backend.ProjectHandle{
			Backend: proj.Backend, ID: proj.WorkspaceHandle, Name: proj.Name, Cwd: proj.Cwd,
		})
		if err != nil {
			log.Printf("cc-orchestrate: reconcile skips agents of %s: list agents: %v", proj.ID, err)
			continue
		}
		agents, err := listAgents(ctx, db, proj.ID)
		if err != nil {
			return err
		}
		for _, ag := range agents {
			if ag.Status != StatusActive || containsAgentHandle(live, ag.TerminalHandle) {
				continue
			}
			tailers.stop(ag.ID)
			if err := setAgentLifecycle(ctx, db, ag.ID, StatusExited); err != nil {
				return err
			}
			if _, err := appendFn(ctx, &event.Event{
				SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventExited, Payload: exitedPayload(),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func containsProjectHandle(handles []backend.ProjectHandle, id string) bool {
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
