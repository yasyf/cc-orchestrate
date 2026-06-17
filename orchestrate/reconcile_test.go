package orchestrate

import (
	"context"
	"database/sql"
	"testing"

	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// reconcileBackend is a registered test backend returning controlled ListProjects
// / ListAgents sets, with a toggleable CanEnumerate, so reconcile can be exercised
// without a live CLI. Its name is outside backend.Precedence.
type reconcileBackend struct {
	projects  []backend.ProjectHandle
	agents    []backend.AgentHandle
	enumerate bool
}

func (reconcileBackend) Name() string                      { return "recontest" }
func (reconcileBackend) Available() bool                   { return true }
func (reconcileBackend) EnsureReady(context.Context) error { return nil }
func (b reconcileBackend) ListProjects(context.Context) ([]backend.ProjectHandle, error) {
	return b.projects, nil
}
func (reconcileBackend) CreateProject(context.Context, backend.ProjectSpec) (backend.ProjectHandle, error) {
	return backend.ProjectHandle{}, nil
}
func (reconcileBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (b reconcileBackend) ListAgents(context.Context, backend.ProjectHandle) ([]backend.AgentHandle, error) {
	return b.agents, nil
}
func (reconcileBackend) Kill(context.Context, backend.AgentHandle) error          { return nil }
func (reconcileBackend) KillProject(context.Context, backend.ProjectHandle) error { return nil }
func (b reconcileBackend) Caps() backend.Caps {
	if b.enumerate {
		return backend.Capabilities(backend.CanEnumerate)
	}
	return backend.Caps{}
}

func seedProject(t *testing.T, db *sql.DB, id, bname, workspace string) {
	t.Helper()
	if err := insertProject(context.Background(), db, projectRow{
		ID: id, Name: id, Backend: bname, WorkspaceHandle: workspace,
		Cwd: "/s", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertProject %s: %v", id, err)
	}
}

func seedAgent(t *testing.T, db *sql.DB, id, projectID, bname, terminal string) {
	t.Helper()
	mustInsertAgent(t, db, agentRow{
		ID: id, ProjectID: projectID, Backend: bname, TerminalHandle: terminal,
		SubjectID: "subj-" + id, Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})
}

func assertProjectStatus(t *testing.T, db *sql.DB, id string, want LifecycleStatus) {
	t.Helper()
	p, err := getProject(context.Background(), db, id)
	if err != nil {
		t.Fatalf("getProject %s: %v", id, err)
	}
	if p.Status != want {
		t.Fatalf("project %s status = %q, want %q", id, p.Status, want)
	}
}

func assertAgentStatus(t *testing.T, db *sql.DB, id string, want LifecycleStatus) {
	t.Helper()
	a, err := getAgent(context.Background(), db, id)
	if err != nil {
		t.Fatalf("getAgent %s: %v", id, err)
	}
	if a.Status != want {
		t.Fatalf("agent %s status = %q, want %q", id, a.Status, want)
	}
}

func TestReconcile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	noopAppend := func(context.Context, *event.Event) (int64, error) { return 1, nil }

	t.Run("present project and agent: no change", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.ProjectHandle{{ID: "ws-1"}},
			agents:    []backend.AgentHandle{{ID: "term-1"}},
			enumerate: true,
		})
		seedProject(t, db, "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "p1", "recontest", "term-1")

		if err := reconcileProjects(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertProjectStatus(t, db, "p1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusActive)
	})

	t.Run("vanished project is killed and its agents exited", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{projects: nil, enumerate: true}) // ws-1 gone
		seedProject(t, db, "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "p1", "recontest", "term-1")

		if err := reconcileProjects(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertProjectStatus(t, db, "p1", StatusKilled)
		assertAgentStatus(t, db, "a1", StatusExited)
	})

	t.Run("vanished agent with CanEnumerate is exited", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.ProjectHandle{{ID: "ws-1"}}, // project present
			agents:    nil,                                   // term-1 gone
			enumerate: true,
		})
		seedProject(t, db, "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "p1", "recontest", "term-1")

		if err := reconcileProjects(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertProjectStatus(t, db, "p1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusExited)
	})

	// The superset guarantee: an empty ListAgents from a backend that cannot
	// enumerate must never be read as "all agents gone".
	t.Run("agent survives when backend cannot enumerate", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.ProjectHandle{{ID: "ws-1"}},
			agents:    nil,
			enumerate: false,
		})
		seedProject(t, db, "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "p1", "recontest", "term-1")

		if err := reconcileProjects(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertProjectStatus(t, db, "p1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusActive)
	})

	t.Run("unknown backend is skipped without aborting boot", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		seedProject(t, db, "p2", "ghostbackend", "ws-2")
		seedAgent(t, db, "a2", "p2", "ghostbackend", "term-2")

		if err := reconcileProjects(ctx, db, noopAppend); err != nil {
			t.Fatalf("reconcileProjects aborted on unknown backend: %v", err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatalf("reconcileAgents aborted on unknown backend: %v", err)
		}
		assertProjectStatus(t, db, "p2", StatusActive)
		assertAgentStatus(t, db, "a2", StatusActive)
	})
}
