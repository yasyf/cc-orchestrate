package orchestrate

import (
	"context"
	"database/sql"
	"testing"

	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// reconcileBackend is a registered test backend returning controlled ListWorkstreams
// / ListAgents sets, with a toggleable CanEnumerate, so reconcile can be exercised
// without a live CLI. Its name is outside backend.Precedence.
type reconcileBackend struct {
	projects  []backend.WorkstreamHandle
	agents    []backend.AgentHandle
	enumerate bool
}

func (reconcileBackend) Name() backend.BackendName         { return "recontest" }
func (reconcileBackend) Available() bool                   { return true }
func (reconcileBackend) EnsureReady(context.Context) error { return nil }
func (b reconcileBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return b.projects, nil
}
func (reconcileBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}
func (reconcileBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (b reconcileBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return b.agents, nil
}
func (reconcileBackend) Kill(context.Context, backend.AgentHandle) error                { return nil }
func (reconcileBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (b reconcileBackend) Caps() backend.Caps {
	if b.enumerate {
		return backend.Capabilities(backend.CanEnumerate)
	}
	return backend.Caps{}
}

// seedWorkstream inserts a workstream and its default sprint (id workstreamID+"-s"),
// so a seeded agent has a sprint to attach to and reconcile can reach it through the
// sprint join.
func seedWorkstream(t *testing.T, db *sql.DB, id, repoID, bname, workspace string) {
	t.Helper()
	if err := insertWorkstream(context.Background(), db, workstreamRow{
		ID: id, RepoID: repoID, Name: id, Backend: backend.BackendName(bname), WorkspaceHandle: workspace,
		Branch: "main", Worktree: "/s", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream %s: %v", id, err)
	}
	if err := insertSprint(context.Background(), db, sprintRow{
		ID: id + "-s", WorkstreamID: id, Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint for %s: %v", id, err)
	}
}

// seedAgent inserts an agent under workstreamID's default sprint (id
// workstreamID+"-s", seeded by seedWorkstream).
func seedAgent(t *testing.T, db *sql.DB, id, workstreamID, bname, terminal string) {
	t.Helper()
	mustInsertAgent(t, db, agentRow{
		ID: id, SprintID: workstreamID + "-s", Backend: backend.BackendName(bname), TerminalHandle: terminal,
		SubjectID: "subj-" + id, Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})
}

func assertWorkstreamStatus(t *testing.T, db *sql.DB, id string, want LifecycleStatus) {
	t.Helper()
	w, err := getWorkstream(context.Background(), db, id, "")
	if err != nil {
		t.Fatalf("getWorkstream %s: %v", id, err)
	}
	if w.Status != want {
		t.Fatalf("workstream %s status = %q, want %q", id, w.Status, want)
	}
}

func assertSprintStatus(t *testing.T, db *sql.DB, id string, want LifecycleStatus) {
	t.Helper()
	sp, err := getSprint(context.Background(), db, id, "")
	if err != nil {
		t.Fatalf("getSprint %s: %v", id, err)
	}
	if sp.Status != want {
		t.Fatalf("sprint %s status = %q, want %q", id, sp.Status, want)
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

	t.Run("present workstream and agent: no change", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.WorkstreamHandle{{ID: "ws-1"}},
			agents:    []backend.AgentHandle{{ID: "term-1"}},
			enumerate: true,
		})
		seedWorkstream(t, db, "w1", "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "w1", "recontest", "term-1")

		if err := reconcileWorkstreams(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertWorkstreamStatus(t, db, "w1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusActive)
	})

	t.Run("vanished workstream is killed and its agents exited", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{projects: nil, enumerate: true}) // ws-1 gone
		seedWorkstream(t, db, "w1", "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "w1", "recontest", "term-1")

		if err := reconcileWorkstreams(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertWorkstreamStatus(t, db, "w1", StatusKilled)
		assertSprintStatus(t, db, "w1-s", StatusKilled)
		assertAgentStatus(t, db, "a1", StatusExited)
	})

	t.Run("vanished agent with CanEnumerate is exited", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.WorkstreamHandle{{ID: "ws-1"}}, // workstream present
			agents:    nil,                                      // term-1 gone
			enumerate: true,
		})
		seedWorkstream(t, db, "w1", "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "w1", "recontest", "term-1")

		if err := reconcileWorkstreams(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertWorkstreamStatus(t, db, "w1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusExited)
	})

	// The superset guarantee: an empty ListAgents from a backend that cannot
	// enumerate must never be read as "all agents gone".
	t.Run("agent survives when backend cannot enumerate", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		backend.Register(reconcileBackend{
			projects:  []backend.WorkstreamHandle{{ID: "ws-1"}},
			agents:    nil,
			enumerate: false,
		})
		seedWorkstream(t, db, "w1", "p1", "recontest", "ws-1")
		seedAgent(t, db, "a1", "w1", "recontest", "term-1")

		if err := reconcileWorkstreams(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatal(err)
		}
		assertWorkstreamStatus(t, db, "w1", StatusActive)
		assertAgentStatus(t, db, "a1", StatusActive)
	})

	t.Run("unknown backend is skipped without aborting boot", func(t *testing.T) {
		tailers = newTailerManager(ctx)
		db := newTestDB(t)
		seedWorkstream(t, db, "w2", "p2", "ghostbackend", "ws-2")
		seedAgent(t, db, "a2", "w2", "ghostbackend", "term-2")

		if err := reconcileWorkstreams(ctx, db, noopAppend); err != nil {
			t.Fatalf("reconcileWorkstreams aborted on unknown backend: %v", err)
		}
		if err := reconcileAgents(ctx, db, noopAppend); err != nil {
			t.Fatalf("reconcileAgents aborted on unknown backend: %v", err)
		}
		assertWorkstreamStatus(t, db, "w2", StatusActive)
		assertAgentStatus(t, db, "a2", StatusActive)
	})
}
