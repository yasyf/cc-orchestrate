package orchestrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// newTestDB opens a real ephemeral on-disk sqlite database with the orchestrate
// schema applied, closed automatically when the test ends.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestProjectCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	p := projectRow{
		ID: "p1", Name: "alpha", Backend: "tmux", WorkspaceHandle: "ws-1",
		Cwd: "/tmp/alpha", Status: "active", CreatedAt: "2026-06-16T00:00:00Z",
	}
	if err := insertProject(ctx, db, p); err != nil {
		t.Fatalf("insertProject: %v", err)
	}

	t.Run("get by id", func(t *testing.T) {
		got, err := getProject(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got != p {
			t.Fatalf("getProject by id = %+v, want %+v", got, p)
		}
	})
	t.Run("get by name", func(t *testing.T) {
		got, err := getProject(ctx, db, "alpha")
		if err != nil {
			t.Fatal(err)
		}
		if got != p {
			t.Fatalf("getProject by name = %+v, want %+v", got, p)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		if _, err := getProject(ctx, db, "ghost"); err == nil {
			t.Fatal("getProject(ghost) returned nil error, want not-found")
		}
	})
	t.Run("list", func(t *testing.T) {
		got, err := listProjects(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != p {
			t.Fatalf("listProjects = %+v, want [%+v]", got, p)
		}
	})
	t.Run("set status", func(t *testing.T) {
		if err := setProjectStatus(ctx, db, "p1", "archived"); err != nil {
			t.Fatal(err)
		}
		got, err := getProject(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != "archived" {
			t.Fatalf("status = %q, want archived", got.Status)
		}
	})
	t.Run("id match beats name match", func(t *testing.T) {
		other := projectRow{
			ID: "p2", Name: "p1", Backend: "tmux", Cwd: "/tmp/p2",
			Status: "active", CreatedAt: "2026-06-16T01:00:00Z",
		}
		if err := insertProject(ctx, db, other); err != nil {
			t.Fatal(err)
		}
		got, err := getProject(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "p1" {
			t.Fatalf("getProject(p1).ID = %q, want p1 (id match must beat name match)", got.ID)
		}
	})
}

func TestAgentCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	active := agentRow{
		ID: "a1", ProjectID: "p1", Backend: "tmux", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/tmp/a1", Name: "worker", Prompt: "do the thing",
		SubjectID: "subj-1", Status: "active", State: StateUnknown,
		CreatedAt: "2026-06-16T00:00:00Z",
	}
	idle := agentRow{
		ID: "a2", ProjectID: "p2", Backend: "tmux", Scope: "/tmp/a2",
		Status: "exited", State: StateIdle, CreatedAt: "2026-06-16T01:00:00Z",
	}
	for _, a := range []agentRow{active, idle} {
		if err := insertAgent(ctx, db, a); err != nil {
			t.Fatalf("insertAgent %s: %v", a.ID, err)
		}
	}

	t.Run("get round-trips every column", func(t *testing.T) {
		got, err := getAgent(ctx, db, "a1")
		if err != nil {
			t.Fatal(err)
		}
		if got != active {
			t.Fatalf("getAgent = %+v, want %+v", got, active)
		}
	})
	t.Run("list all", func(t *testing.T) {
		got, err := listAgents(ctx, db, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("listAgents(all) len = %d, want 2", len(got))
		}
	})
	t.Run("list filtered by project", func(t *testing.T) {
		got, err := listAgents(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a1" {
			t.Fatalf("listAgents(p1) = %+v, want [a1]", got)
		}
	})
	t.Run("list active", func(t *testing.T) {
		got, err := listActiveAgents(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a1" {
			t.Fatalf("listActiveAgents = %+v, want [a1]", got)
		}
	})
	t.Run("set lifecycle", func(t *testing.T) {
		if err := setAgentLifecycle(ctx, db, "a1", "exited"); err != nil {
			t.Fatal(err)
		}
		got, err := getAgent(ctx, db, "a1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != "exited" {
			t.Fatalf("status = %q, want exited", got.Status)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		if _, err := getAgent(ctx, db, "ghost"); err == nil {
			t.Fatal("getAgent(ghost) returned nil error, want not-found")
		}
	})
}

func TestApplyStatus(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	if err := insertAgent(ctx, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/tmp/a1",
		Status: "active", State: StateUnknown, CreatedAt: "2026-06-16T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}

	st := Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", LastText: "running", Tokens: 42}
	if err := applyStatus(ctx, db, "a1", st); err != nil {
		t.Fatalf("applyStatus: %v", err)
	}

	got, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateWorking {
		t.Errorf("state = %q, want %q", got.State, StateWorking)
	}
	if got.Activity != "Bash: go test ./..." {
		t.Errorf("activity = %q, want %q", got.Activity, "Bash: go test ./...")
	}
	if got.Tokens != 42 {
		t.Errorf("tokens = %d, want 42", got.Tokens)
	}
	if got.UpdatedAt == "" {
		t.Error("updated_at not stamped")
	}
}

func TestStatusActivity(t *testing.T) {
	cases := []struct {
		name string
		st   Status
		want string
	}{
		{name: "tool and target", st: Status{Tool: "Bash", Target: "ls -la"}, want: "Bash: ls -la"},
		{name: "tool without target", st: Status{Tool: "Read"}, want: "Read"},
		{name: "no tool", st: Status{Target: "stray"}, want: ""},
		{name: "empty", st: Status{}, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusActivity(tc.st); got != tc.want {
				t.Errorf("statusActivity(%+v) = %q, want %q", tc.st, got, tc.want)
			}
		})
	}
}

func TestConfigGetSet(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	if _, found, err := getConfig(ctx, db, "backend"); err != nil || found {
		t.Fatalf("getConfig(unset) = found %v, err %v; want false, nil", found, err)
	}
	if err := setConfig(ctx, db, "backend", "tmux"); err != nil {
		t.Fatal(err)
	}
	v, found, err := getConfig(ctx, db, "backend")
	if err != nil || !found || v != "tmux" {
		t.Fatalf("getConfig = %q, %v, %v; want tmux, true, nil", v, found, err)
	}
	if err := setConfig(ctx, db, "backend", "superset"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := getConfig(ctx, db, "backend"); v != "superset" {
		t.Fatalf("getConfig after upsert = %q, want superset", v)
	}
}
