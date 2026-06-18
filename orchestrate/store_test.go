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

func TestRepoCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	p := repoRow{
		ID: "p1", Name: "alpha", Backend: "tmux",
		Cwd: "/tmp/alpha", Status: "active", CreatedAt: "2026-06-16T00:00:00Z",
	}
	if err := insertRepo(ctx, db, p); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}

	t.Run("get by id", func(t *testing.T) {
		got, err := getRepo(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got != p {
			t.Fatalf("getRepo by id = %+v, want %+v", got, p)
		}
	})
	t.Run("get by name", func(t *testing.T) {
		got, err := getRepo(ctx, db, "alpha")
		if err != nil {
			t.Fatal(err)
		}
		if got != p {
			t.Fatalf("getRepo by name = %+v, want %+v", got, p)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		if _, err := getRepo(ctx, db, "ghost"); err == nil {
			t.Fatal("getRepo(ghost) returned nil error, want not-found")
		}
	})
	t.Run("list", func(t *testing.T) {
		got, err := listRepos(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != p {
			t.Fatalf("listRepos = %+v, want [%+v]", got, p)
		}
	})
	t.Run("set status", func(t *testing.T) {
		if err := setRepoStatus(ctx, db, "p1", "archived"); err != nil {
			t.Fatal(err)
		}
		got, err := getRepo(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != "archived" {
			t.Fatalf("status = %q, want archived", got.Status)
		}
	})
	t.Run("id match beats name match", func(t *testing.T) {
		other := repoRow{
			ID: "p2", Name: "p1", Backend: "tmux", Cwd: "/tmp/p2",
			Status: "active", CreatedAt: "2026-06-16T01:00:00Z",
		}
		if err := insertRepo(ctx, db, other); err != nil {
			t.Fatal(err)
		}
		got, err := getRepo(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "p1" {
			t.Fatalf("getRepo(p1).ID = %q, want p1 (id match must beat name match)", got.ID)
		}
	})
}

func TestAgentCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	active := agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/tmp/a1", Name: "worker", Prompt: "do the thing",
		SubjectID: "subj-1", Status: "active", State: StateUnknown,
		CreatedAt: "2026-06-16T00:00:00Z",
	}
	idle := agentRow{
		ID: "a2", SprintID: "s2", Backend: "tmux", Scope: "/tmp/a2",
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
	t.Run("list filtered by sprint", func(t *testing.T) {
		got, err := listAgents(ctx, db, "s1")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a1" {
			t.Fatalf("listAgents(s1) = %+v, want [a1]", got)
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

func TestWorkstreamCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	primary := workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "tmux", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/alpha", IsPrimary: true,
		Status: StatusActive, CreatedAt: "2026-06-16T00:00:00Z",
	}
	feature := workstreamRow{
		ID: "w2", RepoID: "p1", Name: "feat-x", Backend: "tmux", WorkspaceHandle: "ws-2",
		Branch: "feat-x", Worktree: "/tmp/wt/feat-x", IsPrimary: false,
		Status: StatusActive, CreatedAt: "2026-06-16T01:00:00Z",
	}
	other := workstreamRow{
		ID: "w3", RepoID: "p2", Name: "feat-x", Backend: "tmux", WorkspaceHandle: "ws-3",
		Branch: "feat-x", Worktree: "/tmp/wt/other", IsPrimary: false,
		Status: StatusActive, CreatedAt: "2026-06-16T02:00:00Z",
	}
	for _, w := range []workstreamRow{primary, feature, other} {
		if err := insertWorkstream(ctx, db, w); err != nil {
			t.Fatalf("insertWorkstream %s: %v", w.ID, err)
		}
	}

	t.Run("get by id round-trips every column", func(t *testing.T) {
		got, err := getWorkstream(ctx, db, "w2", "")
		if err != nil {
			t.Fatal(err)
		}
		if got != feature {
			t.Fatalf("getWorkstream(w2) = %+v, want %+v", got, feature)
		}
	})
	t.Run("get by name within repo", func(t *testing.T) {
		got, err := getWorkstream(ctx, db, "feat-x", "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "w2" {
			t.Fatalf("getWorkstream(feat-x, p1).ID = %q, want w2", got.ID)
		}
		got, err = getWorkstream(ctx, db, "feat-x", "p2")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "w3" {
			t.Fatalf("getWorkstream(feat-x, p2).ID = %q, want w3", got.ID)
		}
	})
	t.Run("list filtered by repo", func(t *testing.T) {
		got, err := listWorkstreams(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].ID != "w1" || got[1].ID != "w2" {
			t.Fatalf("listWorkstreams(p1) = %+v, want [w1 w2]", got)
		}
	})
	t.Run("primary", func(t *testing.T) {
		got, err := getPrimaryWorkstream(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "w1" || !got.IsPrimary {
			t.Fatalf("getPrimaryWorkstream(p1) = %+v, want w1 with is_primary", got)
		}
		if _, err := getPrimaryWorkstream(ctx, db, "p2"); err == nil {
			t.Fatal("getPrimaryWorkstream(p2) = nil error, want not-found (p2 has no primary)")
		}
	})
	t.Run("set status", func(t *testing.T) {
		if err := setWorkstreamStatus(ctx, db, "w2", StatusKilled); err != nil {
			t.Fatal(err)
		}
		got, err := getWorkstream(ctx, db, "w2", "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != StatusKilled {
			t.Fatalf("status = %q, want killed", got.Status)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		if _, err := getWorkstream(ctx, db, "ghost", ""); err == nil {
			t.Fatal("getWorkstream(ghost) = nil error, want not-found")
		}
	})
}

// TestSprintCRUD exercises the sprint store: insert, get by id and by
// workstream-scoped name, list filtered by workstream, the default-sprint lookup,
// and status mutation.
func TestSprintCRUD(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	main := sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "2026-06-16T00:00:00Z"}
	feature := sprintRow{ID: "s2", WorkstreamID: "w1", Name: "feat", Status: StatusActive, CreatedAt: "2026-06-16T01:00:00Z"}
	other := sprintRow{ID: "s3", WorkstreamID: "w2", Name: "feat", Status: StatusActive, CreatedAt: "2026-06-16T02:00:00Z"}
	for _, sp := range []sprintRow{main, feature, other} {
		if err := insertSprint(ctx, db, sp); err != nil {
			t.Fatalf("insertSprint %s: %v", sp.ID, err)
		}
	}

	t.Run("get by id round-trips every column", func(t *testing.T) {
		got, err := getSprint(ctx, db, "s2", "")
		if err != nil {
			t.Fatal(err)
		}
		if got != feature {
			t.Fatalf("getSprint(s2) = %+v, want %+v", got, feature)
		}
	})
	t.Run("get by name within workstream", func(t *testing.T) {
		got, err := getSprint(ctx, db, "feat", "w1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "s2" {
			t.Fatalf("getSprint(feat, w1).ID = %q, want s2", got.ID)
		}
		got, err = getSprint(ctx, db, "feat", "w2")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "s3" {
			t.Fatalf("getSprint(feat, w2).ID = %q, want s3", got.ID)
		}
	})
	t.Run("list filtered by workstream", func(t *testing.T) {
		got, err := listSprints(ctx, db, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].ID != "s1" || got[1].ID != "s2" {
			t.Fatalf("listSprints(w1) = %+v, want [s1 s2]", got)
		}
	})
	t.Run("default is the workstream's first sprint", func(t *testing.T) {
		got, err := getDefaultSprint(ctx, db, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "s1" {
			t.Fatalf("getDefaultSprint(w1).ID = %q, want s1 (earliest created)", got.ID)
		}
		if _, err := getDefaultSprint(ctx, db, "ghost"); err == nil {
			t.Fatal("getDefaultSprint(ghost) = nil error, want not-found")
		}
	})
	t.Run("set status", func(t *testing.T) {
		if err := setSprintStatus(ctx, db, "s2", StatusKilled); err != nil {
			t.Fatal(err)
		}
		got, err := getSprint(ctx, db, "s2", "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != StatusKilled {
			t.Fatalf("status = %q, want killed", got.Status)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		if _, err := getSprint(ctx, db, "ghost", ""); err == nil {
			t.Fatal("getSprint(ghost) = nil error, want not-found")
		}
	})
}

// TestListRepoAgents proves the repo-level aggregation joins agents through their
// sprints and workstreams, returning every agent of a repo across its streams.
func TestListRepoAgents(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for _, w := range []workstreamRow{
		{ID: "w1", RepoID: "p1", Name: "main", Backend: "tmux", Branch: "main", Worktree: "/r1", IsPrimary: true, Status: StatusActive, CreatedAt: "t0"},
		{ID: "w2", RepoID: "p1", Name: "feat", Backend: "tmux", Branch: "feat", Worktree: "/r1/feat", Status: StatusActive, CreatedAt: "t1"},
		{ID: "w3", RepoID: "p2", Name: "main", Backend: "tmux", Branch: "main", Worktree: "/r2", IsPrimary: true, Status: StatusActive, CreatedAt: "t2"},
	} {
		if err := insertWorkstream(ctx, db, w); err != nil {
			t.Fatalf("insertWorkstream %s: %v", w.ID, err)
		}
	}
	for _, sp := range []sprintRow{
		{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"},
		{ID: "s2", WorkstreamID: "w2", Name: "main", Status: StatusActive, CreatedAt: "t1"},
		{ID: "s3", WorkstreamID: "w3", Name: "main", Status: StatusActive, CreatedAt: "t2"},
	} {
		if err := insertSprint(ctx, db, sp); err != nil {
			t.Fatalf("insertSprint %s: %v", sp.ID, err)
		}
	}
	for _, a := range []agentRow{
		{ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/r1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"},
		{ID: "a2", SprintID: "s2", Backend: "tmux", Scope: "/r1/feat", Status: StatusActive, State: StateIdle, CreatedAt: "t1"},
		{ID: "a3", SprintID: "s3", Backend: "tmux", Scope: "/r2", Status: StatusActive, State: StateIdle, CreatedAt: "t2"},
	} {
		if err := insertAgent(ctx, db, a); err != nil {
			t.Fatalf("insertAgent %s: %v", a.ID, err)
		}
	}

	got, err := listRepoAgents(ctx, db, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "a1" || got[1].ID != "a2" {
		t.Fatalf("listRepoAgents(p1) = %+v, want [a1 a2] across w1 and w2", got)
	}
}

// TestListWorkstreamAgents proves agents are reached through the sprint join: a
// workstream's agents are the union across all its sprints.
func TestListWorkstreamAgents(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for _, sp := range []sprintRow{
		{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"},
		{ID: "s2", WorkstreamID: "w1", Name: "feat", Status: StatusActive, CreatedAt: "t1"},
		{ID: "s3", WorkstreamID: "w2", Name: "main", Status: StatusActive, CreatedAt: "t2"},
	} {
		if err := insertSprint(ctx, db, sp); err != nil {
			t.Fatalf("insertSprint %s: %v", sp.ID, err)
		}
	}
	for _, a := range []agentRow{
		{ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/r1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"},
		{ID: "a2", SprintID: "s2", Backend: "tmux", Scope: "/r1", Status: StatusActive, State: StateIdle, CreatedAt: "t1"},
		{ID: "a3", SprintID: "s3", Backend: "tmux", Scope: "/r2", Status: StatusActive, State: StateIdle, CreatedAt: "t2"},
	} {
		if err := insertAgent(ctx, db, a); err != nil {
			t.Fatalf("insertAgent %s: %v", a.ID, err)
		}
	}

	got, err := listWorkstreamAgents(ctx, db, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "a1" || got[1].ID != "a2" {
		t.Fatalf("listWorkstreamAgents(w1) = %+v, want [a1 a2] across s1 and s2", got)
	}
}

func TestApplyStatus(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	if err := insertAgent(ctx, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/tmp/a1",
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
