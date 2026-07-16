package orchestrate

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// TestListStatusFilter drives the lifecycle-status filter pushed into the five list
// functions against a real on-disk sqlite: two repos (active, killed), two
// workstreams under the active repo, two sprints under the active workstream, and
// three agents under the active sprint (active, exited, killed).
func TestListStatusFilter(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)

	for _, p := range []repoRow{
		{ID: "p-act", Name: "act", Backend: "tmux", Cwd: "/a", Status: StatusActive, CreatedAt: "t0"},
		{ID: "p-kill", Name: "kill", Backend: "tmux", Cwd: "/k", Status: StatusKilled, CreatedAt: "t1"},
	} {
		if err := insertRepo(ctx, db, p); err != nil {
			t.Fatalf("insertRepo %s: %v", p.ID, err)
		}
	}
	for _, w := range []workstreamRow{
		{ID: "w-act", RepoID: "p-act", Name: "main", Backend: "tmux", Branch: "main", Worktree: "/a", IsPrimary: true, Status: StatusActive, CreatedAt: "t0"},
		{ID: "w-kill", RepoID: "p-act", Name: "feat", Backend: "tmux", Branch: "feat", Worktree: "/a/feat", Status: StatusKilled, CreatedAt: "t1"},
	} {
		if err := insertWorkstream(ctx, db, w); err != nil {
			t.Fatalf("insertWorkstream %s: %v", w.ID, err)
		}
	}
	for _, sp := range []sprintRow{
		{ID: "s-act", WorkstreamID: "w-act", Name: "main", Status: StatusActive, CreatedAt: "t0"},
		{ID: "s-kill", WorkstreamID: "w-act", Name: "old", Status: StatusKilled, CreatedAt: "t1"},
	} {
		if err := insertSprint(ctx, db, sp); err != nil {
			t.Fatalf("insertSprint %s: %v", sp.ID, err)
		}
	}
	mustInsertAgent(ctx, t, db, agentRow{ID: "a-act", SprintID: "s-act", Backend: "tmux", Scope: "/a", Status: StatusActive, State: StateUnknown, CreatedAt: "t0"})
	mustInsertAgent(ctx, t, db, agentRow{ID: "a-exit", SprintID: "s-act", Backend: "tmux", Scope: "/a", Status: StatusExited, State: StateUnknown, CreatedAt: "t1"})
	mustInsertAgent(ctx, t, db, agentRow{ID: "a-kill", SprintID: "s-act", Backend: "tmux", Scope: "/a", Status: StatusKilled, State: StateUnknown, CreatedAt: "t2"})

	t.Run("listRepos", func(t *testing.T) {
		assertLen(t, "all", mustRepos(ctx, t, db, ""), 2)
		got := mustRepos(ctx, t, db, StatusActive)
		assertLen(t, "active", got, 1)
		if len(got) == 1 && got[0].ID != "p-act" {
			t.Errorf("active repo = %q, want p-act", got[0].ID)
		}
		assertLen(t, "killed", mustRepos(ctx, t, db, StatusKilled), 1)
	})

	t.Run("listWorkstreams", func(t *testing.T) {
		assertLen(t, "all in repo", mustWorkstreams(ctx, t, db, "p-act", ""), 2)
		assertLen(t, "active", mustWorkstreams(ctx, t, db, "", StatusActive), 1)
		got := mustWorkstreams(ctx, t, db, "p-act", StatusKilled)
		assertLen(t, "killed in repo", got, 1)
		if len(got) == 1 && got[0].ID != "w-kill" {
			t.Errorf("killed workstream = %q, want w-kill", got[0].ID)
		}
	})

	t.Run("listSprints", func(t *testing.T) {
		assertLen(t, "all in workstream", mustSprints(ctx, t, db, "w-act", ""), 2)
		got := mustSprints(ctx, t, db, "w-act", StatusActive)
		assertLen(t, "active", got, 1)
		if len(got) == 1 && got[0].ID != "s-act" {
			t.Errorf("active sprint = %q, want s-act", got[0].ID)
		}
	})

	t.Run("listAgents", func(t *testing.T) {
		assertLen(t, "all in sprint", mustAgents(ctx, t, db, "s-act", ""), 3)
		got := mustAgents(ctx, t, db, "s-act", StatusExited)
		assertLen(t, "exited", got, 1)
		if len(got) == 1 && got[0].ID != "a-exit" {
			t.Errorf("exited agent = %q, want a-exit", got[0].ID)
		}
		assertLen(t, "active across all", mustAgents(ctx, t, db, "", StatusActive), 1)
	})

	t.Run("listRepoAgents", func(t *testing.T) {
		assertLen(t, "all in repo", mustRepoAgents(ctx, t, db, "p-act", ""), 3)
		got := mustRepoAgents(ctx, t, db, "p-act", StatusKilled)
		assertLen(t, "killed in repo", got, 1)
		if len(got) == 1 && got[0].ID != "a-kill" {
			t.Errorf("killed agent = %q, want a-kill", got[0].ID)
		}
	})
}

// TestStatusFilterValidation asserts the request-level status filter: the three
// lifecycle values and empty are accepted, anything else is an InvalidRequest.
func TestStatusFilterValidation(t *testing.T) {
	for _, ok := range []string{"", "active", "exited", "killed"} {
		if _, err := parseStatusFilter(ok); err != nil {
			t.Errorf("parseStatusFilter(%q) errored: %v", ok, err)
		}
	}
	_, err := parseStatusFilter("bogus")
	if err == nil {
		t.Fatal("parseStatusFilter(bogus) did not error")
	}
	var oe *opError
	if !errors.As(err, &oe) || oe.Code != codeInvalidRequest {
		t.Errorf("parseStatusFilter(bogus) = %v, want an InvalidRequest opError", err)
	}
}

func mustRepos(ctx context.Context, t *testing.T, db *sql.DB, status LifecycleStatus) []repoRow {
	t.Helper()
	got, err := listRepos(ctx, db, status)
	if err != nil {
		t.Fatalf("listRepos(%q): %v", status, err)
	}
	return got
}

func mustWorkstreams(ctx context.Context, t *testing.T, db *sql.DB, repo string, status LifecycleStatus) []workstreamRow {
	t.Helper()
	got, err := listWorkstreams(ctx, db, repo, status)
	if err != nil {
		t.Fatalf("listWorkstreams(%q,%q): %v", repo, status, err)
	}
	return got
}

func mustSprints(ctx context.Context, t *testing.T, db *sql.DB, ws string, status LifecycleStatus) []sprintRow {
	t.Helper()
	got, err := listSprints(ctx, db, ws, status)
	if err != nil {
		t.Fatalf("listSprints(%q,%q): %v", ws, status, err)
	}
	return got
}

func mustAgents(ctx context.Context, t *testing.T, db *sql.DB, sprint string, status LifecycleStatus) []agentRow {
	t.Helper()
	got, err := listAgents(ctx, db, sprint, status)
	if err != nil {
		t.Fatalf("listAgents(%q,%q): %v", sprint, status, err)
	}
	return got
}

func mustRepoAgents(ctx context.Context, t *testing.T, db *sql.DB, repo string, status LifecycleStatus) []agentRow {
	t.Helper()
	got, err := listRepoAgents(ctx, db, repo, status)
	if err != nil {
		t.Fatalf("listRepoAgents(%q,%q): %v", repo, status, err)
	}
	return got
}

func assertLen[T any](t *testing.T, label string, got []T, want int) {
	t.Helper()
	if len(got) != want {
		t.Errorf("%s: got %d rows, want %d", label, len(got), want)
	}
}
