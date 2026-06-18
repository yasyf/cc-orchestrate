package orchestrate

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ccnotes"
)

// TestCCNotesDisabledLeavesBindingsEmpty proves the gate: against a fresh repo that
// does not use cc-notes (no refs/cc-notes/*), workstream-create and sprint-create
// both succeed and leave their ccnotes_* columns empty, with no cc-notes shell-out.
// It exercises the real ccnotes.Enabled against a real git repo rather than a stub,
// so the false branch is what production hits for a non-cc-notes repo.
func TestCCNotesDisabledLeavesBindingsEmpty(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // worktreesBase resolves under the temp home
	ctx := context.Background()
	repo := gitRepo(t, "main")

	// Precondition: a fresh repo reports cc-notes disabled, so the not-enabled path
	// is the one under test. (cc-notes may or may not be installed; either way a
	// repo with no refs/cc-notes/* is gated off.)
	if ccnotes.Enabled(ctx, repo) {
		t.Fatal("fresh repo reports cc-notes enabled; the not-enabled path can't be exercised")
	}

	db := newTestDB(t)
	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "demo", Backend: "wstest", Cwd: repo, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	backend.Register(workstreamBackend{manages: false})

	// workstream-create: no cc-notes project, and its default sprint is unbound.
	wsReply := handleWorkstreamCreate(opCtx(db, mustJSON(t, map[string]string{"repo": "p1", "name": "feat-x"}), nil))
	if !wsReply.OK {
		t.Fatalf("workstream-create not ok: %s", wsReply.Error)
	}
	var wsOut struct {
		WorkstreamID string `json:"workstream_id"`
	}
	if err := json.Unmarshal(wsReply.Body, &wsOut); err != nil {
		t.Fatal(err)
	}
	ws, err := getWorkstream(ctx, db, wsOut.WorkstreamID, "")
	if err != nil {
		t.Fatalf("getWorkstream: %v", err)
	}
	if ws.CCNotesProject != "" {
		t.Errorf("workstream ccnotes_project = %q, want empty", ws.CCNotesProject)
	}
	defSprint, err := getDefaultSprint(ctx, db, ws.ID)
	if err != nil {
		t.Fatalf("getDefaultSprint: %v", err)
	}
	if defSprint.CCNotesSprint != "" {
		t.Errorf("default sprint ccnotes_sprint = %q, want empty", defSprint.CCNotesSprint)
	}

	// sprint-create under that workstream: no cc-notes sprint either.
	spReply := handleSprintCreate(opCtx(db, mustJSON(t, map[string]string{"workstream": ws.ID, "name": "qa"}), nil))
	if !spReply.OK {
		t.Fatalf("sprint-create not ok: %s", spReply.Error)
	}
	var spOut struct {
		SprintID string `json:"sprint_id"`
	}
	if err := json.Unmarshal(spReply.Body, &spOut); err != nil {
		t.Fatal(err)
	}
	sp, err := getSprint(ctx, db, spOut.SprintID, "")
	if err != nil {
		t.Fatalf("getSprint: %v", err)
	}
	if sp.CCNotesSprint != "" {
		t.Errorf("sprint ccnotes_sprint = %q, want empty", sp.CCNotesSprint)
	}
}

// TestCCNotesDisabledSpawnLeavesTaskEmpty proves a spawn into a non-cc-notes repo
// records no cc-notes task on the agent row and never shells out to cc-notes. The
// workstream's worktree is a real git repo with no refs/cc-notes/*, so the gate is
// off the way production sees it.
func TestCCNotesDisabledSpawnLeavesTaskEmpty(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

	repo := gitRepo(t, "main")
	if ccnotes.Enabled(ctx, repo) {
		t.Fatal("fresh repo reports cc-notes enabled; the not-enabled path can't be exercised")
	}

	backend.Register(spawnBackend{spec: &backend.SpawnSpec{}})

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()

	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: repo, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: repo, IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}

	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}
	appendFn := func(_ context.Context, _ *event.Event) (int64, error) { return 1, nil }
	body := mustJSON(t, map[string]string{"repo": "p1", "name": "worker"})
	hc := daemon.HandlerCtx{
		Ctx: ctx, Env: daemon.Envelope{Body: body},
		Window: subject.Window{Session: "parent"},
		Scope:  repo, Subjects: subjects, DB: db, Append: appendFn,
	}

	reply := handleSpawn(hc)
	if !reply.OK {
		t.Fatalf("spawn not ok: %s", reply.Error)
	}
	var out struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatal(err)
	}
	ag, err := getAgent(ctx, db, out.AgentID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.CCNotesTask != "" {
		t.Errorf("agent ccnotes_task = %q, want empty", ag.CCNotesTask)
	}
}

// stubFailingBin prepends a temp dir to PATH holding an executable named bin that
// always exits non-zero, so a CLI shell-out to it fails deterministically without
// the real binary installed.
func stubFailingBin(t *testing.T, bin string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, bin), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", bin, err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestCCNotesSpawnFailureLeavesNoSubjectOrAgent proves cc-notes task creation runs
// before any subject or terminal exists: when cc-notes fails, the spawn fails loud and
// leaves no subject, no agent row, and never reaches the backend — only a residual
// git-ref task, the same tradeoff provisionCCNotes already accepts.
func TestCCNotesSpawnFailureLeavesNoSubjectOrAgent(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

	repo := gitRepo(t, "main")
	// A ref under refs/cc-notes/ makes the repo look cc-notes-enabled.
	if out, err := exec.CommandContext(ctx, "git", "-C", repo, "update-ref", "refs/cc-notes/test", "HEAD").CombinedOutput(); err != nil {
		t.Fatalf("git update-ref: %v\n%s", err, out)
	}
	// A fake cc-notes on PATH that always fails, so CreateTask errors deterministically.
	stubFailingBin(t, "cc-notes")
	if !ccnotes.Enabled(ctx, repo) {
		t.Fatal("precondition: repo must report cc-notes enabled (binary on PATH + a refs/cc-notes/ ref)")
	}

	var gotSpec backend.SpawnSpec
	backend.Register(spawnBackend{spec: &gotSpec})

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: repo, Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: repo, IsPrimary: true, CCNotesProject: "proj-x", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", CCNotesSprint: "sprint-x", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}

	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}
	appendFn := func(context.Context, *event.Event) (int64, error) {
		t.Fatal("Append must not run when cc-notes fails before the subject is started")
		return 0, nil
	}
	hc := daemon.HandlerCtx{
		Ctx: ctx, Env: daemon.Envelope{Body: mustJSON(t, map[string]string{"repo": "p1", "name": "worker"})},
		Window: subject.Window{Session: "parent"}, Scope: repo,
		Subjects: subjects, DB: db, Append: appendFn,
	}

	reply := handleSpawn(hc)
	if reply.OK || reply.Error == "" {
		t.Fatalf("reply = %+v, want ok=false when cc-notes fails", reply)
	}

	// The backend was never reached: no terminal was spawned (Spawn records the
	// session id on the spec, so a zero SessionID proves it was never called).
	if gotSpec.SessionID != "" {
		t.Fatalf("backend.Spawn was called despite the cc-notes failure: %+v", gotSpec)
	}
	// No agent row was persisted.
	agents, err := listAgents(ctx, db, "")
	if err != nil {
		t.Fatalf("listAgents: %v", err)
	}
	if len(agents) != 0 {
		t.Fatalf("agents = %d, want 0", len(agents))
	}
	// No subject was started.
	var subjectCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subjects`).Scan(&subjectCount); err != nil {
		t.Fatalf("count subjects: %v", err)
	}
	if subjectCount != 0 {
		t.Fatalf("subjects = %d, want 0 (cc-notes failed before Subjects.Start)", subjectCount)
	}
}
