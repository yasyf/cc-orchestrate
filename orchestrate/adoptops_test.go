package orchestrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
)

// adoptFakeBackend is a registered test backend for the adopt handlers: onSpawn fires
// inside Spawn (so a test can observe DB state at spawn time), spawnErr forces a
// post-spawn failure, killed records torn-down terminals, workspaces feeds
// ListWorkstreams for the superset-match path, and caps selects the git-managed vs
// ManagesWorktree branch.
type adoptFakeBackend struct {
	onSpawn    func(backend.SpawnSpec)
	spawnErr   error
	killed     *[]backend.AgentHandle
	workspaces []backend.WorkstreamHandle
	caps       backend.Caps
}

func (adoptFakeBackend) Name() backend.Name                { return "adopttest" }
func (adoptFakeBackend) Available() bool                   { return true }
func (adoptFakeBackend) EnsureReady(context.Context) error { return nil }

func (adoptFakeBackend) CreateWorkstream(_ context.Context, spec backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{Backend: "adopttest", ID: "ws-" + spec.Name, Name: spec.Name, Cwd: spec.Cwd, Worktree: spec.Cwd}, nil
}

func (b adoptFakeBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return b.workspaces, nil
}

func (b adoptFakeBackend) Spawn(_ context.Context, spec backend.SpawnSpec) (backend.AgentHandle, error) {
	if b.onSpawn != nil {
		b.onSpawn(spec)
	}
	if b.spawnErr != nil {
		return backend.AgentHandle{}, b.spawnErr
	}
	return backend.AgentHandle{Backend: "adopttest", ID: "term-adopt", SessionID: spec.SessionID}, nil
}

func (adoptFakeBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}

func (b adoptFakeBackend) Kill(_ context.Context, agent backend.AgentHandle) error {
	if b.killed != nil {
		*b.killed = append(*b.killed, agent)
	}
	return nil
}
func (adoptFakeBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (adoptFakeBackend) Capture(context.Context, backend.AgentHandle) (string, error)   { return "", nil }

func (b adoptFakeBackend) Caps() backend.Caps {
	if b.caps == (backend.Caps{}) {
		return backend.Capabilities(backend.CanCapture)
	}
	return b.caps
}

type nonCapturingAdoptBackend struct {
	adoptFakeBackend
}

func (nonCapturingAdoptBackend) Caps() backend.Caps { return backend.Caps{} }

// newAdoptOpEnv sets up an adopt-handler test: a short poll interval, no ccp on PATH, a
// scratch CLAUDE_CONFIG_DIR, a resolved HOME (so worktree paths carry no symlinks), an
// isolated git config, a test tailer manager, and a real on-disk DB with cc-interact's
// core schema plus the orchestrate schema.
func newAdoptOpEnv(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	oldPoll := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = oldPoll })
	oldLookup := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = oldLookup })
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve home: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"), databaseStoreSchema())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return ctx, st.DB()
}

func adoptHC(ctx context.Context, db *sql.DB, log *eventLog) daemon.HandlerCtx {
	return daemon.HandlerCtx{
		Ctx: ctx, Subjects: subject.Resolver{Store: store.NewSubjectStore(db)},
		DB: db, Append: log.append, Scope: "/parent",
	}
}

func initAdoptGitRepo(ctx context.Context, t *testing.T, dir string) {
	t.Helper()
	runAdoptGit(ctx, t, dir, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hi\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runAdoptGit(ctx, t, dir, "add", "README.md")
	runAdoptGit(ctx, t, dir, "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "init")
}

func runAdoptGit(ctx context.Context, t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.CommandContext(ctx, "git", args...) //nolint:gosec // G204: test helper runs git with fixed args in a temp repo
	c.Dir = dir
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// mustResolve is EvalSymlinks or a fatal test failure.
func mustResolve(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("EvalSymlinks %q: %v", path, err)
	}
	return resolved
}

// idleTranscript writes an idle (end_turn) transcript for sid under resolvedCwd's
// project slug and returns its path.
func idleTranscript(t *testing.T, resolvedCwd, sid, branch, prompt string) string {
	t.Helper()
	return writeAdoptTranscript(t, resolvedCwd, sid, []string{
		adoptPromptFixture(t, prompt, resolvedCwd, sid, branch),
		adoptFixture(t, lineText, resolvedCwd, sid, branch),
	})
}

const adoptTestSID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func TestKillForeignClaude(t *testing.T) {
	t.Run("signals a re-verified process and waits for it to exit", func(t *testing.T) {
		cmd := exec.Command("sleep", "60")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}
		pid := cmd.Process.Pid
		// Reap on exit so the ESRCH poll can observe the process leave the table.
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()

		cwd, start := "/tmp/foreign", time.Now()
		// The re-enumeration filters to claude processes, so present the sleep as one whose
		// identity matches what was passed in.
		stubClaudeProcs(t, []claudeProc{{pid: pid, argv: []string{"claude"}, cwd: cwd, start: start}})

		if err := killForeignClaude(context.Background(), pid, cwd, start); err != nil {
			t.Fatalf("killForeignClaude(%d): %v", pid, err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("sleep child not reaped after killForeignClaude returned")
		}
		if processAlive(pid) {
			t.Errorf("process %d still alive after killForeignClaude", pid)
		}
	})

	t.Run("a pid gone since identification is already dead", func(t *testing.T) {
		stubClaudeProcs(t, nil)
		if err := killForeignClaude(context.Background(), 999999, "/tmp/foreign", time.Now()); err != nil {
			t.Errorf("killForeignClaude on a vanished pid = %v, want success", err)
		}
	})

	t.Run("a changed identity refuses without signalling", func(t *testing.T) {
		cmd := exec.Command("sleep", "60")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}
		pid := cmd.Process.Pid
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		// The re-enumeration reports the pid running in a different cwd — a recycled pid.
		stubClaudeProcs(t, []claudeProc{{pid: pid, argv: []string{"claude"}, cwd: "/tmp/other", start: time.Now()}})
		if err := killForeignClaude(context.Background(), pid, "/tmp/foreign", time.Now()); err == nil {
			t.Error("killForeignClaude on a changed identity = nil, want a refusal")
		}
		if !processAlive(pid) {
			t.Error("process signalled despite a changed identity")
		}
	})
}

func TestHandleAdoptList(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	idleTranscript(t, resolved, adoptTestSID, "main", "do the thing")
	stubClaudeProcs(t, []claudeProc{{pid: 42, argv: []string{"claude", "--resume", adoptTestSID}, cwd: resolved}})

	views, err := handleAdoptList(daemon.HandlerCtx{Ctx: ctx, DB: db}, adoptListRequest{Cwd: cwd})
	if err != nil {
		t.Fatalf("handleAdoptList: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("views = %+v, want 1", views)
	}
	v := views[0]
	if v.SessionID != adoptTestSID || v.GitBranch != "main" || v.FirstPrompt != "do the thing" || v.State != string(StateIdle) || !v.Live || v.PID != 42 {
		t.Errorf("view = %+v", v)
	}
	if _, err := time.Parse(time.RFC3339, v.MTime); err != nil {
		t.Errorf("mtime %q not RFC3339: %v", v.MTime, err)
	}
}

func TestHandleAdoptListEmptyCwd(t *testing.T) {
	ctx, db, _, _ := newAdoptTest(t)
	_, err := handleAdoptList(daemon.HandlerCtx{Ctx: ctx, DB: db}, adoptListRequest{})
	if err == nil || !strings.HasPrefix(err.Error(), "InvalidRequest: ") {
		t.Fatalf("err = %v, want InvalidRequest", err)
	}
}

func TestHandleAdoptRepoAutoCreate(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "keep going")
	stubClaudeProcs(t, nil) // no live process → adopt the finished transcript directly

	log := &eventLog{}
	res, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if res.AgentID != adoptTestSID || res.Backend != "adopttest" || res.Terminal != "term-adopt" {
		t.Fatalf("res = %+v", res)
	}
	if res.RepoID == "" || res.WorkstreamID == "" {
		t.Fatalf("res missing hierarchy ids: %+v", res)
	}
	repo, err := getRepo(ctx, db, res.RepoID)
	if err != nil {
		t.Fatalf("getRepo: %v", err)
	}
	if repo.Cwd != resolved {
		t.Errorf("repo cwd = %q, want %q", repo.Cwd, resolved)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Scope != resolved || ag.Prompt != "keep going" || ag.SessionID != adoptTestSID || ag.Status != StatusActive {
		t.Errorf("agent = %+v", ag)
	}
	if ag.CreatedAt == "" {
		t.Error("created_at not stamped at adopt time")
	}
	if log.count(EventAdopted) != 1 {
		t.Errorf("EventAdopted count = %d, want 1; events=%v", log.count(EventAdopted), log.types())
	}
}

func TestHandleAdoptPrimaryMatchSubdirScope(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	sub := filepath.Join(repoDir, "pkg", "sub")
	if err := os.MkdirAll(sub, 0o750); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	resolvedSub := mustResolve(t, sub)
	idleTranscript(t, resolvedSub, adoptTestSID, "main", "subdir work")
	stubClaudeProcs(t, nil)

	res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: sub})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if res.RepoID != repoRes.RepoID {
		t.Errorf("repo id = %q, want the existing %q (no auto-create)", res.RepoID, repoRes.RepoID)
	}
	primary, err := getPrimaryWorkstream(ctx, db, repoRes.RepoID)
	if err != nil {
		t.Fatalf("getPrimaryWorkstream: %v", err)
	}
	if res.WorkstreamID != primary.ID {
		t.Errorf("workstream id = %q, want the primary %q", res.WorkstreamID, primary.ID)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Scope != resolvedSub {
		t.Errorf("agent scope = %q, want the exact session subdir %q", ag.Scope, resolvedSub)
	}
}

func TestHandleAdoptHandMadeWorktree(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	wt := filepath.Join(t.TempDir(), "handmade")
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "feature", wt)
	resolvedWT := mustResolve(t, wt)
	idleTranscript(t, resolvedWT, adoptTestSID, "feature", "worktree work")
	stubClaudeProcs(t, nil)

	res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: wt})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if res.RepoID != repoRes.RepoID {
		t.Errorf("repo id = %q, want the existing main repo %q", res.RepoID, repoRes.RepoID)
	}
	ws, err := getWorkstream(ctx, db, res.WorkstreamID, "")
	if err != nil {
		t.Fatalf("getWorkstream: %v", err)
	}
	if ws.IsPrimary {
		t.Error("adopted workstream must not be primary")
	}
	if ws.Worktree != resolvedWT || ws.Branch != "feature" {
		t.Errorf("workstream = %+v, want worktree %q branch feature", ws, resolvedWT)
	}
	if ws.WorkspaceHandle != "ws-feature" {
		t.Errorf("workspace handle = %q, want the backend-created ws-feature", ws.WorkspaceHandle)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.SprintID == "" || ag.Scope != resolvedWT {
		t.Errorf("agent = %+v, want scope %q and a sprint", ag, resolvedWT)
	}
}

func TestHandleAdoptSupersetWorktree(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	wt := filepath.Join(t.TempDir(), "superset-wt")
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "feature", wt)
	resolvedWT := mustResolve(t, wt)

	t.Run("matches an existing workspace by worktree path", func(t *testing.T) {
		backend.Register(adoptFakeBackend{
			caps:       backend.Capabilities(backend.CanCapture, backend.ManagesWorktree),
			workspaces: []backend.WorkstreamHandle{{Backend: "adopttest", ID: "srv-1", Worktree: resolvedWT}},
		})
		log := &eventLog{}
		hc := adoptHC(ctx, db, log)
		if _, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
			t.Fatalf("handleRepoCreate: %v", err)
		}
		idleTranscript(t, resolvedWT, adoptTestSID, "feature", "superset match")
		stubClaudeProcs(t, nil)

		//nolint:contextcheck // handleAdopt's tailer derives from the daemon-lifetime base ctx by design (see tailerManager doc)
		res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: wt})
		if err != nil {
			t.Fatalf("handleAdopt: %v", err)
		}
		ws, err := getWorkstream(ctx, db, res.WorkstreamID, "")
		if err != nil {
			t.Fatalf("getWorkstream: %v", err)
		}
		if ws.WorkspaceHandle != "srv-1" || ws.IsPrimary {
			t.Errorf("workstream = %+v, want the matched srv-1 non-primary", ws)
		}
		if res.Warning != "" {
			t.Errorf("warning = %q, want none on a match", res.Warning)
		}
	})

	t.Run("falls back to the primary workstream with a warning", func(t *testing.T) {
		db := newAdoptOpDB(ctx, t)
		backend.Register(adoptFakeBackend{
			caps:       backend.Capabilities(backend.CanCapture, backend.ManagesWorktree),
			workspaces: nil, // no server-side workspace matches the checkout
		})
		log := &eventLog{}
		hc := adoptHC(ctx, db, log)
		repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir})
		if err != nil {
			t.Fatalf("handleRepoCreate: %v", err)
		}
		idleTranscript(t, resolvedWT, adoptTestSID, "feature", "superset fallback")
		stubClaudeProcs(t, nil)

		//nolint:contextcheck // handleAdopt's tailer derives from the daemon-lifetime base ctx by design (see tailerManager doc)
		res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: wt})
		if err != nil {
			t.Fatalf("handleAdopt: %v", err)
		}
		primary, err := getPrimaryWorkstream(ctx, db, repoRes.RepoID)
		if err != nil {
			t.Fatalf("getPrimaryWorkstream: %v", err)
		}
		if res.WorkstreamID != primary.ID {
			t.Errorf("workstream id = %q, want the primary %q", res.WorkstreamID, primary.ID)
		}
		if !strings.Contains(res.Warning, "primary workstream") {
			t.Errorf("warning = %q, want a primary-workstream fallback note", res.Warning)
		}
		ag, err := getAgent(ctx, db, adoptTestSID)
		if err != nil {
			t.Fatalf("getAgent: %v", err)
		}
		if ag.Scope != resolvedWT {
			t.Errorf("agent scope = %q, want the checkout %q", ag.Scope, resolvedWT)
		}
	})
}

func newAdoptOpDB(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"), databaseStoreSchema())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st.DB()
}

func TestHandleAdoptAlreadyManaged(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "already here")
	stubClaudeProcs(t, nil)
	mustInsertAgent(ctx, t, db, agentRow{
		ID: adoptTestSID, SprintID: "s1", Backend: "adopttest", Scope: resolved,
		Status: StatusExited, State: StateIdle, CreatedAt: "t0",
	})

	log := &eventLog{}
	_, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err == nil || !strings.HasPrefix(err.Error(), "Conflict: ") || !strings.Contains(err.Error(), "respawn") {
		t.Fatalf("err = %v, want a Conflict pointing at respawn", err)
	}
}

func TestHandleAdoptNonQuiescent(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	// A live, working session with a fresh mtime is mid-turn → not quiescent.
	path := writeAdoptTranscript(t, resolved, adoptTestSID, []string{
		adoptPromptFixture(t, "busy", resolved, adoptTestSID, "main"),
		adoptFixture(t, lineBash, resolved, adoptTestSID, "main"),
	})
	setModTime(t, path, time.Now())
	stubClaudeProcs(t, []claudeProc{{pid: 77, argv: []string{"claude", "--resume", adoptTestSID}, cwd: resolved}})

	log := &eventLog{}
	_, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err == nil || !strings.HasPrefix(err.Error(), "NotReady: ") || !strings.Contains(err.Error(), "mid-turn") {
		t.Fatalf("err = %v, want a NotReady with the mid-turn reason", err)
	}
}

func TestHandleAdoptAmbiguousPID(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "who am i")
	// Two bare-claude processes in the cwd, neither carrying the sid → tier-two ambiguity.
	stubClaudeProcs(t, []claudeProc{
		{pid: 11, argv: []string{"claude"}, cwd: resolved},
		{pid: 12, argv: []string{"claude", "--model", "opus"}, cwd: resolved},
	})

	log := &eventLog{}
	_, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err == nil || !strings.Contains(err.Error(), "--pid") {
		t.Fatalf("err = %v, want an error pointing at --pid", err)
	}
}

func TestHandleAdoptNonGit(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	cwd := t.TempDir() // a plain dir, not a git repo
	resolved := mustResolve(t, cwd)
	idleTranscript(t, resolved, adoptTestSID, "main", "not git")
	stubClaudeProcs(t, []claudeProc{{pid: 5, argv: []string{"claude", "--resume", adoptTestSID}, cwd: resolved}})
	killed := stubKillForeign(t)

	log := &eventLog{}
	_, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: cwd})
	if err == nil || !strings.HasPrefix(err.Error(), "InvalidRequest: ") || !strings.Contains(err.Error(), "git repository") {
		t.Fatalf("err = %v, want an InvalidRequest about a git repository", err)
	}
	if *killed != 0 {
		t.Errorf("killForeignClaude called %d times; a deterministic refusal must precede the kill", *killed)
	}
}

// stubKillForeign replaces killForeignClaude with a no-op that counts its calls.
func stubKillForeign(t *testing.T) *int {
	t.Helper()
	prev := killForeignClaude
	calls := new(int)
	killForeignClaude = func(context.Context, int, string, time.Time) error {
		*calls++
		return nil
	}
	t.Cleanup(func() { killForeignClaude = prev })
	return calls
}

func TestHandleAdoptSpawnBeforeInsert(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	var rowAtSpawn, spawnCalled bool
	backend.Register(adoptFakeBackend{onSpawn: func(backend.SpawnSpec) {
		spawnCalled = true
		exists, err := agentExists(ctx, db, adoptTestSID)
		if err != nil {
			t.Errorf("agentExists in Spawn: %v", err)
		}
		rowAtSpawn = exists
	}})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "ordering")
	stubClaudeProcs(t, nil)

	log := &eventLog{}
	if _, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir}); err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if !spawnCalled {
		t.Fatal("backend Spawn was never called")
	}
	if rowAtSpawn {
		t.Error("agents row existed at Spawn time; adopt must spawn before insert")
	}
	if exists, _ := agentExists(ctx, db, adoptTestSID); !exists {
		t.Error("agents row absent after adopt; insert must follow a successful spawn")
	}
}

func TestHandleAdoptPassesSpawnNonceToPTYHost(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	lookupPath = func(name string) (string, error) {
		if name == "ccp" {
			return "/test/ccp", nil
		}
		return "", exec.ErrNotFound
	}
	var gotSpec backend.SpawnSpec
	backend.Register(nonCapturingAdoptBackend{adoptFakeBackend{onSpawn: func(spec backend.SpawnSpec) {
		gotSpec = spec
	}}})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "nonce")
	stubClaudeProcs(t, nil)

	log := &eventLog{}
	if _, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir}); err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.SpawnNonce == "" {
		t.Fatal("spawn_nonce not stamped on adopted agent")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	wantPrefix := []string{
		self, scrubExecCmdName, "--", self, ptyHostCmdName,
		"--session-id", adoptTestSID, "--spawn-nonce", ag.SpawnNonce, "--", "/test/ccp", "run",
	}
	if len(gotSpec.Command) < len(wantPrefix) || !slices.Equal(gotSpec.Command[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("spawn command = %v, want prefix %v", gotSpec.Command, wantPrefix)
	}
}

func TestHandleAdoptUnresolvableLauncherLeavesNoSubjectOrAgent(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	lookupPath = func(name string) (string, error) {
		if name == "ccp" {
			return "/test/ccp", nil
		}
		return "", exec.ErrNotFound
	}
	var spawnCalled bool
	backend.Register(nonCapturingAdoptBackend{adoptFakeBackend{onSpawn: func(backend.SpawnSpec) {
		spawnCalled = true
	}}})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	if err := setConfig(ctx, db, childLauncherKey, `["missing-launcher","wrap","--"]`); err != nil {
		t.Fatalf("set child launcher: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "ordering")
	stubClaudeProcs(t, nil)

	log := &eventLog{}
	_, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err == nil || !strings.Contains(err.Error(), `resolve launcher "missing-launcher"`) {
		t.Fatalf("err = %v, want launcher resolution error", err)
	}
	if spawnCalled {
		t.Fatal("backend Spawn was called despite launcher resolution failure")
	}
	agents, err := listAgents(ctx, db, "", "")
	if err != nil {
		t.Fatalf("listAgents: %v", err)
	}
	if len(agents) != 0 {
		t.Fatalf("agents = %d, want 0", len(agents))
	}
	var subjectCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subjects`).Scan(&subjectCount); err != nil {
		t.Fatalf("count subjects: %v", err)
	}
	if subjectCount != 0 {
		t.Fatalf("subjects = %d, want 0 (launcher resolution failed before Subjects.Start)", subjectCount)
	}
}

func TestHandleAdoptCompensatesPostSpawnFailure(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	killed := &[]backend.AgentHandle{}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	// Kill the target sprint from inside Spawn — after placement resolved it active,
	// before the post-insert hierarchy re-check — to drive the compensation path.
	backend.Register(adoptFakeBackend{
		killed: killed,
		onSpawn: func(backend.SpawnSpec) {
			primary, err := getPrimaryWorkstream(ctx, db, repoIDByName(ctx, t, db, "alpha"))
			if err != nil {
				t.Errorf("getPrimaryWorkstream: %v", err)
				return
			}
			sp, err := getDefaultSprint(ctx, db, primary.ID)
			if err != nil {
				t.Errorf("getDefaultSprint: %v", err)
				return
			}
			if err := setSprintStatus(ctx, db, sp.ID, StatusKilled); err != nil {
				t.Errorf("setSprintStatus: %v", err)
			}
		},
	})
	if _, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "will be compensated")
	stubClaudeProcs(t, nil)

	_, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err == nil || !strings.HasPrefix(err.Error(), "Conflict: ") {
		t.Fatalf("err = %v, want a Conflict from the post-insert hierarchy re-check", err)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Status != StatusExited {
		t.Errorf("agent status = %q, want exited (compensated), not a live orphan", ag.Status)
	}
	if len(*killed) != 1 {
		t.Errorf("terminal kills = %d, want 1 (compensation tore down the spawned terminal)", len(*killed))
	}
}

func repoIDByName(ctx context.Context, t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	repos, err := listRepos(ctx, db, StatusActive)
	if err != nil {
		t.Fatalf("listRepos: %v", err)
	}
	for _, r := range repos {
		if r.Name == name {
			return r.ID
		}
	}
	t.Fatalf("repo %q not found", name)
	return ""
}

func TestHandleAdoptTrailing(t *testing.T) {
	cases := []struct {
		name     string
		flush    string // the line the "kill" appends to the transcript
		wantUser bool
		wantTorn bool
	}{
		{
			name:     "a trailing user turn warns",
			flush:    `{"type":"user","message":{"content":"one more thing"}}`,
			wantUser: true,
		},
		{
			// A clean SIGTERM flushes only a last-prompt metadata line; it must not warn,
			// or every live adoption would cry wolf.
			name:  "a clean last-prompt flush does not warn",
			flush: `{"type":"last-prompt","text":"draft in the box"}`,
		},
		{
			// A just-submitted prompt cut mid-write warns about the torn final entry.
			name:     "a torn trailing write warns",
			flush:    `{"type":"user","message":{"content":`,
			wantTorn: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newAdoptOpEnv(t)
			backend.Register(adoptFakeBackend{})
			if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
				t.Fatalf("setConfig: %v", err)
			}
			repoDir := t.TempDir()
			initAdoptGitRepo(ctx, t, repoDir)
			resolved := mustResolve(t, repoDir)
			path := idleTranscript(t, resolved, adoptTestSID, "main", "idle then killed")
			stubClaudeProcs(t, []claudeProc{{pid: 88, argv: []string{"claude", "--resume", adoptTestSID}, cwd: resolved}})
			// The kill "flushes" a line to the transcript, growing it in both cases.
			prev := killForeignClaude
			killForeignClaude = func(context.Context, int, string, time.Time) error {
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // G304: fixture path under t.TempDir
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				_, err = f.WriteString(tc.flush + "\n")
				return err
			}
			t.Cleanup(func() { killForeignClaude = prev })

			log := &eventLog{}
			res, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
			if err != nil {
				t.Fatalf("handleAdopt: %v", err)
			}
			gotUser := strings.Contains(res.Warning, "trailing user message")
			gotTorn := strings.Contains(res.Warning, "partial trailing write")
			if gotUser != tc.wantUser || gotTorn != tc.wantTorn {
				t.Errorf("warnings user=%v torn=%v (warning=%q), want user=%v torn=%v", gotUser, gotTorn, res.Warning, tc.wantUser, tc.wantTorn)
			}
		})
	}
}

// TestHandleAdoptWarnsOnTornTail covers fix 10a: a transcript that already ends mid-write
// at validation time carries a warning rather than failing, even for a dead session with
// no live process to kill.
func TestHandleAdoptWarnsOnTornTail(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	// A valid metadata line and an idle turn, then a final line cut mid-write (no newline).
	content := adoptPromptFixture(t, "torn tail", resolved, adoptTestSID, "main") + "\n" +
		adoptFixture(t, lineText, resolved, adoptTestSID, "main") + "\n" +
		`{"type":"assistant","message":{"content":`
	writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), adoptTestSID, content)
	stubClaudeProcs(t, nil) // dead session — no kill

	log := &eventLog{}
	res, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if !strings.Contains(res.Warning, "ends mid-write") {
		t.Errorf("warning = %q, want an ends-mid-write note for the torn tail", res.Warning)
	}
}

func TestHandleAdoptRelocateDirtyRefusal(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	if _, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	// An untracked file makes the checkout dirty.
	if err := os.WriteFile(filepath.Join(repoDir, "scratch.txt"), []byte("wip\n"), 0o600); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "dirty")
	stubClaudeProcs(t, []claudeProc{{pid: 9, argv: []string{"claude", "--resume", adoptTestSID}, cwd: resolved}})
	killed := stubKillForeign(t)

	_, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: repoDir, Relocate: true})
	if err == nil || !strings.HasPrefix(err.Error(), "Conflict: ") || !strings.Contains(err.Error(), "uncommitted") {
		t.Fatalf("err = %v, want a Conflict about uncommitted changes", err)
	}
	if *killed != 0 {
		t.Errorf("killForeignClaude called %d times; the dirty refusal must precede the kill", *killed)
	}
}

func TestHandleAdoptRelocateRenamesTranscript(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	var spawnCommand []string
	backend.Register(adoptFakeBackend{onSpawn: func(spec backend.SpawnSpec) { spawnCommand = spec.Command }})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	if _, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	resolved := mustResolve(t, repoDir)
	oldPath := idleTranscript(t, resolved, adoptTestSID, "main", "move me")
	stubClaudeProcs(t, nil)

	res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: repoDir, Relocate: true, Name: "moved"})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Errorf("old transcript still present (stat err = %v), want it moved", err)
	}
	ws, err := getWorkstream(ctx, db, res.WorkstreamID, "")
	if err != nil {
		t.Fatalf("getWorkstream: %v", err)
	}
	if ws.IsPrimary {
		t.Error("relocate must land in a fresh non-primary workstream")
	}
	projectsDir, err := claudeProjectsDir()
	if err != nil {
		t.Fatalf("claudeProjectsDir: %v", err)
	}
	newPath := filepath.Join(projectsDir, adoptSlug(mustResolve(t, ws.Worktree)), adoptTestSID+".jsonl")
	if _, err := os.Stat(newPath); err != nil {
		t.Errorf("relocated transcript missing at %s: %v", newPath, err)
	}
	ag, err := getAgent(ctx, db, adoptTestSID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Scope != ws.Worktree {
		t.Errorf("agent scope = %q, want the new worktree %q", ag.Scope, ws.Worktree)
	}
	if !hasArgContaining(spawnCommand, "RELOCATION") {
		t.Errorf("resume command carried no relocation note: %v", spawnCommand)
	}
}

func hasArgContaining(argv []string, sub string) bool {
	for _, a := range argv {
		if strings.Contains(a, sub) {
			return true
		}
	}
	return false
}

func TestRelocateAdoptTargetCollision(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	repo, err := getRepo(ctx, db, repoRes.RepoID)
	if err != nil {
		t.Fatalf("getRepo: %v", err)
	}
	resolved := mustResolve(t, repoDir)
	oldPath := idleTranscript(t, resolved, adoptTestSID, "main", "collide")
	projectsDir, err := claudeProjectsDir()
	if err != nil {
		t.Fatalf("claudeProjectsDir: %v", err)
	}
	// Pre-create a transcript at the deterministic relocation destination (HOME is
	// resolved, so the worktree path carries no symlinks).
	destWorktree := filepath.Join(worktreesBase(), repo.ID, pathComponent("moved"))
	destDir := filepath.Join(projectsDir, adoptSlug(destWorktree))
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		t.Fatalf("mkdir destDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destDir, adoptTestSID+".jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write colliding transcript: %v", err)
	}

	_, _, err = relocateAdopt(hc, repo, resolved, oldPath, projectsDir, adoptRequest{Name: "moved"}, adoptTestSID)
	if err == nil || !strings.HasPrefix(err.Error(), "Conflict: ") || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("err = %v, want a Conflict about an existing transcript", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Errorf("original transcript disturbed by a refused relocate: %v", err)
	}
	// The workstream created before the collision must be torn back down, never left active.
	active, err := listWorkstreams(ctx, db, repoRes.RepoID, StatusActive)
	if err != nil {
		t.Fatalf("listWorkstreams: %v", err)
	}
	for _, ws := range active {
		if !ws.IsPrimary {
			t.Errorf("refused relocate left an active non-primary workstream %q", ws.ID)
		}
	}
}

func TestIdentifyAdoptPID(t *testing.T) {
	cwd := "/private/tmp/proj"
	sid := "the-sid"
	procs := []claudeProc{
		{pid: 10, argv: []string{"claude"}, cwd: cwd, start: time.Now()},
		{pid: 11, argv: []string{"claude", "--session-id", "other-sid"}, cwd: cwd},
		{pid: 12, argv: []string{"claude", "--resume", "the-sid"}, cwd: cwd},
		{pid: 13, argv: []string{"claude"}, cwd: "/other"},
	}
	metas := []candidateMeta{{sid: sid, mtime: time.Now().Add(time.Minute)}}
	for _, tc := range []struct {
		name    string
		reqPID  int
		wantPID int
		wantErr string
	}{
		{name: "absent pid", reqPID: 99, wantErr: "not a running claude process"},
		{name: "pid in another directory", reqPID: 13, wantErr: "not the session directory"},
		{name: "pid resuming a different session", reqPID: 11, wantErr: "resuming or serving a different session"},
		{name: "pid resuming the target session", reqPID: 12, wantPID: 12},
		{name: "bare cwd pid", reqPID: 10, wantPID: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, live, err := identifyAdoptPID(procs, cwd, metas, sid, tc.reqPID)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("identifyAdoptPID: %v", err)
			}
			if !live || proc.pid != tc.wantPID {
				t.Errorf("identifyAdoptPID = (%+v, %v), want pid %d live", proc, live, tc.wantPID)
			}
		})
	}
}

func TestHandleAdoptReusesWorktreeWorkstream(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	if _, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	wt := filepath.Join(t.TempDir(), "handmade")
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "feature", wt)
	resolvedWT := mustResolve(t, wt)

	sidA := "aaaaaaaa-1111-1111-1111-111111111111"
	sidB := "bbbbbbbb-2222-2222-2222-222222222222"
	idleTranscript(t, resolvedWT, sidA, "feature", "first session")
	idleTranscript(t, resolvedWT, sidB, "feature", "second session")
	stubClaudeProcs(t, nil)

	resA, err := handleAdopt(hc, adoptRequest{SessionID: sidA, Cwd: wt})
	if err != nil {
		t.Fatalf("handleAdopt A: %v", err)
	}
	resB, err := handleAdopt(hc, adoptRequest{SessionID: sidB, Cwd: wt})
	if err != nil {
		t.Fatalf("handleAdopt B: %v", err)
	}
	if resB.WorkstreamID != resA.WorkstreamID {
		t.Errorf("second adopt workstream = %q, want the reused %q", resB.WorkstreamID, resA.WorkstreamID)
	}
	// Exactly one non-primary workstream wraps the checkout; the second adopt reused it.
	wss, err := listWorkstreams(ctx, db, resA.RepoID, "")
	if err != nil {
		t.Fatalf("listWorkstreams: %v", err)
	}
	wrapping := 0
	for _, ws := range wss {
		if mustResolve(t, ws.Worktree) == resolvedWT {
			wrapping++
		}
	}
	if wrapping != 1 {
		t.Errorf("workstreams wrapping the checkout = %d, want 1 (no duplicate)", wrapping)
	}
}

func TestHandleAdoptLinkedWorktreeRepoMatch(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	// Register a repo whose own Cwd is a linked worktree, then adopt a session running in a
	// sibling worktree that shares the same git common dir.
	wtA := filepath.Join(t.TempDir(), "wtA")
	wtB := filepath.Join(t.TempDir(), "wtB")
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "a", wtA)
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "b", wtB)
	repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "linked", Backend: "adopttest", Cwd: wtA})
	if err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	resolvedWtB := mustResolve(t, wtB)
	idleTranscript(t, resolvedWtB, adoptTestSID, "b", "sibling worktree")
	stubClaudeProcs(t, nil)

	res, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: wtB})
	if err != nil {
		t.Fatalf("handleAdopt: %v", err)
	}
	if res.RepoID != repoRes.RepoID {
		t.Errorf("repo id = %q, want the sibling-sharing repo %q (no auto-create)", res.RepoID, repoRes.RepoID)
	}
	repos, err := listRepos(ctx, db, StatusActive)
	if err != nil {
		t.Fatalf("listRepos: %v", err)
	}
	if len(repos) != 1 {
		t.Errorf("repo count = %d, want 1 — the linked worktree must match, not auto-create", len(repos))
	}
}

func TestHandleAdoptKillsTerminalOnInsertFailure(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	killed := &[]backend.AgentHandle{}
	backend.Register(adoptFakeBackend{
		killed: killed,
		onSpawn: func(backend.SpawnSpec) {
			// Drop the table so the post-spawn insertAgent fails, exercising the fix-11
			// orphaned-terminal teardown.
			if _, err := db.ExecContext(ctx, `DROP TABLE orchestrate_agents`); err != nil {
				t.Errorf("drop agents: %v", err)
			}
		},
	})
	if err := setConfig(ctx, db, configBackend, "adopttest"); err != nil {
		t.Fatalf("setConfig: %v", err)
	}
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	resolved := mustResolve(t, repoDir)
	idleTranscript(t, resolved, adoptTestSID, "main", "insert will fail")
	stubClaudeProcs(t, nil)

	log := &eventLog{}
	if _, err := handleAdopt(adoptHC(ctx, db, log), adoptRequest{SessionID: adoptTestSID, Cwd: repoDir}); err == nil {
		t.Fatal("handleAdopt succeeded despite a dropped agents table")
	}
	if len(*killed) != 1 {
		t.Errorf("terminal kills = %d, want 1 (the orphaned terminal was torn down)", len(*killed))
	}
}

// TestTeardownAdoptWorkstreamSkipsInflightAgent is the R1 self-deadlock regression: with
// agentLock(sid) held (as handleAdopt holds it for its whole body) and the sid agent still
// Active under the workstream — the state a compensation that failed to flip it off Active
// leaves behind — the teardown must skip sid rather than re-lock its mutex and hang.
func TestTeardownAdoptWorkstreamSkipsInflightAgent(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	log := &eventLog{}
	hc := adoptHC(ctx, db, log)
	repoRes, err := handleRepoCreate(hc, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir})
	if err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	repo, err := getRepo(ctx, db, repoRes.RepoID)
	if err != nil {
		t.Fatalf("getRepo: %v", err)
	}
	wt := filepath.Join(t.TempDir(), "handmade")
	runAdoptGit(ctx, t, repoDir, "worktree", "add", "-b", "feature", wt)
	ws, sp, err := insertAdoptedWorkstream(hc, repo, mustResolve(t, wt), "feature", "ws-feature")
	if err != nil {
		t.Fatalf("insertAdoptedWorkstream: %v", err)
	}
	sid := "inflight-sid"
	mustInsertAgent(ctx, t, db, agentRow{
		ID: sid, SprintID: sp.ID, Backend: "adopttest", Scope: mustResolve(t, wt),
		SubjectID: "sub", Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
	})

	// Hold the in-flight agent's lock, exactly as handleAdopt does across its whole op.
	mu := agentLock(sid)
	mu.Lock()
	defer mu.Unlock()

	done := make(chan struct{})
	go func() {
		teardownAdoptWorkstream(hc, ws.ID, sid)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("teardownAdoptWorkstream deadlocked re-locking agentLock(sid)")
	}

	got, err := getWorkstream(ctx, db, ws.ID, "")
	if err != nil {
		t.Fatalf("getWorkstream: %v", err)
	}
	if got.Status != StatusKilled {
		t.Errorf("workstream status = %q, want killed", got.Status)
	}
	ag, err := getAgent(ctx, db, sid)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Status != StatusActive {
		t.Errorf("skipped agent status = %q, want still active — the cascade must leave the caller's agent alone", ag.Status)
	}
}

// TestCompensateSpawnLockedFlipsOffActiveFirst is the R1 hardening check: even when the
// terminal teardown fails (an unregistered backend here), the agent row is flipped off
// Active first, so a single teardown failure can never leave an Active-but-dead orphan.
func TestCompensateSpawnLockedFlipsOffActiveFirst(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "compensate-sid", SprintID: "s1", Backend: "no-such-backend", Scope: "/x",
		SubjectID: "sub", Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
	})
	appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }
	ag, err := getAgent(ctx, db, "compensate-sid")
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if cerr := compensateSpawnLocked(ctx, db, appendFn, ag); cerr == nil {
		t.Error("compensateSpawnLocked = nil, want the terminal-teardown error surfaced")
	}
	cur, err := getAgent(ctx, db, "compensate-sid")
	if err != nil {
		t.Fatalf("getAgent after: %v", err)
	}
	if cur.Status != StatusExited {
		t.Errorf("agent status = %q, want exited — the flip must precede the failing teardown", cur.Status)
	}
}

// TestHandleAdoptRelocateLaunchFailureRollsBack drives the full R1 path end to end: a
// relocate adopt whose launch fails after the transcript move must roll the transcript back,
// tear the created workstream down, return the error, and never hang.
func TestHandleAdoptRelocateLaunchFailureRollsBack(t *testing.T) {
	ctx, db := newAdoptOpEnv(t)
	backend.Register(adoptFakeBackend{})
	repoDir := t.TempDir()
	initAdoptGitRepo(ctx, t, repoDir)
	if _, err := handleRepoCreate(daemon.HandlerCtx{Ctx: ctx, Subjects: subject.Resolver{Store: store.NewSubjectStore(db)}, DB: db, Append: (&eventLog{}).append, Scope: "/parent"}, repoCreateRequest{Name: "alpha", Backend: "adopttest", Cwd: repoDir}); err != nil {
		t.Fatalf("handleRepoCreate: %v", err)
	}
	resolved := mustResolve(t, repoDir)
	oldPath := idleTranscript(t, resolved, adoptTestSID, "main", "roll me back")
	stubClaudeProcs(t, nil)

	// Fail the launch at the EventAdopted append, after insertAgent and the transcript move.
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		if e.Type == EventAdopted {
			return 0, errors.New("append boom")
		}
		return 1, nil
	}
	hc := daemon.HandlerCtx{
		Ctx: ctx, Subjects: subject.Resolver{Store: store.NewSubjectStore(db)},
		DB: db, Append: appendFn, Scope: "/parent",
	}

	done := make(chan error, 1)
	go func() {
		_, err := handleAdopt(hc, adoptRequest{SessionID: adoptTestSID, Cwd: repoDir, Relocate: true, Name: "rollback"})
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleAdopt hung on the relocate launch-failure rollback")
	}
	if err == nil {
		t.Fatal("handleAdopt = nil, want the launch failure surfaced")
	}
	if _, statErr := os.Stat(oldPath); statErr != nil {
		t.Errorf("transcript not rolled back to %s: %v", oldPath, statErr)
	}
	active, err := listWorkstreams(ctx, db, "", StatusActive)
	if err != nil {
		t.Fatalf("listWorkstreams: %v", err)
	}
	for _, ws := range active {
		if !ws.IsPrimary {
			t.Errorf("relocate launch failure left an active non-primary workstream %q", ws.ID)
		}
	}
}
