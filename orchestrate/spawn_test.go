package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
)

func TestChildMCPConfig(t *testing.T) {
	raw := childMCPConfig("/opt/cc-orchestrate", "sid-1", "/work/scope")

	var got struct {
		MCPServers map[string]mcpServer `json:"mcpServers"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("childMCPConfig produced invalid JSON: %v\n%s", err, raw)
	}
	srv, ok := got.MCPServers["cc-orchestrate"]
	if !ok {
		t.Fatalf("missing cc-orchestrate server in %s", raw)
	}
	if srv.Command != "/opt/cc-orchestrate" {
		t.Errorf("command = %q, want /opt/cc-orchestrate", srv.Command)
	}
	want := []string{"channel", "--session", "sid-1", "--cwd", "/work/scope"}
	if len(srv.Args) != len(want) {
		t.Fatalf("args = %v, want %v", srv.Args, want)
	}
	for i := range want {
		if srv.Args[i] != want[i] {
			t.Fatalf("args = %v, want %v", srv.Args, want)
		}
	}
}

func TestChildSettings(t *testing.T) {
	cases := []struct {
		name        string
		self        string
		wantSession string
		wantGuard   string
	}{
		{
			name:        "plain path",
			self:        "/opt/cc-orchestrate",
			wantSession: "/opt/cc-orchestrate session-record",
			wantGuard:   "/opt/cc-orchestrate guard-edit",
		},
		{
			name:        "path with spaces is shell-quoted",
			self:        "/Applications/My Tools/cc-orchestrate",
			wantSession: "'/Applications/My Tools/cc-orchestrate' session-record",
			wantGuard:   "'/Applications/My Tools/cc-orchestrate' guard-edit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := childSettings(tc.self)
			var got struct {
				Hooks map[string][]hookMatcher `json:"hooks"`
			}
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatalf("childSettings produced invalid JSON: %v\n%s", err, raw)
			}

			session := got.Hooks["SessionStart"]
			if len(session) != 1 || session[0].Matcher != "" {
				t.Fatalf("SessionStart = %+v, want one matcher-less entry", session)
			}
			if len(session[0].Hooks) != 1 || session[0].Hooks[0].Type != "command" ||
				session[0].Hooks[0].Command != tc.wantSession {
				t.Errorf("SessionStart hook = %+v, want command %q", session[0].Hooks, tc.wantSession)
			}

			guard := got.Hooks["PreToolUse"]
			if len(guard) != 1 || guard[0].Matcher != "Edit|Write|NotebookEdit" {
				t.Fatalf("PreToolUse = %+v, want matcher Edit|Write|NotebookEdit", guard)
			}
			if len(guard[0].Hooks) != 1 || guard[0].Hooks[0].Type != "command" ||
				guard[0].Hooks[0].Command != tc.wantGuard {
				t.Errorf("PreToolUse hook = %+v, want command %q", guard[0].Hooks, tc.wantGuard)
			}
		})
	}
}

func TestClaudeCommand(t *testing.T) {
	old := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = old })

	const (
		self  = "/opt/cc-orchestrate"
		sid   = "sid-1"
		scope = "/work"
	)

	t.Run("with prompt", func(t *testing.T) {
		got := claudeCommand(self, sid, scope, "fix the bug")
		want := []string{
			"claude",
			"--session-id", sid,
			"--mcp-config", childMCPConfig(self, sid, scope),
			"--strict-mcp-config",
			"--settings", childSettings(self),
			"--append-system-prompt", spawnBrief(self, sid, scope),
			"fix the bug",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("argv =\n  %v\nwant\n  %v", got, want)
		}
	})
	t.Run("empty prompt omits the trailing arg", func(t *testing.T) {
		got := claudeCommand(self, sid, scope, "")
		want := []string{
			"claude",
			"--session-id", sid,
			"--mcp-config", childMCPConfig(self, sid, scope),
			"--strict-mcp-config",
			"--settings", childSettings(self),
			"--append-system-prompt", spawnBrief(self, sid, scope),
		}
		if !slices.Equal(got, want) {
			t.Fatalf("argv =\n  %v\nwant\n  %v", got, want)
		}
	})
}

func TestResumeCommand(t *testing.T) {
	old := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = old })

	const (
		self  = "/opt/cc-orchestrate"
		sid   = "sid-1"
		scope = "/work"
	)
	got := resumeCommand(self, sid, scope)
	want := []string{
		"claude",
		"--resume", sid,
		"--mcp-config", childMCPConfig(self, sid, scope),
		"--strict-mcp-config",
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("argv =\n  %v\nwant\n  %v", got, want)
	}
}

func TestPooledClaudeCommands(t *testing.T) {
	const (
		self  = "/opt/cc-orchestrate"
		sid   = "sid-1"
		scope = "/work"
	)
	tests := []struct {
		name    string
		command func() []string
		want    []string
	}{
		{
			name:    "spawn",
			command: func() []string { return claudeCommand(self, sid, scope, "fix the bug") },
			want: []string{
				"/opt/homebrew/bin/ccp", "run",
				"--session-id", sid,
				"--mcp-config", childMCPConfig(self, sid, scope),
				"--strict-mcp-config",
				"--settings", childSettings(self),
				"--append-system-prompt", spawnBrief(self, sid, scope),
				"fix the bug",
			},
		},
		{
			name:    "resume",
			command: func() []string { return resumeCommand(self, sid, scope) },
			want: []string{
				"/opt/homebrew/bin/ccp", "run",
				"--resume", sid,
				"--mcp-config", childMCPConfig(self, sid, scope),
				"--strict-mcp-config",
				"--settings", childSettings(self),
				"--append-system-prompt", spawnBrief(self, sid, scope),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := lookupPath
			calls := 0
			lookupPath = func(bin string) (string, error) {
				calls++
				if bin != "ccp" {
					t.Fatalf("lookup binary = %q, want ccp", bin)
				}
				return "/opt/homebrew/bin/ccp", nil
			}
			t.Cleanup(func() { lookupPath = old })

			got := tc.command()
			if calls != 1 {
				t.Errorf("lookup calls = %d, want 1", calls)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("argv =\n  %v\nwant\n  %v", got, tc.want)
			}
		})
	}
}

func TestSpawnBrief(t *testing.T) {
	brief := spawnBrief("/opt/cc-orchestrate", "sid-1", "/work")
	if want := "/opt/cc-orchestrate watch --session sid-1 --cwd /work"; !strings.Contains(brief, want) {
		t.Errorf("brief missing watch command %q:\n%s", want, brief)
	}
	if !strings.Contains(brief, "orchestrate.message") {
		t.Errorf("brief does not name the orchestrate.message event:\n%s", brief)
	}
	if !strings.Contains(brief, `"report"`) {
		t.Errorf("brief does not mention the report tool:\n%s", brief)
	}
}

func TestSpawnBriefShellQuotesSpaces(t *testing.T) {
	brief := spawnBrief("/Apps/My Tools/cc-orchestrate", "sid-1", "/my work")
	if want := "'/Apps/My Tools/cc-orchestrate' watch --session sid-1 --cwd '/my work'"; !strings.Contains(brief, want) {
		t.Errorf("brief missing shell-quoted watch command %q:\n%s", want, brief)
	}
}

func TestTailerManagerStartStop(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir()) // no transcript will ever resolve, so tailers just poll

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	noopAppend := func(context.Context, *event.Event) (int64, error) { return 0, nil }

	ag := agentRow{ID: "a1", SessionID: "sess-x", Scope: "/s", SubjectID: "subj-1"}

	m.start(db, noopAppend, ag)
	if _, ok := m.cancels["a1"]; !ok || len(m.cancels) != 1 {
		t.Fatalf("after start cancels = %v, want one entry for a1", keysOf(m.cancels))
	}

	// A sessionless agent has no transcript, so start is a no-op.
	m.start(db, noopAppend, agentRow{ID: "a2"})
	if _, ok := m.cancels["a2"]; ok || len(m.cancels) != 1 {
		t.Fatalf("sessionless start registered a tailer: %v", keysOf(m.cancels))
	}

	// Restarting the same id cancels-and-replaces rather than doubling up.
	m.start(db, noopAppend, ag)
	if len(m.cancels) != 1 {
		t.Fatalf("restart doubled the registration: %v", keysOf(m.cancels))
	}

	m.stop("a1")
	if _, ok := m.cancels["a1"]; ok || len(m.cancels) != 0 {
		t.Fatalf("after stop cancels = %v, want empty", keysOf(m.cancels))
	}
	m.stop("a1") // idempotent: stopping an unknown id is a no-op
}

// spawnBackend is a registered test backend that records the SpawnSpec it
// receives, so handleSpawn's wiring can be asserted without a live claude.
type spawnBackend struct {
	spec *backend.SpawnSpec
}

func (spawnBackend) Name() backend.Name                { return "spawntest" }
func (spawnBackend) Available() bool                   { return true }
func (spawnBackend) EnsureReady(context.Context) error { return nil }
func (spawnBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}

func (spawnBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}

func (b spawnBackend) Spawn(_ context.Context, spec backend.SpawnSpec) (backend.AgentHandle, error) {
	*b.spec = spec
	return backend.AgentHandle{Backend: "spawntest", ID: "term-1", SessionID: spec.SessionID}, nil
}

func (spawnBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (spawnBackend) Kill(context.Context, backend.AgentHandle) error                { return nil }
func (spawnBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }

// Capture + CanCapture make spawnBackend a capturing backend (like tmux), the common
// case spawned without the pty-host wrapper; the wrapped path is covered by
// TestWrapForCapture.
func (spawnBackend) Capture(context.Context, backend.AgentHandle) (string, error) { return "", nil }
func (spawnBackend) Caps() backend.Caps                                           { return backend.Capabilities(backend.CanCapture) }

func TestHandleSpawn(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	oldLookup := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = oldLookup })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)

	var gotSpec backend.SpawnSpec
	backend.Register(spawnBackend{spec: &gotSpec})

	// store.Open applies cc-interact's core schema (subjects/events) plus the
	// orchestrate migrate, so the subject resolver has a real table to write.
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()

	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "alpha", Backend: "spawntest",
		Cwd: "/tmp/alpha", Status: "active", CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/alpha", IsPrimary: true, Status: "active", CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	// The primary workstream's default sprint: a spawn keyed only to --repo resolves
	// the repo's primary workstream and then its default sprint.
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: "active", CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}

	subjects := subject.Resolver{Store: store.NewSubjectStore(db)}
	// Capture appended events under a mutex: the agent's transcript tailer runs in
	// a background goroutine sharing this appendFn.
	var mu sync.Mutex
	var events []*event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		return 1, nil
	}
	body := mustJSON(t, map[string]string{"repo": "p1", "name": "worker", "prompt": "fix it"})
	hc := daemon.HandlerCtx{
		Ctx: ctx, Env: daemon.Envelope{Body: body},
		Window: subject.Window{Session: "parent", ClaudePID: 4242},
		Scope:  "/parent", Subjects: subjects, DB: db, Append: appendFn,
	}

	reply := runTyped(handleSpawn,hc)
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var out struct {
		AgentID   string `json:"agent_id"`
		SubjectID string `json:"subject_id"`
		Terminal  string `json:"terminal"`
		Backend   string `json:"backend"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatalf("reply body: %v", err)
	}
	if out.AgentID == "" || out.SubjectID == "" {
		t.Fatalf("reply ids empty: %+v", out)
	}
	if out.Terminal != "term-1" || out.Backend != "spawntest" {
		t.Fatalf("reply = %+v, want terminal term-1 backend spawntest", out)
	}

	// The agent row is keyed by the generated session id and bound to the subject.
	ag, err := getAgent(ctx, db, out.AgentID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	want := agentRow{
		ID: out.AgentID, SprintID: "s1", Backend: "spawntest", TerminalHandle: "term-1",
		SessionID: out.AgentID, Scope: "/tmp/alpha", Name: "worker", Prompt: "fix it",
		SubjectID: out.SubjectID, Status: "active", State: StateUnknown,
		CreatedAt: ag.CreatedAt,
	}
	if ag != want {
		t.Fatalf("agent row = %+v, want %+v", ag, want)
	}
	if ag.CreatedAt == "" {
		t.Error("created_at not stamped")
	}

	// The subject was created for the child session with claude_pid 0 (unknown
	// until the child's SessionStart hook rebinds it) — never the parent's pid.
	var session string
	var pid int
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(session_id, ''), claude_pid FROM subjects WHERE id = ?`, out.SubjectID,
	).Scan(&session, &pid); err != nil {
		t.Fatalf("query subject: %v", err)
	}
	if session != out.AgentID || pid != 0 {
		t.Fatalf("subject session=%q pid=%d, want session=%s pid=0", session, pid, out.AgentID)
	}

	// A lifecycle EventSpawned was appended on the child's subject log, symmetric
	// with the EventExited on kill.
	mu.Lock()
	var spawned *event.Event
	for _, e := range events {
		if e.Type == EventSpawned {
			spawned = e
		}
	}
	mu.Unlock()
	if spawned == nil {
		t.Fatal("no EventSpawned appended on spawn")
	}
	if spawned.SubjectID != out.SubjectID || spawned.Origin != event.OriginSystem {
		t.Fatalf("spawned event = subject %q origin %q, want %s/system", spawned.SubjectID, spawned.Origin, out.SubjectID)
	}
	var spl struct {
		Type     string `json:"type"`
		AgentID  string `json:"agent_id"`
		Backend  string `json:"backend"`
		Terminal string `json:"terminal"`
	}
	if err := json.Unmarshal(spawned.Payload, &spl); err != nil {
		t.Fatalf("spawned payload: %v", err)
	}
	if spl.Type != EventSpawned || spl.AgentID != out.AgentID || spl.Backend != "spawntest" || spl.Terminal != "term-1" {
		t.Fatalf("spawned payload = %+v, want agent %s backend spawntest terminal term-1", spl, out.AgentID)
	}

	// The backend received the assembled claude argv keyed to the same session,
	// wrapped outermost by scrub-exec so the child sheds the host's Claude markers.
	if gotSpec.SessionID != out.AgentID {
		t.Errorf("spawn spec session = %q, want %s", gotSpec.SessionID, out.AgentID)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	wantPrefix := []string{self, scrubExecCmdName, "--", "claude"}
	if len(gotSpec.Command) < len(wantPrefix) || !slices.Equal(gotSpec.Command[:len(wantPrefix)], wantPrefix) {
		t.Errorf("spawn command = %v, want prefix %v", gotSpec.Command, wantPrefix)
	}
	if last := gotSpec.Command[len(gotSpec.Command)-1]; last != "fix it" {
		t.Errorf("spawn command trailing arg = %q, want the prompt", last)
	}
}

// TestHandleSpawnDefaultsEmptyName proves an omitted name defaults deterministically
// to "agent-" + the session id's first 8 chars, before it reaches SpawnSpec or the
// DB row — herd rejects an empty --name, so every backend and the DB must agree on
// one non-empty name.
func TestHandleSpawnDefaultsEmptyName(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	oldLookup := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = oldLookup })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)

	var gotSpec backend.SpawnSpec
	backend.Register(spawnBackend{spec: &gotSpec})

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()
	spawnPoolingHierarchy(ctx, t, db)

	subjects := subject.Resolver{Store: store.NewSubjectStore(db)}
	appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }
	body := mustJSON(t, map[string]string{"repo": "p1", "prompt": "fix it"})
	hc := daemon.HandlerCtx{
		Ctx: ctx, Env: daemon.Envelope{Body: body},
		Window: subject.Window{Session: "parent", ClaudePID: 4242},
		Scope:  "/parent", Subjects: subjects, DB: db, Append: appendFn,
	}

	reply := runTyped(handleSpawn,hc)
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var out struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatalf("reply body: %v", err)
	}

	want := "agent-" + out.AgentID[:8]
	if gotSpec.Name != want {
		t.Errorf("spawn spec name = %q, want %q", gotSpec.Name, want)
	}
	ag, err := getAgent(ctx, db, out.AgentID)
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if ag.Name != want {
		t.Errorf("agent row name = %q, want %q", ag.Name, want)
	}
}

// spawnPoolingHierarchy inserts the repo/workstream/sprint a pooling test spawns into.
func spawnPoolingHierarchy(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "alpha", Backend: "spawntest",
		Cwd: "/tmp/alpha", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/alpha", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}
}

// pooledLookupCases is the shared {fallback, pooled} lookupPath table for the
// handleSpawn and respawnAgent pooling tests.
var pooledLookupCases = []struct {
	name     string
	lookup   func(string) (string, error)
	wantHead []string
}{
	{"fallback", func(string) (string, error) { return "", exec.ErrNotFound }, []string{"claude"}},
	{"pooled", func(string) (string, error) { return "/opt/homebrew/bin/ccp", nil }, []string{"/opt/homebrew/bin/ccp", "run"}},
}

// TestHandleSpawnPooling proves the argv handleSpawn hands the backend carries
// claudeInvocation's ccp/claude decision, head and all.
func TestHandleSpawnPooling(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	for _, tc := range pooledLookupCases {
		t.Run(tc.name, func(t *testing.T) {
			oldLookup := lookupPath
			lookupPath = tc.lookup
			t.Cleanup(func() { lookupPath = oldLookup })

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tailers = newTestTailerManager(ctx)

			var gotSpec backend.SpawnSpec
			backend.Register(spawnBackend{spec: &gotSpec})

			st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
			if err != nil {
				t.Fatalf("store.Open: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			db := st.DB()
			spawnPoolingHierarchy(ctx, t, db)

			subjects := subject.Resolver{Store: store.NewSubjectStore(db)}
			appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }
			body := mustJSON(t, map[string]string{"repo": "p1", "name": "worker", "prompt": "fix it"})
			hc := daemon.HandlerCtx{
				Ctx: ctx, Env: daemon.Envelope{Body: body},
				Window: subject.Window{Session: "parent", ClaudePID: 4242},
				Scope:  "/parent", Subjects: subjects, DB: db, Append: appendFn,
			}

			reply := runTyped(handleSpawn,hc)
			if !reply.OK {
				t.Fatalf("reply not ok: %s", reply.Error)
			}
			self, err := os.Executable()
			if err != nil {
				t.Fatalf("os.Executable: %v", err)
			}
			wantHead := append([]string{self, scrubExecCmdName, "--"}, tc.wantHead...)
			if len(gotSpec.Command) < len(wantHead) || !slices.Equal(gotSpec.Command[:len(wantHead)], wantHead) {
				t.Fatalf("spawn command head = %v, want %v", gotSpec.Command, wantHead)
			}
		})
	}
}

// TestRespawnAgentPooling proves respawnAgent's resumeCommand argv reflects the
// same claudeInvocation ccp/claude decision handleSpawn's claudeCommand does.
func TestRespawnAgentPooling(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	for _, tc := range pooledLookupCases {
		t.Run(tc.name, func(t *testing.T) {
			oldLookup := lookupPath
			lookupPath = tc.lookup
			t.Cleanup(func() { lookupPath = oldLookup })

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tailers = newTestTailerManager(ctx)

			var gotSpec backend.SpawnSpec
			backend.Register(spawnBackend{spec: &gotSpec})

			db := newTestDB(ctx, t)
			spawnPoolingHierarchy(ctx, t, db)
			ag := agentRow{
				ID: "a1", SprintID: "s1", Backend: "spawntest", TerminalHandle: "term-0",
				SessionID: "sess-1", Scope: "/tmp/alpha", Name: "worker",
				Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
			}
			mustInsertAgent(ctx, t, db, ag)

			appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }
			if _, err := respawnAgent(ctx, db, appendFn, ag); err != nil {
				t.Fatalf("respawnAgent: %v", err)
			}
			self, err := os.Executable()
			if err != nil {
				t.Fatalf("os.Executable: %v", err)
			}
			wantHead := append([]string{self, scrubExecCmdName, "--"}, tc.wantHead...)
			if len(gotSpec.Command) < len(wantHead) || !slices.Equal(gotSpec.Command[:len(wantHead)], wantHead) {
				t.Fatalf("respawn command head = %v, want %v", gotSpec.Command, wantHead)
			}
		})
	}
}

// TestSpawnedPayloadTerminalKey guards the map→struct conversion of the
// EventSpawned body: the "terminal" key must survive serialization even when the
// agent has no terminal handle yet, proving the struct field carries no omitempty.
func TestSpawnedPayloadTerminalKey(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ag       agentRow
		wantTerm string
	}{
		{"with terminal handle", agentRow{ID: "a1", Backend: "spawntest", TerminalHandle: "term-1"}, "term-1"},
		{"empty terminal handle still emits the key", agentRow{ID: "a2", Backend: "spawntest", TerminalHandle: ""}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got map[string]json.RawMessage
			if err := json.Unmarshal(spawnedPayload(tc.ag), &got); err != nil {
				t.Fatalf("spawnedPayload produced invalid JSON: %v", err)
			}
			raw, ok := got["terminal"]
			if !ok {
				t.Fatalf("spawned payload missing \"terminal\" key: %v", got)
			}
			var term string
			if err := json.Unmarshal(raw, &term); err != nil {
				t.Fatalf("terminal value not a string: %v", err)
			}
			if term != tc.wantTerm {
				t.Errorf("terminal = %q, want %q", term, tc.wantTerm)
			}
		})
	}
}

func keysOf(m map[string]*tailerCancel) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestHandleSpawnRejectsKilledHierarchy proves a spawn fails loud when any level of
// the resolved hierarchy — sprint, workstream, or repo — is no longer active, so a
// live agent can never attach to a soft-killed target. The guard fires before the
// backend or subject is touched, so no event is ever appended.
func TestHandleSpawnRejectsKilledHierarchy(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name                           string
		repoStatus, wsStatus, spStatus LifecycleStatus
	}{
		{"killed sprint", StatusActive, StatusActive, StatusKilled},
		{"killed workstream", StatusActive, StatusKilled, StatusActive},
		{"killed repo", StatusKilled, StatusActive, StatusActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(ctx, t)
			if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: "/tmp/a", Status: tc.repoStatus, CreatedAt: "t0"}); err != nil {
				t.Fatalf("insertRepo: %v", err)
			}
			if err := insertWorkstream(ctx, db, workstreamRow{
				ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
				Branch: "main", Worktree: "/tmp/a", IsPrimary: true, Status: tc.wsStatus, CreatedAt: "t0",
			}); err != nil {
				t.Fatalf("insertWorkstream: %v", err)
			}
			if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: tc.spStatus, CreatedAt: "t0"}); err != nil {
				t.Fatalf("insertSprint: %v", err)
			}
			appendFn := func(context.Context, *event.Event) (int64, error) {
				t.Fatal("Append must not be called when the hierarchy is not active")
				return 0, nil
			}
			//nolint:contextcheck // handleSpawn's tailer derives from the daemon-lifetime base ctx by design (see tailerManager doc)
			reply := runTyped(handleSpawn,opCtx(db, mustJSON(t, map[string]string{"sprint": "s1"}), appendFn))
			if reply.OK || reply.Error == "" {
				t.Fatalf("reply = %+v, want ok=false for a non-active hierarchy", reply)
			}
		})
	}
}

// TestHandleSpawnRejectsRelativeCwdWithNoScope proves a relative --cwd fails loud
// over a scopeless (HTTP) envelope instead of silently resolving against "" (the
// long-lived daemon's own cwd, never the caller's).
func TestHandleSpawnRejectsRelativeCwdWithNoScope(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)
	backend.Register(spawnBackend{spec: &backend.SpawnSpec{}})
	spawnPoolingHierarchy(ctx, t, db)

	body := mustJSON(t, map[string]string{"repo": "p1", "cwd": "sub/dir"})
	reply := runTyped(handleSpawn, opCtx(db, body, func(context.Context, *event.Event) (int64, error) {
		t.Fatal("Append must not be called when cwd cannot be resolved")
		return 0, nil
	}))
	if reply.OK || !strings.HasPrefix(reply.Error, "InvalidRequest: ") {
		t.Fatalf("reply = %+v, want InvalidRequest", reply)
	}
}

// TestHandleAgentRespawn covers cco.agent.respawn's eligibility matrix: the
// agent_id/dead XOR validation, an active or chain-killed agent rejected as a
// Conflict, an eligible exited agent revived into its same session with a fresh
// restart budget (not resetRestart's semantics) and an EventRestored, and the dead
// sweep silently skipping ineligible agents.
func TestHandleAgentRespawn(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)

	db := newTestDB(ctx, t)
	var gotSpec backend.SpawnSpec
	backend.Register(spawnBackend{spec: &gotSpec})

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: "/tmp/a", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/a", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s2", WorkstreamID: "w1", Name: "dead", Status: StatusKilled, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}

	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a-active", SprintID: "s1", Backend: "spawntest", TerminalHandle: "term-old",
		SessionID: "sess-active", Scope: "/tmp/a", Name: "active", SubjectID: "subj-active",
		Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a-chain-dead", SprintID: "s2", Backend: "spawntest", TerminalHandle: "term-old2",
		SessionID: "sess-chain-dead", Scope: "/tmp/a", Name: "chain-dead", SubjectID: "subj-chain-dead",
		Status: StatusExited, State: StateIdle, CreatedAt: "t0", RestartCount: 2, LastRestartAt: "t0",
	})
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a-eligible", SprintID: "s1", Backend: "spawntest", TerminalHandle: "term-old3",
		SessionID: "sess-eligible", Scope: "/tmp/a", Name: "eligible", SubjectID: "subj-eligible",
		Status: StatusExited, State: StateIdle, CreatedAt: "t0", RestartCount: 3, LastRestartAt: "2026-06-01T00:00:00Z",
	})

	t.Run("both agent_id and dead set is invalid", func(t *testing.T) {
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"agent_id": "a-eligible", "dead": true}), log.append))
		if reply.OK || !strings.HasPrefix(reply.Error, "InvalidRequest: ") {
			t.Fatalf("reply = %+v, want InvalidRequest", reply)
		}
	})

	t.Run("neither agent_id nor dead set is invalid", func(t *testing.T) {
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{}), log.append))
		if reply.OK || !strings.HasPrefix(reply.Error, "InvalidRequest: ") {
			t.Fatalf("reply = %+v, want InvalidRequest", reply)
		}
	})

	t.Run("an active agent is a conflict", func(t *testing.T) {
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"agent_id": "a-active"}), log.append))
		if reply.OK || !strings.HasPrefix(reply.Error, "Conflict: ") {
			t.Fatalf("reply = %+v, want Conflict", reply)
		}
	})

	t.Run("an exited agent whose sprint is killed is a conflict", func(t *testing.T) {
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"agent_id": "a-chain-dead"}), log.append))
		if reply.OK || !strings.HasPrefix(reply.Error, "Conflict: ") {
			t.Fatalf("reply = %+v, want Conflict", reply)
		}
	})

	t.Run("an eligible exited agent respawns into the same session", func(t *testing.T) {
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"agent_id": "a-eligible"}), log.append))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var out agentRespawnResult
		if err := json.Unmarshal(reply.Body, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Respawned) != 1 || out.Respawned[0].ID != "a-eligible" || out.Respawned[0].SessionID != "sess-eligible" {
			t.Fatalf("respawned = %+v, want [{ID:a-eligible SessionID:sess-eligible ...}]", out.Respawned)
		}
		if out.Respawned[0].Status != string(StatusActive) {
			t.Fatalf("respawned status = %q, want active", out.Respawned[0].Status)
		}

		ag, err := getAgent(ctx, db, "a-eligible")
		if err != nil {
			t.Fatal(err)
		}
		if ag.RestartCount != 0 {
			t.Fatalf("RestartCount = %d, want 0 (a fresh budget, not resetRestart's semantics)", ag.RestartCount)
		}
		if ag.LastRestartAt == "" || ag.LastRestartAt == "2026-06-01T00:00:00Z" {
			t.Fatalf("LastRestartAt = %q, want a fresh stamp (resetRestart would blank it instead)", ag.LastRestartAt)
		}
		if log.count(EventRestored) != 1 {
			t.Fatalf("EventRestored count = %d, want 1; events=%v", log.count(EventRestored), log.types())
		}
	})

	t.Run("dead sweep respawns eligible agents and silently skips ineligible ones", func(t *testing.T) {
		// a-eligible is active from the prior subtest; re-exit it so the sweep has an
		// eligible candidate alongside the still-ineligible a-chain-dead.
		if err := setAgentLifecycle(ctx, db, "a-eligible", StatusExited); err != nil {
			t.Fatal(err)
		}
		log := &eventLog{}
		reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"dead": true}), log.append))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var out agentRespawnResult
		if err := json.Unmarshal(reply.Body, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Respawned) != 1 || out.Respawned[0].ID != "a-eligible" {
			t.Fatalf("respawned = %+v, want exactly [a-eligible] (a-chain-dead skipped, a-active not exited)", out.Respawned)
		}
	})
}
