package orchestrate

import (
	"context"
	"encoding/json"
	"path/filepath"
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
	t.Run("with prompt", func(t *testing.T) {
		argv := claudeCommand("/opt/cc-orchestrate", "sid-1", "/work", "fix the bug")
		if argv[0] != "claude" {
			t.Fatalf("argv[0] = %q, want claude", argv[0])
		}
		if got := flagValue(argv, "--session-id"); got != "sid-1" {
			t.Errorf("--session-id = %q, want sid-1", got)
		}
		if !contains(argv, "--strict-mcp-config") {
			t.Errorf("argv missing --strict-mcp-config: %v", argv)
		}
		if got := flagValue(argv, "--mcp-config"); got != childMCPConfig("/opt/cc-orchestrate", "sid-1", "/work") {
			t.Errorf("--mcp-config = %q", got)
		}
		if got := flagValue(argv, "--settings"); got != childSettings("/opt/cc-orchestrate") {
			t.Errorf("--settings = %q", got)
		}
		if got := flagValue(argv, "--append-system-prompt"); got != spawnBrief("/opt/cc-orchestrate", "sid-1", "/work") {
			t.Errorf("--append-system-prompt = %q", got)
		}
		if last := argv[len(argv)-1]; last != "fix the bug" {
			t.Errorf("trailing arg = %q, want the prompt", last)
		}
	})
	t.Run("empty prompt omits the trailing arg", func(t *testing.T) {
		argv := claudeCommand("/opt/cc-orchestrate", "sid-1", "/work", "")
		if last := argv[len(argv)-1]; last != spawnBrief("/opt/cc-orchestrate", "sid-1", "/work") {
			t.Errorf("trailing arg = %q, want the brief value (no prompt)", last)
		}
	})
}

func TestResumeCommand(t *testing.T) {
	argv := resumeCommand("/opt/cc-orchestrate", "sid-1", "/work")
	if argv[0] != "claude" {
		t.Fatalf("argv[0] = %q, want claude", argv[0])
	}
	if got := flagValue(argv, "--resume"); got != "sid-1" {
		t.Errorf("--resume = %q, want sid-1", got)
	}
	if contains(argv, "--session-id") {
		t.Errorf("resume argv carries --session-id: %v", argv)
	}
	if contains(argv, "--fork-session") {
		t.Errorf("resume argv carries --fork-session: %v", argv)
	}
	if !contains(argv, "--strict-mcp-config") {
		t.Errorf("argv missing --strict-mcp-config: %v", argv)
	}
	if got := flagValue(argv, "--mcp-config"); got != childMCPConfig("/opt/cc-orchestrate", "sid-1", "/work") {
		t.Errorf("--mcp-config = %q, want the shared child config", got)
	}
	if got := flagValue(argv, "--settings"); got != childSettings("/opt/cc-orchestrate") {
		t.Errorf("--settings = %q, want the shared child settings", got)
	}
	if got := flagValue(argv, "--append-system-prompt"); got != spawnBrief("/opt/cc-orchestrate", "sid-1", "/work") {
		t.Errorf("--append-system-prompt = %q, want the re-arming brief", got)
	}
	// The brief is the last token: a resume carries no trailing positional prompt.
	if last := argv[len(argv)-1]; last != spawnBrief("/opt/cc-orchestrate", "sid-1", "/work") {
		t.Errorf("trailing arg = %q, want the brief (no positional prompt)", last)
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
	m := newTailerManager(ctx)
	db := newTestDB(t)
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

func (spawnBackend) Name() backend.BackendName         { return "spawntest" }
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
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

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

	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}
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

	reply := handleSpawn(hc)
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

	// The backend received the assembled claude argv keyed to the same session.
	if gotSpec.SessionID != out.AgentID {
		t.Errorf("spawn spec session = %q, want %s", gotSpec.SessionID, out.AgentID)
	}
	if len(gotSpec.Command) == 0 || gotSpec.Command[0] != "claude" {
		t.Errorf("spawn command = %v, want it to start with claude", gotSpec.Command)
	}
	if last := gotSpec.Command[len(gotSpec.Command)-1]; last != "fix it" {
		t.Errorf("spawn command trailing arg = %q, want the prompt", last)
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

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// flagValue returns the argument following the named flag in argv.
func flagValue(argv []string, flag string) string {
	for i, a := range argv {
		if a == flag && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
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
			db := newTestDB(t)
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
			reply := handleSpawn(opCtx(db, mustJSON(t, map[string]string{"sprint": "s1"}), appendFn))
			if reply.OK || reply.Error == "" {
				t.Fatalf("reply = %+v, want ok=false for a non-active hierarchy", reply)
			}
		})
	}
}
