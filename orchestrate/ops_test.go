package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/worktree"
)

// gitInitAt creates dir (and any parents) and initializes a git repository there
// on branch with one commit, so a handler that reads the repo's current branch
// (handleRepoCreate via worktree.CurrentBranch) has a real repo to inspect. The
// global/system git config is neutralized so a developer's global core.hooksPath
// can never abort the helper's commit.
func gitInitAt(t *testing.T, dir, branch string) {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	run := func(args ...string) {
		cmd := exec.CommandContext(context.Background(), "git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-b", branch)
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "cc-orchestrate test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "README.md")
	run("-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "init")
}

// gitRepo returns a fresh temp git repository on branch.
func gitRepo(t *testing.T, branch string) string {
	t.Helper()
	dir := t.TempDir()
	gitInitAt(t, dir, branch)
	return dir
}

// opBackend is a registered test backend that records the CreateWorkstream and Kill
// calls it receives, so the repo/kill ops can be exercised without a live CLI.
// Its Name is outside backend.Precedence, so it never interferes with the default
// availability/selection ordering; ops address it by passing its name explicitly.
type opBackend struct {
	createdSpec       *backend.WorkstreamSpec
	killedAgent       *backend.AgentHandle
	killedWorkstream  *backend.WorkstreamHandle
	killedWorkstreams *[]backend.WorkstreamHandle
	killErr           error
}

func (opBackend) Name() backend.BackendName         { return "optest" }
func (opBackend) Available() bool                   { return true }
func (opBackend) EnsureReady(context.Context) error { return nil }
func (opBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}
func (b opBackend) CreateWorkstream(_ context.Context, spec backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	if b.createdSpec != nil {
		*b.createdSpec = spec
	}
	return backend.WorkstreamHandle{Backend: "optest", ID: "ws-" + spec.Name, Name: spec.Name, Cwd: spec.Cwd}, nil
}
func (opBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (opBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (b opBackend) Kill(_ context.Context, agent backend.AgentHandle) error {
	if b.killedAgent != nil {
		*b.killedAgent = agent
	}
	return b.killErr
}
func (b opBackend) KillWorkstream(_ context.Context, project backend.WorkstreamHandle) error {
	if b.killedWorkstream != nil {
		*b.killedWorkstream = project
	}
	if b.killedWorkstreams != nil {
		*b.killedWorkstreams = append(*b.killedWorkstreams, project)
	}
	return b.killErr
}
func (opBackend) Caps() backend.Caps { return backend.Caps{} }

// sendBackend is a registered test backend that advertises CanSendText and
// implements Sender, recording the native SendText call so the dispatcher's
// native path can be exercised without a live CLI. Its name is outside
// backend.Precedence.
type sendBackend struct {
	sentTo   *backend.AgentHandle
	sentText *string
}

func (sendBackend) Name() backend.BackendName         { return "sendtest" }
func (sendBackend) Available() bool                   { return true }
func (sendBackend) EnsureReady(context.Context) error { return nil }
func (sendBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}
func (sendBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}
func (sendBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (sendBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (sendBackend) Kill(context.Context, backend.AgentHandle) error                { return nil }
func (sendBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (sendBackend) Caps() backend.Caps                                             { return backend.Capabilities(backend.CanSendText) }
func (b sendBackend) SendText(_ context.Context, agent backend.AgentHandle, text string) error {
	if b.sentTo != nil {
		*b.sentTo = agent
	}
	if b.sentText != nil {
		*b.sentText = text
	}
	return nil
}

// TestResolveBackend covers the explicit, persisted-selection, and unknown-name
// branches; the no-selection backend.Select() fallback depends on which runtimes
// are installed, so it is exercised by the spawn-smoke and integration paths.
func TestResolveBackend(t *testing.T) {
	ctx := context.Background()
	backend.Register(opBackend{}) // registers the "optest" backend, outside Precedence

	t.Run("explicit known backend wins", func(t *testing.T) {
		hc := daemon.HandlerCtx{Ctx: ctx, DB: newTestDB(t)}
		b, name, err := resolveBackend(hc, "optest")
		if err != nil || name != "optest" || b == nil {
			t.Fatalf("resolveBackend(optest) = %v, %q, %v; want the optest backend", b, name, err)
		}
	})
	t.Run("explicit unknown backend errors", func(t *testing.T) {
		hc := daemon.HandlerCtx{Ctx: ctx, DB: newTestDB(t)}
		if _, _, err := resolveBackend(hc, "ghost"); err == nil {
			t.Fatal("resolveBackend(ghost) err = nil, want an unknown-backend error")
		}
	})
	t.Run("falls back to the persisted selection", func(t *testing.T) {
		db := newTestDB(t)
		if err := setConfig(ctx, db, "backend", "optest"); err != nil {
			t.Fatal(err)
		}
		b, name, err := resolveBackend(daemon.HandlerCtx{Ctx: ctx, DB: db}, "")
		if err != nil || name != "optest" || b == nil {
			t.Fatalf("resolveBackend(persisted) = %v, %q, %v; want the optest backend", b, name, err)
		}
	})
	t.Run("persisted selection that is unknown errors", func(t *testing.T) {
		db := newTestDB(t)
		if err := setConfig(ctx, db, "backend", "ghost"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := resolveBackend(daemon.HandlerCtx{Ctx: ctx, DB: db}, ""); err == nil {
			t.Fatal("resolveBackend(persisted ghost) err = nil, want an unknown-backend error")
		}
	})
}

func mustInsertAgent(t *testing.T, db *sql.DB, a agentRow) {
	t.Helper()
	if err := insertAgent(context.Background(), db, a); err != nil {
		t.Fatalf("insertAgent %s: %v", a.ID, err)
	}
}

func opCtx(db *sql.DB, body []byte, appendFn daemon.AppendFunc) daemon.HandlerCtx {
	return daemon.HandlerCtx{Ctx: context.Background(), Env: daemon.Envelope{Body: body}, DB: db, Append: appendFn}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return b
}

// TestHandleSendMessage covers the LCD path: a backend without CanSendText
// (optest) delivers by appending an OriginHuman EventMessage to the subject log.
func TestHandleSendMessage(t *testing.T) {
	backend.Register(opBackend{}) // "optest": no Sender, no CanSendText → LCD
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "optest", Scope: "/s",
		SubjectID: "subj-1", Status: "active", State: StateUnknown, CreatedAt: "t0",
	})

	var captured *event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		captured = e
		return 7, nil
	}
	body := mustJSON(t, map[string]string{"agent_id": "a1", "text": "hello"})
	reply := handleSendMessage(opCtx(db, body, appendFn))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	if captured == nil {
		t.Fatal("Append was not called")
	}
	if captured.SubjectID != "subj-1" {
		t.Errorf("SubjectID = %q, want subj-1", captured.SubjectID)
	}
	if captured.Type != EventMessage {
		t.Errorf("Type = %q, want %q", captured.Type, EventMessage)
	}
	if captured.Origin != event.OriginHuman {
		t.Errorf("Origin = %q, want %q", captured.Origin, event.OriginHuman)
	}
	var pl struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(captured.Payload, &pl); err != nil || pl.Type != EventMessage || pl.Text != "hello" {
		t.Errorf("payload = %s, want type=%s text=hello (err %v)", captured.Payload, EventMessage, err)
	}
	var rb struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil || rb.Seq != 7 {
		t.Errorf("reply body = %s, want seq=7 (err %v)", reply.Body, err)
	}
}

// TestHandleSendMessageNative covers the native path: a Sender advertising
// CanSendText delivers by typing into the agent's terminal, writes no event-plane
// frame, and reports transport=native. The handle carries the workstream's backend
// WorkspaceHandle, not the orchestrate workstream id.
func TestHandleSendMessageNative(t *testing.T) {
	ctx := context.Background()
	var sentTo backend.AgentHandle
	var sentText string
	backend.Register(sendBackend{sentTo: &sentTo, sentText: &sentText})

	db := newTestDB(t)
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "sendtest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}
	mustInsertAgent(t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "sendtest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", Name: "worker", SubjectID: "subj-1",
		Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
	})

	appendFn := func(context.Context, *event.Event) (int64, error) {
		t.Fatal("native send must not append to the event plane")
		return 0, nil
	}
	body := mustJSON(t, map[string]string{"agent_id": "a1", "text": "hello"})
	reply := handleSendMessage(opCtx(db, body, appendFn))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}

	wantHandle := backend.AgentHandle{Backend: "sendtest", ID: "term-1", WorkstreamID: "ws-1", Name: "worker", SessionID: "sess-1"}
	if sentTo != wantHandle {
		t.Fatalf("SendText handle = %+v, want %+v", sentTo, wantHandle)
	}
	if sentText != "hello" {
		t.Fatalf("SendText text = %q, want hello", sentText)
	}

	var rb struct {
		Seq       int64  `json:"seq"`
		Transport string `json:"transport"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil {
		t.Fatal(err)
	}
	if rb.Transport != "native" || rb.Seq != 0 {
		t.Fatalf("reply = %+v, want transport=native seq=0", rb)
	}
}

// TestHandleSendMessageMultilineUsesLCD proves a multi-line message is delivered
// over the event plane even when the backend can send natively: typing it would
// submit each line as its own turn.
func TestHandleSendMessageMultilineUsesLCD(t *testing.T) {
	ctx := context.Background()
	var sentText string
	backend.Register(sendBackend{sentText: &sentText})

	db := newTestDB(t)
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "sendtest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	mustInsertAgent(t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "sendtest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", SubjectID: "subj-1",
		Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
	})

	var captured *event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		captured = e
		return 3, nil
	}
	body := mustJSON(t, map[string]string{"agent_id": "a1", "text": "line one\nline two"})
	reply := handleSendMessage(opCtx(db, body, appendFn))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	if sentText != "" {
		t.Fatalf("SendText was called with %q; multi-line must not go native", sentText)
	}
	if captured == nil || captured.Type != EventMessage {
		t.Fatalf("multi-line message was not appended as an EventMessage: %+v", captured)
	}
	var rb struct {
		Transport string `json:"transport"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil || rb.Transport != "event" {
		t.Fatalf("reply = %s, want transport=event (err %v)", reply.Body, err)
	}
}

func TestHandleSendMessageErrors(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "nosub", SprintID: "s1", Backend: "tmux", Scope: "/s",
		Status: "active", State: StateUnknown, CreatedAt: "t0",
	})
	appendFn := func(context.Context, *event.Event) (int64, error) {
		t.Fatal("Append must not be called when the op errors")
		return 0, nil
	}
	cases := []struct {
		name string
		body map[string]string
	}{
		{name: "missing agent", body: map[string]string{"agent_id": "ghost", "text": "x"}},
		{name: "agent without subject", body: map[string]string{"agent_id": "nosub", "text": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reply := handleSendMessage(opCtx(db, mustJSON(t, tc.body), appendFn))
			if reply.OK || reply.Error == "" {
				t.Fatalf("reply = %+v, want ok=false with an error", reply)
			}
		})
	}
}

// newFullDB opens a real ephemeral sqlite database with cc-interact's core schema
// (subjects/events) plus the orchestrate schema, so the subject resolver has a
// real table to write — newTestDB applies only the orchestrate tables.
func newFullDB(t *testing.T) *sql.DB {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), migrate)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st.DB()
}

func TestHandleReport(t *testing.T) {
	ctx := context.Background()
	db := newFullDB(t)
	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}

	// Create the child's subject keyed by its session id and scope, exactly as a
	// spawn would. handleReport must resolve it from session + scope alone.
	sub, _, err := subjects.Start(ctx, subject.Window{Session: "child-sid"}, "/work", "agent-child-sid", lifecycle, true)
	if err != nil {
		t.Fatalf("Start subject: %v", err)
	}

	var captured *event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		captured = e
		return 5, nil
	}
	hc := daemon.HandlerCtx{
		Ctx:      ctx,
		Env:      daemon.Envelope{Session: "child-sid", Body: mustJSON(t, map[string]string{"text": "halfway done", "state": "working"})},
		Window:   subject.Window{Session: "child-sid"},
		Scope:    "/work",
		Subjects: subjects, DB: db, Append: appendFn,
	}

	reply := handleReport(hc)
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	if captured == nil {
		t.Fatal("Append was not called")
	}
	if captured.SubjectID != sub.ID {
		t.Errorf("SubjectID = %q, want %q", captured.SubjectID, sub.ID)
	}
	if captured.Type != EventReport {
		t.Errorf("Type = %q, want %q", captured.Type, EventReport)
	}
	if captured.Origin != event.OriginAgent {
		t.Errorf("Origin = %q, want %q", captured.Origin, event.OriginAgent)
	}
	var pl reportPayload
	if err := json.Unmarshal(captured.Payload, &pl); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if pl.Type != EventReport || pl.Text != "halfway done" || pl.State != "working" {
		t.Errorf("payload = %+v, want type=%s text=halfway done state=working", pl, EventReport)
	}
	var rb struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil || rb.Seq != 5 {
		t.Errorf("reply body = %s, want seq=5 (err %v)", reply.Body, err)
	}
}

func TestHandleReportNoSubject(t *testing.T) {
	ctx := context.Background()
	db := newFullDB(t)
	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}
	appendFn := func(context.Context, *event.Event) (int64, error) {
		t.Fatal("Append must not be called without a subject")
		return 0, nil
	}
	hc := daemon.HandlerCtx{
		Ctx:      ctx,
		Env:      daemon.Envelope{Session: "ghost", Body: mustJSON(t, map[string]string{"text": "x"})},
		Window:   subject.Window{Session: "ghost"},
		Scope:    "/nowhere",
		Subjects: subjects, DB: db, Append: appendFn,
	}
	reply := handleReport(hc)
	if reply.OK || reply.Error == "" {
		t.Fatalf("reply = %+v, want ok=false with an error", reply)
	}
}

func TestHandleStatus(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/s", SessionID: "sess-1",
		Name: "worker", SubjectID: "subj-1", Status: "active", State: StateWorking,
		Activity: "Bash: ls", Tokens: 10, UpdatedAt: "2026-06-16T00:00:00Z", CreatedAt: "t0",
	})

	reply := handleStatus(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var got agentView
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	want := agentView{
		ID: "a1", Name: "worker", SprintID: "s1", Backend: "tmux", Status: "active",
		State: string(StateWorking), Activity: "Bash: ls", Tokens: 10,
		UpdatedAt: "2026-06-16T00:00:00Z", SessionID: "sess-1", Scope: "/s",
	}
	if got != want {
		t.Fatalf("status view = %+v, want %+v", got, want)
	}
}

func TestHandleStatusMissing(t *testing.T) {
	db := newTestDB(t)
	reply := handleStatus(opCtx(db, mustJSON(t, map[string]string{"agent_id": "ghost"}), nil))
	if reply.OK || reply.Error == "" {
		t.Fatalf("reply = %+v, want ok=false with an error", reply)
	}
}

func TestHandleList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for _, p := range []repoRow{
		{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "tmux", Cwd: "/tmp/b", Status: "active", CreatedAt: "t1"},
	} {
		if err := insertRepo(ctx, db, p); err != nil {
			t.Fatalf("insertRepo %s: %v", p.ID, err)
		}
	}
	for _, w := range []workstreamRow{
		{ID: "w1", RepoID: "p1", Name: "main", Backend: "tmux", Branch: "main", Worktree: "/tmp/a", IsPrimary: true, Status: "active", CreatedAt: "t0"},
		{ID: "w2", RepoID: "p2", Name: "main", Backend: "tmux", Branch: "main", Worktree: "/tmp/b", IsPrimary: true, Status: "active", CreatedAt: "t1"},
	} {
		if err := insertWorkstream(ctx, db, w); err != nil {
			t.Fatalf("insertWorkstream %s: %v", w.ID, err)
		}
	}
	for _, sp := range []sprintRow{
		{ID: "s1", WorkstreamID: "w1", Name: "main", Status: "active", CreatedAt: "t0"},
		{ID: "s2", WorkstreamID: "w2", Name: "main", Status: "active", CreatedAt: "t1"},
	} {
		if err := insertSprint(ctx, db, sp); err != nil {
			t.Fatalf("insertSprint %s: %v", sp.ID, err)
		}
	}
	mustInsertAgent(t, db, agentRow{ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/s", Status: "active", State: StateWorking, CreatedAt: "t0"})
	mustInsertAgent(t, db, agentRow{ID: "a2", SprintID: "s2", Backend: "tmux", Scope: "/s", Status: "active", State: StateIdle, CreatedAt: "t1"})

	t.Run("all with absent body", func(t *testing.T) {
		reply := handleList(opCtx(db, nil, nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})
	t.Run("filtered by repo id", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"repo": "p2"}), nil))
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a2" {
			t.Fatalf("filtered = %+v, want [a2]", got)
		}
	})
	t.Run("filtered by repo name resolves to its id", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"repo": "beta"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a2" {
			t.Fatalf("name-filtered = %+v, want [a2]", got)
		}
	})
	t.Run("unknown repo is an error", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"repo": "ghost"}), nil))
		if reply.OK || reply.Error == "" {
			t.Fatalf("reply = %+v, want ok=false for an unknown repo", reply)
		}
	})
}

func TestHandleConfigGetSet(t *testing.T) {
	db := newTestDB(t)
	getBody := mustJSON(t, map[string]string{"key": "backend"})

	var got struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	reply := handleConfigGet(opCtx(db, getBody, nil))
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("found before set = true, want false")
	}

	if reply := handleConfigSet(opCtx(db, mustJSON(t, map[string]string{"key": "backend", "value": "superset"}), nil)); !reply.OK {
		t.Fatalf("config-set not ok: %s", reply.Error)
	}

	reply = handleConfigGet(opCtx(db, getBody, nil))
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Value != "superset" {
		t.Fatalf("after set = %+v, want value=superset found=true", got)
	}
}

func TestHandleRepoCreate(t *testing.T) {
	db := newTestDB(t)

	var gotSpec backend.WorkstreamSpec
	backend.Register(opBackend{createdSpec: &gotSpec})

	cwd := gitRepo(t, "feature/login")
	body := mustJSON(t, map[string]string{"name": "demo", "backend": "optest", "cwd": cwd})
	reply := handleRepoCreate(opCtx(db, body, nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var out struct {
		RepoID    string `json:"repo_id"`
		Workspace string `json:"workspace"`
		Backend   string `json:"backend"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatal(err)
	}
	if out.RepoID == "" || out.Workspace != "ws-feature/login" || out.Backend != "optest" {
		t.Fatalf("reply = %+v, want non-empty id, workspace ws-feature/login, backend optest", out)
	}

	// The backend received the primary workstream spec: the repo root as both cwd
	// and the repo it derives from, on the repo's current branch (the workstream
	// name is the branch, never a git worktree add).
	if gotSpec.Name != "feature/login" || gotSpec.Cwd != cwd || gotSpec.RepoCwd != cwd || gotSpec.Branch != "feature/login" {
		t.Fatalf("CreateWorkstream spec = %+v, want {Name:feature/login Cwd:%s RepoCwd:%s Branch:feature/login}", gotSpec, cwd, cwd)
	}

	// The repo row persisted with no backend workspace handle — that now lives on
	// the workstream.
	p, err := getRepo(context.Background(), db, out.RepoID)
	if err != nil {
		t.Fatalf("getRepo: %v", err)
	}
	wantRepo := repoRow{
		ID: out.RepoID, Name: "demo", Backend: "optest",
		Cwd: cwd, Status: "active", CreatedAt: p.CreatedAt,
	}
	if p != wantRepo {
		t.Fatalf("repo row = %+v, want %+v", p, wantRepo)
	}
	if p.CreatedAt == "" {
		t.Error("created_at not stamped")
	}

	// A primary workstream was auto-created: the repo's own checkout (worktree is
	// the repo root, never a git worktree add) on the current branch, backed by the
	// backend workspace handle.
	ws, err := getPrimaryWorkstream(context.Background(), db, out.RepoID)
	if err != nil {
		t.Fatalf("getPrimaryWorkstream: %v", err)
	}
	if !ws.IsPrimary || ws.Worktree != cwd || ws.Branch != "feature/login" ||
		ws.Name != "feature/login" || ws.WorkspaceHandle != "ws-feature/login" || ws.Backend != "optest" {
		t.Fatalf("primary workstream = %+v, want is_primary, worktree=%s, branch/name=feature/login, workspace=ws-feature/login", ws, cwd)
	}
}

// TestHandleRepoCreateResolvesCwdAgainstScope proves an empty or relative cwd
// resolves against the caller's scope (the CLI/MCP working directory carried on
// the envelope), not the long-lived daemon's process cwd. The resolved cwd must be
// a real repo because handleRepoCreate now reads its current branch.
func TestHandleRepoCreateResolvesCwdAgainstScope(t *testing.T) {
	runRepoCreate := func(t *testing.T, cwd, scope string) backend.WorkstreamSpec {
		t.Helper()
		db := newTestDB(t)
		var gotSpec backend.WorkstreamSpec
		backend.Register(opBackend{createdSpec: &gotSpec})
		body := mustJSON(t, map[string]string{"name": "demo", "backend": "optest", "cwd": cwd})
		hc := daemon.HandlerCtx{Ctx: context.Background(), Env: daemon.Envelope{Body: body}, Scope: scope, DB: db}
		reply := handleRepoCreate(hc)
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		return gotSpec
	}

	t.Run("empty cwd uses caller scope", func(t *testing.T) {
		scope := gitRepo(t, "main")
		if got := runRepoCreate(t, "", scope).Cwd; got != scope {
			t.Fatalf("CreateWorkstream cwd = %q, want %q", got, scope)
		}
	})
	t.Run("relative cwd joins caller scope", func(t *testing.T) {
		scope := t.TempDir()
		want := filepath.Join(scope, "sub", "dir")
		gitInitAt(t, want, "main")
		if got := runRepoCreate(t, "sub/dir", scope).Cwd; got != want {
			t.Fatalf("CreateWorkstream cwd = %q, want %q", got, want)
		}
	})
	t.Run("absolute cwd is kept", func(t *testing.T) {
		repo := gitRepo(t, "main")
		if got := runRepoCreate(t, repo, "/caller/here").Cwd; got != repo {
			t.Fatalf("CreateWorkstream cwd = %q, want %q", got, repo)
		}
	})
}

func TestHandleRepoList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for _, p := range []repoRow{
		{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "superset", Cwd: "/tmp/b", Status: "archived", CreatedAt: "t1"},
	} {
		if err := insertRepo(ctx, db, p); err != nil {
			t.Fatalf("insertRepo %s: %v", p.ID, err)
		}
	}

	reply := handleRepoList(opCtx(db, nil, nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var got []repoView
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	want := []repoView{
		{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "superset", Cwd: "/tmp/b", Status: "archived", CreatedAt: "t1"},
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("view[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestHandleRepoActivate(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: "archived", CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}

	t.Run("activates by id", func(t *testing.T) {
		reply := handleRepoActivate(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		p, err := getRepo(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if p.Status != "active" {
			t.Fatalf("status = %q, want active", p.Status)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		reply := handleRepoActivate(opCtx(db, mustJSON(t, map[string]string{"id": "ghost"}), nil))
		if reply.OK || reply.Error == "" {
			t.Fatalf("reply = %+v, want ok=false with an error", reply)
		}
	})
}

func TestHandleAgentKill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

	db := newTestDB(t)
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}
	mustInsertAgent(t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "optest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", Name: "worker", SubjectID: "subj-1",
		Status: "active", State: StateWorking, CreatedAt: "t0",
	})

	t.Run("kills and marks exited", func(t *testing.T) {
		var killed backend.AgentHandle
		backend.Register(opBackend{killedAgent: &killed})

		var captured *event.Event
		appendFn := func(_ context.Context, e *event.Event) (int64, error) {
			captured = e
			return 9, nil
		}
		reply := handleAgentKill(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}

		// The backend received the agent handle assembled from the row, with
		// WorkstreamID carrying the workstream's backend WorkspaceHandle (ws-1), not
		// the orchestrate workstream id (w1).
		want := backend.AgentHandle{
			Backend: "optest", ID: "term-1", WorkstreamID: "ws-1", Name: "worker", SessionID: "sess-1",
		}
		if killed != want {
			t.Fatalf("kill handle = %+v, want %+v", killed, want)
		}

		// The row is now exited.
		ag, err := getAgent(ctx, db, "a1")
		if err != nil {
			t.Fatal(err)
		}
		if ag.Status != "exited" {
			t.Fatalf("status = %q, want exited", ag.Status)
		}

		// A terminal EventExited was appended on the subject log.
		if captured == nil {
			t.Fatal("Append was not called")
		}
		if captured.SubjectID != "subj-1" || captured.Type != EventExited || captured.Origin != event.OriginSystem {
			t.Fatalf("event = %+v, want subj-1/%s/system", captured, EventExited)
		}
		var pl struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(captured.Payload, &pl); err != nil || pl.Type != EventExited {
			t.Errorf("exited payload = %s, want type=%s (err %v)", captured.Payload, EventExited, err)
		}
	})

	t.Run("kill error is surfaced but the row is still exited", func(t *testing.T) {
		mustInsertAgent(t, db, agentRow{
			ID: "a2", SprintID: "s1", Backend: "optest", TerminalHandle: "term-2",
			SessionID: "sess-2", Scope: "/s", SubjectID: "subj-2",
			Status: "active", State: StateWorking, CreatedAt: "t1",
		})
		backend.Register(opBackend{killErr: errors.New("tmux: no such window")})
		appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }

		reply := handleAgentKill(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a2"}), appendFn))
		if reply.OK || reply.Error == "" {
			t.Fatalf("reply = %+v, want ok=false with the kill error", reply)
		}
		ag, err := getAgent(ctx, db, "a2")
		if err != nil {
			t.Fatal(err)
		}
		if ag.Status != "exited" {
			t.Fatalf("status = %q, want exited even after kill error", ag.Status)
		}
	})

	t.Run("already-exited agent is an idempotent no-op", func(t *testing.T) {
		mustInsertAgent(t, db, agentRow{
			ID: "a3", SprintID: "s1", Backend: "optest", TerminalHandle: "term-3",
			SessionID: "sess-3", Scope: "/s", SubjectID: "subj-3",
			Status: StatusExited, State: StateIdle, CreatedAt: "t2",
		})
		var killed backend.AgentHandle
		backend.Register(opBackend{killedAgent: &killed})
		appendFn := func(context.Context, *event.Event) (int64, error) {
			t.Fatal("Append must not be called for an already-exited agent")
			return 0, nil
		}
		reply := handleAgentKill(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a3"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		if (killed != backend.AgentHandle{}) {
			t.Fatalf("bk.Kill was called for an already-exited agent: %+v", killed)
		}
	})
}

// TestHandleRepoKill proves repo-kill is a real cascade: it tears down every
// workstream's backend workspace, removes each non-primary worktree (the leak it
// used to leave), cascades the workstreams' sprints to killed and their agents to
// exited, and marks the repo killed.
func TestHandleRepoKill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

	repo := gitRepo(t, "main")
	wtPath, err := worktree.Add(ctx, repo, filepath.Join(t.TempDir(), "wt"), "feat-x")
	if err != nil {
		t.Fatalf("worktree.Add: %v", err)
	}

	db := newTestDB(t)
	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "proj", Backend: "optest", Cwd: repo, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	// The primary workstream is the repo root; a non-primary workstream owns a real
	// worktree that repo-kill must remove.
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: repo, IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream w1: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w2", RepoID: "p1", Name: "feat-x", Backend: "optest", WorkspaceHandle: "ws-2",
		Branch: "feat-x", Worktree: wtPath, IsPrimary: false, Status: StatusActive, CreatedAt: "t1",
	}); err != nil {
		t.Fatalf("insertWorkstream w2: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{
		ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}
	mustInsertAgent(t, db, agentRow{ID: "a1", SprintID: "s1", Backend: "optest", SubjectID: "subj-1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"})
	mustInsertAgent(t, db, agentRow{ID: "a2", SprintID: "s1", Backend: "optest", SubjectID: "subj-2", Status: StatusActive, State: StateIdle, CreatedAt: "t1"})
	mustInsertAgent(t, db, agentRow{ID: "a3", SprintID: "s1", Backend: "optest", SubjectID: "subj-3", Status: StatusExited, State: StateIdle, CreatedAt: "t2"})

	var killed []backend.WorkstreamHandle
	backend.Register(opBackend{killedWorkstreams: &killed})

	var exited []string
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		if e.Type == EventExited {
			exited = append(exited, e.SubjectID)
		}
		return 1, nil
	}
	reply := handleRepoKill(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), appendFn))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}

	// Every workstream's backend workspace — primary and non-primary — is torn down.
	killedIDs := map[string]bool{}
	for _, h := range killed {
		killedIDs[h.ID] = true
	}
	if len(killed) != 2 || !killedIDs["ws-1"] || !killedIDs["ws-2"] {
		t.Fatalf("KillWorkstream handles = %+v, want ws-1 and ws-2", killed)
	}
	// The non-primary worktree is gone from disk — the leak repo-kill used to leave.
	if _, err := os.Stat(wtPath); !os.IsNotExist(err) {
		t.Fatalf("non-primary worktree still present after repo-kill: stat err = %v", err)
	}

	proj, err := getRepo(ctx, db, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if proj.Status != StatusKilled {
		t.Fatalf("repo status = %q, want killed", proj.Status)
	}
	for _, id := range []string{"w1", "w2"} {
		if ws, err := getWorkstream(ctx, db, id, ""); err != nil {
			t.Fatal(err)
		} else if ws.Status != StatusKilled {
			t.Fatalf("workstream %s status = %q, want killed", id, ws.Status)
		}
	}
	if sp, err := getSprint(ctx, db, "s1", ""); err != nil {
		t.Fatal(err)
	} else if sp.Status != StatusKilled {
		t.Fatalf("sprint status = %q, want killed", sp.Status)
	}
	for _, id := range []string{"a1", "a2"} {
		ag, err := getAgent(ctx, db, id)
		if err != nil {
			t.Fatal(err)
		}
		if ag.Status != StatusExited {
			t.Fatalf("agent %s status = %q, want exited", id, ag.Status)
		}
	}

	// Exactly the two active agents got an EventExited; the pre-exited a3 did not.
	got := map[string]bool{}
	for _, s := range exited {
		got[s] = true
	}
	if len(exited) != 2 || !got["subj-1"] || !got["subj-2"] || got["subj-3"] {
		t.Fatalf("EventExited subjects = %v, want exactly subj-1 and subj-2", exited)
	}
}

// workstreamBackend is a registered test backend whose ManagesWorktree capability
// is toggleable, recording the CreateWorkstream spec and (when it manages its own
// worktree) reporting a fixed forked path, so handleWorkstreamCreate's create- and
// adopt-worktree branches can be exercised without a live CLI.
type workstreamBackend struct {
	createdSpec *backend.WorkstreamSpec
	manages     bool
	worktree    string
}

func (workstreamBackend) Name() backend.BackendName         { return "wstest" }
func (workstreamBackend) Available() bool                   { return true }
func (workstreamBackend) EnsureReady(context.Context) error { return nil }
func (workstreamBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}
func (b workstreamBackend) CreateWorkstream(_ context.Context, spec backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	if b.createdSpec != nil {
		*b.createdSpec = spec
	}
	return backend.WorkstreamHandle{Backend: "wstest", ID: "ws-" + spec.Name, Name: spec.Name, Cwd: spec.Cwd, Worktree: b.worktree}, nil
}
func (workstreamBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (workstreamBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (workstreamBackend) Kill(context.Context, backend.AgentHandle) error                { return nil }
func (workstreamBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (b workstreamBackend) Caps() backend.Caps {
	if b.manages {
		return backend.Capabilities(backend.ManagesWorktree)
	}
	return backend.Caps{}
}

// gitWorktreeCount returns how many worktrees git reports for the repository at
// repoRoot (one for the repo's own checkout, plus one per added worktree).
func gitWorktreeCount(t *testing.T, repoRoot string) int {
	t.Helper()
	c := exec.CommandContext(context.Background(), "git", "-C", repoRoot, "worktree", "list", "--porcelain")
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git worktree list: %v\n%s", err, out)
	}
	n := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "worktree ") {
			n++
		}
	}
	return n
}

// TestHandleWorkstreamCreate covers the two worktree-ownership paths against a real
// ephemeral git repo and a stub backend.
func TestHandleWorkstreamCreate(t *testing.T) {
	t.Run("non-ManagesWorktree backend gets a git worktree we create", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir()) // worktreesBase resolves under the temp home
		repo := gitRepo(t, "main")
		db := newTestDB(t)
		if err := insertRepo(context.Background(), db, repoRow{
			ID: "p1", Name: "demo", Backend: "wstest", Cwd: repo, Status: StatusActive, CreatedAt: "t0",
		}); err != nil {
			t.Fatalf("insertRepo: %v", err)
		}

		var gotSpec backend.WorkstreamSpec
		backend.Register(workstreamBackend{createdSpec: &gotSpec, manages: false})

		body := mustJSON(t, map[string]string{"repo": "p1", "name": "feat-x"})
		reply := handleWorkstreamCreate(opCtx(db, body, nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var out struct {
			WorkstreamID string `json:"workstream_id"`
			Worktree     string `json:"worktree"`
			Workspace    string `json:"workspace"`
			Branch       string `json:"branch"`
		}
		if err := json.Unmarshal(reply.Body, &out); err != nil {
			t.Fatal(err)
		}

		// A real worktree dir was created under the worktrees base on the requested
		// branch, off the repo's HEAD.
		wantDest := filepath.Join(worktreesBase(), "p1", "feat-x")
		if out.Worktree != wantDest {
			t.Fatalf("worktree = %q, want %q", out.Worktree, wantDest)
		}
		if _, err := os.Stat(out.Worktree); err != nil {
			t.Fatalf("worktree dir %s: %v", out.Worktree, err)
		}
		real, err := filepath.EvalSymlinks(out.Worktree)
		if err != nil {
			t.Fatalf("eval symlinks: %v", err)
		}
		listed := func() string {
			c := exec.CommandContext(context.Background(), "git", "-C", repo, "worktree", "list", "--porcelain")
			b, err := c.CombinedOutput()
			if err != nil {
				t.Fatalf("git worktree list: %v\n%s", err, b)
			}
			return string(b)
		}()
		if !strings.Contains(listed, real) && !strings.Contains(listed, out.Worktree) {
			t.Fatalf("worktree %s not listed:\n%s", out.Worktree, listed)
		}
		if !strings.Contains(listed, "refs/heads/feat-x") {
			t.Fatalf("branch feat-x not listed:\n%s", listed)
		}

		// The backend was handed the worktree as its cwd, the repo root it derives
		// from, and the branch.
		if gotSpec.Name != "feat-x" || gotSpec.Cwd != out.Worktree || gotSpec.RepoCwd != repo || gotSpec.Branch != "feat-x" {
			t.Fatalf("CreateWorkstream spec = %+v, want {Name:feat-x Cwd:%s RepoCwd:%s Branch:feat-x}", gotSpec, out.Worktree, repo)
		}

		// The worktree path and the backend workspace handle are recorded on a
		// non-primary workstream row.
		ws, err := getWorkstream(context.Background(), db, out.WorkstreamID, "")
		if err != nil {
			t.Fatalf("getWorkstream: %v", err)
		}
		if ws.Worktree != out.Worktree || ws.WorkspaceHandle != "ws-feat-x" || ws.Branch != "feat-x" || ws.IsPrimary {
			t.Fatalf("workstream row = %+v, want worktree=%s workspace=ws-feat-x branch=feat-x non-primary", ws, out.Worktree)
		}

		// The new workstream auto-created its default sprint, active.
		sp, err := getDefaultSprint(context.Background(), db, out.WorkstreamID)
		if err != nil {
			t.Fatalf("getDefaultSprint: %v", err)
		}
		if sp.Name != defaultSprintName || sp.Status != StatusActive {
			t.Fatalf("default sprint = %+v, want name %q active", sp, defaultSprintName)
		}
	})

	t.Run("ManagesWorktree backend forks its own; no git worktree add", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		repo := gitRepo(t, "main")
		db := newTestDB(t)
		if err := insertRepo(context.Background(), db, repoRow{
			ID: "p1", Name: "demo", Backend: "wstest", Cwd: repo, Status: StatusActive, CreatedAt: "t0",
		}); err != nil {
			t.Fatalf("insertRepo: %v", err)
		}

		forked := t.TempDir() // the path the backend forked and now owns
		var gotSpec backend.WorkstreamSpec
		backend.Register(workstreamBackend{createdSpec: &gotSpec, manages: true, worktree: forked})

		if before := gitWorktreeCount(t, repo); before != 1 {
			t.Fatalf("git worktree count before = %d, want 1 (just the repo checkout)", before)
		}

		body := mustJSON(t, map[string]string{"repo": "p1", "name": "feat-y"})
		reply := handleWorkstreamCreate(opCtx(db, body, nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var out struct {
			WorkstreamID string `json:"workstream_id"`
			Worktree     string `json:"worktree"`
		}
		if err := json.Unmarshal(reply.Body, &out); err != nil {
			t.Fatal(err)
		}

		// No git worktree add ran: the backend forks its own, and cc-orchestrate
		// adopts the path it returned.
		if after := gitWorktreeCount(t, repo); after != 1 {
			t.Fatalf("git worktree count after = %d, want 1 (backend manages its own worktree)", after)
		}
		if out.Worktree != forked {
			t.Fatalf("adopted worktree = %q, want the backend's forked path %q", out.Worktree, forked)
		}
		// The backend was pointed at the repo root, not a path we created.
		if gotSpec.Cwd != repo || gotSpec.RepoCwd != repo || gotSpec.Branch != "feat-y" {
			t.Fatalf("CreateWorkstream spec = %+v, want Cwd=RepoCwd=%s Branch=feat-y", gotSpec, repo)
		}
		ws, err := getWorkstream(context.Background(), db, out.WorkstreamID, "")
		if err != nil {
			t.Fatalf("getWorkstream: %v", err)
		}
		if ws.Worktree != forked || ws.IsPrimary {
			t.Fatalf("workstream row = %+v, want worktree=%s non-primary", ws, forked)
		}

		// The new workstream auto-created its default sprint, active.
		sp, err := getDefaultSprint(context.Background(), db, out.WorkstreamID)
		if err != nil {
			t.Fatalf("getDefaultSprint: %v", err)
		}
		if sp.Name != defaultSprintName || sp.Status != StatusActive {
			t.Fatalf("default sprint = %+v, want name %q active", sp, defaultSprintName)
		}
	})
}

// TestHandleWorkstreamKill proves the real teardown: a non-primary workstream's
// backend workspace is torn down and its git worktree removed, while a primary
// workstream's worktree (the repo root) is never removed; both cascade their agents
// to exited.
func TestHandleWorkstreamKill(t *testing.T) {
	t.Run("non-primary tears down the backend and removes its worktree", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)

		repo := gitRepo(t, "main")
		wtPath, err := worktree.Add(ctx, repo, filepath.Join(t.TempDir(), "feat"), "feat")
		if err != nil {
			t.Fatalf("worktree.Add: %v", err)
		}

		db := newTestDB(t)
		if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "demo", Backend: "optest", Cwd: repo, Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertRepo: %v", err)
		}
		if err := insertWorkstream(ctx, db, workstreamRow{
			ID: "w1", RepoID: "p1", Name: "feat", Backend: "optest", WorkspaceHandle: "ws-1",
			Branch: "feat", Worktree: wtPath, IsPrimary: false, Status: StatusActive, CreatedAt: "t0",
		}); err != nil {
			t.Fatalf("insertWorkstream: %v", err)
		}
		if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertSprint: %v", err)
		}
		mustInsertAgent(t, db, agentRow{ID: "a1", SprintID: "s1", Backend: "optest", SubjectID: "subj-1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"})

		var killed backend.WorkstreamHandle
		backend.Register(opBackend{killedWorkstream: &killed})
		var exited []string
		appendFn := func(_ context.Context, e *event.Event) (int64, error) {
			if e.Type == EventExited {
				exited = append(exited, e.SubjectID)
			}
			return 1, nil
		}

		reply := handleWorkstreamKill(opCtx(db, mustJSON(t, map[string]string{"id": "w1"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}

		// The backend tore down the workstream's workspace handle (ws-1).
		want := backend.WorkstreamHandle{Backend: "optest", ID: "ws-1", Name: "feat", Cwd: wtPath, Worktree: wtPath}
		if killed != want {
			t.Fatalf("killed workstream = %+v, want %+v", killed, want)
		}
		// The git worktree was removed.
		if _, err := os.Stat(wtPath); !os.IsNotExist(err) {
			t.Fatalf("worktree dir present after kill: stat err = %v", err)
		}
		if got := gitWorktreeCount(t, repo); got != 1 {
			t.Fatalf("git worktree count = %d after kill, want 1", got)
		}
		// The workstream is killed and its agent exited.
		if ws, err := getWorkstream(ctx, db, "w1", ""); err != nil {
			t.Fatal(err)
		} else if ws.Status != StatusKilled {
			t.Fatalf("workstream status = %q, want killed", ws.Status)
		}
		assertSprintStatus(t, db, "s1", StatusKilled)
		assertAgentStatus(t, db, "a1", StatusExited)
		if len(exited) != 1 || exited[0] != "subj-1" {
			t.Fatalf("EventExited subjects = %v, want [subj-1]", exited)
		}
	})

	t.Run("primary never removes its worktree (the repo root)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)

		repo := gitRepo(t, "main")
		db := newTestDB(t)
		if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "demo", Backend: "optest", Cwd: repo, Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertRepo: %v", err)
		}
		if err := insertWorkstream(ctx, db, workstreamRow{
			ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
			Branch: "main", Worktree: repo, IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
		}); err != nil {
			t.Fatalf("insertWorkstream: %v", err)
		}
		if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertSprint: %v", err)
		}
		mustInsertAgent(t, db, agentRow{ID: "a1", SprintID: "s1", Backend: "optest", SubjectID: "subj-1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"})

		var killed backend.WorkstreamHandle
		backend.Register(opBackend{killedWorkstream: &killed})
		appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }

		reply := handleWorkstreamKill(opCtx(db, mustJSON(t, map[string]string{"id": "w1"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		// The backend was still torn down.
		if killed.ID != "ws-1" {
			t.Fatalf("killed workstream ID = %q, want ws-1", killed.ID)
		}
		// The repo root survived: never git worktree remove a primary.
		if _, err := os.Stat(repo); err != nil {
			t.Fatalf("repo root gone after primary kill: %v", err)
		}
		if got := gitWorktreeCount(t, repo); got != 1 {
			t.Fatalf("git worktree count = %d, want 1 (repo intact)", got)
		}
		if ws, err := getWorkstream(ctx, db, "w1", ""); err != nil {
			t.Fatal(err)
		} else if ws.Status != StatusKilled {
			t.Fatalf("workstream status = %q, want killed", ws.Status)
		}
		assertAgentStatus(t, db, "a1", StatusExited)
	})
}

// assertConfig asserts a config key's value and presence.
func assertConfig(t *testing.T, db *sql.DB, key, wantValue string, wantFound bool) {
	t.Helper()
	value, found, err := getConfig(context.Background(), db, key)
	if err != nil {
		t.Fatalf("getConfig %s: %v", key, err)
	}
	if found != wantFound || value != wantValue {
		t.Fatalf("config %s = (%q, found=%t), want (%q, found=%t)", key, value, found, wantValue, wantFound)
	}
}

// TestActivateResetsPrecedenceChain proves each activate resets the active-* config
// chain so the most recent activation wins and a stale higher-precedence selection
// can never silently misroute a bare spawn.
func TestActivateResetsPrecedenceChain(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertRepo: %v", err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "tmux", Branch: "main",
		Worktree: "/tmp/a", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertWorkstream: %v", err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatalf("insertSprint: %v", err)
	}

	// Seed a stale full chain pointing at unrelated ids before every case.
	seed := func() {
		for _, kv := range [][2]string{{configActiveRepo, "stale-repo"}, {configActiveWorkstream, "stale-ws"}, {configActiveSprint, "stale-sprint"}} {
			if err := setConfig(ctx, db, kv[0], kv[1]); err != nil {
				t.Fatalf("setConfig %s: %v", kv[0], err)
			}
		}
	}

	t.Run("repo-activate sets active-repo and clears workstream+sprint", func(t *testing.T) {
		seed()
		reply := handleRepoActivate(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		assertConfig(t, db, configActiveRepo, "p1", true)
		assertConfig(t, db, configActiveWorkstream, "", false)
		assertConfig(t, db, configActiveSprint, "", false)
	})

	t.Run("workstream-activate sets active-repo to its repo and clears the stale sprint", func(t *testing.T) {
		seed()
		reply := handleWorkstreamActivate(opCtx(db, mustJSON(t, map[string]string{"id": "w1"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		assertConfig(t, db, configActiveWorkstream, "w1", true)
		assertConfig(t, db, configActiveRepo, "p1", true)
		assertConfig(t, db, configActiveSprint, "", false)
	})

	t.Run("sprint-activate sets its workstream and repo", func(t *testing.T) {
		seed()
		reply := handleSprintActivate(opCtx(db, mustJSON(t, map[string]string{"id": "s1"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		assertConfig(t, db, configActiveSprint, "s1", true)
		assertConfig(t, db, configActiveWorkstream, "w1", true)
		assertConfig(t, db, configActiveRepo, "p1", true)
	})
}

// TestKillClearsActiveSelection proves a kill cascade drops any active-* config key
// pointing at the killed entity (mirroring the terminal-state guard in
// handleAgentKill), while a selection pointing elsewhere survives.
func TestKillClearsActiveSelection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)

	setup := func(t *testing.T) *sql.DB {
		t.Helper()
		db := newTestDB(t)
		if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "optest", Cwd: "/s", Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertRepo: %v", err)
		}
		if err := insertWorkstream(ctx, db, workstreamRow{
			ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
			Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
		}); err != nil {
			t.Fatalf("insertWorkstream: %v", err)
		}
		if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
			t.Fatalf("insertSprint: %v", err)
		}
		return db
	}
	appendFn := func(context.Context, *event.Event) (int64, error) { return 1, nil }

	t.Run("repo-kill clears the matching chain", func(t *testing.T) {
		db := setup(t)
		for _, kv := range [][2]string{{configActiveRepo, "p1"}, {configActiveWorkstream, "w1"}, {configActiveSprint, "s1"}} {
			if err := setConfig(ctx, db, kv[0], kv[1]); err != nil {
				t.Fatalf("setConfig: %v", err)
			}
		}
		reply := handleRepoKill(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		assertConfig(t, db, configActiveRepo, "", false)
		assertConfig(t, db, configActiveWorkstream, "", false)
		assertConfig(t, db, configActiveSprint, "", false)
	})

	t.Run("repo-kill preserves selections pointing elsewhere", func(t *testing.T) {
		db := setup(t)
		for _, kv := range [][2]string{{configActiveRepo, "other-repo"}, {configActiveWorkstream, "other-ws"}, {configActiveSprint, "other-sprint"}} {
			if err := setConfig(ctx, db, kv[0], kv[1]); err != nil {
				t.Fatalf("setConfig: %v", err)
			}
		}
		reply := handleRepoKill(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), appendFn))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		assertConfig(t, db, configActiveRepo, "other-repo", true)
		assertConfig(t, db, configActiveWorkstream, "other-ws", true)
		assertConfig(t, db, configActiveSprint, "other-sprint", true)
	})
}

// TestHandleRepoCreateManagesWorktreeAdoptsFork proves that for a ManagesWorktree
// backend (superset) the primary workstream adopts the backend's forked worktree
// rather than the repo root, keeping the workspace handle and worktree co-located.
func TestHandleRepoCreateManagesWorktreeAdoptsFork(t *testing.T) {
	db := newTestDB(t)
	repo := gitRepo(t, "main")
	forked := t.TempDir() // the path the backend forked and now owns
	backend.Register(workstreamBackend{manages: true, worktree: forked})

	body := mustJSON(t, map[string]string{"name": "demo", "backend": "wstest", "cwd": repo})
	reply := handleRepoCreate(opCtx(db, body, nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var out struct {
		RepoID string `json:"repo_id"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatal(err)
	}
	ws, err := getPrimaryWorkstream(context.Background(), db, out.RepoID)
	if err != nil {
		t.Fatalf("getPrimaryWorkstream: %v", err)
	}
	if !ws.IsPrimary || ws.Worktree != forked || ws.WorkspaceHandle != "ws-main" {
		t.Fatalf("primary workstream = %+v, want is_primary, worktree=%s (the fork), workspace=ws-main", ws, forked)
	}
}
