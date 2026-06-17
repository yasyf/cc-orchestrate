package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
)

// opBackend is a registered test backend that records the CreateProject and Kill
// calls it receives, so the project/kill ops can be exercised without a live CLI.
// Its Name is outside backend.Precedence, so it never interferes with the default
// availability/selection ordering; ops address it by passing its name explicitly.
type opBackend struct {
	createdSpec *backend.ProjectSpec
	killedAgent *backend.AgentHandle
	killErr     error
}

func (opBackend) Name() string                      { return "optest" }
func (opBackend) Available() bool                   { return true }
func (opBackend) EnsureReady(context.Context) error { return nil }
func (opBackend) ListProjects(context.Context) ([]backend.ProjectHandle, error) {
	return nil, nil
}
func (b opBackend) CreateProject(_ context.Context, spec backend.ProjectSpec) (backend.ProjectHandle, error) {
	if b.createdSpec != nil {
		*b.createdSpec = spec
	}
	return backend.ProjectHandle{Backend: "optest", ID: "ws-" + spec.Name, Name: spec.Name, Cwd: spec.Cwd}, nil
}
func (opBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (opBackend) ListAgents(context.Context, backend.ProjectHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (b opBackend) Kill(_ context.Context, agent backend.AgentHandle) error {
	if b.killedAgent != nil {
		*b.killedAgent = agent
	}
	return b.killErr
}
func (opBackend) KillProject(context.Context, backend.ProjectHandle) error { return nil }
func (opBackend) Caps() backend.Caps                                       { return backend.Caps{} }

// sendBackend is a registered test backend that advertises CanSendText and
// implements Sender, recording the native SendText call so the dispatcher's
// native path can be exercised without a live CLI. Its name is outside
// backend.Precedence.
type sendBackend struct {
	sentTo   *backend.AgentHandle
	sentText *string
}

func (sendBackend) Name() string                      { return "sendtest" }
func (sendBackend) Available() bool                   { return true }
func (sendBackend) EnsureReady(context.Context) error { return nil }
func (sendBackend) ListProjects(context.Context) ([]backend.ProjectHandle, error) {
	return nil, nil
}
func (sendBackend) CreateProject(context.Context, backend.ProjectSpec) (backend.ProjectHandle, error) {
	return backend.ProjectHandle{}, nil
}
func (sendBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}
func (sendBackend) ListAgents(context.Context, backend.ProjectHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (sendBackend) Kill(context.Context, backend.AgentHandle) error          { return nil }
func (sendBackend) KillProject(context.Context, backend.ProjectHandle) error { return nil }
func (sendBackend) Caps() backend.Caps                                       { return backend.Capabilities(backend.CanSendText) }
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
		ID: "a1", ProjectID: "p1", Backend: "optest", Scope: "/s",
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
// frame, and reports transport=native. The handle carries the project's backend
// WorkspaceHandle, not the orchestrate project id.
func TestHandleSendMessageNative(t *testing.T) {
	ctx := context.Background()
	var sentTo backend.AgentHandle
	var sentText string
	backend.Register(sendBackend{sentTo: &sentTo, sentText: &sentText})

	db := newTestDB(t)
	if err := insertProject(ctx, db, projectRow{
		ID: "p1", Name: "proj", Backend: "sendtest", WorkspaceHandle: "ws-1",
		Cwd: "/s", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertProject: %v", err)
	}
	mustInsertAgent(t, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "sendtest", TerminalHandle: "term-1",
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

	wantHandle := backend.AgentHandle{Backend: "sendtest", ID: "term-1", ProjectID: "ws-1", Name: "worker", SessionID: "sess-1"}
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

func TestHandleSendMessageErrors(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "nosub", ProjectID: "p1", Backend: "tmux", Scope: "/s",
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
		ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s", SessionID: "sess-1",
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
		ID: "a1", Name: "worker", ProjectID: "p1", Backend: "tmux", Status: "active",
		State: StateWorking, Activity: "Bash: ls", Tokens: 10,
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
	for _, p := range []projectRow{
		{ID: "p1", Name: "alpha", Backend: "tmux", WorkspaceHandle: "ws-1", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "tmux", WorkspaceHandle: "ws-2", Cwd: "/tmp/b", Status: "active", CreatedAt: "t1"},
	} {
		if err := insertProject(ctx, db, p); err != nil {
			t.Fatalf("insertProject %s: %v", p.ID, err)
		}
	}
	mustInsertAgent(t, db, agentRow{ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s", Status: "active", State: StateWorking, CreatedAt: "t0"})
	mustInsertAgent(t, db, agentRow{ID: "a2", ProjectID: "p2", Backend: "tmux", Scope: "/s", Status: "active", State: StateIdle, CreatedAt: "t1"})

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
	t.Run("filtered by project id", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"project": "p2"}), nil))
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a2" {
			t.Fatalf("filtered = %+v, want [a2]", got)
		}
	})
	t.Run("filtered by project name resolves to its id", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"project": "beta"}), nil))
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
	t.Run("unknown project is an error", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"project": "ghost"}), nil))
		if reply.OK || reply.Error == "" {
			t.Fatalf("reply = %+v, want ok=false for an unknown project", reply)
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

func TestHandleProjectCreate(t *testing.T) {
	db := newTestDB(t)

	var gotSpec backend.ProjectSpec
	backend.Register(opBackend{createdSpec: &gotSpec})

	body := mustJSON(t, map[string]string{"name": "demo", "backend": "optest", "cwd": "/tmp/ccodemo"})
	reply := handleProjectCreate(opCtx(db, body, nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var out struct {
		ProjectID string `json:"project_id"`
		Workspace string `json:"workspace"`
		Backend   string `json:"backend"`
	}
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatal(err)
	}
	if out.ProjectID == "" || out.Workspace != "ws-demo" || out.Backend != "optest" {
		t.Fatalf("reply = %+v, want non-empty id, workspace ws-demo, backend optest", out)
	}

	// The backend received the project spec with the absolute cwd.
	if gotSpec.Name != "demo" || gotSpec.Cwd != "/tmp/ccodemo" {
		t.Fatalf("CreateProject spec = %+v, want {Name:demo Cwd:/tmp/ccodemo}", gotSpec)
	}

	// The project row persisted with the backend's workspace handle.
	p, err := getProject(context.Background(), db, out.ProjectID)
	if err != nil {
		t.Fatalf("getProject: %v", err)
	}
	want := projectRow{
		ID: out.ProjectID, Name: "demo", Backend: "optest", WorkspaceHandle: "ws-demo",
		Cwd: "/tmp/ccodemo", Status: "active", CreatedAt: p.CreatedAt,
	}
	if p != want {
		t.Fatalf("project row = %+v, want %+v", p, want)
	}
	if p.CreatedAt == "" {
		t.Error("created_at not stamped")
	}
}

// TestHandleProjectCreateResolvesCwdAgainstScope proves an empty or relative cwd
// resolves against the caller's scope (the CLI/MCP working directory carried on
// the envelope), not the long-lived daemon's process cwd.
func TestHandleProjectCreateResolvesCwdAgainstScope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cwd     string
		scope   string
		wantCwd string
	}{
		{"empty cwd uses caller scope", "", "/caller/here", "/caller/here"},
		{"relative cwd joins caller scope", "sub/dir", "/caller/here", "/caller/here/sub/dir"},
		{"absolute cwd is kept", "/abs/path", "/caller/here", "/abs/path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			var gotSpec backend.ProjectSpec
			backend.Register(opBackend{createdSpec: &gotSpec})
			body := mustJSON(t, map[string]string{"name": "demo", "backend": "optest", "cwd": tc.cwd})
			hc := daemon.HandlerCtx{Ctx: context.Background(), Env: daemon.Envelope{Body: body}, Scope: tc.scope, DB: db}
			reply := handleProjectCreate(hc)
			if !reply.OK {
				t.Fatalf("reply not ok: %s", reply.Error)
			}
			if gotSpec.Cwd != tc.wantCwd {
				t.Fatalf("CreateProject cwd = %q, want %q", gotSpec.Cwd, tc.wantCwd)
			}
		})
	}
}

func TestHandleProjectList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for _, p := range []projectRow{
		{ID: "p1", Name: "alpha", Backend: "tmux", WorkspaceHandle: "ws-1", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "superset", WorkspaceHandle: "ws-2", Cwd: "/tmp/b", Status: "archived", CreatedAt: "t1"},
	} {
		if err := insertProject(ctx, db, p); err != nil {
			t.Fatalf("insertProject %s: %v", p.ID, err)
		}
	}

	reply := handleProjectList(opCtx(db, nil, nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var got []projectView
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	want := []projectView{
		{ID: "p1", Name: "alpha", Backend: "tmux", Workspace: "ws-1", Cwd: "/tmp/a", Status: "active", CreatedAt: "t0"},
		{ID: "p2", Name: "beta", Backend: "superset", Workspace: "ws-2", Cwd: "/tmp/b", Status: "archived", CreatedAt: "t1"},
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

func TestHandleProjectActivate(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	if err := insertProject(ctx, db, projectRow{
		ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/tmp/a", Status: "archived", CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertProject: %v", err)
	}

	t.Run("activates by id", func(t *testing.T) {
		reply := handleProjectActivate(opCtx(db, mustJSON(t, map[string]string{"id": "p1"}), nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		p, err := getProject(ctx, db, "p1")
		if err != nil {
			t.Fatal(err)
		}
		if p.Status != "active" {
			t.Fatalf("status = %q, want active", p.Status)
		}
	})
	t.Run("missing is an error", func(t *testing.T) {
		reply := handleProjectActivate(opCtx(db, mustJSON(t, map[string]string{"id": "ghost"}), nil))
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
	if err := insertProject(ctx, db, projectRow{
		ID: "p1", Name: "proj", Backend: "optest", WorkspaceHandle: "ws-1",
		Cwd: "/s", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatalf("insertProject: %v", err)
	}
	mustInsertAgent(t, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "optest", TerminalHandle: "term-1",
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
		// ProjectID carrying the project's backend WorkspaceHandle (ws-1), not the
		// orchestrate project id (p1).
		want := backend.AgentHandle{
			Backend: "optest", ID: "term-1", ProjectID: "ws-1", Name: "worker", SessionID: "sess-1",
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
			ID: "a2", ProjectID: "p1", Backend: "optest", TerminalHandle: "term-2",
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
}
