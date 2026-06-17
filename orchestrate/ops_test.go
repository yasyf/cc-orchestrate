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

func TestHandleSendMessage(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s",
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
		Text string `json:"text"`
	}
	if err := json.Unmarshal(captured.Payload, &pl); err != nil || pl.Text != "hello" {
		t.Errorf("payload = %s, want text=hello (err %v)", captured.Payload, err)
	}
	var rb struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil || rb.Seq != 7 {
		t.Errorf("reply body = %s, want seq=7 (err %v)", reply.Body, err)
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
	if pl.Text != "halfway done" || pl.State != "working" {
		t.Errorf("payload = %+v, want text=halfway done state=working", pl)
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
	db := newTestDB(t)
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
	t.Run("filtered by project", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"project": "p2"}), nil))
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a2" {
			t.Fatalf("filtered = %+v, want [a2]", got)
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

		// The backend received the agent handle assembled from the row.
		want := backend.AgentHandle{
			Backend: "optest", ID: "term-1", ProjectID: "p1", Name: "worker", SessionID: "sess-1",
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
