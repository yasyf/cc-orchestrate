package orchestrate

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// slugInvalid collapses every run of non-slug characters in a project name to a
// single hyphen when deriving a project's canonical id.
var slugInvalid = regexp.MustCompile(`[^a-z0-9]+`)

// agentView is the JSON shape every agent-facing op returns: the persisted agent
// fields a parent inspects, flattened from agentRow.
type agentView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ProjectID string `json:"project_id"`
	Backend   string `json:"backend"`
	Status    string `json:"status"`
	State     string `json:"state"`
	Activity  string `json:"activity"`
	Tokens    int    `json:"tokens"`
	UpdatedAt string `json:"updated_at"`
	SessionID string `json:"session_id"`
	Scope     string `json:"scope"`
}

func newAgentView(a agentRow) agentView {
	return agentView{
		ID: a.ID, Name: a.Name, ProjectID: a.ProjectID, Backend: string(a.Backend),
		Status: string(a.Status), State: a.State, Activity: a.Activity, Tokens: a.Tokens,
		UpdatedAt: a.UpdatedAt, SessionID: a.SessionID, Scope: a.Scope,
	}
}

// statusPayload is the EventStatus event body the transcript tailer appends. Type
// discriminates the frame for a stream consumer reading the SSE payload alone.
type statusPayload struct {
	Type     string `json:"type"`
	State    string `json:"state"`
	Tool     string `json:"tool"`
	Target   string `json:"target"`
	LastText string `json:"last_text"`
	Tokens   int    `json:"tokens"`
}

func jsonStatus(st Status) json.RawMessage {
	b, _ := json.Marshal(statusPayload{
		Type: EventStatus, State: st.State, Tool: st.Tool, Target: st.Target, LastText: st.LastText, Tokens: st.Tokens,
	})
	return b
}

func messagePayload(text string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": EventMessage, "text": text})
	return b
}

func exitedPayload() json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": EventExited})
	return b
}

func inboundPayload(text string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": EventInbound, "text": text})
	return b
}

func spawnedPayload(ag agentRow) json.RawMessage {
	b, _ := json.Marshal(map[string]string{
		"type": EventSpawned, "agent_id": ag.ID, "backend": string(ag.Backend), "terminal": ag.TerminalHandle,
	})
	return b
}

// handleStatus answers agent-status with one agent's persisted snapshot.
func handleStatus(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-status body: " + err.Error()}
	}
	ag, err := getAgent(hc.Ctx, hc.DB, b.AgentID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, err := json.Marshal(newAgentView(ag))
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleList answers agent-list with every agent, optionally filtered to one
// project resolved by id or name. An absent body lists all.
func handleList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Project string `json:"project"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad agent-list body: " + err.Error()}
		}
	}
	filter := b.Project
	if filter != "" {
		proj, err := getProject(hc.Ctx, hc.DB, b.Project)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		filter = proj.ID
	}
	agents, err := listAgents(hc.Ctx, hc.DB, filter)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]agentView, len(agents))
	for i, a := range agents {
		views[i] = newAgentView(a)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleSendMessage answers agent-send-message: it delivers the text to the agent
// natively when the backend can type into its terminal (CanSendText), else by
// appending an OriginHuman EventMessage the agent's watch Monitor consumes (the
// LCD). The native path writes no event-plane frame; the transcript tailer emits
// an EventInbound audit frame when the typed turn lands.
func handleSendMessage(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		AgentID string `json:"agent_id"`
		Text    string `json:"text"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-send-message body: " + err.Error()}
	}
	ag, err := getAgent(hc.Ctx, hc.DB, b.AgentID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if ag.SubjectID == "" {
		return daemon.Reply{OK: false, Error: "agent has no subject: " + b.AgentID}
	}
	bk, ok := backend.Get(ag.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(ag.Backend)}
	}
	native, seq, err := deliverMessage(hc, bk, ag, b.Text)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]any{"seq": seq, "transport": transportLabel(native)})
	return daemon.Reply{OK: true, Body: body}
}

// deliverMessage routes a message to a running agent: natively when the backend is
// a Sender advertising CanSendText (typing into the agent's terminal), else by
// appending an OriginHuman EventMessage the agent's watch Monitor consumes (the
// LCD). seq is the event-log sequence on the LCD path and 0 on the native path,
// which writes no event-plane frame.
func deliverMessage(hc daemon.HandlerCtx, bk backend.Backend, ag agentRow, text string) (native bool, seq int64, err error) {
	if s, ok := bk.(backend.Sender); ok && bk.Caps().Has(backend.CanSendText) {
		handle, err := backendAgentHandle(hc, ag)
		if err != nil {
			return true, 0, err
		}
		if err := s.SendText(hc.Ctx, handle, text); err != nil {
			return true, 0, err
		}
		return true, 0, nil
	}
	seq, err = hc.Append(hc.Ctx, &event.Event{
		SubjectID: ag.SubjectID, Origin: event.OriginHuman, Type: EventMessage, Payload: messagePayload(text),
	})
	return false, seq, err
}

// transportLabel names the delivery path on the send-message reply, so a caller
// (and the tests) can tell native typing from an event-plane append.
func transportLabel(native bool) string {
	if native {
		return "native"
	}
	return "event"
}

// reportPayload is the EventReport event body an agent's report tool appends: the
// agent's message and its optional run state. Type discriminates the frame for a
// stream consumer reading the SSE payload alone.
type reportPayload struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	State string `json:"state,omitempty"`
}

// handleReport answers agent-report by appending an OriginAgent EventReport to the
// reporting agent's subject log. The subject is resolved from the child's session
// and scope (the channel server stamps both), so the agent never needs to know its
// own subject id. An unresolvable subject is an error.
func handleReport(hc daemon.HandlerCtx) daemon.Reply {
	var b reportPayload
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-report body: " + err.Error()}
	}
	sub, ok, err := hc.Subjects.Find(hc.Ctx, hc.Window, hc.Scope)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if !ok {
		return daemon.Reply{OK: false, Error: "no subject for session " + hc.Env.Session + " in scope " + hc.Scope}
	}
	b.Type = EventReport
	payload, _ := json.Marshal(b)
	seq, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: sub.ID, Origin: event.OriginAgent, Type: EventReport, Payload: payload,
	})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]int64{"seq": seq})
	return daemon.Reply{OK: true, Body: body}
}

// handleConfigSet answers config-set by upserting one config key.
func handleConfigSet(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad config-set body: " + err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, b.Key, b.Value); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true}
}

// projectView is the JSON shape every project-facing op returns, flattened from
// projectRow so a parent never sees the internal column names.
type projectView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Backend   string `json:"backend"`
	Workspace string `json:"workspace"`
	Cwd       string `json:"cwd"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

func newProjectView(p projectRow) projectView {
	return projectView{
		ID: p.ID, Name: p.Name, Backend: string(p.Backend), Workspace: p.WorkspaceHandle,
		Cwd: p.Cwd, Status: string(p.Status), CreatedAt: p.CreatedAt,
	}
}

// projectSlug derives a project's stable, unique canonical id from its name: the
// name slugified, plus a short uuid suffix so two projects of the same name never
// collide on the primary key.
func projectSlug(name string) string {
	base := strings.Trim(slugInvalid.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if base == "" {
		base = "project"
	}
	return base + "-" + uuid.NewString()[:8]
}

// resolveBackend picks the backend for a project: the explicit name, else the
// persisted selection, else the first available one. It errors when an explicit
// or selected backend is unknown, or when none is available.
func resolveBackend(hc daemon.HandlerCtx, explicit string) (backend.Backend, backend.BackendName, error) {
	name := backend.BackendName(explicit)
	if name == "" {
		value, found, err := getConfig(hc.Ctx, hc.DB, "backend")
		if err != nil {
			return nil, "", err
		}
		if found {
			name = backend.BackendName(value)
		}
	}
	if name != "" {
		b, ok := backend.Get(name)
		if !ok {
			return nil, "", fmt.Errorf("unknown backend: %s", name)
		}
		return b, name, nil
	}
	b, ok := backend.Select()
	if !ok {
		installable := make([]string, len(backend.Precedence))
		for i, n := range backend.Precedence {
			installable[i] = string(n)
		}
		return nil, "", fmt.Errorf("no available backend; install one of %s", strings.Join(installable, ", "))
	}
	return b, b.Name(), nil
}

// handleProjectCreate answers project-create: it resolves the backend, creates
// the backend workspace, persists the project row keyed by a slug of its name,
// and reports the new id, workspace handle, and backend.
func handleProjectCreate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Name    string `json:"name"`
		Backend string `json:"backend"`
		Cwd     string `json:"cwd"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad project-create body: " + err.Error()}
	}
	bk, bname, err := resolveBackend(hc, b.Backend)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := bk.EnsureReady(hc.Ctx); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	cwd := cmp.Or(b.Cwd, ".")
	if !filepath.IsAbs(cwd) {
		cwd = filepath.Join(hc.Scope, cwd)
	}
	handle, err := bk.CreateProject(hc.Ctx, backend.ProjectSpec{Name: b.Name, Cwd: cwd})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	p := projectRow{
		ID: projectSlug(b.Name), Name: b.Name, Backend: bname, WorkspaceHandle: handle.ID,
		Cwd: cwd, Status: StatusActive, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := insertProject(hc.Ctx, hc.DB, p); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"project_id": p.ID, "workspace": handle.ID, "backend": string(bname)})
	return daemon.Reply{OK: true, Body: out}
}

// handleProjectList answers project-list with every project's flattened view.
func handleProjectList(hc daemon.HandlerCtx) daemon.Reply {
	projects, err := listProjects(hc.Ctx, hc.DB)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]projectView, len(projects))
	for i, p := range projects {
		views[i] = newProjectView(p)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleProjectActivate answers project-activate: it resolves the project (by id
// or name, erroring when missing) and marks it active.
func handleProjectActivate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad project-activate body: " + err.Error()}
	}
	proj, err := getProject(hc.Ctx, hc.DB, b.ID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setProjectStatus(hc.Ctx, hc.DB, proj.ID, StatusActive); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"project_id": proj.ID, "status": string(StatusActive)})
	return daemon.Reply{OK: true, Body: out}
}

// backendAgentHandle resolves an agent's backend handle: its terminal addressed
// within the project's backend workspace. The handle's ProjectID is the backend
// WorkspaceHandle (what cmux's --workspace and zellij's --session expect), not the
// orchestrate project id stored on the agent row — those are different values, and
// addressing the terminal needs the backend one.
func backendAgentHandle(hc daemon.HandlerCtx, ag agentRow) (backend.AgentHandle, error) {
	proj, err := getProject(hc.Ctx, hc.DB, ag.ProjectID)
	if err != nil {
		return backend.AgentHandle{}, err
	}
	return backend.AgentHandle{
		Backend:   ag.Backend,
		ID:        ag.TerminalHandle,
		ProjectID: proj.WorkspaceHandle,
		Name:      ag.Name,
		SessionID: ag.SessionID,
	}, nil
}

// handleAgentKill answers agent-kill: it stops the agent's transcript tailer,
// terminates the backend terminal, marks the row exited, and appends a terminal
// EventExited. A backend kill failure is surfaced after the row is already marked
// exited, so a half-dead agent never lingers as active.
func handleAgentKill(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-kill body: " + err.Error()}
	}
	ag, err := getAgent(hc.Ctx, hc.DB, b.AgentID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	bk, ok := backend.Get(ag.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(ag.Backend)}
	}
	handle, err := backendAgentHandle(hc, ag)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	tailers.stop(ag.ID)
	killErr := bk.Kill(hc.Ctx, handle)
	if err := setAgentLifecycle(hc.Ctx, hc.DB, ag.ID, StatusExited); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if _, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventExited, Payload: exitedPayload(),
	}); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if killErr != nil {
		return daemon.Reply{OK: false, Error: fmt.Errorf("kill agent %q: %w", ag.ID, killErr).Error()}
	}
	out, _ := json.Marshal(map[string]string{"agent_id": ag.ID, "status": string(StatusExited)})
	return daemon.Reply{OK: true, Body: out}
}

// markProjectKilled stops every active agent's tailer, marks each exited with a
// terminal EventExited, then marks the project killed. It never calls the backend:
// a caller that must also tear down the workspace calls bk.KillProject first and
// surfaces its error after these row writes (mirroring handleAgentKill). Reused by
// boot reconcile when a workspace has vanished out-of-band.
func markProjectKilled(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, proj projectRow) error {
	agents, err := listAgents(ctx, db, proj.ID)
	if err != nil {
		return err
	}
	for _, ag := range agents {
		if ag.Status != StatusActive {
			continue
		}
		tailers.stop(ag.ID)
		if err := setAgentLifecycle(ctx, db, ag.ID, StatusExited); err != nil {
			return err
		}
		if _, err := appendFn(ctx, &event.Event{
			SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventExited, Payload: exitedPayload(),
		}); err != nil {
			return err
		}
	}
	return setProjectStatus(ctx, db, proj.ID, StatusKilled)
}

// handleProjectKill answers project-kill: it tears down the project's backend
// workspace (which kills all its terminals at once), cascades its agents to exited,
// and marks the project killed. The backend-kill error is surfaced after the row
// mutations, mirroring handleAgentKill, so a half-dead workspace never lingers
// active.
func handleProjectKill(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad project-kill body: " + err.Error()}
	}
	proj, err := getProject(hc.Ctx, hc.DB, b.ID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	bk, ok := backend.Get(proj.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(proj.Backend)}
	}
	killErr := bk.KillProject(hc.Ctx, backend.ProjectHandle{
		Backend: proj.Backend, ID: proj.WorkspaceHandle, Name: proj.Name, Cwd: proj.Cwd,
	})
	if err := markProjectKilled(hc.Ctx, hc.DB, hc.Append, proj); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if killErr != nil {
		return daemon.Reply{OK: false, Error: fmt.Errorf("kill project %q: %w", proj.ID, killErr).Error()}
	}
	out, _ := json.Marshal(map[string]string{"project_id": proj.ID, "status": string(StatusKilled)})
	return daemon.Reply{OK: true, Body: out}
}

// handleConfigGet answers config-get with one config key's value and whether it
// is set.
func handleConfigGet(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad config-get body: " + err.Error()}
	}
	value, found, err := getConfig(hc.Ctx, hc.DB, b.Key)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]any{"value": value, "found": found})
	return daemon.Reply{OK: true, Body: body}
}
