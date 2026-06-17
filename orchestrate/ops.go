package orchestrate

import (
	"cmp"
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
		ID: a.ID, Name: a.Name, ProjectID: a.ProjectID, Backend: a.Backend,
		Status: a.Status, State: a.State, Activity: a.Activity, Tokens: a.Tokens,
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
// project. An absent body lists all.
func handleList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Project string `json:"project"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad agent-list body: " + err.Error()}
		}
	}
	agents, err := listAgents(hc.Ctx, hc.DB, b.Project)
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

// handleSendMessage answers agent-send-message by appending an OriginHuman
// EventMessage to the target agent's subject log.
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
	seq, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: ag.SubjectID, Origin: event.OriginHuman, Type: EventMessage, Payload: messagePayload(b.Text),
	})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]int64{"seq": seq})
	return daemon.Reply{OK: true, Body: body}
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
		ID: p.ID, Name: p.Name, Backend: p.Backend, Workspace: p.WorkspaceHandle,
		Cwd: p.Cwd, Status: p.Status, CreatedAt: p.CreatedAt,
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
func resolveBackend(hc daemon.HandlerCtx, explicit string) (backend.Backend, string, error) {
	name := explicit
	if name == "" {
		value, found, err := getConfig(hc.Ctx, hc.DB, "backend")
		if err != nil {
			return nil, "", err
		}
		if found {
			name = value
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
		return nil, "", fmt.Errorf("no available backend; install one of %s", strings.Join(backend.Precedence, ", "))
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
	cwd, err := filepath.Abs(cmp.Or(b.Cwd, "."))
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	handle, err := bk.CreateProject(hc.Ctx, backend.ProjectSpec{Name: b.Name, Cwd: cwd})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	p := projectRow{
		ID: projectSlug(b.Name), Name: b.Name, Backend: bname, WorkspaceHandle: handle.ID,
		Cwd: cwd, Status: "active", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := insertProject(hc.Ctx, hc.DB, p); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"project_id": p.ID, "workspace": handle.ID, "backend": bname})
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
	if err := setProjectStatus(hc.Ctx, hc.DB, proj.ID, "active"); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"project_id": proj.ID, "status": "active"})
	return daemon.Reply{OK: true, Body: out}
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
		return daemon.Reply{OK: false, Error: "unknown backend: " + ag.Backend}
	}
	tailers.stop(ag.ID)
	killErr := bk.Kill(hc.Ctx, backend.AgentHandle{
		Backend: ag.Backend, ID: ag.TerminalHandle, ProjectID: ag.ProjectID, Name: ag.Name, SessionID: ag.SessionID,
	})
	if err := setAgentLifecycle(hc.Ctx, hc.DB, ag.ID, "exited"); err != nil {
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
	out, _ := json.Marshal(map[string]string{"agent_id": ag.ID, "status": "exited"})
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
