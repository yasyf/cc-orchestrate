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

	"github.com/google/uuid"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ccnotes"
	"github.com/yasyf/cc-orchestrate/worktree"
)

// slugInvalid collapses every run of non-slug characters in a repo name to a
// single hyphen when deriving a repo's canonical id.
var slugInvalid = regexp.MustCompile(`[^a-z0-9]+`)

// Config keys persisted in the config table that record the orchestrator's
// current selection, the defaults an agent spawn falls back through.
const (
	configBackend          = "backend"
	configActiveRepo       = "active-repo"
	configActiveWorkstream = "active-workstream"
	configActiveSprint     = "active-sprint"
)

// defaultSprintName is the name of the default sprint auto-created with every
// workstream — the planning group an agent spawns into when no sprint is named.
const defaultSprintName = "main"

// agentView is the JSON shape every agent-facing op returns: the persisted agent
// fields a parent inspects, flattened from agentRow.
type agentView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	SprintID  string `json:"sprint_id"`
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
		ID: a.ID, Name: a.Name, SprintID: a.SprintID, Backend: string(a.Backend),
		Status: string(a.Status), State: string(a.State), Activity: a.Activity, Tokens: a.Tokens,
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
		Type: EventStatus, State: string(st.State), Tool: st.Tool, Target: st.Target, LastText: st.LastText, Tokens: st.Tokens,
	})
	return b
}

// messageEvent is the EventMessage body delivered over the LCD; Type discriminates
// the frame for a stream consumer reading the SSE payload alone.
type messageEvent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func messagePayload(text string) json.RawMessage {
	b, _ := json.Marshal(messageEvent{Type: EventMessage, Text: text})
	return b
}

// exitedEvent is the terminal EventExited body; Type discriminates the frame.
type exitedEvent struct {
	Type string `json:"type"`
}

func exitedPayload() json.RawMessage {
	b, _ := json.Marshal(exitedEvent{Type: EventExited})
	return b
}

// inboundEvent is the EventInbound audit body the transcript tailer appends when a
// typed turn lands; Type discriminates the frame.
type inboundEvent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func inboundPayload(text string) json.RawMessage {
	b, _ := json.Marshal(inboundEvent{Type: EventInbound, Text: text})
	return b
}

// spawnedEvent is the EventSpawned body appended when an agent is created; Type
// discriminates the frame.
type spawnedEvent struct {
	Type     string `json:"type"`
	AgentID  string `json:"agent_id"`
	Backend  string `json:"backend"`
	Terminal string `json:"terminal"`
}

func spawnedPayload(ag agentRow) json.RawMessage {
	b, _ := json.Marshal(spawnedEvent{
		Type: EventSpawned, AgentID: ag.ID, Backend: string(ag.Backend), Terminal: ag.TerminalHandle,
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
// repo resolved by id or name. An absent body lists all.
func handleList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Repo string `json:"repo"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad agent-list body: " + err.Error()}
		}
	}
	var agents []agentRow
	var err error
	if b.Repo != "" {
		repo, repoErr := getRepo(hc.Ctx, hc.DB, b.Repo)
		if repoErr != nil {
			return daemon.Reply{OK: false, Error: repoErr.Error()}
		}
		agents, err = listRepoAgents(hc.Ctx, hc.DB, repo.ID)
	} else {
		agents, err = listAgents(hc.Ctx, hc.DB, "")
	}
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
//
// A message containing a newline is always delivered over the LCD, even to a
// native backend: typing it into a terminal would submit each line as its own
// turn (and cmux reinterprets \t too), fragmenting the message. The LCD delivers
// it whole as a single EventMessage.
func deliverMessage(hc daemon.HandlerCtx, bk backend.Backend, ag agentRow, text string) (native bool, seq int64, err error) {
	if s, ok := bk.(backend.Sender); ok && bk.Caps().Has(backend.CanSendText) && !strings.ContainsAny(text, "\n\r") {
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

// reportRequest is the agent-report inbound request body an agent's report tool
// sends: the agent's message and its optional run state.
type reportRequest struct {
	Text  string `json:"text"`
	State string `json:"state,omitempty"`
}

// reportPayload is the EventReport event body the handler appends from a
// reportRequest. Type discriminates the frame for a stream consumer reading the
// SSE payload alone.
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
	var b reportRequest
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
	payload, _ := json.Marshal(reportPayload{Type: EventReport, Text: b.Text, State: b.State})
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

// repoView is the JSON shape every repo-facing op returns, flattened from
// repoRow so a parent never sees the internal column names.
type repoView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Backend   string `json:"backend"`
	Cwd       string `json:"cwd"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

func newRepoView(p repoRow) repoView {
	return repoView{
		ID: p.ID, Name: p.Name, Backend: string(p.Backend),
		Cwd: p.Cwd, Status: string(p.Status), CreatedAt: p.CreatedAt,
	}
}

// workstreamView is the JSON shape every workstream-facing op returns, flattened
// from workstreamRow so a parent never sees the internal column names.
type workstreamView struct {
	ID        string `json:"id"`
	RepoID    string `json:"repo_id"`
	Name      string `json:"name"`
	Backend   string `json:"backend"`
	Branch    string `json:"branch"`
	Worktree  string `json:"worktree"`
	IsPrimary bool   `json:"is_primary"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

func newWorkstreamView(w workstreamRow) workstreamView {
	return workstreamView{
		ID: w.ID, RepoID: w.RepoID, Name: w.Name, Backend: string(w.Backend),
		Branch: w.Branch, Worktree: w.Worktree, IsPrimary: w.IsPrimary,
		Status: string(w.Status), CreatedAt: w.CreatedAt,
	}
}

// sprintView is the JSON shape every sprint-facing op returns, flattened from
// sprintRow so a parent never sees the internal column names.
type sprintView struct {
	ID           string `json:"id"`
	WorkstreamID string `json:"workstream_id"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	CreatedAt    string `json:"created_at"`
}

func newSprintView(sp sprintRow) sprintView {
	return sprintView{
		ID: sp.ID, WorkstreamID: sp.WorkstreamID, Name: sp.Name,
		Status: string(sp.Status), CreatedAt: sp.CreatedAt,
	}
}

// slugID derives a stable, unique canonical id from a display name: the name
// slugified, plus a short uuid suffix so two records of the same name never
// collide on the primary key. fallback is used when the name has no slug
// characters.
func slugID(name, fallback string) string {
	base := strings.Trim(slugInvalid.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if base == "" {
		base = fallback
	}
	return base + "-" + uuid.NewString()[:8]
}

func repoSlug(name string) string       { return slugID(name, "repo") }
func workstreamSlug(name string) string { return slugID(name, "workstream") }
func sprintSlug(name string) string     { return slugID(name, "sprint") }

// resolveRepoID resolves a repo reference to its canonical id, returning "" when ref
// is empty so a by-name lookup downstream stays unscoped. It is the shared
// repo-scoping step the workstream and sprint ops thread into their by-name lookups.
func resolveRepoID(hc daemon.HandlerCtx, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	repo, err := getRepo(hc.Ctx, hc.DB, ref)
	if err != nil {
		return "", err
	}
	return repo.ID, nil
}

// resolveWorkstreamRef resolves a workstream by id or name, scoping a by-name lookup
// to repoRef's repo when repoRef is set so two repos may hold a workstream of the
// same name without colliding.
func resolveWorkstreamRef(hc daemon.HandlerCtx, ref, repoRef string) (workstreamRow, error) {
	repoID, err := resolveRepoID(hc, repoRef)
	if err != nil {
		return workstreamRow{}, err
	}
	return getWorkstream(hc.Ctx, hc.DB, ref, repoID)
}

// createDefaultSprint inserts a workstream's default sprint — the planning group an
// agent spawns into when no sprint is named — named defaultSprintName and active,
// bound to ccnotesSprint (empty when cc-notes is not in play). It is the shared step
// both handleRepoCreate (for the primary workstream) and handleWorkstreamCreate run
// right after persisting their workstream row.
func createDefaultSprint(ctx context.Context, db *sql.DB, workstreamID, ccnotesSprint string) error {
	return insertSprint(ctx, db, sprintRow{
		ID: sprintSlug(defaultSprintName), WorkstreamID: workstreamID,
		Name: defaultSprintName, CCNotesSprint: ccnotesSprint, Status: StatusActive, CreatedAt: nowStamp(),
	})
}

// provisionCCNotes provisions a cc-notes project for a new workstream and a sprint
// for its default sprint, when cc-notes is enabled for the repo at repoRoot. It
// returns the project and default-sprint ids (both empty when cc-notes is not in
// play, so the caller stores empty bindings and skips every cc-notes call).
// projectName titles the project; the sprint is titled defaultSprintName. It is the
// shared cc-notes step both handleRepoCreate (primary workstream) and
// handleWorkstreamCreate run before persisting their workstream row, so a cc-notes
// failure surfaces before the local insert and leaves the row uncreated.
func provisionCCNotes(ctx context.Context, repoRoot, projectName string) (project, sprint string, err error) {
	if !ccnotes.Enabled(ctx, repoRoot) {
		return "", "", nil
	}
	project, err = ccnotes.CreateProject(ctx, repoRoot, projectName)
	if err != nil {
		return "", "", err
	}
	sprint, err = ccnotes.CreateSprint(ctx, repoRoot, project, defaultSprintName)
	if err != nil {
		return "", "", err
	}
	return project, sprint, nil
}

// colocateJJ colocates an independent jj repo inside path when repoRoot tracks with
// jj, so a worktree on a jj-managed repo gets its own jj store; it is a no-op for a
// plain git repo. path must be the worktree itself, not repoRoot.
func colocateJJ(ctx context.Context, repoRoot, path string) error {
	if !worktree.UsesJJ(repoRoot) {
		return nil
	}
	return worktree.InitJJ(ctx, path)
}

// pathComponent sanitizes a workstream name into a single filesystem path
// component (no slashes, lowercased), so a worktree directory derived from a
// branchy name like "feature/x" stays one level deep under the worktrees base.
func pathComponent(name string) string {
	base := strings.Trim(slugInvalid.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if base == "" {
		base = "workstream"
	}
	return base
}

// resolveBackend picks the backend for a repo: the explicit name, else the
// persisted selection, else the first available one. It errors when an explicit
// or selected backend is unknown, or when none is available.
func resolveBackend(hc daemon.HandlerCtx, explicit string) (backend.Backend, backend.BackendName, error) {
	name := backend.BackendName(explicit)
	if name == "" {
		value, found, err := getConfig(hc.Ctx, hc.DB, configBackend)
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

// handleRepoCreate answers repo-create: it resolves the backend, forks the repo's
// primary workstream backend workspace and provisions its cc-notes bindings, and only
// once those succeed persists the repo row keyed by a slug of its name, the primary
// workstream bound to that workspace, and the workstream's own default sprint — so a
// backend or cc-notes failure never orphans a repo row. The primary workstream tracks
// the repo's current branch: its worktree is the repo root for a backend cc-orchestrate
// drives with git, or the backend's own forked worktree for a ManagesWorktree backend
// (superset). It reports the new repo id, the primary workstream's workspace handle,
// and the backend.
func handleRepoCreate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Name    string `json:"name"`
		Backend string `json:"backend"`
		Cwd     string `json:"cwd"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad repo-create body: " + err.Error()}
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
	branch, err := worktree.CurrentBranch(hc.Ctx, cwd)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: branch, Cwd: cwd, RepoCwd: cwd, Branch: branch})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	ccProject, ccSprint, err := provisionCCNotes(hc.Ctx, cwd, b.Name)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	// A ManagesWorktree backend owns its own forked worktree; adopt the path it
	// returns. Every other backend's primary workstream is the repo root itself.
	worktreePath := cwd
	if bk.Caps().Has(backend.ManagesWorktree) {
		worktreePath = handle.Worktree
	}
	p := repoRow{
		ID: repoSlug(b.Name), Name: b.Name, Backend: bname,
		Cwd: cwd, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertRepo(hc.Ctx, hc.DB, p); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	w := workstreamRow{
		ID: workstreamSlug(branch), RepoID: p.ID, Name: branch, Backend: bname,
		WorkspaceHandle: handle.ID, Branch: branch, Worktree: worktreePath, IsPrimary: true,
		CCNotesProject: ccProject, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertWorkstream(hc.Ctx, hc.DB, w); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := createDefaultSprint(hc.Ctx, hc.DB, w.ID, ccSprint); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"repo_id": p.ID, "workspace": handle.ID, "backend": string(bname)})
	return daemon.Reply{OK: true, Body: out}
}

// handleRepoList answers repo-list with every repo's flattened view.
func handleRepoList(hc daemon.HandlerCtx) daemon.Reply {
	repos, err := listRepos(hc.Ctx, hc.DB)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]repoView, len(repos))
	for i, p := range repos {
		views[i] = newRepoView(p)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleRepoActivate answers repo-activate: it resolves the repo (by id
// or name, erroring when missing), marks it active, and records it as the active
// repo so an agent spawn with no repo or workstream falls back to its primary
// workstream. Activating a repo resets the precedence chain — it clears the
// higher-precedence active workstream and sprint — so the most recent activation
// wins and a stale active sprint can never silently misroute a bare spawn.
func handleRepoActivate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad repo-activate body: " + err.Error()}
	}
	repo, err := getRepo(hc.Ctx, hc.DB, b.ID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setRepoStatus(hc.Ctx, hc.DB, repo.ID, StatusActive); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, repo.ID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveWorkstream); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveSprint); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"repo_id": repo.ID, "status": string(StatusActive)})
	return daemon.Reply{OK: true, Body: out}
}

// backendAgentHandle resolves an agent's backend handle: its terminal addressed
// within its workstream's backend workspace, reached through the agent's sprint. The
// handle's WorkstreamID is the backend WorkspaceHandle (what cmux's --workspace and
// zellij's --session expect), not the orchestrate workstream id — those are
// different values, and addressing the terminal needs the backend one.
func backendAgentHandle(hc daemon.HandlerCtx, ag agentRow) (backend.AgentHandle, error) {
	sprint, err := getSprint(hc.Ctx, hc.DB, ag.SprintID, "")
	if err != nil {
		return backend.AgentHandle{}, err
	}
	ws, err := getWorkstream(hc.Ctx, hc.DB, sprint.WorkstreamID, "")
	if err != nil {
		return backend.AgentHandle{}, err
	}
	return backend.AgentHandle{
		Backend:      ag.Backend,
		ID:           ag.TerminalHandle,
		WorkstreamID: ws.WorkspaceHandle,
		Name:         ag.Name,
		SessionID:    ag.SessionID,
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
	if ag.Status != StatusActive {
		// Already terminal (e.g. its repo was killed while a caller held the id):
		// idempotent no-op, never a second EventExited or a re-kill of a dead terminal.
		out, _ := json.Marshal(map[string]string{"agent_id": ag.ID, "status": string(ag.Status)})
		return daemon.Reply{OK: true, Body: out}
	}
	bk, ok := backend.Get(ag.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(ag.Backend)}
	}
	handle, err := backendAgentHandle(hc, ag)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	killErr := bk.Kill(hc.Ctx, handle)
	if err := softExitAgent(hc.Ctx, hc.DB, hc.Append, ag); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if killErr != nil {
		return daemon.Reply{OK: false, Error: fmt.Errorf("kill agent %q: %w", ag.ID, killErr).Error()}
	}
	out, _ := json.Marshal(map[string]string{"agent_id": ag.ID, "status": string(StatusExited)})
	return daemon.Reply{OK: true, Body: out}
}

// softExitAgent stops an agent's transcript tailer, marks its row exited, and
// appends a terminal OriginSystem EventExited. It is the shared teardown for an
// agent the backend no longer hosts — the repo-kill cascade and the boot
// reconcile prune — and never calls the backend itself.
func softExitAgent(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow) error {
	tailers.stop(ag.ID)
	if err := setAgentLifecycle(ctx, db, ag.ID, StatusExited); err != nil {
		return err
	}
	_, err := appendFn(ctx, &event.Event{
		SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventExited, Payload: exitedPayload(),
	})
	return err
}

// clearActiveSelection drops the active-* config key when its stored id is the
// entity being killed, so a killed repo, workstream, or sprint can never linger as a
// bare-spawn fallback target. It mirrors the terminal-state guard in handleAgentKill.
func clearActiveSelection(ctx context.Context, db *sql.DB, key, killedID string) error {
	id, found, err := getConfig(ctx, db, key)
	if err != nil {
		return err
	}
	if found && id == killedID {
		return clearConfig(ctx, db, key)
	}
	return nil
}

// markSprintKilled soft-exits every active agent of a sprint, then marks the sprint
// killed and drops it as the active sprint. Like markWorkstreamKilled it never calls
// the backend nor removes a worktree — a sprint has none, it shares its workstream's.
// Reused by the repo- and workstream-kill cascades and boot reconcile.
func markSprintKilled(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, sp sprintRow) error {
	agents, err := listAgents(ctx, db, sp.ID)
	if err != nil {
		return err
	}
	for _, ag := range agents {
		if ag.Status != StatusActive {
			continue
		}
		if err := softExitAgent(ctx, db, appendFn, ag); err != nil {
			return err
		}
	}
	if err := setSprintStatus(ctx, db, sp.ID, StatusKilled); err != nil {
		return err
	}
	return clearActiveSelection(ctx, db, configActiveSprint, sp.ID)
}

// markWorkstreamKilled cascades a workstream to killed: every active sprint is marked
// killed and its agents exited, then the workstream itself, dropping it as the active
// workstream. It never calls the backend nor removes the worktree: a caller that must
// also tear those down
// (workstream-kill) does so first and surfaces the error after these row writes
// (mirroring handleAgentKill). Reused by the repo-kill cascade and boot reconcile
// when a workspace has vanished out-of-band.
func markWorkstreamKilled(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ws workstreamRow) error {
	sprints, err := listSprints(ctx, db, ws.ID)
	if err != nil {
		return err
	}
	for _, sp := range sprints {
		if sp.Status != StatusActive {
			continue
		}
		if err := markSprintKilled(ctx, db, appendFn, sp); err != nil {
			return err
		}
	}
	if err := setWorkstreamStatus(ctx, db, ws.ID, StatusKilled); err != nil {
		return err
	}
	return clearActiveSelection(ctx, db, configActiveWorkstream, ws.ID)
}

// tearDownWorkstream tears down a workstream's real resources: its backend
// workspace and — only for a non-primary workstream on a backend that does not
// manage its own worktree — its git worktree. It never removes a primary
// workstream's worktree (the repo root) nor a ManagesWorktree backend's worktree
// (the backend drops that on KillWorkstream). It touches no DB rows; the caller
// marks the workstream killed and surfaces this error afterward, mirroring
// handleAgentKill, so a half-dead workstream never lingers active.
func tearDownWorkstream(ctx context.Context, bk backend.Backend, repo repoRow, ws workstreamRow) error {
	killErr := bk.KillWorkstream(ctx, backend.WorkstreamHandle{
		Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree, Worktree: ws.Worktree,
	})
	var removeErr error
	if !bk.Caps().Has(backend.ManagesWorktree) && !ws.IsPrimary {
		removeErr = worktree.Remove(ctx, repo.Cwd, ws.Worktree)
	}
	if killErr != nil {
		return fmt.Errorf("kill workstream %q: %w", ws.ID, killErr)
	}
	if removeErr != nil {
		return fmt.Errorf("remove worktree for workstream %q: %w", ws.ID, removeErr)
	}
	return nil
}

// killRepo tears a repo down for real: it tears down every active workstream's
// backend workspace and non-primary worktree, cascades each workstream's sprints to
// killed and agents to exited, marks the repo killed, and drops it as the active
// repo. Teardown errors are collected and surfaced only after every row is mutated,
// mirroring handleAgentKill, so a half-dead repo never lingers active; an unknown
// backend or failed teardown on one workstream does not abort the others.
func killRepo(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, repo repoRow) error {
	wss, err := listWorkstreams(ctx, db, repo.ID)
	if err != nil {
		return err
	}
	var teardownErr error
	for _, ws := range wss {
		if ws.Status != StatusActive {
			continue
		}
		switch bk, ok := backend.Get(ws.Backend); {
		case !ok:
			if teardownErr == nil {
				teardownErr = fmt.Errorf("unknown backend %q for workstream %q", ws.Backend, ws.ID)
			}
		default:
			if err := tearDownWorkstream(ctx, bk, repo, ws); err != nil && teardownErr == nil {
				teardownErr = err
			}
		}
		if err := markWorkstreamKilled(ctx, db, appendFn, ws); err != nil {
			return err
		}
	}
	if err := setRepoStatus(ctx, db, repo.ID, StatusKilled); err != nil {
		return err
	}
	if err := clearActiveSelection(ctx, db, configActiveRepo, repo.ID); err != nil {
		return err
	}
	return teardownErr
}

// handleRepoKill answers repo-kill: a real teardown that tears down every workstream's
// backend workspace and non-primary worktree, cascades their sprints to killed and
// agents to exited, then marks the repo killed. Teardown errors are surfaced after the
// row mutations (mirroring handleAgentKill), so a half-dead repo never lingers active.
func handleRepoKill(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad repo-kill body: " + err.Error()}
	}
	repo, err := getRepo(hc.Ctx, hc.DB, b.ID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := killRepo(hc.Ctx, hc.DB, hc.Append, repo); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"repo_id": repo.ID, "status": string(StatusKilled)})
	return daemon.Reply{OK: true, Body: out}
}

// handleWorkstreamCreate answers workstream-create: it resolves the repo, then
// branches on whether the backend manages its own worktree. A ManagesWorktree
// backend (superset) forks its own worktree off the branch and cc-orchestrate
// adopts the path it returns; every other backend takes a worktree cc-orchestrate
// creates with git (an independent jj repo colocated inside it when the repo tracks
// with jj) and runs in it as its cwd. Either path yields exactly one worktree per
// workstream. The new workstream gets its own default sprint, so a simple flow never
// touches sprints.
func handleWorkstreamCreate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Repo   string `json:"repo"`
		Name   string `json:"name"`
		Branch string `json:"branch"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad workstream-create body: " + err.Error()}
	}
	repo, err := getRepo(hc.Ctx, hc.DB, b.Repo)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	bk, ok := backend.Get(repo.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(repo.Backend)}
	}
	if err := bk.EnsureReady(hc.Ctx); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	branch := cmp.Or(b.Branch, b.Name)
	repoRoot := repo.Cwd

	var path, workspaceHandle string
	if bk.Caps().Has(backend.ManagesWorktree) {
		handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: b.Name, Cwd: repoRoot, RepoCwd: repoRoot, Branch: branch})
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		path, workspaceHandle = handle.Worktree, handle.ID
		if err := colocateJJ(hc.Ctx, repoRoot, path); err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
	} else {
		dest := filepath.Join(worktreesBase(), repo.ID, pathComponent(b.Name))
		path, err = worktree.Add(hc.Ctx, repoRoot, dest, branch)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		if err := colocateJJ(hc.Ctx, repoRoot, path); err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: b.Name, Cwd: path, RepoCwd: repoRoot, Branch: branch})
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		workspaceHandle = handle.ID
	}

	ccProject, ccSprint, err := provisionCCNotes(hc.Ctx, repoRoot, b.Name)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	w := workstreamRow{
		ID: workstreamSlug(b.Name), RepoID: repo.ID, Name: b.Name, Backend: repo.Backend,
		WorkspaceHandle: workspaceHandle, Branch: branch, Worktree: path, IsPrimary: false,
		CCNotesProject: ccProject, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertWorkstream(hc.Ctx, hc.DB, w); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := createDefaultSprint(hc.Ctx, hc.DB, w.ID, ccSprint); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{
		"workstream_id": w.ID, "repo_id": repo.ID, "workspace": workspaceHandle, "branch": branch, "worktree": path,
	})
	return daemon.Reply{OK: true, Body: out}
}

// handleWorkstreamList answers workstream-list with every workstream's flattened
// view, optionally filtered to one repo resolved by id or name.
func handleWorkstreamList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Repo string `json:"repo"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad workstream-list body: " + err.Error()}
		}
	}
	filter := ""
	if b.Repo != "" {
		repo, err := getRepo(hc.Ctx, hc.DB, b.Repo)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		filter = repo.ID
	}
	wss, err := listWorkstreams(hc.Ctx, hc.DB, filter)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]workstreamView, len(wss))
	for i, w := range wss {
		views[i] = newWorkstreamView(w)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleWorkstreamActivate answers workstream-activate: it resolves the workstream
// (by id or name, scoped to a repo when given to disambiguate; erroring when
// missing), marks it active, and records it as the active workstream so an agent
// spawn with no explicit target lands in it. Activating a workstream resets the
// precedence chain — it sets the active repo to the workstream's repo and clears the
// higher-precedence active sprint — so the most recent activation wins.
func handleWorkstreamActivate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID   string `json:"id"`
		Repo string `json:"repo"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad workstream-activate body: " + err.Error()}
	}
	ws, err := resolveWorkstreamRef(hc, b.ID, b.Repo)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setWorkstreamStatus(hc.Ctx, hc.DB, ws.ID, StatusActive); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveWorkstream, ws.ID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, ws.RepoID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveSprint); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"workstream_id": ws.ID, "status": string(StatusActive)})
	return daemon.Reply{OK: true, Body: out}
}

// handleWorkstreamKill answers workstream-kill: a real teardown. It tears down the
// workstream's backend workspace and — for a backend that does not manage its own
// worktree, and only for a non-primary workstream — removes its git worktree, then
// cascades its agents to exited and marks the workstream killed. It never removes a
// primary workstream's worktree (the repo root) nor a ManagesWorktree backend's
// worktree (the backend owns that and drops it on KillWorkstream). It then cascades
// the workstream's sprints to killed and their agents to exited. The teardown errors
// are surfaced after the row mutations, mirroring handleAgentKill, so a half-dead
// workstream never lingers active.
func handleWorkstreamKill(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID   string `json:"id"`
		Repo string `json:"repo"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad workstream-kill body: " + err.Error()}
	}
	ws, err := resolveWorkstreamRef(hc, b.ID, b.Repo)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	bk, ok := backend.Get(ws.Backend)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(ws.Backend)}
	}
	repo, err := getRepo(hc.Ctx, hc.DB, ws.RepoID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	teardownErr := tearDownWorkstream(hc.Ctx, bk, repo, ws)
	if err := markWorkstreamKilled(hc.Ctx, hc.DB, hc.Append, ws); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if teardownErr != nil {
		return daemon.Reply{OK: false, Error: teardownErr.Error()}
	}
	out, _ := json.Marshal(map[string]string{"workstream_id": ws.ID, "status": string(StatusKilled)})
	return daemon.Reply{OK: true, Body: out}
}

// handleSprintCreate answers sprint-create: it resolves the workstream (by id or
// name, scoped to a repo when given to disambiguate), inserts a new sprint under it,
// and reports the sprint id. A sprint shares its workstream's worktree — it has no
// worktree of its own.
func handleSprintCreate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Workstream string `json:"workstream"`
		Repo       string `json:"repo"`
		Name       string `json:"name"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad sprint-create body: " + err.Error()}
	}
	ws, err := resolveWorkstreamRef(hc, b.Workstream, b.Repo)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	ccSprint := ""
	if ws.CCNotesProject != "" && ccnotes.Enabled(hc.Ctx, ws.Worktree) {
		ccSprint, err = ccnotes.CreateSprint(hc.Ctx, ws.Worktree, ws.CCNotesProject, b.Name)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
	}
	sp := sprintRow{
		ID: sprintSlug(b.Name), WorkstreamID: ws.ID, Name: b.Name,
		CCNotesSprint: ccSprint, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertSprint(hc.Ctx, hc.DB, sp); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"sprint_id": sp.ID, "workstream_id": ws.ID, "name": sp.Name})
	return daemon.Reply{OK: true, Body: out}
}

// handleSprintList answers sprint-list with every sprint's flattened view, optionally
// filtered to one workstream resolved by id or name (scoped to a repo when given).
func handleSprintList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Workstream string `json:"workstream"`
		Repo       string `json:"repo"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad sprint-list body: " + err.Error()}
		}
	}
	filter := ""
	if b.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, b.Workstream, b.Repo)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		filter = ws.ID
	}
	sprints, err := listSprints(hc.Ctx, hc.DB, filter)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]sprintView, len(sprints))
	for i, sp := range sprints {
		views[i] = newSprintView(sp)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleSprintActivate answers sprint-activate: it resolves the sprint (by id or
// name, scoped to a workstream when given to disambiguate), marks it active, and
// records it as the active sprint so an agent spawn with no explicit target lands in
// it. Activating a sprint resets the precedence chain — it sets the active workstream
// to the sprint's workstream and the active repo to that workstream's repo — so the
// whole bare-spawn fallback chain points at the most recent activation.
func handleSprintActivate(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		ID         string `json:"id"`
		Workstream string `json:"workstream"`
		Repo       string `json:"repo"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad sprint-activate body: " + err.Error()}
	}
	workstreamID := ""
	if b.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, b.Workstream, b.Repo)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
		workstreamID = ws.ID
	}
	sp, err := getSprint(hc.Ctx, hc.DB, b.ID, workstreamID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	ws, err := getWorkstream(hc.Ctx, hc.DB, sp.WorkstreamID, "")
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setSprintStatus(hc.Ctx, hc.DB, sp.ID, StatusActive); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveSprint, sp.ID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveWorkstream, sp.WorkstreamID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, ws.RepoID); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	out, _ := json.Marshal(map[string]string{"sprint_id": sp.ID, "status": string(StatusActive)})
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
