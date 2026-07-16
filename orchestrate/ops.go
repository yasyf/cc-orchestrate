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
	"sync"

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

// agentLocksMu guards the lazily-grown map of per-agent-id mutexes agentLock hands out.
var (
	agentLocksMu sync.Mutex
	agentLocks   = map[string]*sync.Mutex{}
)

// agentLock returns the mutex serializing all mutating lifecycle work for one
// agent id — kill, restart, and the kill cascades. It mirrors cc-interact's
// daemon repoLock per-key serialization so a kill handler and a future supervisor
// goroutine can never race over the same agent's row and backend terminal.
func agentLock(id string) *sync.Mutex {
	agentLocksMu.Lock()
	defer agentLocksMu.Unlock()
	mu, ok := agentLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		agentLocks[id] = mu
	}
	return mu
}

// agentView is the JSON shape every agent-facing op returns: the persisted agent
// fields a parent inspects, flattened from agentRow.
type agentView struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	SprintID       string `json:"sprint_id"`
	Backend        string `json:"backend"`
	TerminalHandle string `json:"terminal_handle"`
	Status         string `json:"status"`
	State          string `json:"state"`
	Activity       string `json:"activity"`
	Tokens         int    `json:"tokens"`
	Prompt         string `json:"prompt"`
	UpdatedAt      string `json:"updated_at"`
	CreatedAt      string `json:"created_at"`
	SessionID      string `json:"session_id"`
	SubjectID      string `json:"subject_id"`
	Scope          string `json:"scope"`
	CCNotesTask    string `json:"ccnotes_task"`
	RestartCount   int    `json:"restart_count"`
	LastRestartAt  string `json:"last_restart_at"`
}

func newAgentView(a agentRow) agentView {
	return agentView{
		ID: a.ID, Name: a.Name, SprintID: a.SprintID, Backend: string(a.Backend), TerminalHandle: a.TerminalHandle,
		Status: string(a.Status), State: string(a.State), Activity: a.Activity, Tokens: a.Tokens, Prompt: a.Prompt,
		UpdatedAt: a.UpdatedAt, CreatedAt: a.CreatedAt, SessionID: a.SessionID, SubjectID: a.SubjectID, Scope: a.Scope,
		CCNotesTask: a.CCNotesTask, RestartCount: a.RestartCount, LastRestartAt: a.LastRestartAt,
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

// restartedEvent is the EventRestarted body appended when the supervisor re-spawns
// a vanished agent: the new terminal handle and the attempt count it reached. Type
// discriminates the frame.
type restartedEvent struct {
	Type     string `json:"type"`
	AgentID  string `json:"agent_id"`
	Backend  string `json:"backend"`
	Terminal string `json:"terminal"`
	Attempt  int    `json:"attempt"`
}

func restartedPayload(ag agentRow, terminal string, attempt int) json.RawMessage {
	b, _ := json.Marshal(restartedEvent{
		Type: EventRestarted, AgentID: ag.ID, Backend: string(ag.Backend), Terminal: terminal, Attempt: attempt,
	})
	return b
}

// abandonedEvent is the EventAbandoned body appended when an agent's restart budget
// is exhausted, just before the terminal EventExited. Type discriminates the frame.
type abandonedEvent struct {
	Type     string `json:"type"`
	AgentID  string `json:"agent_id"`
	Attempts int    `json:"attempts"`
}

func abandonedPayload(ag agentRow) json.RawMessage {
	b, _ := json.Marshal(abandonedEvent{Type: EventAbandoned, AgentID: ag.ID, Attempts: ag.RestartCount})
	return b
}

// parseStatusFilter validates an optional lifecycle-status filter from a list
// request: "" means every status, otherwise it must name one of the three states.
func parseStatusFilter(s string) (LifecycleStatus, error) {
	switch LifecycleStatus(s) {
	case "", StatusActive, StatusExited, StatusKilled:
		return LifecycleStatus(s), nil
	default:
		return "", opErr(codeInvalidRequest, fmt.Errorf("unknown status filter %q (want active, exited, or killed)", s))
	}
}

// agentShowRequest addresses one agent by id.
type agentShowRequest struct {
	AgentID string `json:"agent_id"`
}

// handleStatus answers cco.agent.show with one agent's flattened view.
func handleStatus(hc daemon.HandlerCtx, req agentShowRequest) (agentView, error) {
	ag, err := getAgent(hc.Ctx, hc.DB, req.AgentID)
	if err != nil {
		return agentView{}, err
	}
	return newAgentView(ag), nil
}

// agentListRequest lists agents, optionally filtered by repo and lifecycle status.
type agentListRequest struct {
	Repo   string `json:"repo,omitempty"`
	Status string `json:"status,omitempty"`
}

// handleList answers cco.agent.list with every agent, optionally filtered to one repo
// (by id or name) and one lifecycle status.
func handleList(hc daemon.HandlerCtx, req agentListRequest) ([]agentView, error) {
	status, err := parseStatusFilter(req.Status)
	if err != nil {
		return nil, err
	}
	var agents []agentRow
	if req.Repo != "" {
		repo, repoErr := getRepo(hc.Ctx, hc.DB, req.Repo)
		if repoErr != nil {
			return nil, repoErr
		}
		agents, err = listRepoAgents(hc.Ctx, hc.DB, repo.ID, status)
	} else {
		agents, err = listAgents(hc.Ctx, hc.DB, "", status)
	}
	if err != nil {
		return nil, err
	}
	views := make([]agentView, len(agents))
	for i, a := range agents {
		views[i] = newAgentView(a)
	}
	return views, nil
}

// handleSendMessage answers cco.agent.sendMessage: it delivers the text to the agent
// natively when the backend can type into its terminal (CanSendText), else by
// appending an OriginHuman EventMessage the agent's watch Monitor consumes (the
// LCD). The native path writes no event-plane frame; the transcript tailer emits
// an EventInbound audit frame when the typed turn lands.
// agentSendMessageRequest delivers text to a running agent.
type agentSendMessageRequest struct {
	AgentID string `json:"agent_id"`
	Text    string `json:"text"`
}

// agentSendMessageResult reports the delivery: the event-log seq on the LCD path (0
// on the native path) and which transport carried it.
type agentSendMessageResult struct {
	Seq       int64  `json:"seq"`
	Transport string `json:"transport"`
}

func handleSendMessage(hc daemon.HandlerCtx, req agentSendMessageRequest) (agentSendMessageResult, error) {
	ag, err := getAgent(hc.Ctx, hc.DB, req.AgentID)
	if err != nil {
		return agentSendMessageResult{}, err
	}
	if ag.SubjectID == "" {
		return agentSendMessageResult{}, opErr(codeConflict, fmt.Errorf("agent has no subject: %s", req.AgentID))
	}
	bk, ok := backend.Get(ag.Backend)
	if !ok {
		return agentSendMessageResult{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", ag.Backend))
	}
	native, seq, err := deliverMessage(hc, bk, ag, req.Text)
	if err != nil {
		return agentSendMessageResult{}, err
	}
	fleetLog.emit(hc.Ctx, messageFrame(ag.ID))
	return agentSendMessageResult{Seq: seq, Transport: transportLabel(native)}, nil
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
		handle, err := backendAgentHandle(hc.Ctx, hc.DB, ag)
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

// handleReport answers cco.agent.report by appending an OriginAgent EventReport to the
// reporting agent's subject log. The subject is resolved from the child's session
// and scope (the channel server stamps both), so the agent never needs to know its
// own subject id. An unresolvable subject is an error.
// agentReportResult reports the event-log seq the report frame landed at.
type agentReportResult struct {
	Seq int64 `json:"seq"`
}

func handleReport(hc daemon.HandlerCtx, req reportRequest) (agentReportResult, error) {
	sub, ok, err := hc.Subjects.Find(hc.Ctx, hc.Window, hc.Scope)
	if err != nil {
		return agentReportResult{}, err
	}
	if !ok {
		return agentReportResult{}, opErr(codeNotFound, fmt.Errorf("no subject for session %s in scope %s", hc.Env.Session, hc.Scope))
	}
	payload, _ := json.Marshal(reportPayload{Type: EventReport, Text: req.Text, State: req.State})
	seq, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: sub.ID, Origin: event.OriginAgent, Type: EventReport, Payload: payload,
	})
	if err != nil {
		return agentReportResult{}, err
	}
	emitReport(hc.Ctx, hc.DB, sub.ID, req.State)
	return agentReportResult{Seq: seq}, nil
}

// configSetRequest upserts one config key.
type configSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// configSetResult echoes the upserted key and value.
type configSetResult struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// handleConfigSet answers cco.config.set by upserting one config key.
func handleConfigSet(hc daemon.HandlerCtx, req configSetRequest) (configSetResult, error) {
	if err := setConfig(hc.Ctx, hc.DB, req.Key, req.Value); err != nil {
		return configSetResult{}, err
	}
	return configSetResult{Key: req.Key, Value: req.Value}, nil
}

// configListRequest takes no arguments; cco.config.list returns every key.
type configListRequest struct{}

// handleConfigList answers cco.config.list with every persisted key-value pair.
func handleConfigList(hc daemon.HandlerCtx, _ configListRequest) ([]configEntry, error) {
	return listConfig(hc.Ctx, hc.DB)
}

// configUnsetRequest deletes one config key.
type configUnsetRequest struct {
	Key string `json:"key"`
}

// configUnsetResult echoes the key and whether it was set before deletion.
type configUnsetResult struct {
	Key   string `json:"key"`
	Found bool   `json:"found"`
}

// handleConfigUnset answers cco.config.unset by deleting one config key, reporting
// whether it was set beforehand.
func handleConfigUnset(hc daemon.HandlerCtx, req configUnsetRequest) (configUnsetResult, error) {
	_, found, err := getConfig(hc.Ctx, hc.DB, req.Key)
	if err != nil {
		return configUnsetResult{}, err
	}
	if err := clearConfig(hc.Ctx, hc.DB, req.Key); err != nil {
		return configUnsetResult{}, err
	}
	return configUnsetResult{Key: req.Key, Found: found}, nil
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
	ID              string `json:"id"`
	RepoID          string `json:"repo_id"`
	Name            string `json:"name"`
	Backend         string `json:"backend"`
	WorkspaceHandle string `json:"workspace_handle"`
	Branch          string `json:"branch"`
	Worktree        string `json:"worktree"`
	IsPrimary       bool   `json:"is_primary"`
	CCNotesProject  string `json:"ccnotes_project"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
}

func newWorkstreamView(w workstreamRow) workstreamView {
	return workstreamView{
		ID: w.ID, RepoID: w.RepoID, Name: w.Name, Backend: string(w.Backend), WorkspaceHandle: w.WorkspaceHandle,
		Branch: w.Branch, Worktree: w.Worktree, IsPrimary: w.IsPrimary, CCNotesProject: w.CCNotesProject,
		Status: string(w.Status), CreatedAt: w.CreatedAt,
	}
}

// sprintView is the JSON shape every sprint-facing op returns, flattened from
// sprintRow so a parent never sees the internal column names.
type sprintView struct {
	ID            string `json:"id"`
	WorkstreamID  string `json:"workstream_id"`
	Name          string `json:"name"`
	CCNotesSprint string `json:"ccnotes_sprint"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
}

func newSprintView(sp sprintRow) sprintView {
	return sprintView{
		ID: sp.ID, WorkstreamID: sp.WorkstreamID, Name: sp.Name, CCNotesSprint: sp.CCNotesSprint,
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
// agent spawns into when no sprint is named — named defaultSprintName and active, bound
// to ccnotesSprint (empty when cc-notes is not in play), returning its id so the caller's
// fleet.sprint.created frame carries the row's actual slug. It is the shared step both
// handleRepoCreate (for the primary workstream) and handleWorkstreamCreate run right
// after persisting their workstream row.
func createDefaultSprint(ctx context.Context, db *sql.DB, workstreamID, ccnotesSprint string) (string, error) {
	sp := sprintRow{
		ID: sprintSlug(defaultSprintName), WorkstreamID: workstreamID,
		Name: defaultSprintName, CCNotesSprint: ccnotesSprint, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertSprint(ctx, db, sp); err != nil {
		return "", err
	}
	return sp.ID, nil
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
func resolveBackend(hc daemon.HandlerCtx, explicit string) (backend.Backend, backend.Name, error) {
	name := backend.Name(explicit)
	if name == "" {
		value, found, err := getConfig(hc.Ctx, hc.DB, configBackend)
		if err != nil {
			return nil, "", err
		}
		if found {
			name = backend.Name(value)
		}
	}
	if name != "" {
		b, ok := backend.Get(name)
		if !ok {
			return nil, "", opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", name))
		}
		return b, name, nil
	}
	b, ok := backend.Select()
	if !ok {
		installable := make([]string, len(backend.Precedence))
		for i, n := range backend.Precedence {
			installable[i] = string(n)
		}
		return nil, "", opErr(codeUnsupported, fmt.Errorf("no available backend; install one of %s", strings.Join(installable, ", ")))
	}
	return b, b.Name(), nil
}

// handleRepoCreate answers cco.repo.create: it resolves the backend, forks the repo's
// primary workstream backend workspace and provisions its cc-notes bindings, and only
// once those succeed persists the repo row keyed by a slug of its name, the primary
// workstream bound to that workspace, and the workstream's own default sprint — so a
// backend or cc-notes failure never orphans a repo row. The primary workstream tracks
// the repo's current branch: its worktree is the repo root for a backend cc-orchestrate
// drives with git, or the backend's own forked worktree for a ManagesWorktree backend
// (superset). It reports the new repo id, the primary workstream's workspace handle,
// and the backend.
// repoCreateRequest creates a repo and its primary workstream's backend workspace.
type repoCreateRequest struct {
	Name    string `json:"name"`
	Backend string `json:"backend,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
}

// repoCreateResult reports the new repo id, its primary workstream's workspace
// handle, and the backend it landed on.
type repoCreateResult struct {
	RepoID    string `json:"repo_id"`
	Workspace string `json:"workspace"`
	Backend   string `json:"backend"`
}

func handleRepoCreate(hc daemon.HandlerCtx, req repoCreateRequest) (repoCreateResult, error) {
	bk, bname, err := resolveBackend(hc, req.Backend)
	if err != nil {
		return repoCreateResult{}, err
	}
	if err := bk.EnsureReady(hc.Ctx); err != nil {
		return repoCreateResult{}, err
	}
	cwd := cmp.Or(req.Cwd, ".")
	if !filepath.IsAbs(cwd) {
		if hc.Scope == "" {
			return repoCreateResult{}, opErr(codeInvalidRequest, fmt.Errorf("relative cwd %q requires an absolute path when called with no scope", cwd))
		}
		cwd = filepath.Join(hc.Scope, cwd)
	}
	canonical, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return repoCreateResult{}, fmt.Errorf("canonicalizing repo cwd %q: %w", cwd, err)
	}
	cwd = canonical
	branch, err := worktree.CurrentBranch(hc.Ctx, cwd)
	if err != nil {
		return repoCreateResult{}, err
	}
	handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: branch, Cwd: cwd, RepoCwd: cwd, Branch: branch})
	if err != nil {
		return repoCreateResult{}, err
	}
	ccProject, ccSprint, err := provisionCCNotes(hc.Ctx, cwd, req.Name)
	if err != nil {
		return repoCreateResult{}, err
	}
	// A ManagesWorktree backend owns its own forked worktree; adopt the path it
	// returns. Every other backend's primary workstream is the repo root itself.
	worktreePath := cwd
	if bk.Caps().Has(backend.ManagesWorktree) {
		worktreePath = handle.Worktree
	}
	p := repoRow{
		ID: repoSlug(req.Name), Name: req.Name, Backend: bname,
		Cwd: cwd, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertRepo(hc.Ctx, hc.DB, p); err != nil {
		return repoCreateResult{}, err
	}
	w := workstreamRow{
		ID: workstreamSlug(branch), RepoID: p.ID, Name: branch, Backend: bname,
		WorkspaceHandle: handle.ID, Branch: branch, Worktree: worktreePath, IsPrimary: true,
		CCNotesProject: ccProject, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertWorkstream(hc.Ctx, hc.DB, w); err != nil {
		return repoCreateResult{}, err
	}
	sprintID, err := createDefaultSprint(hc.Ctx, hc.DB, w.ID, ccSprint)
	if err != nil {
		return repoCreateResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameRepoCreated, p.ID, p.Name))
	fleetLog.emit(hc.Ctx, containerFrame(FrameWorkstreamCreated, w.ID, w.Name))
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintCreated, sprintID, defaultSprintName))
	return repoCreateResult{RepoID: p.ID, Workspace: handle.ID, Backend: string(bname)}, nil
}

// repoListRequest lists repos, optionally filtered by lifecycle status.
type repoListRequest struct {
	Status string `json:"status,omitempty"`
}

// handleRepoList answers cco.repo.list with every repo's flattened view, optionally
// filtered by lifecycle status.
func handleRepoList(hc daemon.HandlerCtx, req repoListRequest) ([]repoView, error) {
	status, err := parseStatusFilter(req.Status)
	if err != nil {
		return nil, err
	}
	repos, err := listRepos(hc.Ctx, hc.DB, status)
	if err != nil {
		return nil, err
	}
	views := make([]repoView, len(repos))
	for i, p := range repos {
		views[i] = newRepoView(p)
	}
	return views, nil
}

// repoShowRequest addresses one repo by id or name.
type repoShowRequest struct {
	ID string `json:"id"`
}

// handleRepoShow answers cco.repo.show with one repo's flattened view.
func handleRepoShow(hc daemon.HandlerCtx, req repoShowRequest) (repoView, error) {
	repo, err := getRepo(hc.Ctx, hc.DB, req.ID)
	if err != nil {
		return repoView{}, err
	}
	return newRepoView(repo), nil
}

// handleRepoActivate answers cco.repo.activate: it resolves the repo (by id
// or name, erroring when missing), marks it active, and records it as the active
// repo so an agent spawn with no repo or workstream falls back to its primary
// workstream. Activating a repo resets the precedence chain — it clears the
// higher-precedence active workstream and sprint — so the most recent activation
// wins and a stale active sprint can never silently misroute a bare spawn.
// repoActivateRequest and repoKillRequest address one repo by id or name.
type repoActivateRequest struct {
	ID string `json:"id"`
}

// repoLifecycleResult reports a repo's id and its post-op lifecycle status, the shape
// both cco.repo.activate and cco.repo.kill return.
type repoLifecycleResult struct {
	RepoID string `json:"repo_id"`
	Status string `json:"status"`
}

func handleRepoActivate(hc daemon.HandlerCtx, req repoActivateRequest) (repoLifecycleResult, error) {
	repo, err := getRepo(hc.Ctx, hc.DB, req.ID)
	if err != nil {
		return repoLifecycleResult{}, err
	}
	if err := setRepoStatus(hc.Ctx, hc.DB, repo.ID, StatusActive); err != nil {
		return repoLifecycleResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, repo.ID); err != nil {
		return repoLifecycleResult{}, err
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveWorkstream); err != nil {
		return repoLifecycleResult{}, err
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveSprint); err != nil {
		return repoLifecycleResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameRepoActivated, repo.ID, repo.Name))
	return repoLifecycleResult{RepoID: repo.ID, Status: string(StatusActive)}, nil
}

// backendAgentHandle resolves an agent's backend handle: its terminal addressed
// within its workstream's backend workspace, reached through the agent's sprint. The
// handle's WorkstreamID is the backend WorkspaceHandle (what cmux's --workspace and
// zellij's --session expect), not the orchestrate workstream id — those are
// different values, and addressing the terminal needs the backend one.
func backendAgentHandle(ctx context.Context, db *sql.DB, ag agentRow) (backend.AgentHandle, error) {
	sprint, err := getSprint(ctx, db, ag.SprintID, "")
	if err != nil {
		return backend.AgentHandle{}, err
	}
	ws, err := getWorkstream(ctx, db, sprint.WorkstreamID, "")
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

// handleAgentKill answers cco.agent.kill: it stops the agent's transcript tailer,
// terminates the backend terminal, marks the row exited, and appends a terminal
// EventExited. A backend kill failure is surfaced after the row is already marked
// exited, so a half-dead agent never lingers as active.
//
// It re-reads the row under agentLock before deciding anything — mirroring
// reconcileVanished — so a supervisor respawn that lands between the request and the
// lock is observed: the kill targets the agent's current terminal (a freshly respawned
// one), never the stale handle the request raced against, and a concurrent kill that
// already exited the row is an idempotent no-op rather than a second EventExited.
// agentKillRequest addresses one agent by id.
type agentKillRequest struct {
	AgentID string `json:"agent_id"`
}

// agentKillResult reports the agent id and its post-kill lifecycle status.
type agentKillResult struct {
	AgentID string `json:"agent_id"`
	Status  string `json:"status"`
}

func handleAgentKill(hc daemon.HandlerCtx, req agentKillRequest) (agentKillResult, error) {
	mu := agentLock(req.AgentID)
	mu.Lock()
	defer mu.Unlock()
	ag, err := getAgent(hc.Ctx, hc.DB, req.AgentID)
	if err != nil {
		return agentKillResult{}, err
	}
	if ag.Status != StatusActive {
		// Already terminal (its repo was killed, or a concurrent kill won the lock
		// first): idempotent no-op, never a second EventExited or a re-kill of a dead
		// terminal.
		return agentKillResult{AgentID: ag.ID, Status: string(ag.Status)}, nil
	}
	// Mark the row exited and append the terminal EventExited before the backend
	// kill, so a failed kill still ends the row — a half-dead agent never lingers
	// active.
	if err := softExitAgent(hc.Ctx, hc.DB, hc.Append, ag); err != nil {
		return agentKillResult{}, err
	}
	// Emit after the durable mutations, before the backend teardown, so a teardown
	// failure can no longer suppress the exited frame.
	fleetLog.emit(hc.Ctx, exitedFrame(ag.ID, reasonKilled))
	if err := killAgentTerminal(hc.Ctx, hc.DB, ag); err != nil {
		return agentKillResult{}, err
	}
	return agentKillResult{AgentID: ag.ID, Status: string(StatusExited)}, nil
}

// agentCaptureRequest addresses one active agent by id.
type agentCaptureRequest struct {
	AgentID string `json:"agent_id"`
}

// agentCaptureResult reports an agent's captured terminal screen.
type agentCaptureResult struct {
	AgentID    string `json:"agent_id"`
	Content    string `json:"content"`
	CapturedAt string `json:"captured_at"`
}

// handleAgentCapture answers cco.agent.capture: it reads an active agent's current
// terminal screen via captureScreenText, the same codepath handleSerialize snapshots
// through — capture is universal via the pty-host wrapper, so this never gates on a
// backend's CanCapture.
func handleAgentCapture(hc daemon.HandlerCtx, req agentCaptureRequest) (agentCaptureResult, error) {
	ag, err := getAgent(hc.Ctx, hc.DB, req.AgentID)
	if err != nil {
		return agentCaptureResult{}, err
	}
	if ag.Status != StatusActive {
		return agentCaptureResult{}, opErr(codeConflict, fmt.Errorf("agent %s is %s, not active", ag.ID, ag.Status))
	}
	text, err := captureScreenText(hc.Ctx, hc.DB, ag)
	if err != nil {
		return agentCaptureResult{}, err
	}
	return agentCaptureResult{AgentID: ag.ID, Content: text, CapturedAt: nowStamp()}, nil
}

// killAgentTerminal terminates an agent's backend terminal, resolving the backend and
// the kill target from the row's current terminal handle. It is the terminal-teardown
// half handleAgentKill and restore share; callers hold agentLock so the handle it
// reads cannot be swapped mid-kill by a concurrent respawn.
func killAgentTerminal(ctx context.Context, db *sql.DB, ag agentRow) error {
	bk, ok := backend.Get(ag.Backend)
	if !ok {
		return opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", ag.Backend))
	}
	handle, err := backendAgentHandle(ctx, db, ag)
	if err != nil {
		return err
	}
	if err := bk.Kill(ctx, handle); err != nil {
		return fmt.Errorf("kill agent %q: %w", ag.ID, err)
	}
	return nil
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

// markSprintKilled flips a sprint active→killed (compare-and-set) and, when this call
// won the flip, soft-exits its still-active agents and returns their rows (with terminal
// handles) so the caller can emit their exited frames and tear down their terminals. The
// agent list is read AFTER the status commit, so a spawn racing this kill is either
// captured here (its insert landed before the read) or observes the killed sprint on its
// own post-insert re-check — the two cases that close the spawn/kill orphan window. A
// loser of a concurrent flip returns (nil, false, nil) and touches nothing. It never
// calls the backend nor removes a worktree — a sprint has none, it shares its
// workstream's. Reused by the repo- and workstream-kill cascades and boot reconcile.
func markSprintKilled(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, sp sprintRow) ([]agentRow, bool, error) {
	won, err := casSprintKilled(ctx, db, sp.ID)
	if err != nil {
		return nil, false, err
	}
	if !won {
		return nil, false, nil
	}
	agents, err := listAgents(ctx, db, sp.ID, StatusActive)
	if err != nil {
		return nil, false, err
	}
	flipped := make([]agentRow, 0, len(agents))
	for _, ag := range agents {
		row, err := flipAgentExited(ctx, db, appendFn, ag.ID)
		if err != nil {
			return nil, false, err
		}
		if row.ID != "" {
			flipped = append(flipped, row)
		}
	}
	if err := clearActiveSelection(ctx, db, configActiveSprint, sp.ID); err != nil {
		return nil, false, err
	}
	return flipped, true, nil
}

// flipAgentExited soft-exits one still-active agent under agentLock and returns its
// pre-exit row (carrying the terminal handle) for the caller to tear down. An agent some
// other path already exited between the sprint's agent list and this lock — a concurrent
// spawn compensation racing the kill — returns the zero row, so the exit and its terminal
// teardown happen exactly once.
func flipAgentExited(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, id string) (agentRow, error) {
	mu := agentLock(id)
	mu.Lock()
	defer mu.Unlock()
	cur, err := getAgent(ctx, db, id)
	if err != nil {
		return agentRow{}, err
	}
	if cur.Status != StatusActive {
		return agentRow{}, nil
	}
	if err := softExitAgent(ctx, db, appendFn, cur); err != nil {
		return agentRow{}, err
	}
	return cur, nil
}

// teardownAgents kills each formerly-active agent's backend terminal under agentLock,
// re-reading the row so a handle a concurrent respawn swapped is honored, and returns the
// first teardown error after attempting every one. The rows are the exact set a mark step
// flipped, so a terminal is torn down exactly once.
func teardownAgents(ctx context.Context, db *sql.DB, agents []agentRow) error {
	var teardownErr error
	for _, a := range agents {
		if err := func() error {
			mu := agentLock(a.ID)
			mu.Lock()
			defer mu.Unlock()
			cur, err := getAgent(ctx, db, a.ID)
			if err != nil {
				return err
			}
			return killAgentTerminal(ctx, db, cur)
		}(); err != nil && teardownErr == nil {
			teardownErr = err
		}
	}
	return teardownErr
}

// markWorkstreamKilled cascades a workstream to killed (compare-and-set): it flips the
// workstream, then marks every still-active sprint killed and exits their agents,
// returning the aggregate flipped agent rows so the caller can emit their frames and tear
// down their terminals. Like markSprintKilled it reads its children after the status
// commit and returns (nil, false, nil) when it loses a concurrent flip. It never calls
// the backend nor removes the worktree — a caller that must also tear those down does so
// after these row writes. Reused by the repo-kill cascade and boot reconcile when a
// workspace has vanished out-of-band.
func markWorkstreamKilled(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ws workstreamRow) ([]agentRow, bool, error) {
	won, err := casWorkstreamKilled(ctx, db, ws.ID)
	if err != nil {
		return nil, false, err
	}
	if !won {
		return nil, false, nil
	}
	sprints, err := listSprints(ctx, db, ws.ID, StatusActive)
	if err != nil {
		return nil, false, err
	}
	var flipped []agentRow
	for _, sp := range sprints {
		rows, _, err := markSprintKilled(ctx, db, appendFn, sp)
		if err != nil {
			return nil, false, err
		}
		flipped = append(flipped, rows...)
	}
	if err := clearActiveSelection(ctx, db, configActiveWorkstream, ws.ID); err != nil {
		return nil, false, err
	}
	return flipped, true, nil
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
		if removeErr = worktree.Remove(ctx, repo.Cwd, ws.Worktree); removeErr == nil {
			// Drop the now-empty per-repo worktrees dir (…/worktrees/<repoId>)
			// once its last child worktree is gone.
			removeErr = worktree.RemoveDirIfEmpty(filepath.Dir(ws.Worktree))
		}
	}
	if killErr != nil {
		return fmt.Errorf("kill workstream %q: %w", ws.ID, killErr)
	}
	if removeErr != nil {
		return fmt.Errorf("remove worktree for workstream %q: %w", ws.ID, removeErr)
	}
	return nil
}

// killRepo marks a repo killed (compare-and-set) and cascades every active workstream to
// killed, exiting their sprints' agents. It performs only the durable row/subject-log
// mutations, returning the flipped agent rows and the workstreams it flipped so the
// caller can emit their frames and then tear down the backend workspaces and worktrees. A
// loser of a concurrent repo flip returns (nil, nil, false, nil).
func killRepo(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, repo repoRow) (flipped []agentRow, torn []workstreamRow, won bool, err error) {
	won, err = casRepoKilled(ctx, db, repo.ID)
	if err != nil {
		return nil, nil, false, err
	}
	if !won {
		return nil, nil, false, nil
	}
	wss, err := listWorkstreams(ctx, db, repo.ID, StatusActive)
	if err != nil {
		return nil, nil, false, err
	}
	for _, ws := range wss {
		rows, wsWon, err := markWorkstreamKilled(ctx, db, appendFn, ws)
		if err != nil {
			return nil, nil, false, err
		}
		if !wsWon {
			continue
		}
		flipped = append(flipped, rows...)
		torn = append(torn, ws)
	}
	if err := clearActiveSelection(ctx, db, configActiveRepo, repo.ID); err != nil {
		return nil, nil, false, err
	}
	return flipped, torn, true, nil
}

// tearDownWorkstreams tears down each flipped workstream's backend workspace and, for a
// non-primary worktree on a backend that does not manage its own, its git worktree —
// after the repo-kill's row mutations are committed and its frames emitted. It returns the
// first teardown error after attempting every one; an unknown backend does not abort the
// others.
func tearDownWorkstreams(ctx context.Context, repo repoRow, wss []workstreamRow) error {
	var teardownErr error
	for _, ws := range wss {
		bk, ok := backend.Get(ws.Backend)
		if !ok {
			if teardownErr == nil {
				teardownErr = fmt.Errorf("unknown backend %q for workstream %q", ws.Backend, ws.ID)
			}
			continue
		}
		if err := tearDownWorkstream(ctx, bk, repo, ws); err != nil && teardownErr == nil {
			teardownErr = err
		}
	}
	return teardownErr
}

// handleRepoKill answers cco.repo.kill: a real teardown that tears down every workstream's
// backend workspace and non-primary worktree, cascades their sprints to killed and
// agents to exited, then marks the repo killed. Teardown errors are surfaced after the
// row mutations (mirroring handleAgentKill), so a half-dead repo never lingers active.
// repoKillRequest addresses one repo by id or name.
type repoKillRequest struct {
	ID string `json:"id"`
}

func handleRepoKill(hc daemon.HandlerCtx, req repoKillRequest) (repoLifecycleResult, error) {
	repo, err := getRepo(hc.Ctx, hc.DB, req.ID)
	if err != nil {
		return repoLifecycleResult{}, err
	}
	if repo.Status != StatusActive {
		return repoLifecycleResult{}, opErr(codeConflict, fmt.Errorf("repo %s is %s, not active", repo.ID, repo.Status))
	}
	flipped, torn, won, err := killRepo(hc.Ctx, hc.DB, hc.Append, repo)
	if err != nil {
		return repoLifecycleResult{}, err
	}
	if !won {
		return repoLifecycleResult{}, opErr(codeConflict, fmt.Errorf("repo %s is already being killed", repo.ID))
	}
	// Emit the exact flipped set before teardown, so a teardown failure never suppresses
	// the frames.
	for _, ag := range flipped {
		fleetLog.emit(hc.Ctx, exitedFrame(ag.ID, reasonKilled))
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameRepoKilled, repo.ID, repo.Name))
	if err := tearDownWorkstreams(hc.Ctx, repo, torn); err != nil {
		return repoLifecycleResult{}, err
	}
	return repoLifecycleResult{RepoID: repo.ID, Status: string(StatusKilled)}, nil
}

// handleWorkstreamCreate answers cco.workstream.create: it resolves the repo, then
// branches on whether the backend manages its own worktree. A ManagesWorktree
// backend (superset) forks its own worktree off the branch and cc-orchestrate
// adopts the path it returns; every other backend takes a worktree cc-orchestrate
// creates with git (an independent jj repo colocated inside it when the repo tracks
// with jj) and runs in it as its cwd. Either path yields exactly one worktree per
// workstream. The new workstream gets its own default sprint, so a simple flow never
// touches sprints.
// workstreamCreateRequest creates a workstream — a branch and its worktree — in a
// repo.
type workstreamCreateRequest struct {
	Repo   string `json:"repo,omitempty"`
	Name   string `json:"name"`
	Branch string `json:"branch,omitempty"`
}

// workstreamCreateResult reports the new workstream and the resources it created.
type workstreamCreateResult struct {
	WorkstreamID string `json:"workstream_id"`
	RepoID       string `json:"repo_id"`
	Workspace    string `json:"workspace"`
	Branch       string `json:"branch"`
	Worktree     string `json:"worktree"`
}

func handleWorkstreamCreate(hc daemon.HandlerCtx, req workstreamCreateRequest) (workstreamCreateResult, error) {
	repo, err := getRepo(hc.Ctx, hc.DB, req.Repo)
	if err != nil {
		return workstreamCreateResult{}, err
	}
	bk, ok := backend.Get(repo.Backend)
	if !ok {
		return workstreamCreateResult{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", repo.Backend))
	}
	if err := bk.EnsureReady(hc.Ctx); err != nil {
		return workstreamCreateResult{}, err
	}
	branch := cmp.Or(req.Branch, req.Name)
	repoRoot := repo.Cwd

	var path, workspaceHandle string
	if bk.Caps().Has(backend.ManagesWorktree) {
		handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: req.Name, Cwd: repoRoot, RepoCwd: repoRoot, Branch: branch})
		if err != nil {
			return workstreamCreateResult{}, err
		}
		path, workspaceHandle = handle.Worktree, handle.ID
		if err := colocateJJ(hc.Ctx, repoRoot, path); err != nil {
			return workstreamCreateResult{}, err
		}
	} else {
		dest := filepath.Join(worktreesBase(), repo.ID, pathComponent(req.Name))
		path, err = worktree.Add(hc.Ctx, repoRoot, dest, branch)
		if err != nil {
			return workstreamCreateResult{}, err
		}
		if err := colocateJJ(hc.Ctx, repoRoot, path); err != nil {
			return workstreamCreateResult{}, err
		}
		handle, err := bk.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: req.Name, Cwd: path, RepoCwd: repoRoot, Branch: branch})
		if err != nil {
			return workstreamCreateResult{}, err
		}
		workspaceHandle = handle.ID
	}

	ccProject, ccSprint, err := provisionCCNotes(hc.Ctx, repoRoot, req.Name)
	if err != nil {
		return workstreamCreateResult{}, err
	}
	w := workstreamRow{
		ID: workstreamSlug(req.Name), RepoID: repo.ID, Name: req.Name, Backend: repo.Backend,
		WorkspaceHandle: workspaceHandle, Branch: branch, Worktree: path, IsPrimary: false,
		CCNotesProject: ccProject, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertWorkstream(hc.Ctx, hc.DB, w); err != nil {
		return workstreamCreateResult{}, err
	}
	sprintID, err := createDefaultSprint(hc.Ctx, hc.DB, w.ID, ccSprint)
	if err != nil {
		return workstreamCreateResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameWorkstreamCreated, w.ID, w.Name))
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintCreated, sprintID, defaultSprintName))
	return workstreamCreateResult{
		WorkstreamID: w.ID, RepoID: repo.ID, Workspace: workspaceHandle, Branch: branch, Worktree: path,
	}, nil
}

// workstreamListRequest lists workstreams, optionally filtered by repo and status.
type workstreamListRequest struct {
	Repo   string `json:"repo,omitempty"`
	Status string `json:"status,omitempty"`
}

// handleWorkstreamList answers cco.workstream.list with every workstream's flattened
// view, optionally filtered to one repo (by id or name) and one lifecycle status.
func handleWorkstreamList(hc daemon.HandlerCtx, req workstreamListRequest) ([]workstreamView, error) {
	status, err := parseStatusFilter(req.Status)
	if err != nil {
		return nil, err
	}
	filter := ""
	if req.Repo != "" {
		repo, err := getRepo(hc.Ctx, hc.DB, req.Repo)
		if err != nil {
			return nil, err
		}
		filter = repo.ID
	}
	wss, err := listWorkstreams(hc.Ctx, hc.DB, filter, status)
	if err != nil {
		return nil, err
	}
	views := make([]workstreamView, len(wss))
	for i, w := range wss {
		views[i] = newWorkstreamView(w)
	}
	return views, nil
}

// workstreamShowRequest addresses one workstream by id or name, scoped to a repo when
// given to disambiguate the name.
type workstreamShowRequest struct {
	ID   string `json:"id"`
	Repo string `json:"repo,omitempty"`
}

// handleWorkstreamShow answers cco.workstream.show with one workstream's flattened
// view.
func handleWorkstreamShow(hc daemon.HandlerCtx, req workstreamShowRequest) (workstreamView, error) {
	ws, err := resolveWorkstreamRef(hc, req.ID, req.Repo)
	if err != nil {
		return workstreamView{}, err
	}
	return newWorkstreamView(ws), nil
}

// handleWorkstreamActivate answers cco.workstream.activate: it resolves the workstream
// (by id or name, scoped to a repo when given to disambiguate; erroring when
// missing), marks it active, and records it as the active workstream so an agent
// spawn with no explicit target lands in it. Activating a workstream resets the
// precedence chain — it sets the active repo to the workstream's repo and clears the
// higher-precedence active sprint — so the most recent activation wins.
// workstreamActivateRequest and workstreamKillRequest address one workstream by id or
// name, scoped to a repo when given to disambiguate the name.
type workstreamActivateRequest struct {
	ID   string `json:"id"`
	Repo string `json:"repo,omitempty"`
}

// workstreamLifecycleResult reports a workstream's id and its post-op lifecycle
// status, the shape both cco.workstream.activate and cco.workstream.kill return.
type workstreamLifecycleResult struct {
	WorkstreamID string `json:"workstream_id"`
	Status       string `json:"status"`
}

func handleWorkstreamActivate(hc daemon.HandlerCtx, req workstreamActivateRequest) (workstreamLifecycleResult, error) {
	ws, err := resolveWorkstreamRef(hc, req.ID, req.Repo)
	if err != nil {
		return workstreamLifecycleResult{}, err
	}
	if err := setWorkstreamStatus(hc.Ctx, hc.DB, ws.ID, StatusActive); err != nil {
		return workstreamLifecycleResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveWorkstream, ws.ID); err != nil {
		return workstreamLifecycleResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, ws.RepoID); err != nil {
		return workstreamLifecycleResult{}, err
	}
	if err := clearConfig(hc.Ctx, hc.DB, configActiveSprint); err != nil {
		return workstreamLifecycleResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameWorkstreamActivated, ws.ID, ws.Name))
	return workstreamLifecycleResult{WorkstreamID: ws.ID, Status: string(StatusActive)}, nil
}

// handleWorkstreamKill answers cco.workstream.kill: a real teardown. It tears down the
// workstream's backend workspace and — for a backend that does not manage its own
// worktree, and only for a non-primary workstream — removes its git worktree, then
// cascades its agents to exited and marks the workstream killed. It never removes a
// primary workstream's worktree (the repo root) nor a ManagesWorktree backend's
// worktree (the backend owns that and drops it on KillWorkstream). It then cascades
// the workstream's sprints to killed and their agents to exited. The teardown errors
// are surfaced after the row mutations, mirroring handleAgentKill, so a half-dead
// workstream never lingers active.
// workstreamKillRequest addresses one workstream by id or name, scoped to a repo when
// given to disambiguate the name.
type workstreamKillRequest struct {
	ID   string `json:"id"`
	Repo string `json:"repo,omitempty"`
}

func handleWorkstreamKill(hc daemon.HandlerCtx, req workstreamKillRequest) (workstreamLifecycleResult, error) {
	ws, err := resolveWorkstreamRef(hc, req.ID, req.Repo)
	if err != nil {
		return workstreamLifecycleResult{}, err
	}
	if ws.Status != StatusActive {
		return workstreamLifecycleResult{}, opErr(codeConflict, fmt.Errorf("workstream %s is %s, not active", ws.ID, ws.Status))
	}
	bk, ok := backend.Get(ws.Backend)
	if !ok {
		return workstreamLifecycleResult{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", ws.Backend))
	}
	repo, err := getRepo(hc.Ctx, hc.DB, ws.RepoID)
	if err != nil {
		return workstreamLifecycleResult{}, err
	}
	flipped, won, err := markWorkstreamKilled(hc.Ctx, hc.DB, hc.Append, ws)
	if err != nil {
		return workstreamLifecycleResult{}, err
	}
	if !won {
		return workstreamLifecycleResult{}, opErr(codeConflict, fmt.Errorf("workstream %s is already being killed", ws.ID))
	}
	// Emit the exact flipped set before teardown, so a teardown failure never suppresses
	// the frames.
	for _, ag := range flipped {
		fleetLog.emit(hc.Ctx, exitedFrame(ag.ID, reasonKilled))
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameWorkstreamKilled, ws.ID, ws.Name))
	if err := tearDownWorkstream(hc.Ctx, bk, repo, ws); err != nil {
		return workstreamLifecycleResult{}, err
	}
	return workstreamLifecycleResult{WorkstreamID: ws.ID, Status: string(StatusKilled)}, nil
}

// handleSprintCreate answers cco.sprint.create: it resolves the workstream (by id or
// name, scoped to a repo when given to disambiguate), inserts a new sprint under it,
// and reports the sprint id. A sprint shares its workstream's worktree — it has no
// worktree of its own.
// sprintCreateRequest creates a sprint in a workstream, scoped to a repo when given to
// disambiguate the workstream name.
type sprintCreateRequest struct {
	Workstream string `json:"workstream,omitempty"`
	Repo       string `json:"repo,omitempty"`
	Name       string `json:"name"`
}

// sprintCreateResult reports the new sprint and the workstream it landed in.
type sprintCreateResult struct {
	SprintID     string `json:"sprint_id"`
	WorkstreamID string `json:"workstream_id"`
	Name         string `json:"name"`
}

func handleSprintCreate(hc daemon.HandlerCtx, req sprintCreateRequest) (sprintCreateResult, error) {
	ws, err := resolveWorkstreamRef(hc, req.Workstream, req.Repo)
	if err != nil {
		return sprintCreateResult{}, err
	}
	ccSprint := ""
	if ws.CCNotesProject != "" && ccnotes.Enabled(hc.Ctx, ws.Worktree) {
		ccSprint, err = ccnotes.CreateSprint(hc.Ctx, ws.Worktree, ws.CCNotesProject, req.Name)
		if err != nil {
			return sprintCreateResult{}, err
		}
	}
	sp := sprintRow{
		ID: sprintSlug(req.Name), WorkstreamID: ws.ID, Name: req.Name,
		CCNotesSprint: ccSprint, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertSprint(hc.Ctx, hc.DB, sp); err != nil {
		return sprintCreateResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintCreated, sp.ID, sp.Name))
	return sprintCreateResult{SprintID: sp.ID, WorkstreamID: ws.ID, Name: sp.Name}, nil
}

// sprintListRequest lists sprints, optionally filtered by workstream and status.
type sprintListRequest struct {
	Workstream string `json:"workstream,omitempty"`
	Repo       string `json:"repo,omitempty"`
	Status     string `json:"status,omitempty"`
}

// handleSprintList answers cco.sprint.list with every sprint's flattened view,
// optionally filtered to one workstream (by id or name, scoped to a repo when given)
// and one lifecycle status.
func handleSprintList(hc daemon.HandlerCtx, req sprintListRequest) ([]sprintView, error) {
	status, err := parseStatusFilter(req.Status)
	if err != nil {
		return nil, err
	}
	filter := ""
	if req.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, req.Workstream, req.Repo)
		if err != nil {
			return nil, err
		}
		filter = ws.ID
	}
	sprints, err := listSprints(hc.Ctx, hc.DB, filter, status)
	if err != nil {
		return nil, err
	}
	views := make([]sprintView, len(sprints))
	for i, sp := range sprints {
		views[i] = newSprintView(sp)
	}
	return views, nil
}

// sprintShowRequest addresses one sprint by id or name, scoped to a workstream when
// given to disambiguate the name.
type sprintShowRequest struct {
	ID         string `json:"id"`
	Workstream string `json:"workstream,omitempty"`
}

// handleSprintShow answers cco.sprint.show with one sprint's flattened view.
func handleSprintShow(hc daemon.HandlerCtx, req sprintShowRequest) (sprintView, error) {
	workstreamID := ""
	if req.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, req.Workstream, "")
		if err != nil {
			return sprintView{}, err
		}
		workstreamID = ws.ID
	}
	sp, err := getSprint(hc.Ctx, hc.DB, req.ID, workstreamID)
	if err != nil {
		return sprintView{}, err
	}
	return newSprintView(sp), nil
}

// handleSprintActivate answers cco.sprint.activate: it resolves the sprint (by id or
// name, scoped to a workstream when given to disambiguate), marks it active, and
// records it as the active sprint so an agent spawn with no explicit target lands in
// it. Activating a sprint resets the precedence chain — it sets the active workstream
// to the sprint's workstream and the active repo to that workstream's repo — so the
// whole bare-spawn fallback chain points at the most recent activation.
// sprintActivateRequest addresses one sprint by id or name, scoped to a workstream
// when given to disambiguate the name.
type sprintActivateRequest struct {
	ID         string `json:"id"`
	Workstream string `json:"workstream,omitempty"`
	Repo       string `json:"repo,omitempty"`
}

// sprintActivateResult reports a sprint's id and its post-op lifecycle status.
type sprintActivateResult struct {
	SprintID string `json:"sprint_id"`
	Status   string `json:"status"`
}

func handleSprintActivate(hc daemon.HandlerCtx, req sprintActivateRequest) (sprintActivateResult, error) {
	workstreamID := ""
	if req.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, req.Workstream, req.Repo)
		if err != nil {
			return sprintActivateResult{}, err
		}
		workstreamID = ws.ID
	}
	sp, err := getSprint(hc.Ctx, hc.DB, req.ID, workstreamID)
	if err != nil {
		return sprintActivateResult{}, err
	}
	ws, err := getWorkstream(hc.Ctx, hc.DB, sp.WorkstreamID, "")
	if err != nil {
		return sprintActivateResult{}, err
	}
	if err := setSprintStatus(hc.Ctx, hc.DB, sp.ID, StatusActive); err != nil {
		return sprintActivateResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveSprint, sp.ID); err != nil {
		return sprintActivateResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveWorkstream, sp.WorkstreamID); err != nil {
		return sprintActivateResult{}, err
	}
	if err := setConfig(hc.Ctx, hc.DB, configActiveRepo, ws.RepoID); err != nil {
		return sprintActivateResult{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintActivated, sp.ID, sp.Name))
	return sprintActivateResult{SprintID: sp.ID, Status: string(StatusActive)}, nil
}

// sprintKillRequest addresses one sprint by id or name, scoped to a workstream when
// given to disambiguate the name.
type sprintKillRequest struct {
	ID         string `json:"id"`
	Workstream string `json:"workstream,omitempty"`
	Repo       string `json:"repo,omitempty"`
}

// sprintKillResult reports a sprint's id and its post-op lifecycle status.
type sprintKillResult struct {
	SprintID string `json:"sprint_id"`
	Status   string `json:"status"`
}

// handleSprintKill answers cco.sprint.kill: a real teardown that marks the sprint killed
// (compare-and-set), exits its agents, emits their frames, then tears down their
// terminals. Unlike agent-kill, killing an already-killed sprint — or losing the flip to
// a concurrent kill — is a Conflict, not an idempotent no-op.
func handleSprintKill(hc daemon.HandlerCtx, req sprintKillRequest) (sprintKillResult, error) {
	workstreamID := ""
	if req.Workstream != "" {
		ws, err := resolveWorkstreamRef(hc, req.Workstream, req.Repo)
		if err != nil {
			return sprintKillResult{}, err
		}
		workstreamID = ws.ID
	}
	sp, err := getSprint(hc.Ctx, hc.DB, req.ID, workstreamID)
	if err != nil {
		return sprintKillResult{}, err
	}
	if sp.Status != StatusActive {
		return sprintKillResult{}, opErr(codeConflict, fmt.Errorf("sprint %s is %s, not active", sp.ID, sp.Status))
	}
	flipped, won, err := markSprintKilled(hc.Ctx, hc.DB, hc.Append, sp)
	if err != nil {
		return sprintKillResult{}, err
	}
	if !won {
		return sprintKillResult{}, opErr(codeConflict, fmt.Errorf("sprint %s is already being killed", sp.ID))
	}
	// Emit the exact flipped set before teardown, so a teardown failure never suppresses
	// the frames.
	for _, ag := range flipped {
		fleetLog.emit(hc.Ctx, exitedFrame(ag.ID, reasonKilled))
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintKilled, sp.ID, sp.Name))
	if err := teardownAgents(hc.Ctx, hc.DB, flipped); err != nil {
		return sprintKillResult{}, err
	}
	return sprintKillResult{SprintID: sp.ID, Status: string(StatusKilled)}, nil
}

// configGetRequest reads one config key.
type configGetRequest struct {
	Key string `json:"key"`
}

// configGetResult reports one config key's value and whether it is set.
type configGetResult struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

// handleConfigGet answers cco.config.get with one config key's value and whether it is
// set.
func handleConfigGet(hc daemon.HandlerCtx, req configGetRequest) (configGetResult, error) {
	value, found, err := getConfig(hc.Ctx, hc.DB, req.Key)
	if err != nil {
		return configGetResult{}, err
	}
	return configGetResult{Value: value, Found: found}, nil
}
