package orchestrate

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ccnotes"
)

// lifecycle is the subject lifecycle the orchestrator writes: a spawned agent's
// subject is born "active" (matching daemon.Config.ActiveStatuses) and closes to
// "exited" when the agent terminates.
var lifecycle = subject.Lifecycle{Initial: string(StatusActive), Closed: string(StatusExited)}

var lookupPath = exec.LookPath

// spawnAfterInsert is a test seam fired between the agent insert and the hierarchy
// re-check, so a test can kill the target sprint in that exact window and drive the
// compensation path deterministically. A no-op in production.
var spawnAfterInsert = func() {}

// hookCommand is one command hook in the child's --settings.
type hookCommand struct {
	Type    string `json:"type"`
	Command string `json:"command"`
}

// hookMatcher binds an optional tool matcher to its command hooks.
type hookMatcher struct {
	Matcher string        `json:"matcher,omitempty"`
	Hooks   []hookCommand `json:"hooks"`
}

// childSettings is the child's --settings: a SessionStart hook that records the
// child's window for session-rotation rebind, and a PreToolUse edit gate over
// the file-mutating tools. Channel opt-in rides the --channels flag, not here —
// the settings channels key does not feed the session channel gate.
func childSettings(self string) string {
	q := backend.ShellQuote(self)
	b, _ := json.Marshal(map[string]map[string][]hookMatcher{
		"hooks": {
			"SessionStart": {{Hooks: []hookCommand{{Type: "command", Command: q + " session-record"}}}},
			"PreToolUse":   {{Matcher: "Edit|Write|NotebookEdit", Hooks: []hookCommand{{Type: "command", Command: q + " guard-edit"}}}},
		},
	})
	return string(b)
}

// claudeCommand is the full argv a backend runs for a child agent. It launches
// through ccp when installed and otherwise starts bare claude. The orchestration
// brief is appended to the child's system prompt so it arms its receive Monitor
// and knows to report back. An empty prompt omits the trailing positional argument.
func claudeCommand(self, sid, scope, prompt string) []string {
	argv := append(claudeInvocation(),
		"--session-id", sid,
		"--channels", channelPlugin.ChannelID(),
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope),
	)
	if prompt != "" {
		argv = append(argv, prompt)
	}
	return argv
}

// resumeCommand is the argv a backend runs to bring a vanished terminal back: it
// resumes the same Claude session (same sid → same transcript), so no work is
// lost and the re-appended spawnBrief re-arms the watch Monitor. Unlike
// claudeCommand it passes --resume sid instead of --session-id sid, never
// --fork-session, and carries no trailing positional prompt — the resumed
// session already holds its history.
func resumeCommand(self, sid, scope string) []string {
	return append(claudeInvocation(),
		"--resume", sid,
		"--channels", channelPlugin.ChannelID(),
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope),
	)
}

func claudeInvocation() []string {
	if ccp, err := lookupPath("ccp"); err == nil {
		return []string{ccp, "run"}
	}
	return []string{"claude"}
}

// spawnBrief is the orchestration brief appended to a child agent's system prompt.
// It tells the agent it is orchestrated, how to RECEIVE instructions (arm a
// persistent watch Monitor), and how to REPORT back (the report MCP tool). The
// watch command is the exact line the agent must run; self and scope are
// POSIX-shell-quoted so the Monitor's shell split sees one token each.
func spawnBrief(self, sid, scope string) string {
	receive := channel.ReceiveProtocol(channel.ReceiveProtocolSpec{
		Watch:      fmt.Sprintf("%s watch --session %s --cwd %s", backend.ShellQuote(self), sid, backend.ShellQuote(scope)),
		Source:     channelPlugin.Source(channelServer),
		Ack:        fmt.Sprintf("%s channel-ack --session %s --cwd %s", backend.ShellQuote(self), sid, backend.ShellQuote(scope)),
		DedupeHint: `Deduplicate by the message's "id" field: the same message may arrive on both paths around the switchover.`,
	})
	return `You are a cc-orchestrate agent: a Claude Code instance spawned and supervised by an orchestrator. Two channels connect you to it.

RECEIVE:
` + receive + `

4. Only "orchestrate.message" events are directives to act on. Their "text" field is an instruction from your orchestrator. The channel also pushes other event types, such as status frames; ignore them.

REPORT: to send progress, a result, or a question back to your orchestrator, call the "report" tool from the cc-orchestrate MCP server with a short "text" and an optional "state" of "working", "blocked", or "done". Report when you start, when you finish, and whenever you are blocked or need a decision.`
}

// agentSlug is a spawned subject's stable, unique URL name, derived from the
// child's session id (itself a uuid).
func agentSlug(sid string) string { return "agent-" + sid }

// resolveSpawnSprint picks the sprint an agent spawns into, in precedence order: an
// explicit sprint (id or name, scoped to an explicit workstream when one is given);
// else the explicit workstream's default sprint; else the explicit repo's primary
// workstream's default sprint; else the active sprint (config); else the active
// workstream's default sprint; else the active repo's primary workstream's default
// sprint. A by-name workstream lookup is scoped to an explicit repo to disambiguate.
// It errors when none resolves.
func resolveSpawnSprint(hc daemon.HandlerCtx, reqSprint, reqWorkstream, reqRepo string) (sprintRow, error) {
	if reqSprint != "" {
		workstreamID := ""
		if reqWorkstream != "" {
			ws, err := resolveWorkstreamRef(hc, reqWorkstream, reqRepo)
			if err != nil {
				return sprintRow{}, err
			}
			workstreamID = ws.ID
		}
		return getSprint(hc.Ctx, hc.DB, reqSprint, workstreamID)
	}
	if reqWorkstream != "" {
		ws, err := resolveWorkstreamRef(hc, reqWorkstream, reqRepo)
		if err != nil {
			return sprintRow{}, err
		}
		return getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
	}
	if reqRepo != "" {
		repo, err := getRepo(hc.Ctx, hc.DB, reqRepo)
		if err != nil {
			return sprintRow{}, err
		}
		ws, err := getPrimaryWorkstream(hc.Ctx, hc.DB, repo.ID)
		if err != nil {
			return sprintRow{}, err
		}
		return getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
	}
	if id, found, err := getConfig(hc.Ctx, hc.DB, configActiveSprint); err != nil {
		return sprintRow{}, err
	} else if found && id != "" {
		return getSprint(hc.Ctx, hc.DB, id, "")
	}
	if id, found, err := getConfig(hc.Ctx, hc.DB, configActiveWorkstream); err != nil {
		return sprintRow{}, err
	} else if found && id != "" {
		ws, err := getWorkstream(hc.Ctx, hc.DB, id, "")
		if err != nil {
			return sprintRow{}, err
		}
		return getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
	}
	if id, found, err := getConfig(hc.Ctx, hc.DB, configActiveRepo); err != nil {
		return sprintRow{}, err
	} else if found && id != "" {
		repo, err := getRepo(hc.Ctx, hc.DB, id)
		if err != nil {
			return sprintRow{}, err
		}
		ws, err := getPrimaryWorkstream(hc.Ctx, hc.DB, repo.ID)
		if err != nil {
			return sprintRow{}, err
		}
		return getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
	}
	return sprintRow{}, opErr(codeInvalidRequest, fmt.Errorf("no sprint specified and no active sprint, workstream, or repo"))
}

// requireActiveHierarchy resolves sprint's workstream and repo and errors Conflict if
// any of the three is not active — the gate a bare spawn enforces so it can never
// attach a live agent to a soft-killed target, reused by respawn eligibility so an
// exited agent is revived under the identical rule.
func requireActiveHierarchy(hc daemon.HandlerCtx, sprint sprintRow) (workstreamRow, repoRow, error) {
	ws, err := getWorkstream(hc.Ctx, hc.DB, sprint.WorkstreamID, "")
	if err != nil {
		return workstreamRow{}, repoRow{}, err
	}
	repo, err := getRepo(hc.Ctx, hc.DB, ws.RepoID)
	if err != nil {
		return workstreamRow{}, repoRow{}, err
	}
	if sprint.Status != StatusActive {
		return workstreamRow{}, repoRow{}, opErr(codeConflict, fmt.Errorf("sprint %s is %s, not active", sprint.ID, sprint.Status))
	}
	if ws.Status != StatusActive {
		return workstreamRow{}, repoRow{}, opErr(codeConflict, fmt.Errorf("workstream %s is %s, not active", ws.ID, ws.Status))
	}
	if repo.Status != StatusActive {
		return workstreamRow{}, repoRow{}, opErr(codeConflict, fmt.Errorf("repo %s is %s, not active", repo.ID, repo.Status))
	}
	return ws, repo, nil
}

// handleSpawn answers cco.agent.spawn: it resolves the target sprint, derives its
// workstream and backend, requires the whole resolved hierarchy (sprint, workstream,
// repo) is still active so a bare spawn can never attach a live agent to a soft-killed
// target, provisions the agent's cc-notes task, creates a subject keyed by the new
// child's session id, spawns the claude command into the workstream's backend
// workspace, persists the agent row bound to the sprint, and starts its transcript
// tailer.
// agentSpawnRequest spawns a child agent into a sprint, a workstream's or repo's
// default sprint, or the active sprint. Every field is optional; the handler resolves
// the target hierarchy through the precedence chain.
type agentSpawnRequest struct {
	Repo       string `json:"repo,omitempty"`
	Workstream string `json:"workstream,omitempty"`
	Sprint     string `json:"sprint,omitempty"`
	Name       string `json:"name,omitempty"`
	Cwd        string `json:"cwd,omitempty"`
	Prompt     string `json:"prompt,omitempty"`
}

// agentSpawnResult reports the new agent id, its subject, terminal handle, and
// backend.
type agentSpawnResult struct {
	ID        string `json:"id"`
	SubjectID string `json:"subject_id"`
	Terminal  string `json:"terminal"`
	Backend   string `json:"backend"`
}

func handleSpawn(hc daemon.HandlerCtx, req agentSpawnRequest) (agentSpawnResult, error) {
	sprint, err := resolveSpawnSprint(hc, req.Sprint, req.Workstream, req.Repo)
	if err != nil {
		return agentSpawnResult{}, err
	}
	ws, _, err := requireActiveHierarchy(hc, sprint)
	if err != nil {
		return agentSpawnResult{}, err
	}
	bname := ws.Backend
	b, ok := backend.Get(bname)
	if !ok {
		return agentSpawnResult{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", bname))
	}
	if err := b.EnsureReady(hc.Ctx); err != nil {
		return agentSpawnResult{}, err
	}
	scope := cmp.Or(req.Cwd, ws.Worktree)
	if !filepath.IsAbs(scope) {
		if hc.Scope == "" {
			return agentSpawnResult{}, opErr(codeInvalidRequest, fmt.Errorf("relative cwd %q requires an absolute path when called with no scope", scope))
		}
		scope = filepath.Join(hc.Scope, scope)
	}
	self, err := os.Executable()
	if err != nil {
		return agentSpawnResult{}, err
	}

	sid := uuid.NewString()
	// herd rejects an empty agent name; other backends tolerate it. Default once,
	// here, so SpawnSpec and the DB row always agree on the same non-empty name.
	name := cmp.Or(req.Name, "agent-"+sid[:8])

	// cc-notes runs first, before any subject/terminal exists: a cc-notes failure
	// here leaves no started subject, no live claude process, and no agent row — only
	// a residual git-ref task, the same tradeoff provisionCCNotes already accepts.
	ccTask := ""
	if ws.CCNotesProject != "" && sprint.CCNotesSprint != "" && ccnotes.Enabled(hc.Ctx, ws.Worktree) {
		ccTask, err = ccnotes.CreateTask(hc.Ctx, ws.Worktree, cmp.Or(req.Name, agentSlug(sid)), ws.Branch, sprint.CCNotesSprint, ws.CCNotesProject)
		if err != nil {
			return agentSpawnResult{}, err
		}
	}

	sub, _, err := hc.Subjects.Start(hc.Ctx, subject.Window{Session: sid}, scope, agentSlug(sid), lifecycle, true)
	if err != nil {
		return agentSpawnResult{}, err
	}
	command, err := wrapForCapture(self, sid, claudeCommand(self, sid, scope, req.Prompt), b.Caps())
	if err != nil {
		return agentSpawnResult{}, err
	}
	command = wrapScrubExec(self, command)
	handle, err := b.Spawn(hc.Ctx, backend.SpawnSpec{
		Workstream: backend.WorkstreamHandle{Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree},
		Name:       name,
		Cwd:        scope,
		Command:    command,
		SessionID:  sid,
	})
	if err != nil {
		return agentSpawnResult{}, err
	}

	ag := agentRow{
		ID: sid, SprintID: sprint.ID, Backend: bname, TerminalHandle: handle.ID,
		SessionID: sid, Scope: scope, Name: name, Prompt: req.Prompt,
		SubjectID: sub.ID, CCNotesTask: ccTask, Status: StatusActive, State: StateUnknown,
		CreatedAt: nowStamp(),
	}
	if err := insertAgent(hc.Ctx, hc.DB, ag); err != nil {
		return agentSpawnResult{}, err
	}
	spawnAfterInsert()
	// Re-check the hierarchy on a fresh sprint read: a container-kill racing this spawn
	// either captured this insert or is observed killed here — closing the orphan window.
	fresh, err := getSprint(hc.Ctx, hc.DB, sprint.ID, "")
	if err != nil {
		return agentSpawnResult{}, err
	}
	if _, _, err := requireActiveHierarchy(hc, fresh); err != nil {
		if cerr := compensateSpawn(hc.Ctx, hc.DB, hc.Append, ag); cerr != nil {
			return agentSpawnResult{}, cerr
		}
		return agentSpawnResult{}, err
	}
	if _, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: sub.ID, Origin: event.OriginSystem, Type: EventSpawned, Payload: spawnedPayload(ag),
	}); err != nil {
		return agentSpawnResult{}, err
	}
	// Announce before starting the tailer, so a fast tailer's status frame can never
	// precede the spawned frame on the stream.
	fleetLog.emit(hc.Ctx, spawnedFrame(ag))
	tailers.start(hc.DB, hc.Append, ag)

	return agentSpawnResult{ID: sid, SubjectID: sub.ID, Terminal: handle.ID, Backend: string(bname)}, nil
}

// compensateSpawn undoes a spawn whose post-insert hierarchy re-check failed: under
// agentLock it re-reads the row and, if still active, soft-exits it and kills its fresh
// terminal, so a concurrent container-kill that missed the insert never leaves a live
// agent orphaned under a killed container. An agent that kill's teardown already exited
// is a no-op, so the exit and terminal teardown happen exactly once.
func compensateSpawn(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow) error {
	mu := agentLock(ag.ID)
	mu.Lock()
	defer mu.Unlock()
	cur, err := getAgent(ctx, db, ag.ID)
	if err != nil {
		return err
	}
	if cur.Status != StatusActive {
		return nil
	}
	if err := softExitAgent(ctx, db, appendFn, cur); err != nil {
		return err
	}
	return killAgentTerminal(ctx, db, cur)
}

// respawnAgent brings a vanished agent's terminal back: it resumes the agent's
// existing Claude session (same sid → same transcript, no work lost) into a fresh
// backend terminal, persists the new terminal handle, and restarts the transcript
// tailer with a snapshot carrying that handle. It reuses every spawn-tail mechanic
// handleSpawn runs — EnsureReady, wrapForCapture, Spawn, tailers.start — but with
// resumeCommand rather than claudeCommand, and it updates the agent's existing row
// instead of inserting a new one. It mints no new subject and writes no lifecycle
// event: the caller (the supervisor) owns markRestartAttempt and the EventRestarted
// it appends, and holds agentLock(ag.ID) across the whole sequence.
func respawnAgent(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, ag agentRow) (backend.AgentHandle, error) {
	b, ok := backend.Get(ag.Backend)
	if !ok {
		return backend.AgentHandle{}, fmt.Errorf("unknown backend: %s", ag.Backend)
	}
	if err := b.EnsureReady(ctx); err != nil {
		return backend.AgentHandle{}, fmt.Errorf("ensure backend %s ready: %w", ag.Backend, err)
	}
	sprint, err := getSprint(ctx, db, ag.SprintID, "")
	if err != nil {
		return backend.AgentHandle{}, err
	}
	ws, err := getWorkstream(ctx, db, sprint.WorkstreamID, "")
	if err != nil {
		return backend.AgentHandle{}, err
	}
	self, err := os.Executable()
	if err != nil {
		return backend.AgentHandle{}, fmt.Errorf("resolve self: %w", err)
	}
	command, err := wrapForCapture(self, ag.SessionID, resumeCommand(self, ag.SessionID, ag.Scope), b.Caps())
	if err != nil {
		return backend.AgentHandle{}, err
	}
	command = wrapScrubExec(self, command)
	handle, err := b.Spawn(ctx, backend.SpawnSpec{
		Workstream: backend.WorkstreamHandle{Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree},
		Name:       ag.Name,
		Cwd:        ag.Scope,
		Command:    command,
		SessionID:  ag.SessionID,
	})
	if err != nil {
		return backend.AgentHandle{}, fmt.Errorf("respawn agent %q: %w", ag.ID, err)
	}
	if err := setAgentTerminalHandle(ctx, db, ag.ID, handle.ID); err != nil {
		return backend.AgentHandle{}, err
	}
	fresh := ag
	fresh.TerminalHandle = handle.ID
	//nolint:contextcheck // tailer intentionally derives from the daemon-lifetime base ctx, not this caller's ctx (see tailerManager doc)
	tailers.start(db, appendFn, fresh)
	return handle, nil
}

// agentRespawnRequest respawns one exited agent by id, or every eligible exited agent
// when Dead is set. Exactly one of AgentID or Dead must be given.
type agentRespawnRequest struct {
	AgentID string `json:"agent_id,omitempty"`
	Dead    bool   `json:"dead,omitempty"`
}

// respawnFailure reports one agent a {dead:true} sweep tried but could not respawn — a
// real failure, distinct from a Conflict-ineligible agent the sweep silently skips.
type respawnFailure struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

// agentRespawnResult reports the agents actually respawned and, for a {dead:true} sweep,
// any that failed to respawn (Conflict-ineligible agents are silently skipped, not
// listed). The sweep succeeds with both lists; a single {agent_id} respawn still returns
// its error.
type agentRespawnResult struct {
	Respawned []agentView      `json:"respawned"`
	Failed    []respawnFailure `json:"failed,omitempty"`
}

// handleAgentRespawn answers cco.agent.respawn. {agent_id} respawns one eligible agent,
// erroring Conflict if it is not; {dead:true} sweeps every StatusExited agent, silently
// skipping the Conflict-ineligible ones and continuing past a real per-agent failure
// (recorded in Failed), so one wedged agent never aborts the rest of the sweep.
func handleAgentRespawn(hc daemon.HandlerCtx, req agentRespawnRequest) (agentRespawnResult, error) {
	hasID := req.AgentID != ""
	if hasID == req.Dead {
		return agentRespawnResult{}, opErr(codeInvalidRequest, fmt.Errorf("respawn requires exactly one of agent_id or dead"))
	}
	if hasID {
		view, err := respawnOneAgent(hc, req.AgentID)
		if err != nil {
			return agentRespawnResult{}, err
		}
		return agentRespawnResult{Respawned: []agentView{view}}, nil
	}
	exited, err := listAgents(hc.Ctx, hc.DB, "", StatusExited)
	if err != nil {
		return agentRespawnResult{}, err
	}
	respawned := make([]agentView, 0, len(exited))
	var failed []respawnFailure
	for _, ag := range exited {
		view, err := respawnOneAgent(hc, ag.ID)
		switch {
		case isConflict(err):
			continue // not eligible under the dead chain; the sweep skips it
		case err != nil:
			failed = append(failed, respawnFailure{ID: ag.ID, Error: err.Error()})
		default:
			respawned = append(respawned, view)
		}
	}
	return agentRespawnResult{Respawned: respawned, Failed: failed}, nil
}

// checkRespawnEligible gates a respawn on the same rule a bare spawn enforces: the
// agent must be StatusExited and its sprint/workstream/repo chain still active.
func checkRespawnEligible(hc daemon.HandlerCtx, ag agentRow) error {
	if ag.Status != StatusExited {
		return opErr(codeConflict, fmt.Errorf("agent %s is %s, not exited", ag.ID, ag.Status))
	}
	sprint, err := getSprint(hc.Ctx, hc.DB, ag.SprintID, "")
	if err != nil {
		return err
	}
	_, _, err = requireActiveHierarchy(hc, sprint)
	return err
}

// respawnOneAgent re-reads and respawns one agent under agentLock. It resets the
// restart budget with a fresh stamp (markRestartAttempt(id, 0, now), never
// resetRestart — that drops the stamp and the staleness prober would kill the fresh
// resume), flips the row active before the spawn (mirroring restoreAgent; a failed
// spawn self-heals via the supervisor), respawns via respawnAgent verbatim, and
// appends EventRestored.
func respawnOneAgent(hc daemon.HandlerCtx, id string) (agentView, error) {
	mu := agentLock(id)
	mu.Lock()
	defer mu.Unlock()
	ag, err := getAgent(hc.Ctx, hc.DB, id)
	if err != nil {
		return agentView{}, err
	}
	if err := checkRespawnEligible(hc, ag); err != nil {
		return agentView{}, err
	}
	if err := markRestartAttempt(hc.Ctx, hc.DB, ag.ID, 0, nowStamp()); err != nil {
		return agentView{}, err
	}
	if err := setAgentLifecycle(hc.Ctx, hc.DB, ag.ID, StatusActive); err != nil {
		return agentView{}, err
	}
	cur, err := getAgent(hc.Ctx, hc.DB, ag.ID)
	if err != nil {
		return agentView{}, err
	}
	// Announce from the committed row state (active, reset budget), before respawnAgent
	// starts the tailer, so a fast status frame can't precede the restarted announcement.
	fleetLog.emit(hc.Ctx, restartedFrame(cur.ID, cur.RestartCount))
	handle, err := respawnAgent(hc.Ctx, hc.DB, hc.Append, cur)
	if err != nil {
		return agentView{}, err
	}
	if _, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventRestored, Payload: restoredPayload(cur.ID, handle.ID),
	}); err != nil {
		return agentView{}, err
	}
	final, err := getAgent(hc.Ctx, hc.DB, ag.ID)
	if err != nil {
		return agentView{}, err
	}
	return newAgentView(final), nil
}

// isConflict reports whether err is opErr-tagged Conflict.
func isConflict(err error) bool {
	var oe *opError
	return errors.As(err, &oe) && oe.Code == codeConflict
}
