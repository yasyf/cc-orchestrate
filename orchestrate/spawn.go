package orchestrate

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"

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

// mcpServer is one entry of the child's --mcp-config: the orchestrate binary
// re-invoked as the child's channel MCP server, scoped to its session and cwd.
type mcpServer struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

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

// childMCPConfig is the child's --mcp-config: a single cc-orchestrate channel
// server, this binary re-invoked with the child's session and scope.
func childMCPConfig(self, sid, scope string) string {
	b, _ := json.Marshal(map[string]map[string]mcpServer{
		"mcpServers": {"cc-orchestrate": {Command: self, Args: []string{"channel", "--session", sid, "--cwd", scope}}},
	})
	return string(b)
}

// childSettings is the child's --settings: a SessionStart hook that records the
// child's window for session-rotation rebind, and a PreToolUse edit gate over
// the file-mutating tools. Both hooks re-invoke this binary, POSIX-shell-quoted
// so any metacharacter in its path survives the hook runner's shell split.
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

// claudeCommand is the full argv a backend runs for a child agent. Command[0] is
// the bare "claude"; the backend driver resolves the absolute real binary. The
// orchestration brief is appended to the child's system prompt so it arms its
// receive Monitor and knows to report back. An empty prompt omits the trailing
// positional argument.
func claudeCommand(self, sid, scope, prompt string) []string {
	argv := []string{
		"claude",
		"--session-id", sid,
		"--mcp-config", childMCPConfig(self, sid, scope),
		"--strict-mcp-config",
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope),
	}
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
	return []string{
		"claude",
		"--resume", sid,
		"--mcp-config", childMCPConfig(self, sid, scope),
		"--strict-mcp-config",
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope),
	}
}

// spawnBrief is the orchestration brief appended to a child agent's system prompt.
// It tells the agent it is orchestrated, how to RECEIVE instructions (arm a
// persistent watch Monitor), and how to REPORT back (the report MCP tool). The
// watch command is the exact line the agent must run; self and scope are
// POSIX-shell-quoted so the Monitor's shell split sees one token each.
func spawnBrief(self, sid, scope string) string {
	return fmt.Sprintf(`You are a cc-orchestrate agent: a Claude Code instance spawned and supervised by an orchestrator. Two channels connect you to it.

RECEIVE: immediately, before doing anything else, arm a persistent Monitor running exactly this command and keep it running for your whole session:

    %s watch --session %s --cwd %s

Each line it prints is one JSON event. An event whose type is "orchestrate.message" carries a new instruction from your orchestrator in its "text" field; treat that text as a directive and act on it.

REPORT: to send progress, a result, or a question back to your orchestrator, call the "report" tool from the cc-orchestrate MCP server with a short "text" and an optional "state" of "working", "blocked", or "done". Report when you start, when you finish, and whenever you are blocked or need a decision.`,
		backend.ShellQuote(self), sid, backend.ShellQuote(scope))
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
	return sprintRow{}, fmt.Errorf("no sprint specified and no active sprint, workstream, or repo")
}

// handleSpawn answers agent-spawn: it resolves the target sprint, derives its
// workstream and backend, requires the whole resolved hierarchy (sprint, workstream,
// repo) is still active so a bare spawn can never attach a live agent to a soft-killed
// target, provisions the agent's cc-notes task, creates a subject keyed by the new
// child's session id, spawns the claude command into the workstream's backend
// workspace, persists the agent row bound to the sprint, and starts its transcript
// tailer.
func handleSpawn(hc daemon.HandlerCtx) daemon.Reply {
	var body struct {
		Repo       string `json:"repo"`
		Workstream string `json:"workstream"`
		Sprint     string `json:"sprint"`
		Name       string `json:"name"`
		Cwd        string `json:"cwd"`
		Prompt     string `json:"prompt"`
	}
	if err := json.Unmarshal(hc.Env.Body, &body); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-spawn body: " + err.Error()}
	}
	sprint, err := resolveSpawnSprint(hc, body.Sprint, body.Workstream, body.Repo)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	ws, err := getWorkstream(hc.Ctx, hc.DB, sprint.WorkstreamID, "")
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	repo, err := getRepo(hc.Ctx, hc.DB, ws.RepoID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if sprint.Status != StatusActive {
		return daemon.Reply{OK: false, Error: fmt.Sprintf("sprint %s is %s, not active", sprint.ID, sprint.Status)}
	}
	if ws.Status != StatusActive {
		return daemon.Reply{OK: false, Error: fmt.Sprintf("workstream %s is %s, not active", ws.ID, ws.Status)}
	}
	if repo.Status != StatusActive {
		return daemon.Reply{OK: false, Error: fmt.Sprintf("repo %s is %s, not active", repo.ID, repo.Status)}
	}
	bname := ws.Backend
	b, ok := backend.Get(bname)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(bname)}
	}
	if err := b.EnsureReady(hc.Ctx); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	scope := cmp.Or(body.Cwd, ws.Worktree)
	if !filepath.IsAbs(scope) {
		scope = filepath.Join(hc.Scope, scope)
	}
	self, err := os.Executable()
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}

	sid := uuid.NewString()

	// cc-notes runs first, before any subject/terminal exists: a cc-notes failure
	// here leaves no started subject, no live claude process, and no agent row — only
	// a residual git-ref task, the same tradeoff provisionCCNotes already accepts.
	ccTask := ""
	if ws.CCNotesProject != "" && sprint.CCNotesSprint != "" && ccnotes.Enabled(hc.Ctx, ws.Worktree) {
		ccTask, err = ccnotes.CreateTask(hc.Ctx, ws.Worktree, cmp.Or(body.Name, agentSlug(sid)), ws.Branch, sprint.CCNotesSprint, ws.CCNotesProject)
		if err != nil {
			return daemon.Reply{OK: false, Error: err.Error()}
		}
	}

	sub, _, err := hc.Subjects.Start(hc.Ctx, subject.Window{Session: sid}, scope, agentSlug(sid), lifecycle, true)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	command, err := wrapForCapture(self, sid, claudeCommand(self, sid, scope, body.Prompt), b.Caps())
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	handle, err := b.Spawn(hc.Ctx, backend.SpawnSpec{
		Workstream: backend.WorkstreamHandle{Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree},
		Name:       body.Name,
		Cwd:        scope,
		Command:    command,
		SessionID:  sid,
	})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}

	ag := agentRow{
		ID: sid, SprintID: sprint.ID, Backend: bname, TerminalHandle: handle.ID,
		SessionID: sid, Scope: scope, Name: body.Name, Prompt: body.Prompt,
		SubjectID: sub.ID, CCNotesTask: ccTask, Status: StatusActive, State: StateUnknown,
		CreatedAt: nowStamp(),
	}
	if err := insertAgent(hc.Ctx, hc.DB, ag); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if _, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: sub.ID, Origin: event.OriginSystem, Type: EventSpawned, Payload: spawnedPayload(ag),
	}); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	tailers.start(hc.DB, hc.Append, ag)

	out, _ := json.Marshal(map[string]string{
		"agent_id": sid, "subject_id": sub.ID, "terminal": handle.ID, "backend": string(bname),
	})
	return daemon.Reply{OK: true, Body: out}
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
