package orchestrate

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
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

// handleSpawn answers agent-spawn: it resolves the project and backend, creates a
// subject keyed by the new child's session id, spawns the claude command into the
// backend, persists the agent row, and starts its transcript tailer.
func handleSpawn(hc daemon.HandlerCtx) daemon.Reply {
	var body struct {
		Project string `json:"project"`
		Name    string `json:"name"`
		Cwd     string `json:"cwd"`
		Prompt  string `json:"prompt"`
	}
	if err := json.Unmarshal(hc.Env.Body, &body); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-spawn body: " + err.Error()}
	}
	proj, err := getProject(hc.Ctx, hc.DB, body.Project)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	bname := proj.Backend
	b, ok := backend.Get(bname)
	if !ok {
		return daemon.Reply{OK: false, Error: "unknown backend: " + string(bname)}
	}
	if err := b.EnsureReady(hc.Ctx); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	scope := cmp.Or(body.Cwd, proj.Cwd)
	if !filepath.IsAbs(scope) {
		scope = filepath.Join(hc.Scope, scope)
	}
	self, err := os.Executable()
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}

	sid := uuid.NewString()
	sub, _, err := hc.Subjects.Start(hc.Ctx, subject.Window{Session: sid}, scope, agentSlug(sid), lifecycle, true)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	handle, err := b.Spawn(hc.Ctx, backend.SpawnSpec{
		Project:   backend.ProjectHandle{Backend: proj.Backend, ID: proj.WorkspaceHandle, Name: proj.Name, Cwd: scope},
		Name:      body.Name,
		Cwd:       scope,
		Command:   claudeCommand(self, sid, scope, body.Prompt),
		SessionID: sid,
	})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}

	ag := agentRow{
		ID: sid, ProjectID: proj.ID, Backend: bname, TerminalHandle: handle.ID,
		SessionID: sid, Scope: scope, Name: body.Name, Prompt: body.Prompt,
		SubjectID: sub.ID, Status: StatusActive, State: StateUnknown,
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
