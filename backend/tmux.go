package backend

import (
	"context"
	"strings"
)

// tmuxBin is the CLI binary the tmux backend drives; tmuxName is its registry
// identity (kept separate from the binary so only the name carries the backend
// type).
const (
	tmuxBin  = "tmux"
	tmuxName = "tmux"
)

// tmuxNameReplacer maps the characters tmux reserves as target separators
// ('.' for panes, ':' for windows) to underscores so a project name survives a
// round trip as a session id. tmux itself rewrites these on new-session, but a
// target spec built from the unsanitized name would address the wrong object.
var tmuxNameReplacer = strings.NewReplacer(".", "_", ":", "_")

// tmux places projects as detached tmux sessions and agents as windows within
// them, falling back to -F format strings since tmux has no JSON output.
type tmux struct{ run runner }

func init() { Register(tmux{run: execRunner}) }

func (b tmux) Name() Name { return tmuxName }

func (b tmux) Available() bool { return installed(tmuxBin) }

func (b tmux) Caps() Caps { return Capabilities(CanSendText, CanCapture, CanEnumerate) }

func (b tmux) EnsureReady(_ context.Context) error { return nil }

func (b tmux) CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error) {
	session := tmuxNameReplacer.Replace(spec.Name)
	if _, err := b.run(ctx, tmuxBin, "new-session", "-d", "-s", session, "-c", spec.Cwd); err != nil {
		return WorkstreamHandle{}, err
	}
	return WorkstreamHandle{Backend: tmuxName, ID: session, Name: spec.Name, Cwd: spec.Cwd, Worktree: spec.Cwd}, nil
}

func (b tmux) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	out, err := b.run(ctx, tmuxBin, "list-sessions", "-F", "#{session_name}")
	if err != nil {
		return nil, err
	}
	workstreams := []WorkstreamHandle{}
	for _, name := range nonEmptyLines(out) {
		workstreams = append(workstreams, WorkstreamHandle{Backend: tmuxName, ID: name, Name: name})
	}
	return workstreams, nil
}

func (b tmux) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	out, err := b.run(ctx, tmuxBin, append([]string{
		"new-window", "-d", "-P", "-F", "#{pane_id}",
		"-t", spec.Workstream.ID, "-n", spec.Name, "-c", spec.Cwd, "--",
	}, spec.Command...)...)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:      tmuxName,
		ID:           strings.TrimSpace(string(out)),
		WorkstreamID: spec.Workstream.ID,
		Name:         spec.Name,
		SessionID:    spec.SessionID,
	}, nil
}

func (b tmux) ListAgents(ctx context.Context, workstream WorkstreamHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, tmuxBin, "list-panes", "-s", "-t", workstream.ID, "-F", "#{pane_id}\t#{window_name}")
	if err != nil {
		return nil, err
	}
	agents := []AgentHandle{}
	for _, line := range nonEmptyLines(out) {
		id, name, _ := strings.Cut(line, "\t")
		agents = append(agents, AgentHandle{Backend: tmuxName, ID: id, WorkstreamID: workstream.ID, Name: name})
	}
	return agents, nil
}

func (b tmux) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, tmuxBin, "kill-pane", "-t", agent.ID)
	return err
}

func (b tmux) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	_, err := b.run(ctx, tmuxBin, "kill-session", "-t", workstream.ID)
	return err
}

// SendText types text into the agent's pane and submits it with a separate Enter
// key: -l sends the text literally so its characters are never read as key names.
func (b tmux) SendText(ctx context.Context, agent AgentHandle, text string) error {
	if _, err := b.run(ctx, tmuxBin, "send-keys", "-t", agent.ID, "-l", "--", text); err != nil {
		return err
	}
	_, err := b.run(ctx, tmuxBin, "send-keys", "-t", agent.ID, "Enter")
	return err
}

// Capture returns the agent pane's visible screen as plain text: -p writes it to
// stdout and -J joins wrapped lines so a reflowed prompt reads intact. agent.ID is
// the globally unique pane id, so it targets the pane without the session.
func (b tmux) Capture(ctx context.Context, agent AgentHandle) (string, error) {
	out, err := b.run(ctx, tmuxBin, "capture-pane", "-p", "-J", "-t", agent.ID)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// AgentAlive reports whether the agent pane's command is still running. Under
// remain-on-exit a pane outlives its command as a dead pane (#{pane_dead} == 1) that
// ListAgents still reports by #{pane_id}, so this is the corroboration the supervisor
// needs before resuming a stale agent. A pane that has truly vanished makes
// display-message error; the caller reads that as "not confirmed dead" and leaves
// the vanished case to the ListAgents diff.
func (b tmux) AgentAlive(ctx context.Context, agent AgentHandle) (bool, error) {
	out, err := b.run(ctx, tmuxBin, "display-message", "-p", "-t", agent.ID, "#{pane_dead}")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) != "1", nil
}
