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

func (b tmux) Name() string { return tmuxName }

func (b tmux) Available() bool { return installed(tmuxBin) }

func (b tmux) Caps() Caps { return Capabilities(CanSendText, CanEnumerate) }

func (b tmux) EnsureReady(ctx context.Context) error { return nil }

func (b tmux) CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error) {
	session := tmuxNameReplacer.Replace(spec.Name)
	if _, err := b.run(ctx, tmuxBin, "new-session", "-d", "-s", session, "-c", spec.Cwd); err != nil {
		return ProjectHandle{}, err
	}
	return ProjectHandle{Backend: tmuxName, ID: session, Name: spec.Name, Cwd: spec.Cwd}, nil
}

func (b tmux) ListProjects(ctx context.Context) ([]ProjectHandle, error) {
	out, err := b.run(ctx, tmuxBin, "list-sessions", "-F", "#{session_name}")
	if err != nil {
		return nil, err
	}
	projects := []ProjectHandle{}
	for _, name := range tmuxLines(out) {
		projects = append(projects, ProjectHandle{Backend: tmuxName, ID: name, Name: name})
	}
	return projects, nil
}

func (b tmux) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	out, err := b.run(ctx, tmuxBin, append([]string{
		"new-window", "-d", "-P", "-F", "#{pane_id}",
		"-t", spec.Project.ID, "-n", spec.Name, "-c", spec.Cwd, "--",
	}, spec.Command...)...)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:   tmuxName,
		ID:        strings.TrimSpace(string(out)),
		ProjectID: spec.Project.ID,
		Name:      spec.Name,
		SessionID: spec.SessionID,
	}, nil
}

func (b tmux) ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, tmuxBin, "list-panes", "-s", "-t", project.ID, "-F", "#{pane_id}\t#{window_name}")
	if err != nil {
		return nil, err
	}
	agents := []AgentHandle{}
	for _, line := range tmuxLines(out) {
		id, name, _ := strings.Cut(line, "\t")
		agents = append(agents, AgentHandle{Backend: tmuxName, ID: id, ProjectID: project.ID, Name: name})
	}
	return agents, nil
}

func (b tmux) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, tmuxBin, "kill-pane", "-t", agent.ID)
	return err
}

func (b tmux) KillProject(ctx context.Context, project ProjectHandle) error {
	_, err := b.run(ctx, tmuxBin, "kill-session", "-t", project.ID)
	return err
}

func tmuxLines(out []byte) []string {
	lines := []string{}
	for _, line := range strings.Split(string(out), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}
