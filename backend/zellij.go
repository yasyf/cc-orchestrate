package backend

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

const zellijBin = "zellij"

// pane mirrors one element of `zellij action list-panes --json`, which is a flat
// array of pane objects (one per pane across every tab), not a tab-keyed map.
type pane struct {
	ID       int    `json:"id"`
	IsPlugin bool   `json:"is_plugin"`
	Title    string `json:"title"`
	Command  string `json:"terminal_command"`
}

// zellij drives the zellij multiplexer: a project is a background session, an
// agent is a named command pane inside it.
type zellij struct{ run runner }

func init() { Register(zellij{run: execRunner}) }

func (b zellij) Name() string { return "zellij" }

func (b zellij) Available() bool { return installed(zellijBin) }

func (b zellij) Caps() Caps { return Caps{SendText: true, Capture: true} }

func (b zellij) EnsureReady(ctx context.Context) error { return nil }

func (b zellij) CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error) {
	session := sanitizeSession(spec.Name)
	if _, err := b.run(ctx, zellijBin, "attach", "--create-background", session); err != nil {
		return ProjectHandle{}, err
	}
	return ProjectHandle{Backend: "zellij", ID: session, Name: spec.Name, Cwd: spec.Cwd}, nil
}

func (b zellij) ListProjects(ctx context.Context) ([]ProjectHandle, error) {
	out, err := b.run(ctx, zellijBin, "list-sessions", "--no-formatting", "--short")
	if err != nil {
		return nil, err
	}
	projects := []ProjectHandle{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if name := strings.TrimSpace(line); name != "" {
			projects = append(projects, ProjectHandle{Backend: "zellij", ID: name, Name: name})
		}
	}
	return projects, nil
}

func (b zellij) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	args := append(
		[]string{"--session", spec.Project.ID, "action", "new-pane", "--cwd", spec.Cwd, "--name", spec.Name, "--"},
		spec.Command...,
	)
	out, err := b.run(ctx, zellijBin, args...)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:   "zellij",
		ID:        strings.TrimSpace(string(out)),
		ProjectID: spec.Project.ID,
		Name:      spec.Name,
		SessionID: spec.SessionID,
	}, nil
}

func (b zellij) ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, zellijBin, "--session", project.ID, "action", "list-panes", "--json")
	if err != nil {
		return nil, err
	}
	var panes []pane
	if err := json.Unmarshal(out, &panes); err != nil {
		return nil, err
	}
	agents := []AgentHandle{}
	for _, p := range panes {
		if p.Command == "" {
			continue
		}
		agents = append(agents, AgentHandle{
			Backend:   "zellij",
			ID:        paneID(p),
			ProjectID: project.ID,
			Name:      p.Title,
		})
	}
	return agents, nil
}

func (b zellij) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, zellijBin, "--session", agent.ProjectID, "action", "close-pane", "--pane-id", agent.ID)
	return err
}

func (b zellij) KillProject(ctx context.Context, project ProjectHandle) error {
	_, err := b.run(ctx, zellijBin, "kill-session", project.ID)
	return err
}

func paneID(p pane) string {
	if p.IsPlugin {
		return "plugin_" + strconv.Itoa(p.ID)
	}
	return "terminal_" + strconv.Itoa(p.ID)
}

func sanitizeSession(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
}
