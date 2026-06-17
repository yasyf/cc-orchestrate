package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// cmuxName is the registry key; cmuxBin is the CLI the driver shells out to.
const (
	cmuxName = "cmux"
	cmuxBin  = "cmux"
)

// cmux places workspaces and spawns agents through the cmux CLI, which talks to a
// local socket daemon. Mutating commands print an "OK <ref> ..." status line of
// short refs (workspace:N / surface:N), while list commands emit JSON under --json.
type cmux struct{ run runner }

func init() { Register(cmux{run: execRunner}) }

// cmuxWorkspace is a workspace entry in `list-workspaces --json`; ref is the
// addressable handle and current_directory is the workspace cwd.
type cmuxWorkspace struct {
	Ref              string `json:"ref"`
	Title            string `json:"title"`
	CurrentDirectory string `json:"current_directory"`
}

type cmuxWorkspaceList struct {
	Workspaces []cmuxWorkspace `json:"workspaces"`
}

// cmuxPane is a pane entry in `list-panes --json`; selected_surface_ref is the
// terminal surface that send and close-surface address.
type cmuxPane struct {
	SelectedSurfaceRef string `json:"selected_surface_ref"`
}

type cmuxPaneList struct {
	WorkspaceRef string     `json:"workspace_ref"`
	Panes        []cmuxPane `json:"panes"`
}

// cmuxRef returns the first whitespace-separated token of an "OK ..." status line
// that carries the given ref prefix (e.g. "workspace:" or "surface:").
func cmuxRef(out []byte, prefix string) (string, error) {
	for _, field := range strings.Fields(string(out)) {
		if strings.HasPrefix(field, prefix) {
			return field, nil
		}
	}
	return "", fmt.Errorf("cmux: no %s ref in output: %q", prefix, out)
}

// cmuxLaunchScript writes command as a self-removing POSIX script under a temp
// path and returns the text cmux send must type into the pane shell: a bash
// invocation of that path plus cmux's documented "\n" Enter. The argv rides the
// script as file bytes (each token POSIX-quoted, the brief's embedded newlines
// preserved) instead of being typed, because cmux send reinterprets any "\n",
// "\r", or "\t" in the typed text as Enter/Enter/Tab — which would split the
// multi-line --append-system-prompt brief into partial commands and let an
// arbitrary prompt inject shell metacharacters. The only typed text is the temp
// path, whose characters never include a backslash escape or a metacharacter.
func cmuxLaunchScript(command []string) (string, error) {
	quoted := make([]string, len(command))
	for i, tok := range command {
		quoted[i] = ShellQuote(tok)
	}
	f, err := os.CreateTemp("", "cc-orchestrate-cmux-*.sh")
	if err != nil {
		return "", fmt.Errorf("cmux: create launch script: %w", err)
	}
	defer f.Close()
	script := "rm -f -- \"$0\"\n" + strings.Join(quoted, " ") + "\n"
	if _, err := f.WriteString(script); err != nil {
		return "", fmt.Errorf("cmux: write launch script %s: %w", f.Name(), err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("cmux: close launch script %s: %w", f.Name(), err)
	}
	return "bash " + ShellQuote(f.Name()) + `\n`, nil
}

func (b cmux) Name() string { return cmuxName }

func (b cmux) Available() bool { return installed(cmuxBin) }

// EnsureReady is a no-op: the cmux socket daemon auto-starts on first command.
func (b cmux) EnsureReady(ctx context.Context) error { return nil }

func (b cmux) CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error) {
	out, err := b.run(ctx, cmuxBin, "new-workspace", "--cwd", spec.Cwd, "--name", spec.Name)
	if err != nil {
		return ProjectHandle{}, err
	}
	id, err := cmuxRef(out, "workspace:")
	if err != nil {
		return ProjectHandle{}, err
	}
	return ProjectHandle{Backend: b.Name(), ID: id, Name: spec.Name, Cwd: spec.Cwd}, nil
}

func (b cmux) ListProjects(ctx context.Context) ([]ProjectHandle, error) {
	out, err := b.run(ctx, cmuxBin, "list-workspaces", "--json")
	if err != nil {
		return nil, err
	}
	var res cmuxWorkspaceList
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	projects := make([]ProjectHandle, len(res.Workspaces))
	for i, w := range res.Workspaces {
		projects[i] = ProjectHandle{Backend: b.Name(), ID: w.Ref, Name: w.Title, Cwd: w.CurrentDirectory}
	}
	return projects, nil
}

func (b cmux) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	out, err := b.run(ctx, cmuxBin, "new-pane", "--workspace", spec.Project.ID)
	if err != nil {
		return AgentHandle{}, err
	}
	surface, err := cmuxRef(out, "surface:")
	if err != nil {
		return AgentHandle{}, err
	}
	launch, err := cmuxLaunchScript(spec.Command)
	if err != nil {
		return AgentHandle{}, err
	}
	if _, err := b.run(ctx, cmuxBin, "send", "--workspace", spec.Project.ID, "--surface", surface, "--", launch); err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:   b.Name(),
		ID:        surface,
		ProjectID: spec.Project.ID,
		Name:      spec.Name,
		SessionID: spec.SessionID,
	}, nil
}

func (b cmux) ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, cmuxBin, "list-panes", "--workspace", project.ID, "--json")
	if err != nil {
		return nil, err
	}
	var res cmuxPaneList
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	agents := make([]AgentHandle, len(res.Panes))
	for i, p := range res.Panes {
		agents[i] = AgentHandle{Backend: b.Name(), ID: p.SelectedSurfaceRef, ProjectID: res.WorkspaceRef}
	}
	return agents, nil
}

func (b cmux) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, cmuxBin, "close-surface", "--workspace", agent.ProjectID, "--surface", agent.ID)
	return err
}

func (b cmux) KillProject(ctx context.Context, project ProjectHandle) error {
	_, err := b.run(ctx, cmuxBin, "close-workspace", "--workspace", project.ID)
	return err
}

func (b cmux) Caps() Caps { return Caps{SendText: true, Capture: true} }
