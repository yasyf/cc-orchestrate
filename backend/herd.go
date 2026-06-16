package backend

import (
	"context"
	"encoding/json"
)

// herdName is the registry key; herdBin is the CLI herd invokes (note the
// trailing r).
const (
	herdName = "herd"
	herdBin  = "herdr"
)

// herd places workspaces and spawns agents through the herdr CLI, which emits a
// JSON envelope {"id":...,"result":{...}} on every command.
type herd struct{ run runner }

func init() { Register(herd{run: execRunner}) }

// herdWorkspace is a workspace entry as it appears in create and list results.
type herdWorkspace struct {
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
}

// herdAgent is an agent entry as it appears in start and list results; the pane
// id is the addressable terminal and the workspace id scopes it to a project.
type herdAgent struct {
	PaneID      string `json:"pane_id"`
	Name        string `json:"name"`
	WorkspaceID string `json:"workspace_id"`
}

type herdCreateResult struct {
	Workspace herdWorkspace `json:"workspace"`
}

type herdWorkspaceListResult struct {
	Workspaces []herdWorkspace `json:"workspaces"`
}

type herdStartResult struct {
	Agent herdAgent `json:"agent"`
}

type herdAgentListResult struct {
	Agents []herdAgent `json:"agents"`
}

// decodeHerd unwraps the result field of a herdr JSON envelope into T.
func decodeHerd[T any](b []byte) (T, error) {
	var env struct {
		Result T `json:"result"`
	}
	err := json.Unmarshal(b, &env)
	return env.Result, err
}

func (b herd) Name() string { return herdName }

func (b herd) Available() bool { return installed(herdBin) }

// EnsureReady is a no-op: the herdr server auto-starts on first command.
func (b herd) EnsureReady(ctx context.Context) error { return nil }

func (b herd) CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error) {
	out, err := b.run(ctx, herdBin, "workspace", "create", "--cwd", spec.Cwd, "--label", spec.Name)
	if err != nil {
		return ProjectHandle{}, err
	}
	res, err := decodeHerd[herdCreateResult](out)
	if err != nil {
		return ProjectHandle{}, err
	}
	return ProjectHandle{Backend: b.Name(), ID: res.Workspace.WorkspaceID, Name: spec.Name, Cwd: spec.Cwd}, nil
}

func (b herd) ListProjects(ctx context.Context) ([]ProjectHandle, error) {
	out, err := b.run(ctx, herdBin, "workspace", "list")
	if err != nil {
		return nil, err
	}
	res, err := decodeHerd[herdWorkspaceListResult](out)
	if err != nil {
		return nil, err
	}
	projects := make([]ProjectHandle, len(res.Workspaces))
	for i, w := range res.Workspaces {
		projects[i] = ProjectHandle{Backend: b.Name(), ID: w.WorkspaceID, Name: w.Label}
	}
	return projects, nil
}

func (b herd) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	args := append([]string{"agent", "start", spec.Name, "--workspace", spec.Project.ID, "--cwd", spec.Cwd, "--"}, spec.Command...)
	out, err := b.run(ctx, herdBin, args...)
	if err != nil {
		return AgentHandle{}, err
	}
	res, err := decodeHerd[herdStartResult](out)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:   b.Name(),
		ID:        res.Agent.PaneID,
		ProjectID: spec.Project.ID,
		Name:      spec.Name,
		SessionID: spec.SessionID,
	}, nil
}

func (b herd) ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, herdBin, "agent", "list")
	if err != nil {
		return nil, err
	}
	res, err := decodeHerd[herdAgentListResult](out)
	if err != nil {
		return nil, err
	}
	agents := []AgentHandle{}
	for _, a := range res.Agents {
		if a.WorkspaceID != project.ID {
			continue
		}
		agents = append(agents, AgentHandle{Backend: b.Name(), ID: a.PaneID, ProjectID: a.WorkspaceID, Name: a.Name})
	}
	return agents, nil
}

func (b herd) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, herdBin, "pane", "close", agent.ID)
	return err
}

func (b herd) KillProject(ctx context.Context, project ProjectHandle) error {
	_, err := b.run(ctx, herdBin, "workspace", "close", project.ID)
	return err
}

func (b herd) Caps() Caps { return Caps{SendText: true, Capture: true} }
