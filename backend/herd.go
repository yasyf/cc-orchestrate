package backend

import (
	"context"
	"encoding/json"
	"fmt"
)

// herdName is the registry key; herdBin is the CLI herd invokes (note the
// trailing r).
const (
	herdName = "herd"
	herdBin  = "herdr"
)

// herd places workspaces and spawns agents through the herdr CLI, which emits a
// JSON envelope {"id":...,"result":{...}} on every command. It implements no
// AgentProber: herd destroys an agent's pane the instant the child exits (its pane type
// has no remain-on-exit mode), so a dead agent is a vanished pane the supervisor's
// ListAgents diff already catches — unlike tmux/zellij, whose terminals outlive a dead
// child and so need the prober to corroborate a staleness signal.
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

// herdPaneRead is the result of `herdr pane read`; text is the captured screen.
type herdPaneRead struct {
	Text string `json:"text"`
}

// decodeHerd unwraps the result field of a herdr JSON envelope into T.
func decodeHerd[T any](b []byte) (T, error) {
	var env struct {
		Result T `json:"result"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return env.Result, fmt.Errorf("herd: cannot parse response: %w", err)
	}
	return env.Result, nil
}

func (b herd) Name() Name { return herdName }

func (b herd) Available() bool { return installed(herdBin) }

// EnsureReady is a no-op: the herdr server auto-starts on first command.
func (b herd) EnsureReady(_ context.Context) error { return nil }

func (b herd) CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error) {
	out, err := b.run(ctx, herdBin, "workspace", "create", "--cwd", spec.Cwd, "--label", spec.Name)
	if err != nil {
		return WorkstreamHandle{}, err
	}
	res, err := decodeHerd[herdCreateResult](out)
	if err != nil {
		return WorkstreamHandle{}, err
	}
	return WorkstreamHandle{Backend: b.Name(), ID: res.Workspace.WorkspaceID, Name: spec.Name, Cwd: spec.Cwd, Worktree: spec.Cwd}, nil
}

func (b herd) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	out, err := b.run(ctx, herdBin, "workspace", "list")
	if err != nil {
		return nil, err
	}
	res, err := decodeHerd[herdWorkspaceListResult](out)
	if err != nil {
		return nil, err
	}
	workstreams := make([]WorkstreamHandle, len(res.Workspaces))
	for i, w := range res.Workspaces {
		workstreams[i] = WorkstreamHandle{Backend: b.Name(), ID: w.WorkspaceID, Name: w.Label}
	}
	return workstreams, nil
}

func (b herd) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	args := append([]string{"agent", "start", spec.Name, "--workspace", spec.Workstream.ID, "--cwd", spec.Cwd, "--"}, spec.Command...)
	out, err := b.run(ctx, herdBin, args...)
	if err != nil {
		return AgentHandle{}, err
	}
	res, err := decodeHerd[herdStartResult](out)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:      b.Name(),
		ID:           res.Agent.PaneID,
		WorkstreamID: spec.Workstream.ID,
		Name:         spec.Name,
		SessionID:    spec.SessionID,
	}, nil
}

func (b herd) ListAgents(ctx context.Context, workstream WorkstreamHandle) ([]AgentHandle, error) {
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
		if a.WorkspaceID != workstream.ID {
			continue
		}
		agents = append(agents, AgentHandle{Backend: b.Name(), ID: a.PaneID, WorkstreamID: a.WorkspaceID, Name: a.Name})
	}
	return agents, nil
}

func (b herd) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, herdBin, "pane", "close", agent.ID)
	return err
}

func (b herd) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	_, err := b.run(ctx, herdBin, "workspace", "close", workstream.ID)
	return err
}

// SendText types text into the agent's pane and submits it with a separate Enter
// key. agent.ID is the herd pane id.
func (b herd) SendText(ctx context.Context, agent AgentHandle, text string) error {
	if _, err := b.run(ctx, herdBin, "pane", "send-text", agent.ID, text); err != nil {
		return err
	}
	_, err := b.run(ctx, herdBin, "pane", "send-keys", agent.ID, "Enter")
	return err
}

// Capture returns the agent pane's visible screen as plain text, decoded from the
// herdr pane read JSON envelope's result.text. agent.ID is the herd pane id.
func (b herd) Capture(ctx context.Context, agent AgentHandle) (string, error) {
	out, err := b.run(ctx, herdBin, "pane", "read", agent.ID, "--source", "visible", "--format", "text")
	if err != nil {
		return "", err
	}
	res, err := decodeHerd[herdPaneRead](out)
	if err != nil {
		return "", err
	}
	return res.Text, nil
}

func (b herd) Caps() Caps { return Capabilities(CanSendText, CanCapture, CanEnumerate) }
