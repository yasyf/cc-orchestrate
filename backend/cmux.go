package backend

import (
	"context"
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

// cmuxLaunchScript writes command as a self-removing POSIX script under a temp path
// and returns the `bash <temp-path>` command line respawn-pane runs as the surface's
// program. The script execs the argv, so the agent replaces bash and becomes the
// surface's top-level process: when the agent exits, cmux auto-closes the surface, which
// the keep-alive supervisor's ListAgents diff sees as a vanished terminal and resumes —
// the same liveness path every other backend rides. Running the agent under the surface's
// own fish login shell instead would leave the shell (and the surface) alive after a
// crash, hiding the death. The argv rides the script as file bytes (each token
// POSIX-quoted, the brief's embedded newlines preserved) rather than the command line, so
// an arbitrary prompt cannot inject shell metacharacters; the only text on the command
// line is the temp path, which carries none.
func cmuxLaunchScript(command []string) (string, error) {
	quoted := quoteAll(command)
	f, err := os.CreateTemp("", "cc-orchestrate-cmux-*.sh")
	if err != nil {
		return "", fmt.Errorf("cmux: create launch script: %w", err)
	}
	defer func() { _ = f.Close() }()
	script := "rm -f -- \"$0\"\nexec " + strings.Join(quoted, " ") + "\n"
	if _, err := f.WriteString(script); err != nil {
		return "", fmt.Errorf("cmux: write launch script %s: %w", f.Name(), err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("cmux: close launch script %s: %w", f.Name(), err)
	}
	return "bash " + ShellQuote(f.Name()), nil
}

func (b cmux) Name() Name { return cmuxName }

func (b cmux) Available() bool { return installed(cmuxBin) }

// EnsureReady is a no-op: the cmux socket daemon auto-starts on first command.
func (b cmux) EnsureReady(_ context.Context) error { return nil }

func (b cmux) CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error) {
	out, err := b.run(ctx, cmuxBin, "new-workspace", "--cwd", spec.Cwd, "--name", spec.Name)
	if err != nil {
		return WorkstreamHandle{}, err
	}
	id, err := cmuxRef(out, "workspace:")
	if err != nil {
		return WorkstreamHandle{}, err
	}
	return WorkstreamHandle{Backend: b.Name(), ID: id, Name: spec.Name, Cwd: spec.Cwd, Worktree: spec.Cwd}, nil
}

func (b cmux) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	out, err := b.run(ctx, cmuxBin, "list-workspaces", "--json")
	if err != nil {
		return nil, err
	}
	res, err := decodeJSON[cmuxWorkspaceList](out, "cmux", "workspaces")
	if err != nil {
		return nil, err
	}
	workstreams := make([]WorkstreamHandle, len(res.Workspaces))
	for i, w := range res.Workspaces {
		workstreams[i] = WorkstreamHandle{Backend: b.Name(), ID: w.Ref, Name: w.Title, Cwd: w.CurrentDirectory}
	}
	return workstreams, nil
}

// Spawn opens a fresh surface with new-pane, then respawn-pane replaces that surface's
// fish login shell with the agent so the agent is the surface's top-level process. That
// matters for liveness: a child of the surface's shell would let the shell — and so the
// surface — outlive a crashed agent, hiding the death from the supervisor; as the surface
// program, the agent's exit auto-closes the surface, which the ListAgents diff resumes.
// The argv rides a self-removing temp script (cmuxLaunchScript) that respawn-pane runs as
// `bash <path>`, keeping an arbitrary prompt off the command line.
func (b cmux) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	out, err := b.run(ctx, cmuxBin, "new-pane", "--workspace", spec.Workstream.ID)
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
	if _, err := b.run(ctx, cmuxBin, "respawn-pane", "--workspace", spec.Workstream.ID, "--surface", surface, "--command", launch); err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:      b.Name(),
		ID:           surface,
		WorkstreamID: spec.Workstream.ID,
		Name:         spec.Name,
		SessionID:    spec.SessionID,
	}, nil
}

func (b cmux) ListAgents(ctx context.Context, workstream WorkstreamHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, cmuxBin, "list-panes", "--workspace", workstream.ID, "--json")
	if err != nil {
		return nil, err
	}
	res, err := decodeJSON[cmuxPaneList](out, "cmux", "panes")
	if err != nil {
		return nil, err
	}
	agents := make([]AgentHandle, len(res.Panes))
	for i, p := range res.Panes {
		agents[i] = AgentHandle{Backend: b.Name(), ID: p.SelectedSurfaceRef, WorkstreamID: res.WorkspaceRef}
	}
	return agents, nil
}

func (b cmux) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, cmuxBin, "close-surface", "--workspace", agent.WorkstreamID, "--surface", agent.ID)
	return err
}

func (b cmux) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	_, err := b.run(ctx, cmuxBin, "close-workspace", "--workspace", workstream.ID)
	return err
}

// SendText types text into the agent's surface, then submits it with a separate
// enter key event rather than an embedded "\n" — cmux send reinterprets \n/\r/\t,
// which would split a multi-line message into partial commands.
func (b cmux) SendText(ctx context.Context, agent AgentHandle, text string) error {
	if _, err := b.run(ctx, cmuxBin, "send", "--workspace", agent.WorkstreamID, "--surface", agent.ID, "--", text); err != nil {
		return err
	}
	_, err := b.run(ctx, cmuxBin, "send-key", "--workspace", agent.WorkstreamID, "--surface", agent.ID, "enter")
	return err
}

// Capture returns the agent surface's visible screen as plain text. read-screen
// writes the viewport to stdout; agent.WorkstreamID is the workspace and agent.ID
// the surface.
func (b cmux) Capture(ctx context.Context, agent AgentHandle) (string, error) {
	out, err := b.run(ctx, cmuxBin, "read-screen", "--workspace", agent.WorkstreamID, "--surface", agent.ID)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (b cmux) Caps() Caps { return Capabilities(CanSendText, CanCapture, CanEnumerate) }
