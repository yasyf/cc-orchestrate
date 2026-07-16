package backend

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// zellijBin is the CLI binary the zellij backend drives; zellijName is its
// registry identity, kept separate so only the name carries the backend type.
const (
	zellijBin  = "zellij"
	zellijName = "zellij"
)

// pane mirrors one element of `zellij action list-panes --json`, which is a flat
// array of pane objects (one per pane across every tab), not a tab-keyed map.
// Exited is the per-pane process-liveness bit: zellij keeps a command pane listed
// after its child exits (it holds the pane and shows the exit status), flipping
// exited true while terminal_command stays populated — the signal AgentAlive reads.
type pane struct {
	ID       int    `json:"id"`
	IsPlugin bool   `json:"is_plugin"`
	Title    string `json:"title"`
	Command  string `json:"terminal_command"`
	Exited   bool   `json:"exited"`
}

// zellij drives the zellij multiplexer: a project is a background session, an
// agent is a named command pane inside it.
type zellij struct{ run runner }

func init() { Register(zellij{run: execRunner}) }

func (b zellij) Name() Name { return zellijName }

func (b zellij) Available() bool { return installed(zellijBin) }

func (b zellij) Caps() Caps { return Capabilities(CanSendText, CanCapture, CanEnumerate) }

func (b zellij) EnsureReady(_ context.Context) error { return nil }

func (b zellij) CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error) {
	session := sanitizeSession(spec.Name)
	if _, err := b.run(ctx, zellijBin, "attach", "--create-background", session); err != nil {
		return WorkstreamHandle{}, err
	}
	return WorkstreamHandle{Backend: zellijName, ID: session, Name: spec.Name, Cwd: spec.Cwd, Worktree: spec.Cwd}, nil
}

func (b zellij) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	out, err := b.run(ctx, zellijBin, "list-sessions", "--no-formatting", "--short")
	if err != nil {
		return nil, err
	}
	workstreams := []WorkstreamHandle{}
	for _, name := range nonEmptyLines(out) {
		workstreams = append(workstreams, WorkstreamHandle{Backend: zellijName, ID: name, Name: name})
	}
	return workstreams, nil
}

func (b zellij) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	args := append(
		[]string{"--session", spec.Workstream.ID, "action", "new-pane", "--cwd", spec.Cwd, "--name", spec.Name, "--"},
		spec.Command...,
	)
	out, err := b.run(ctx, zellijBin, args...)
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:      zellijName,
		ID:           strings.TrimSpace(string(out)),
		WorkstreamID: spec.Workstream.ID,
		Name:         spec.Name,
		SessionID:    spec.SessionID,
	}, nil
}

func (b zellij) ListAgents(ctx context.Context, workstream WorkstreamHandle) ([]AgentHandle, error) {
	out, err := b.run(ctx, zellijBin, "--session", workstream.ID, "action", "list-panes", "--json")
	if err != nil {
		return nil, err
	}
	panes, err := decodeJSON[[]pane](out, "zellij", "panes")
	if err != nil {
		return nil, err
	}
	agents := []AgentHandle{}
	for _, p := range panes {
		if p.Command == "" {
			continue
		}
		agents = append(agents, AgentHandle{
			Backend:      zellijName,
			ID:           paneID(p),
			WorkstreamID: workstream.ID,
			Name:         p.Title,
		})
	}
	return agents, nil
}

func (b zellij) Kill(ctx context.Context, agent AgentHandle) error {
	_, err := b.run(ctx, zellijBin, "--session", agent.WorkstreamID, "action", "close-pane", "--pane-id", agent.ID)
	return err
}

// KillWorkstream ends the zellij session. kill-session terminates a live session and,
// for a fresh never-resurrected session, drops it entirely, so it vanishes from
// list-sessions and needs no delete-session. A session with saved state instead leaves
// an EXITED stub that lingers in list-sessions; a delete-session --force clears that
// stub, but only when the session still appears after kill-session.
func (b zellij) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	if _, err := b.run(ctx, zellijBin, "kill-session", workstream.ID); err != nil {
		return err
	}
	out, err := b.run(ctx, zellijBin, "list-sessions", "--no-formatting", "--short")
	if err != nil {
		return fmt.Errorf("zellij: list sessions after kill %q: %w", workstream.ID, err)
	}
	if !slices.Contains(nonEmptyLines(out), workstream.ID) {
		return nil
	}
	_, err = b.run(ctx, zellijBin, "delete-session", "--force", workstream.ID)
	return err
}

// SendText writes text into the agent's pane within its session, then submits it
// by writing a carriage-return byte (13). agent.WorkstreamID is the zellij session.
func (b zellij) SendText(ctx context.Context, agent AgentHandle, text string) error {
	if _, err := b.run(ctx, zellijBin, "--session", agent.WorkstreamID, "action", "write-chars", "-p", agent.ID, "--", text); err != nil {
		return err
	}
	_, err := b.run(ctx, zellijBin, "--session", agent.WorkstreamID, "action", "write", "-p", agent.ID, "13")
	return err
}

// Capture returns the agent pane's visible screen as plain text. dump-screen prints
// to stdout when --path is omitted, and --pane-id targets the pane without focusing
// it. agent.WorkstreamID is the zellij session.
func (b zellij) Capture(ctx context.Context, agent AgentHandle) (string, error) {
	out, err := b.run(ctx, zellijBin, "--session", agent.WorkstreamID, "action", "dump-screen", "--pane-id", agent.ID)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// AgentAlive reports whether the agent pane's command is still running. zellij holds a
// command pane open after its child exits (it shows the exit status and "<Press ENTER to
// re-run>" rather than closing), so ListAgents still enumerates the pane by its
// terminal_command and the vanished-handle diff cannot see the death; the pane manifest's
// exited flag is the corroboration the supervisor needs before resuming a stale agent. A
// pane that has truly vanished is absent from list-panes, so AgentAlive returns the
// not-found error, which the caller reads as "not confirmed dead" — leaving the vanished
// case to the ListAgents diff. paneID(p) namespaces the match as terminal_N/plugin_N
// because list-panes can emit a plugin pane and a terminal pane sharing a numeric id.
func (b zellij) AgentAlive(ctx context.Context, agent AgentHandle) (bool, error) {
	out, err := b.run(ctx, zellijBin, "--session", agent.WorkstreamID, "action", "list-panes", "--json")
	if err != nil {
		return false, err
	}
	panes, err := decodeJSON[[]pane](out, "zellij", "panes")
	if err != nil {
		return false, err
	}
	for _, p := range panes {
		if paneID(p) == agent.ID {
			return !p.Exited, nil
		}
	}
	return false, fmt.Errorf("zellij: pane %s not found in session %s", agent.ID, agent.WorkstreamID)
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
