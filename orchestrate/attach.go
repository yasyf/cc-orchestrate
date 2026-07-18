package orchestrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-orchestrate/backend"
)

// runAgentAttach hands the caller's terminal to a running agent's backend session.
// agentView carries no workspace handle, so it resolves the backend WorkspaceHandle
// through the agent's sprint and workstream — the chain backendAgentHandle walks
// daemon-side — then execs into the multiplexer's attach client. It emits nothing on
// success because execAttach replaces this process.
func runAgentAttach(c *cobra.Command, id string) error {
	reply, err := runOp(c, mAgentShow.op(), map[string]string{"agent_id": id})
	if err != nil {
		return err
	}
	var a agentView
	if err := json.Unmarshal(reply.Body, &a); err != nil {
		return err
	}
	if a.Status != string(StatusActive) {
		return fmt.Errorf("agent %s is %s, not active", id, a.Status)
	}

	sReply, err := runOp(c, mSprintShow.op(), map[string]string{"id": a.SprintID})
	if err != nil {
		return err
	}
	var sp sprintView
	if err := json.Unmarshal(sReply.Body, &sp); err != nil {
		return err
	}

	wReply, err := runOp(c, mWorkstreamShow.op(), map[string]string{"id": sp.WorkstreamID})
	if err != nil {
		return err
	}
	var ws workstreamView
	if err := json.Unmarshal(wReply.Body, &ws); err != nil {
		return err
	}

	// WorkstreamID is the backend WorkspaceHandle, not the orchestrate workstream id —
	// mirroring backendAgentHandle, since the backend attach targets the workspace.
	return execAttach(c.Context(), backend.AgentHandle{
		Backend:      backend.Name(a.Backend),
		ID:           a.TerminalHandle,
		WorkstreamID: ws.WorkspaceHandle,
		Name:         a.Name,
		SessionID:    a.SessionID,
	})
}

// execAttach resolves the agent's backend, runs its focus pre-steps, and replaces
// this process with the backend's foreground attach client so the multiplexer owns
// the TTY, signals, and exit code. It never returns on success; a backend that does
// not implement Attacher (or is not registered) fails fast. The LookPath ErrDot swap
// mirrors scrubExecCmd — a relative PATH match the host shell would have accepted
// resolves absolute.
func execAttach(ctx context.Context, agent backend.AgentHandle) error {
	b, _ := backend.Get(agent.Backend)
	at, ok := b.(backend.Attacher)
	if !ok {
		return fmt.Errorf("backend %s does not support attach", agent.Backend)
	}
	argv, err := at.AttachArgv(ctx, agent)
	if err != nil {
		return err
	}
	path, err := exec.LookPath(argv[0])
	if errors.Is(err, exec.ErrDot) {
		path, err = filepath.Abs(path)
	}
	if err != nil {
		return fmt.Errorf("resolve %s: %w", argv[0], err)
	}
	if err := execve(path, argv, os.Environ()); err != nil {
		return fmt.Errorf("exec %s: %w", path, err)
	}
	return nil
}
