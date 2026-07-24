package orchestrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ptyhost"
)

// ptyHostCmdName is the hidden subcommand the spawn path prepends to a child's argv
// when its backend cannot capture natively, so the child runs under a pty-host this
// binary owns and the prober can read and drive its screen.
const ptyHostCmdName = "pty-host"

// reportChildExitTimeout bounds the pty-host's best-effort child-exit report to the
// daemon, so a slow or wedged daemon never delays the wrapper's own exit.
const reportChildExitTimeout = 5 * time.Second

// ptySocketPath is the control socket one pty-host incarnation serves, derived
// deterministically from the session id and spawn nonce so the spawn wrapper, the
// host, and the prober client (via the agent row's nonce) all resolve the same path.
// Deriving per incarnation is what makes a kill-driven respawn race-free: the
// replacement binds its own path, so settling the old daemonkit listener cannot
// disturb the replacement's socket. The suffix is 64 bits of the
// full nonce's SHA-256 (16 hex chars) — wide enough that two incarnations of one
// session can never share a path, unlike a truncated-nonce prefix — while the full
// path stays inside the OS sun_path limit under the production StateDir.
func ptySocketPath(sessionID, spawnNonce string) string {
	if spawnNonce == "" {
		panic("pty socket requires spawn nonce")
	}
	sum := sha256.Sum256([]byte(spawnNonce))
	name := sessionID + "-" + hex.EncodeToString(sum[:8])
	return filepath.Join(appPaths().StateDir(), "pty", name+".sock")
}

func ptyProcessStorePath(sessionID string) string {
	if sessionID == "" {
		panic("pty process store requires session id")
	}
	sum := sha256.Sum256([]byte(sessionID))
	return filepath.Join(appPaths().StateDir(), "pty", "process-"+hex.EncodeToString(sum[:8])+".db")
}

// wrapForCapture composes the launcher prefix and child argv, wrapping the result
// under this binary's pty-host when the backend cannot capture its terminal
// natively; a capturing backend gets the composed argv unchanged. Under the
// pty-host, executables are resolved here because the host may run under a
// different PATH: a bare claude resolves past a backend's wrapper shim even behind
// a launcher prefix, and the launcher head resolves via lookupPath. spawnNonce is
// this incarnation's identity, carried by the wrapper into its childExited report.
func wrapForCapture(self, sessionID, spawnNonce string, launcher, command []string, caps backend.Caps) ([]string, error) {
	if caps.Has(backend.CanCapture) {
		return append(slices.Clone(launcher), command...), nil
	}
	if command[0] == "claude" {
		resolved, err := backend.ResolveClaude()
		if err != nil {
			return nil, fmt.Errorf("resolve claude: %w", err)
		}
		command = slices.Clone(command)
		command[0] = resolved
	}
	full := append(slices.Clone(launcher), command...)
	executable := full[0]
	if len(launcher) > 0 {
		resolved, err := lookupPath(launcher[0])
		if err != nil {
			return nil, fmt.Errorf("resolve launcher %q: %w", launcher[0], err)
		}
		executable = resolved
	}
	return wrapPTYHost(self, sessionID, spawnNonce, executable, full), nil
}

// wrapPTYHost rewrites an argv to run under the pty-host with the resolved child
// executable in place of command[0].
func wrapPTYHost(self, sessionID, spawnNonce, executable string, command []string) []string {
	return append([]string{self, ptyHostCmdName, "--session-id", sessionID, "--spawn-nonce", spawnNonce, "--", executable}, command[1:]...)
}

// ptyHostCmd is the hidden `pty-host` command: it runs the argv after `--` under a
// pseudo-terminal and serves the session's control socket, so a non-capturing
// backend's agent can still be read and driven by the prober. It is not user-facing.
func ptyHostCmd() *cobra.Command {
	var sessionID, spawnNonce string
	c := &cobra.Command{
		Use:    ptyHostCmdName,
		Short:  "Host a child under a pseudo-terminal and serve its capture/keys socket",
		Hidden: true,
		Args:   cobra.MinimumNArgs(1),
		PreRunE: func(*cobra.Command, []string) error {
			if sessionID == "" {
				return errors.New("pty-host requires --session-id")
			}
			if spawnNonce == "" {
				return errors.New("pty-host requires --spawn-nonce")
			}
			return nil
		},
		RunE: func(c *cobra.Command, args []string) error {
			return ptyhost.Run(c.Context(), ptyhost.Options{
				Socket:       ptySocketPath(sessionID, spawnNonce),
				ProcessStore: ptyProcessStorePath(sessionID),
				Argv:         args,
				RuntimeBuild: buildVersion(),
				OnChildExit:  func() { reportChildExit(sessionID, spawnNonce) },
			})
		},
	}
	c.Flags().StringVar(&sessionID, "session-id", "", "the child agent's session id")
	c.Flags().StringVar(&spawnNonce, "spawn-nonce", "", "this incarnation's spawn nonce, echoed in the child-exit report")
	return c
}

// reportChildExit tells the daemon its pty-hosted child has exited, so the supervisor
// resumes the agent's session immediately instead of waiting for a membership or
// staleness fallback to notice. It is the pty-host's last act and best-effort: a bare
// Do that never launches a daemon, so a daemon that is down or unreachable yields an
// ignored error and the wrapper still exits cleanly, with the fallbacks covering that
// window. spawnNonce identifies the reporting incarnation, so a report delayed past a
// concurrent kill+respawn can never be mistaken for the fresh incarnation's death.
func reportChildExit(sessionID, spawnNonce string) {
	body, err := json.Marshal(agentChildExitedRequest{SessionID: sessionID, SpawnNonce: spawnNonce})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportChildExitTimeout)
	defer cancel()
	client, err := newClient(ctx)
	if err != nil {
		return
	}
	defer func() { _ = client.Close() }()
	_, _ = client.Do(ctx, daemon.Envelope{Op: mAgentChildExited.op(), Session: AppName, Body: body})
}
