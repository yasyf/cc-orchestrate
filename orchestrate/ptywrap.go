package orchestrate

import (
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ptyhost"
)

// ptyHostCmdName is the hidden subcommand the spawn path prepends to a child's argv
// when its backend cannot capture natively, so the child runs under a pty-host this
// binary owns and the prober can read and drive its screen.
const ptyHostCmdName = "pty-host"

// ptySocketPath is the control socket a session's pty-host serves, derived
// deterministically from the session id so the spawn wrapper, the host, and the
// prober client all resolve the same path.
func ptySocketPath(sessionID string) string {
	return filepath.Join(appPaths().StateDir(), "pty", sessionID+".sock")
}

// wrapForCapture wraps a child argv to run under this binary's pty-host when the
// backend cannot capture its terminal natively; a capturing backend's argv is
// returned unchanged. The child's claude is resolved to the real binary first so the
// host does not recurse through a backend's claude wrapper shim.
func wrapForCapture(self, sessionID string, command []string, caps backend.Caps) ([]string, error) {
	if caps.Has(backend.CanCapture) {
		return command, nil
	}
	realClaude, err := backend.ResolveClaude()
	if err != nil {
		return nil, err
	}
	return wrapPTYHost(self, sessionID, realClaude, command), nil
}

// wrapPTYHost rewrites a claude argv to run under the pty-host: this binary as the
// host, the session id, then the real claude path in place of command[0] ("claude").
func wrapPTYHost(self, sessionID, realClaude string, command []string) []string {
	return append([]string{self, ptyHostCmdName, "--session-id", sessionID, "--", realClaude}, command[1:]...)
}

// ptyHostCmd is the hidden `pty-host` command: it runs the argv after `--` under a
// pseudo-terminal and serves the session's control socket, so a non-capturing
// backend's agent can still be read and driven by the prober. It is not user-facing.
func ptyHostCmd() *cobra.Command {
	var sessionID string
	c := &cobra.Command{
		Use:    ptyHostCmdName,
		Short:  "Host a child under a pseudo-terminal and serve its capture/keys socket",
		Hidden: true,
		Args:   cobra.MinimumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return ptyhost.Run(c.Context(), ptyhost.Options{
				Socket: ptySocketPath(sessionID),
				Argv:   args,
			})
		},
	}
	c.Flags().StringVar(&sessionID, "session-id", "", "the child agent's session id")
	return c
}
