package orchestrate

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
)

// scrubExecCmdName is the hidden subcommand the spawn path prepends outermost to a
// child's argv so the child runs with the Claude session markers scrubbed from its
// environment before it execs the real command.
const scrubExecCmdName = "scrub-exec"

// execve replaces the running process with the resolved child. It is a seam so tests
// can stub it: the real syscall.Exec never returns on success, so it cannot be driven
// directly.
var execve = syscall.Exec

// wrapScrubExec wraps a child argv to run under this binary's scrub-exec, which clears
// the inherited Claude session markers before exec'ing the child. It wraps outermost,
// so when the argv is already a pty-host wrapper the pty-host and the claude child
// both run scrubbed.
func wrapScrubExec(self string, command []string) []string {
	return append([]string{self, scrubExecCmdName, "--"}, command...)
}

// scrubExecCmd is the hidden `scrub-exec` command: it scrubs the inherited Claude
// session markers, then execs the argv after `--`. Spawned children must not inherit a
// Claude session's CLAUDECODE/CLAUDE_CODE_* markers or they skip persisting a
// transcript; the terminal's host process (herdr server, cmux app) may carry them from
// whatever session launched it, out of reach of the daemon-level scrub in serve.go. It
// is not user-facing.
func scrubExecCmd() *cobra.Command {
	return &cobra.Command{
		Use:    scrubExecCmdName,
		Short:  "Scrub inherited Claude session env markers, then exec the child argv",
		Hidden: true,
		Args:   cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := scrubClaudeCodeEnv(); err != nil {
				return err
			}
			path, err := exec.LookPath(args[0])
			// A match through a relative PATH entry returns ErrDot; the host
			// shell's own PATH search accepted such a match before this wrapper
			// existed, so keep that behavior by resolving it absolute.
			if errors.Is(err, exec.ErrDot) {
				path, err = filepath.Abs(path)
			}
			if err != nil {
				return fmt.Errorf("resolve %s: %w", args[0], err)
			}
			if err := execve(path, args, os.Environ()); err != nil {
				return fmt.Errorf("exec %s: %w", path, err)
			}
			return nil
		},
	}
}
