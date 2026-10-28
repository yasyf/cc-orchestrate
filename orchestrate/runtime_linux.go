package orchestrate

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"
)

func platformCmds() []*cobra.Command { return []*cobra.Command{superviseCmd()} }

// superviseCmd runs the daemon's foreground supervisor, the process linux runs
// in launchd's place. The workspace starts and owns it; while it runs, every
// command that needs the daemon converges it through this supervisor.
func superviseCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "supervise",
		Short:  "Supervise the background daemon in the foreground",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return daemonkit.Supervise(c.Context(), runtimeAgentLabel)
		},
	}
}
