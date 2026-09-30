package orchestrate

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"
)

// appTrust holds every lane to the same-EUID floor: linux has no code identity
// to pin, so any process of the VM's one user can drive or stand in for the
// daemon. That is sound only on a single-user host.
func appTrust() daemonkit.Trust {
	return daemonkit.Trust{Serving: daemonkit.ServingSameUser()}
}

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
