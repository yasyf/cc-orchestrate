package orchestrate

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"
)

const (
	teamID            = "SXKCTF23Q2"
	signingIdentifier = "cc-orchestrate"
)

// appTrust gates the drain and the broker handoff on this binary's own signing
// identity; Business is left to the same-EUID floor, and Serving takes the
// named waiver, because a dev build of the CLI is unsigned and could satisfy
// no requirement.
func appTrust() daemonkit.Trust {
	requirement := daemonkit.Requirement{TeamID: teamID, SigningIdentifier: signingIdentifier}
	return daemonkit.Trust{
		Control: &requirement,
		Serving: daemonkit.ServingSameUser(),
	}
}

func platformCmds() []*cobra.Command { return nil }
