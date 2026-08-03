package orchestrate

import (
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit"
)

const (
	runtimeAgentLabel = "com.yasyf.cc-orchestrate"
	teamID            = "SXKCTF23Q2"
	signingIdentifier = "cc-orchestrate"
)

// appDaemon is the one identity the launcher half and the serving half both
// read: label, program, launchd policy, and every trust lane declared once.
// Control gates the drain and the broker handoff on this binary's own signing
// identity; Business is left to the same-EUID floor, and Serving takes the
// named waiver, because a dev build of the CLI is unsigned and could satisfy
// no requirement.
func appDaemon() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, err
	}
	requirement := daemonkit.Requirement{TeamID: teamID, SigningIdentifier: signingIdentifier}
	return daemon.Spec(daemonkit.Daemon{
		Label:   runtimeAgentLabel,
		Program: program,
		Args:    []string{"daemon"},
		Log:     appPaths().LogPath(),
		Restart: daemonkit.RestartOnFailure,
		Trust: daemonkit.Trust{
			Control: &requirement,
			Serving: daemonkit.ServingSameUser(),
		},
	}), nil
}
