package orchestrate

import (
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit"
)

const runtimeAgentLabel = "com.yasyf.cc-orchestrate"

// appDaemon is the one identity the launcher half and the serving half both
// read: label, program, and service policy declared once.
func appDaemon() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable(buildVersion())
	if err != nil {
		return daemonkit.Daemon{}, err
	}
	return daemon.Spec(daemonkit.Daemon{
		Label:   runtimeAgentLabel,
		Program: program,
		Args:    []string{"daemon"},
		Log:     appPaths().LogPath(),
		Restart: daemonkit.RestartOnFailure,
	}), nil
}
