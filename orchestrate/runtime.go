package orchestrate

import (
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit"
)

const runtimeAgentLabel = "com.yasyf.cc-orchestrate"

// appDaemon is the one identity the launcher half and the serving half both
// read: label, program, service policy, and every trust lane declared once.
func appDaemon() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, err
	}
	return daemon.Spec(daemonkit.Daemon{
		Label:   runtimeAgentLabel,
		Program: program,
		Args:    []string{"daemon"},
		Log:     appPaths().LogPath(),
		Restart: daemonkit.RestartOnFailure,
		Trust:   appTrust(),
	}), nil
}
