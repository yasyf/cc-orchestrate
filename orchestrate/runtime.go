package orchestrate

import (
	"os"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
)

const (
	runtimeAgentLabel = "com.yasyf.cc-orchestrate"
	lifecycleRole     = "com.yasyf.cc-orchestrate.lifecycle.v1"
	stopControlRole   = "com.yasyf.cc-orchestrate.stop.v1"
	teamID            = "SXKCTF23Q2"
	signingIdentifier = "cc-orchestrate"
)

func appRoles() daemon.Roles {
	return daemon.Roles{
		Business: trust.UnprotectedRole, Lifecycle: lifecycleRole, StopControl: stopControlRole,
	}
}

func appTrustPolicy() (trust.TrustPolicy, error) {
	roles := appRoles()
	requirement := trust.Requirement{TeamID: teamID, SigningIdentifier: signingIdentifier}
	return trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
		Roles:          map[trust.PeerRole]trust.Requirement{roles.Lifecycle: requirement, roles.StopControl: requirement},
		StopRoles:      []trust.PeerRole{roles.StopControl},
		ReceiptRoles:   []trust.PeerRole{roles.Lifecycle},
		ReadinessRoles: []trust.PeerRole{roles.Lifecycle},
	})
}

func appAgent() (service.Agent, error) {
	executable, err := service.CanonicalExecutable()
	if err != nil {
		return service.Agent{}, err
	}
	return service.Agent{
		Label: runtimeAgentLabel, Program: executable, Args: []string{"daemon"},
		LogPath: appPaths().LogPath(), RestartPolicy: service.RestartOnFailure,
	}, nil
}
