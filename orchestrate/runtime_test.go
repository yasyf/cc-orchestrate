package orchestrate

import (
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func TestRuntimeIdentityIsExact(t *testing.T) {
	roles := appRoles()
	if roles.Business != trust.UnprotectedRole || roles.Lifecycle != lifecycleRole || roles.StopControl != stopControlRole {
		t.Fatalf("roles = %+v", roles)
	}
	policy, err := appTrustPolicy()
	if err != nil {
		t.Fatal(err)
	}
	if !policy.AllowsUnprotected() || !policy.AllowsReceipt(roles.Lifecycle) ||
		!policy.AllowsReadiness(roles.Lifecycle) || !policy.AllowsStop(roles.StopControl) {
		t.Fatal("trust policy lacks an exact declared authority")
	}
	agent, err := appAgent()
	if err != nil {
		t.Fatal(err)
	}
	if agent.Label != runtimeAgentLabel || agent.RestartPolicy == 0 {
		t.Fatalf("agent = %+v", agent)
	}
	if _, err := agent.Plist(); err != nil {
		t.Fatal(err)
	}
}
