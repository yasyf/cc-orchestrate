package orchestrate

import (
	"testing"

	"github.com/yasyf/daemonkit"
)

func assertPlatformTrust(t *testing.T, trust daemonkit.Trust) {
	t.Helper()
	if trust.Control == nil {
		t.Fatal("Trust.Control is nil; the drain and broker handoff would admit any same-EUID peer")
	}
	if trust.Control.TeamID != teamID || trust.Control.SigningIdentifier != signingIdentifier {
		t.Fatalf("Trust.Control = %+v", *trust.Control)
	}
}
