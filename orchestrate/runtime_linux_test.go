package orchestrate

import (
	"testing"

	"github.com/yasyf/daemonkit"
)

func assertPlatformTrust(t *testing.T, trust daemonkit.Trust) {
	t.Helper()
	if trust.Control != nil {
		t.Fatalf("Trust.Control = %+v, want nil: linux has no signing verifier", *trust.Control)
	}
}
