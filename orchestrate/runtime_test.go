package orchestrate

import (
	"slices"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit"
)

func TestRuntimeIdentityIsExact(t *testing.T) {
	sandboxHome(t) // Stable() reads the running executable; every derived path lands under the home
	d, err := appDaemon()
	if err != nil {
		t.Fatal(err)
	}
	if d.Label != runtimeAgentLabel {
		t.Fatalf("Label = %q, want %q", d.Label, runtimeAgentLabel)
	}
	if !slices.Equal(d.Args, []string{"daemon"}) {
		t.Fatalf("Args = %v, want [daemon]", d.Args)
	}
	if d.Restart != daemonkit.RestartOnFailure {
		t.Fatalf("Restart = %v, want RestartOnFailure", d.Restart)
	}
	if d.Log != appPaths().LogPath() {
		t.Fatalf("Log = %q, want %q", d.Log, appPaths().LogPath())
	}
	if len(d.Schemas) == 0 || d.Schemas[0] != daemon.WireBuild {
		t.Fatalf("Schemas = %v, want [%q]", d.Schemas, daemon.WireBuild)
	}
	assertPlatformTrust(t, d.Trust)
	if d.Trust.Business != nil {
		t.Fatalf("Trust.Business = %v, want nil for the same-EUID floor", d.Trust.Business)
	}
	if err := d.ValidateForServe(); err != nil {
		t.Fatalf("ValidateForServe: %v", err)
	}
	// ValidateForClient is what refuses an unstated Trust.Serving.
	if err := d.ValidateForClient(); err != nil {
		t.Fatalf("ValidateForClient: %v", err)
	}
}
