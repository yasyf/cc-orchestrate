package orchestrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
)

func TestAppPathsUseEpochOneNamespace(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if got, want := appPaths().StateDir(), filepath.Join(os.Getenv("HOME"), ".cc-orchestrate-v1"); got != want {
		t.Fatalf("StateDir() = %q, want %q", got, want)
	}
}

func TestLauncherUsesSharedWireBuildAndCurrentRuntimeBuild(t *testing.T) {
	l := launcher()
	if l.WireBuild != daemon.WireBuild {
		t.Fatalf("WireBuild = %q, want %q", l.WireBuild, daemon.WireBuild)
	}
	if l.RuntimeBuild != buildVersion() {
		t.Fatalf("RuntimeBuild = %q, want %q", l.RuntimeBuild, buildVersion())
	}
}

func TestRootIncludesHiddenDaemonStopControl(t *testing.T) {
	control, _, err := Root().Find([]string{daemon.StopControlCommand})
	if err != nil {
		t.Fatalf("Find(%s): %v", daemon.StopControlCommand, err)
	}
	if control.Use != daemon.StopControlCommand || !control.Hidden {
		t.Fatalf("stop control command use=%q hidden=%t", control.Use, control.Hidden)
	}
}

// TestTerminalEventOnlyExited proves the daemon's terminal-event predicate closes a
// subject only on EventExited: every non-terminal lifecycle event must leave the
// subject open.
func TestTerminalEventOnlyExited(t *testing.T) {
	isTerminal := deps().TerminalEvent
	for _, tc := range []struct {
		event string
		want  bool
	}{
		{EventExited, true},
		{EventRestarted, false},
		{EventAbandoned, false},
		{EventStatus, false},
		{EventSpawned, false},
		{EventAdopted, false},
	} {
		t.Run(tc.event, func(t *testing.T) {
			if got := isTerminal(tc.event); got != tc.want {
				t.Fatalf("TerminalEvent(%q) = %v, want %v", tc.event, got, tc.want)
			}
		})
	}
}
