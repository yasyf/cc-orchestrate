package orchestrate

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
)

func TestAppPathsUseEpochOneNamespace(t *testing.T) {
	// The sandbox's own directory, not os.Getenv("HOME"): appPaths resolves through
	// daemonkit, which reads the passwd database or DAEMONKIT_HOME and never HOME.
	home := sandboxHome(t)
	if got, want := appPaths().StateDir(), filepath.Join(home, ".cc-orchestrate-v1"); got != want {
		t.Fatalf("StateDir() = %q, want %q", got, want)
	}
}

// TestLauncherSharesTheRuntimeIdentity proves the launcher half and the serving
// half read one declaration: a launcher built from a different Daemon would dial a
// socket the daemon never binds.
func TestLauncherSharesTheRuntimeIdentity(t *testing.T) {
	sandboxHome(t) // launcher stages the test binary as the stable program under the home
	l, err := launcher()
	if err != nil {
		t.Fatal(err)
	}
	d, err := appDaemon()
	if err != nil {
		t.Fatal(err)
	}
	if l.Daemon.Label != d.Label {
		t.Fatalf("Label = %q, want %q", l.Daemon.Label, d.Label)
	}
	if len(l.Daemon.Schemas) == 0 || l.Daemon.Schemas[0] != daemon.WireBuild {
		t.Fatalf("Schemas = %v, want [%q]", l.Daemon.Schemas, daemon.WireBuild)
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
