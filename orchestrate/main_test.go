package orchestrate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

// sandboxHome points both HOME and daemonkit's DAEMONKIT_HOME override at one fresh
// temp directory and returns it, symlinks resolved. daemonkit resolves the real home
// through the passwd database and never reads HOME, so a test that sandboxes only
// HOME still writes its durable state — plists, sockets, the artifact store — into
// the developer's real home.
func sandboxHome(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve sandbox home: %v", err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("DAEMONKIT_HOME", dir)
	return dir
}

// TestMain mirrors the consumer trust contract: tests that start the daemon make
// this test binary the runtime's trustExecutable, so it must dispatch the verifier
// child verb before anything else runs, or the serve-time self-probe refuses to
// start the daemon.
func TestMain(m *testing.M) {
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
