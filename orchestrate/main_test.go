package orchestrate

import (
	"os"
	"path/filepath"
	"testing"
)

// sandboxHome points both HOME and daemonkit's DAEMONKIT_HOME override at one fresh
// temp directory and returns it, symlinks resolved. daemonkit resolves the real home
// through the passwd database and never reads HOME, so a test that sandboxes only
// HOME still writes its durable state — plists, sockets, the artifact store — into
// the developer's real home. The base is /tmp rather than t.TempDir(): every daemon
// socket is this home joined with a launchd label and "/daemon.sock", and
// t.TempDir()'s macOS path embeds the test name in enough bytes to push that past
// darwin's 104-byte sun_path limit.
func sandboxHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "cco")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	dir, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatalf("resolve sandbox home: %v", err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("DAEMONKIT_HOME", dir)
	return dir
}
