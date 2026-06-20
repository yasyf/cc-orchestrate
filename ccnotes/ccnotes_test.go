package ccnotes

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput() //nolint:gosec // G204: test helper runs git with fixed args in a temp repo
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

// gitInit creates a hermetic git repo at dir on branch main with an identity,
// pinning global/system config to /dev/null and setting CC_NOTES_ACTOR so the
// in-process store resolves an author without leaning on host git config.
func gitInit(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	t.Setenv("CC_NOTES_ACTOR", "cc-orchestrate test <test@example.com>")
	mustGit(t, dir, "init", "-q", "-b", "main")
	mustGit(t, dir, "config", "user.name", "Test User")
	mustGit(t, dir, "config", "user.email", "test@example.com")
}

// isHexID reports whether id looks like a cc-notes entity id: 40 or 64
// lowercase hex characters.
func isHexID(id string) bool {
	if len(id) != 40 && len(id) != 64 {
		return false
	}
	for _, r := range id {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// TestAdapterRoundTrip drives the real cc-notes library through the adapter
// against a fresh git repo: Enabled flips from false to true once entities
// exist, and project, sprint, and task each come back as a distinct full-hex
// entity id — no stubbed binary, real refs on disk.
func TestAdapterRoundTrip(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	gitInit(t, dir)

	if Enabled(ctx, dir) {
		t.Fatal("Enabled on a fresh repo = true, want false (no cc-notes refs yet)")
	}

	project, err := CreateProject(ctx, dir, "Build the thing")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if !isHexID(project) {
		t.Errorf("project id = %q, want a full hex entity id", project)
	}

	if !Enabled(ctx, dir) {
		t.Fatal("Enabled after first create = false, want true (a ref now exists)")
	}

	sprint, err := CreateSprint(ctx, dir, project, "Sprint 1")
	if err != nil {
		t.Fatalf("CreateSprint: %v", err)
	}
	if !isHexID(sprint) || sprint == project {
		t.Errorf("sprint id = %q, want a distinct hex id", sprint)
	}

	task, err := CreateTask(ctx, dir, "Do the work", "feature/x", sprint, project)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if !isHexID(task) || task == sprint || task == project {
		t.Errorf("task id = %q, want a distinct hex id", task)
	}
}

// TestEnabledNonRepo confirms the gate is closed — and never panics — for a
// path that is not a git repository.
func TestEnabledNonRepo(t *testing.T) {
	if Enabled(t.Context(), t.TempDir()) {
		t.Error("Enabled on a non-git dir = true, want false")
	}
}
