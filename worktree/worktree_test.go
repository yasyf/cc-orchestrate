package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorktree(t *testing.T) {
	// Ignore the developer's global/system git config (notably any global
	// core.hooksPath such as a prek pre-commit/post-checkout hook) so the real
	// repos these subtests build behave identically everywhere.
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	ctx := context.Background()

	t.Run("git", func(t *testing.T) {
		repo := t.TempDir()
		initGitRepo(ctx, t, repo)

		dest := filepath.Join(t.TempDir(), "wt")
		got, err := Add(ctx, repo, dest, "feat-x")
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if _, err := os.Stat(got); err != nil {
			t.Fatalf("worktree dir %s: %v", got, err)
		}

		realPath := evalSymlinks(t, got)
		listed := mustRun(ctx, t, repo, "git", "worktree", "list", "--porcelain")
		if !strings.Contains(listed, realPath) && !strings.Contains(listed, got) {
			t.Fatalf("worktree %s not listed:\n%s", got, listed)
		}
		if !strings.Contains(listed, "refs/heads/feat-x") {
			t.Fatalf("branch feat-x not listed:\n%s", listed)
		}

		if err := Remove(ctx, repo, got); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if _, err := os.Stat(got); !os.IsNotExist(err) {
			t.Fatalf("worktree dir present after Remove: stat err = %v", err)
		}
		listed = mustRun(ctx, t, repo, "git", "worktree", "list", "--porcelain")
		if strings.Contains(listed, realPath) || strings.Contains(listed, got) {
			t.Fatalf("worktree still listed after Remove:\n%s", listed)
		}
	})

	t.Run("jj colocated", func(t *testing.T) {
		if _, err := exec.LookPath("jj"); err != nil {
			t.Skip("jj not installed")
		}
		repo := t.TempDir()
		initGitRepo(ctx, t, repo)
		mustRun(ctx, t, repo, "jj", "git", "init", "--colocate")
		if !UsesJJ(repo) {
			t.Fatalf("UsesJJ(%s) = false after --colocate", repo)
		}

		dest := filepath.Join(t.TempDir(), "wt")
		got, err := Add(ctx, repo, dest, "feat-jj")
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := InitJJ(ctx, got); err != nil {
			t.Fatalf("InitJJ: %v", err)
		}
		if _, err := os.Stat(filepath.Join(got, ".jj")); err != nil {
			t.Fatalf("expected .jj in worktree %s: %v", got, err)
		}
	})

	t.Run("jj non-colocated", func(t *testing.T) {
		// A pure (non-colocated) jj repo's git store lives at
		// <repoRoot>/.jj/repo/store/git; Add returns a documented "not yet
		// supported" error there (see the // TODO in Add). Exercising the real
		// path is deferred until that store wiring lands.
		t.Skip("non-colocated jj unsupported; see Add TODO")
	})
}

func TestCurrentBranch(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	ctx := context.Background()

	t.Run("reports the checked-out branch", func(t *testing.T) {
		repo := t.TempDir()
		initGitRepo(ctx, t, repo)
		mustRun(ctx, t, repo, "git", "checkout", "-b", "feature/x")
		got, err := CurrentBranch(ctx, repo)
		if err != nil {
			t.Fatalf("CurrentBranch: %v", err)
		}
		if got != "feature/x" {
			t.Fatalf("CurrentBranch = %q, want feature/x", got)
		}
	})

	t.Run("reports the branch on an unborn HEAD", func(t *testing.T) {
		// A freshly initialized repo with no commits: rev-parse --abbrev-ref HEAD
		// would fail here, but symbolic-ref resolves the unborn branch.
		repo := t.TempDir()
		mustRun(ctx, t, repo, "git", "init", "-b", "main")
		got, err := CurrentBranch(ctx, repo)
		if err != nil {
			t.Fatalf("CurrentBranch on unborn HEAD: %v", err)
		}
		if got != "main" {
			t.Fatalf("CurrentBranch = %q, want main", got)
		}
	})

	t.Run("errors outside a repo", func(t *testing.T) {
		if _, err := CurrentBranch(ctx, t.TempDir()); err == nil {
			t.Fatal("CurrentBranch outside a repo: want error, got nil")
		}
	})
}

func TestRemoveDirIfEmpty(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setup      func(t *testing.T, dir string)
		wantExists bool
	}{
		{
			name: "removes parent after its last child is removed",
			setup: func(t *testing.T, dir string) {
				t.Helper()
				child := filepath.Join(dir, "last-worktree")
				if err := os.MkdirAll(child, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(child); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "leaves a non-empty parent alone",
			setup: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Join(dir, "remaining-worktree"), 0o750); err != nil {
					t.Fatal(err)
				}
			},
			wantExists: true,
		},
		{
			name:  "absent parent is a no-op",
			setup: func(*testing.T, string) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "repo-worktrees")
			tc.setup(t, dir)
			if err := RemoveDirIfEmpty(dir); err != nil {
				t.Fatalf("RemoveDirIfEmpty: %v", err)
			}
			_, err := os.Stat(dir)
			if tc.wantExists && err != nil {
				t.Fatalf("worktree parent should remain: %v", err)
			}
			if !tc.wantExists && !os.IsNotExist(err) {
				t.Fatalf("worktree parent should be absent: %v", err)
			}
		})
	}
}

func initGitRepo(ctx context.Context, t *testing.T, dir string) {
	t.Helper()
	mustRun(ctx, t, dir, "git", "init")
	mustRun(ctx, t, dir, "git", "config", "user.email", "test@example.com")
	mustRun(ctx, t, dir, "git", "config", "user.name", "cc-orchestrate test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustRun(ctx, t, dir, "git", "add", "README.md")
	mustRun(ctx, t, dir, "git", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "init")
}

func mustRun(ctx context.Context, t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	c := exec.CommandContext(ctx, name, args...) //nolint:gosec // G204: test helper runs git/jj with fixed args in a temp repo
	c.Dir = dir
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func evalSymlinks(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatalf("eval symlinks %s: %v", p, err)
	}
	return r
}
