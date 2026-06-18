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
		initGitRepo(t, ctx, repo)

		dest := filepath.Join(t.TempDir(), "wt")
		got, err := Add(ctx, repo, dest, "feat-x")
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if _, err := os.Stat(got); err != nil {
			t.Fatalf("worktree dir %s: %v", got, err)
		}

		real := evalSymlinks(t, got)
		listed := mustRun(t, ctx, repo, "git", "worktree", "list", "--porcelain")
		if !strings.Contains(listed, real) && !strings.Contains(listed, got) {
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
		listed = mustRun(t, ctx, repo, "git", "worktree", "list", "--porcelain")
		if strings.Contains(listed, real) || strings.Contains(listed, got) {
			t.Fatalf("worktree still listed after Remove:\n%s", listed)
		}
	})

	t.Run("jj colocated", func(t *testing.T) {
		if _, err := exec.LookPath("jj"); err != nil {
			t.Skip("jj not installed")
		}
		repo := t.TempDir()
		initGitRepo(t, ctx, repo)
		mustRun(t, ctx, repo, "jj", "git", "init", "--colocate")
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
		initGitRepo(t, ctx, repo)
		mustRun(t, ctx, repo, "git", "checkout", "-b", "feature/x")
		got, err := CurrentBranch(ctx, repo)
		if err != nil {
			t.Fatalf("CurrentBranch: %v", err)
		}
		if got != "feature/x" {
			t.Fatalf("CurrentBranch = %q, want feature/x", got)
		}
	})

	t.Run("errors outside a repo", func(t *testing.T) {
		if _, err := CurrentBranch(ctx, t.TempDir()); err == nil {
			t.Fatal("CurrentBranch outside a repo: want error, got nil")
		}
	})
}

func initGitRepo(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	mustRun(t, ctx, dir, "git", "init")
	mustRun(t, ctx, dir, "git", "config", "user.email", "test@example.com")
	mustRun(t, ctx, dir, "git", "config", "user.name", "cc-orchestrate test")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustRun(t, ctx, dir, "git", "add", "README.md")
	mustRun(t, ctx, dir, "git", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", "init")
}

func mustRun(t *testing.T, ctx context.Context, dir, name string, args ...string) string {
	t.Helper()
	c := exec.CommandContext(ctx, name, args...)
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
