// Package worktree isolates an agent's checkout in its own git worktree,
// branched from the project repo's current HEAD, and colocates an independent
// jj repo inside that worktree when the project tracks with jj.
package worktree

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Add creates a git worktree at dest on a fresh branch off repoRoot's current
// HEAD and returns the absolute path to dest. A repo with a .git entry (file or
// directory) — including a colocated jj repo — is driven directly. A
// non-colocated pure jj repo (a .jj directory with no .git) is not yet
// supported and yields a wrapped error.
func Add(ctx context.Context, repoRoot, dest, branch string) (string, error) {
	if !hasGit(repoRoot) && UsesJJ(repoRoot) {
		// TODO: support non-colocated jj by adding the worktree against the
		// backing git store at <repoRoot>/.jj/repo/store/git.
		return "", fmt.Errorf("git worktree add: non-colocated jj repo at %s is not yet supported", repoRoot)
	}
	abs, err := resolveDest(repoRoot, dest)
	if err != nil {
		return "", err
	}
	if _, err := run(ctx, repoRoot, "git worktree add", "git", "worktree", "add", "-b", branch, abs); err != nil {
		return "", err
	}
	return abs, nil
}

// Remove deletes the worktree at dest from the repository rooted at repoRoot.
func Remove(ctx context.Context, repoRoot, dest string) error {
	_, err := run(ctx, repoRoot, "git worktree remove", "git", "worktree", "remove", dest)
	return err
}

// RemoveDirIfEmpty removes dir when it exists and has no entries.
func RemoveDirIfEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read worktree directory %s: %w", dir, err)
	}
	if len(entries) != 0 {
		return nil
	}
	if err := os.Remove(dir); err != nil {
		return fmt.Errorf("remove empty worktree directory %s: %w", dir, err)
	}
	return nil
}

// CurrentBranch returns the branch checked out at repoRoot, via
// git -C <repoRoot> symbolic-ref --short HEAD. symbolic-ref resolves the branch
// even on an unborn HEAD — a freshly initialized repo with no commits — where
// rev-parse --abbrev-ref HEAD fails; a repo create against such a repo therefore
// names its primary workstream after the unborn branch instead of erroring. A
// detached HEAD has no branch and propagates as a wrapped error, as does any git
// failure (not a repo, git missing).
func CurrentBranch(ctx context.Context, repoRoot string) (string, error) {
	out, err := run(ctx, repoRoot, "git symbolic-ref", "git", "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// UsesJJ reports whether repoRoot is a jj repository, i.e. it holds a .jj entry.
func UsesJJ(repoRoot string) bool {
	_, err := os.Stat(filepath.Join(repoRoot, ".jj"))
	return err == nil
}

// InitJJ colocates an independent jj repo inside an already-created git
// worktree. worktreePath must be the worktree itself, not the parent repo root,
// so the new jj repo binds to the worktree's own git rather than the project's.
func InitJJ(ctx context.Context, worktreePath string) error {
	_, err := run(ctx, worktreePath, "jj git init", "jj", "git", "init", "--git-repo", ".")
	return err
}

func hasGit(repoRoot string) bool {
	_, err := os.Stat(filepath.Join(repoRoot, ".git"))
	return err == nil
}

// resolveDest returns dest as an absolute path, resolving a relative dest
// against repoRoot to match how git resolves a relative worktree path from the
// repo's working directory.
func resolveDest(repoRoot, dest string) (string, error) {
	if filepath.IsAbs(dest) {
		return dest, nil
	}
	abs, err := filepath.Abs(filepath.Join(repoRoot, dest))
	if err != nil {
		return "", fmt.Errorf("resolve worktree dest %q: %w", dest, err)
	}
	return abs, nil
}

// run executes name with args in dir, returning stdout and wrapping a non-zero
// exit with label and the captured stderr so the failure carries git's or jj's
// own diagnostic.
func run(ctx context.Context, dir, label, name string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...) //nolint:gosec // G204: runs git/jj with internally-built args by design
	c.Dir = dir
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", label, err, stderr.String())
	}
	return stdout.Bytes(), nil
}
