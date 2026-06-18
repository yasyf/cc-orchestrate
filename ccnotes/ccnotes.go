// Package ccnotes binds cc-orchestrate's domain entities to the cc-notes CLI: a
// workstream to a cc-notes project, a sprint to a cc-notes sprint, and an agent
// to a cc-notes task on the workstream's branch. It shells out to the cc-notes
// binary scoped to a repo's working directory — cc-notes locates the repo from
// the process cwd, with no repo flag — and is gated by Enabled, so the wiring
// activates only for repos that already use cc-notes.
package ccnotes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// bin is the cc-notes CLI binary name. The in-repo ./cc-notes build is stale; a
// PATH lookup resolves the Homebrew/go-installed binary instead.
const bin = "cc-notes"

// runner runs an external CLI in dir and returns its stdout. It is the seam
// tests stub to assert the argv a helper builds without invoking a real binary,
// mirroring the backend drivers' exec seam.
type runner func(ctx context.Context, dir, name string, args ...string) ([]byte, error)

// run is the package's command seam (cc-notes creates and git ref probes); tests
// swap it to fake the binaries. lookPath reports whether the cc-notes binary
// resolves on PATH; tests swap it to fake installation. Neither carries mutable
// per-call state, so the package keeps no struct.
var (
	run      runner = execRun
	lookPath        = exec.LookPath
)

// execRun runs name with args in dir and returns stdout, wrapping a non-zero exit
// with the captured stderr so the failure carries the command's own diagnostic.
func execRun(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...)
	c.Dir = dir
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%s %v: %w: %s", name, args, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// Enabled gates the whole cc-notes integration: it reports true only when the
// cc-notes binary is on PATH and repoRoot already holds cc-notes entities (any
// ref under refs/cc-notes/). It mirrors backend.Available and worktree.UsesJJ —
// intentional optionality, not a defensive guard — so the bindings activate only
// for repos that have opted into cc-notes. A missing binary or no entities yields
// false and the caller skips every cc-notes call.
func Enabled(ctx context.Context, repoRoot string) bool {
	if _, err := lookPath(bin); err != nil {
		return false
	}
	out, err := run(ctx, repoRoot, "git", "for-each-ref", "--count=1", "refs/cc-notes/")
	if err != nil {
		return false
	}
	return len(bytes.TrimSpace(out)) > 0
}

// CreateProject creates a cc-notes project titled name in the repo at repoRoot and
// returns its full entity id.
func CreateProject(ctx context.Context, repoRoot, name string) (string, error) {
	out, err := run(ctx, repoRoot, bin, "project", "add", name, "--json")
	if err != nil {
		return "", fmt.Errorf("cc-notes project add %q: %w", name, err)
	}
	return parseID(out)
}

// CreateSprint creates a cc-notes sprint titled name under projectID in the repo
// at repoRoot and returns its full entity id.
func CreateSprint(ctx context.Context, repoRoot, projectID, name string) (string, error) {
	out, err := run(ctx, repoRoot, bin, "sprint", "add", name, "--project", projectID, "--json")
	if err != nil {
		return "", fmt.Errorf("cc-notes sprint add %q: %w", name, err)
	}
	return parseID(out)
}

// CreateTask creates a cc-notes task titled name on branch, a member of sprintID
// and projectID, in the repo at repoRoot, and returns its full entity id. It
// passes --no-validation-criteria because task add otherwise requires at least
// one acceptance criterion; an orchestrated agent has none.
func CreateTask(ctx context.Context, repoRoot, name, branch, sprintID, projectID string) (string, error) {
	out, err := run(ctx, repoRoot, bin,
		"task", "add", name,
		"--branch", branch,
		"--sprint", sprintID,
		"--project", projectID,
		"--no-validation-criteria",
		"--json")
	if err != nil {
		return "", fmt.Errorf("cc-notes task add %q: %w", name, err)
	}
	return parseID(out)
}

// parseID reads the id field from a cc-notes --json DTO, the single compact JSON
// line every add command emits on stdout.
func parseID(out []byte) (string, error) {
	var dto struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &dto); err != nil {
		return "", fmt.Errorf("parse cc-notes json %q: %w", out, err)
	}
	if dto.ID == "" {
		return "", fmt.Errorf("cc-notes returned no id: %q", out)
	}
	return dto.ID, nil
}
