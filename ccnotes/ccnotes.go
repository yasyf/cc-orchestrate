// Package ccnotes binds cc-orchestrate's domain entities to cc-notes: a
// workstream to a cc-notes project, a sprint to a cc-notes sprint, and an agent
// to a cc-notes task on the workstream's branch. It drives the cc-notes library
// in-process, scoped to a repo's working directory via notes.Open(repoRoot),
// and is gated by Enabled, so the wiring activates only for repos that already
// use cc-notes.
package ccnotes

import (
	"context"
	"fmt"

	"github.com/yasyf/cc-notes/model"
	"github.com/yasyf/cc-notes/notes"
)

// Enabled gates the whole cc-notes integration: it reports true only when
// repoRoot already holds cc-notes entities (any ref under refs/cc-notes/),
// probed in-process via the facade's HasNotes. It mirrors backend.Available and
// worktree.UsesJJ — intentional optionality, not a defensive guard — so the
// bindings activate only for repos that have opted into cc-notes. A path that is
// not a git repository, or a repo with no cc-notes entities, yields false and
// the caller skips every cc-notes call.
func Enabled(ctx context.Context, repoRoot string) bool {
	c, err := notes.Open(repoRoot)
	if err != nil {
		return false
	}
	ok, err := c.HasNotes(ctx)
	return err == nil && ok
}

// CreateProject creates a cc-notes project titled name in the repo at repoRoot
// and returns its full entity id.
func CreateProject(ctx context.Context, repoRoot, name string) (string, error) {
	c, err := notes.Open(repoRoot)
	if err != nil {
		return "", fmt.Errorf("cc-notes open %q: %w", repoRoot, err)
	}
	project, _, err := c.CreateProject(ctx, notes.ProjectSpec{Title: name})
	if err != nil {
		return "", fmt.Errorf("cc-notes create project %q: %w", name, err)
	}
	return string(project.ID), nil
}

// CreateSprint creates a cc-notes sprint titled name under projectID in the repo
// at repoRoot and returns its full entity id.
func CreateSprint(ctx context.Context, repoRoot, projectID, name string) (string, error) {
	c, err := notes.Open(repoRoot)
	if err != nil {
		return "", fmt.Errorf("cc-notes open %q: %w", repoRoot, err)
	}
	sprint, _, err := c.CreateSprint(ctx, notes.SprintSpec{Title: name, Project: model.EntityID(projectID)})
	if err != nil {
		return "", fmt.Errorf("cc-notes create sprint %q: %w", name, err)
	}
	return string(sprint.ID), nil
}

// CreateTask creates a cc-notes task titled name on branch, a member of sprintID
// and projectID, in the repo at repoRoot, and returns its full entity id. It
// sets no acceptance criteria because an orchestrated agent has none — the
// in-process equivalent of the CLI's --no-validation-criteria.
func CreateTask(ctx context.Context, repoRoot, name, branch, sprintID, projectID string) (string, error) {
	c, err := notes.Open(repoRoot)
	if err != nil {
		return "", fmt.Errorf("cc-notes open %q: %w", repoRoot, err)
	}
	created, err := c.CreateTask(ctx, notes.TaskSpec{
		Title:   name,
		Branch:  model.Branch(branch),
		Sprint:  model.EntityID(sprintID),
		Project: model.EntityID(projectID),
	})
	if err != nil {
		return "", fmt.Errorf("cc-notes create task %q: %w", name, err)
	}
	if created.Degraded {
		return "", fmt.Errorf("cc-notes degraded task %s to backlog (branch %q not honored)", created.Task.ID, branch)
	}
	return string(created.Task.ID), nil
}
