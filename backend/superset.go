package backend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// supersetBin is both the registry key and the CLI the superset backend drives.
const supersetBin = "superset"

// superset places workspaces and spawns terminals through the superset CLI. It is
// spawn-only: there is no send-text, capture, or per-terminal kill CLI, so kills
// happen by claude --session-id identity and workstreams/agents are tracked upstream.
type superset struct{ run runner }

func init() { Register(superset{run: execRunner}) }

// supersetStatus is the subset of `superset status --json` the readiness check reads.
type supersetStatus struct {
	Running bool `json:"running"`
	Healthy bool `json:"healthy"`
}

// supersetIdentity is the subset of `superset auth whoami --json` proving a login.
type supersetIdentity struct {
	UserID string `json:"userId"`
	Email  string `json:"email"`
}

// supersetProject is a `superset projects list --local --json` entry; path is the
// repo root on this machine used to match a project to a working directory.
type supersetProject struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// supersetWorkspace is a `superset workspaces ...` entry; its id is the workspace
// a workstream handle wraps.
type supersetWorkspace struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// supersetTerminal is the `superset terminals create --json` result.
type supersetTerminal struct {
	TerminalID string `json:"terminalId"`
}

// resolveClaude returns the first claude on PATH that is not the superset
// agent-wrapper shim under ~/.superset/bin, so a spawned terminal runs the real
// CLI rather than recursing through superset. It is a package var so tests can
// stub it without touching PATH or the filesystem.
var resolveClaude = func() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	wrapperDir := filepath.Join(home, ".superset", "bin")
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if dir == "" || strings.HasPrefix(dir, wrapperDir) {
			continue
		}
		candidate := filepath.Join(dir, "claude")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("superset: no claude on PATH outside %s", wrapperDir)
}

// ResolveClaude returns the real claude binary, skipping the superset agent-wrapper
// shim, so a pty-host launched for a non-capturing backend runs the real CLI rather
// than recursing through a wrapper.
func ResolveClaude() (string, error) { return resolveClaude() }

// supersetWorktreeBase returns the directory under which the superset host
// service places per-workspace git worktrees on this machine, laid out as
// <base>/<projectID>/<branch>. The superset CLI exposes no filesystem path on the
// workspace object (verified against v0.2.23: `workspaces list --local --json`
// returns only id/name/branch/projectId/projectName/hostId/type/createdAt/
// hostName), so CreateWorkstream reconstructs the adopted worktree path from this
// observed layout. It is a package var so tests can pin it without reading the
// invoking user's home.
var supersetWorktreeBase = func() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".superset", "worktrees"), nil
}

func (b superset) Name() Name { return supersetBin }

func (b superset) Available() bool { return installed(supersetBin) }

// Caps advertises ManagesWorktree: superset forks and owns its own git worktree
// per workspace, so cc-orchestrate adopts the returned WorkstreamHandle.Worktree
// rather than creating one. It is otherwise pure-LCD (no native send/capture/
// enumerate path).
func (b superset) Caps() Caps { return Capabilities(ManagesWorktree) }

// EnsureReady verifies the host service is running and healthy and that a user is
// authenticated; it never logs in. The two checks fail loud with the command to run.
func (b superset) EnsureReady(ctx context.Context) error {
	out, err := b.run(ctx, supersetBin, "status", "--json")
	if err != nil {
		return fmt.Errorf("superset: host service unreachable (run `superset start`): %w", err)
	}
	status, err := decodeJSON[supersetStatus](out, "superset", "status")
	if err != nil {
		return err
	}
	if !status.Running || !status.Healthy {
		return fmt.Errorf("superset: host service not ready (running=%t healthy=%t); run `superset start`", status.Running, status.Healthy)
	}
	out, err = b.run(ctx, supersetBin, "auth", "whoami", "--json")
	if err != nil {
		return fmt.Errorf("superset: not authenticated (run `superset auth login`): %w", err)
	}
	who, err := decodeJSON[supersetIdentity](out, "superset", "identity")
	if err != nil {
		return err
	}
	if who.UserID == "" && who.Email == "" {
		return errors.New("superset: no authenticated identity; run `superset auth login`")
	}
	return nil
}

// CreateWorkstream resolves (or imports) the superset project for spec.Cwd, then
// forks a local workspace for it on spec.Branch. superset owns the worktree, so
// the returned handle's Worktree is the path superset placed it at —
// <supersetWorktreeBase>/<projectID>/<branch> — which cc-orchestrate adopts. The
// branch is required: the superset CLI rejects a workspace create without one, so
// an empty spec.Branch is an error rather than a silent default.
func (b superset) CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error) {
	if spec.Branch == "" {
		return WorkstreamHandle{}, fmt.Errorf("superset: create workspace %q: branch is required", spec.Name)
	}
	projectID, err := b.resolveProjectID(ctx, spec.Cwd)
	if err != nil {
		return WorkstreamHandle{}, err
	}
	out, err := b.run(ctx, supersetBin, "workspaces", "create", "--local",
		"--project", projectID, "--branch", spec.Branch, "--name", spec.Name, "--json")
	if err != nil {
		return WorkstreamHandle{}, err
	}
	ws, err := decodeJSON[supersetWorkspace](out, "superset", "workspace")
	if err != nil {
		return WorkstreamHandle{}, err
	}
	base, err := supersetWorktreeBase()
	if err != nil {
		return WorkstreamHandle{}, err
	}
	return WorkstreamHandle{
		Backend:  b.Name(),
		ID:       ws.ID,
		Name:     spec.Name,
		Cwd:      spec.Cwd,
		Worktree: filepath.Join(base, projectID, spec.Branch),
	}, nil
}

func (b superset) resolveProjectID(ctx context.Context, cwd string) (string, error) {
	projects, err := b.listSetupProjects(ctx)
	if err != nil {
		return "", err
	}
	if id := matchProjectID(projects, cwd); id != "" {
		return id, nil
	}
	if _, err := b.run(ctx, supersetBin, "projects", "setup", "--import", cwd, "--local", "--json"); err != nil {
		return "", err
	}
	projects, err = b.listSetupProjects(ctx)
	if err != nil {
		return "", err
	}
	if id := matchProjectID(projects, cwd); id != "" {
		return id, nil
	}
	return "", fmt.Errorf("superset: no project for %s after import", cwd)
}

func (b superset) listSetupProjects(ctx context.Context) ([]supersetProject, error) {
	out, err := b.run(ctx, supersetBin, "projects", "list", "--local", "--json")
	if err != nil {
		return nil, err
	}
	projects, err := decodeJSON[[]supersetProject](out, "superset", "projects")
	if err != nil {
		return nil, err
	}
	return projects, nil
}

// matchProjectID returns the id of the project whose path is cwd or its nearest
// ancestor, preferring the deepest match when projects nest.
func matchProjectID(projects []supersetProject, cwd string) string {
	best, bestLen := "", -1
	for _, p := range projects {
		if p.Path == "" {
			continue
		}
		if p.Path == cwd {
			return p.ID
		}
		if rel, err := filepath.Rel(p.Path, cwd); err == nil && rel != "." && !strings.HasPrefix(rel, "..") && len(p.Path) > bestLen {
			best, bestLen = p.ID, len(p.Path)
		}
	}
	return best
}

func (b superset) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	out, err := b.run(ctx, supersetBin, "workspaces", "list", "--local", "--json")
	if err != nil {
		return nil, err
	}
	workspaces, err := decodeJSON[[]supersetWorkspace](out, "superset", "workspaces")
	if err != nil {
		return nil, err
	}
	workstreams := make([]WorkstreamHandle, len(workspaces))
	for i, w := range workspaces {
		workstreams[i] = WorkstreamHandle{Backend: b.Name(), ID: w.ID, Name: w.Name}
	}
	return workstreams, nil
}

// Spawn runs spec.Command in a new terminal of the project's workspace. The argv
// is wrapped in `bash -lc` so it runs under bash regardless of the terminal's
// login shell, and a bare "claude" entrypoint is resolved to the real binary so
// it does not recurse through superset's wrapper.
func (b superset) Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error) {
	command := slices.Clone(spec.Command)
	if command[0] == "claude" {
		claude, err := resolveClaude()
		if err != nil {
			return AgentHandle{}, err
		}
		command[0] = claude
	}
	out, err := b.run(ctx, supersetBin, "terminals", "create",
		"--workspace", spec.Workstream.ID, "--cwd", spec.Cwd,
		"--command", wrapBashLogin(command), "--json")
	if err != nil {
		return AgentHandle{}, err
	}
	term, err := decodeJSON[supersetTerminal](out, "superset", "terminal")
	if err != nil {
		return AgentHandle{}, err
	}
	return AgentHandle{
		Backend:      b.Name(),
		ID:           term.TerminalID,
		WorkstreamID: spec.Workstream.ID,
		Name:         spec.Name,
		SessionID:    spec.SessionID,
	}, nil
}

// ListAgents always returns empty: superset has no running-agent CLI, so the
// orchestrate agents table is the source of truth.
func (b superset) ListAgents(_ context.Context, _ WorkstreamHandle) ([]AgentHandle, error) {
	return []AgentHandle{}, nil
}

// Kill terminates the agent by its claude --session-id, since superset exposes no
// per-terminal kill. The pkill pattern is passed after `--` because it begins
// with dashes that pkill would otherwise read as options.
func (b superset) Kill(ctx context.Context, agent AgentHandle) error {
	if agent.SessionID == "" {
		return errors.New("superset: cannot kill agent without a session id")
	}
	_, err := b.run(ctx, "pkill", "-f", "--", "--session-id "+agent.SessionID)
	return err
}

// KillWorkstream deletes the workspace, which SIGHUP→SIGKILLs all of its terminals.
func (b superset) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	_, err := b.run(ctx, supersetBin, "workspaces", "delete", workstream.ID, "--local", "--json")
	return err
}
