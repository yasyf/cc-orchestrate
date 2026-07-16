package backend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

type supersetProjectCreateResult struct {
	ProjectID string `json:"projectId"`
}

// supersetWorkspace is a `superset workspaces ...` entry; its id is the workspace
// a workstream handle wraps.
type supersetWorkspace struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	WorktreePath string `json:"worktreePath"`
}

type supersetWorkspaceCreateResult struct {
	Workspace     supersetWorkspace `json:"workspace"`
	AlreadyExists bool              `json:"alreadyExists"`
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

func (b superset) Name() Name { return supersetBin }

func (b superset) Available() bool { return installed(supersetBin) }

// Caps advertises ManagesWorktree (superset forks and owns its own git worktree
// per workspace, so cc-orchestrate adopts the returned WorkstreamHandle.Worktree)
// and CanEnumerate (ListAgents reads live session liveness from the host's
// pty-daemon, so the supervisor resumes a superset agent whose claude died). It has
// no native send or capture path; those still ride the cc-interact event plane and
// the pty-host wrapper.
func (b superset) Caps() Caps { return Capabilities(ManagesWorktree, CanEnumerate) }

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
// forks a local workspace for it on spec.Branch. Superset owns the worktree, so
// the returned handle adopts the authoritative path reported by the workspace
// list. The branch is required: the superset CLI rejects a workspace create
// without one, so an empty spec.Branch is an error rather than a silent default.
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
		return WorkstreamHandle{}, fmt.Errorf("superset: create workspace %q: %w", spec.Name, err)
	}
	created, err := decodeJSON[supersetWorkspaceCreateResult](out, "superset", "workspace")
	if err != nil {
		return WorkstreamHandle{}, err
	}
	if created.Workspace.ID == "" {
		return WorkstreamHandle{}, fmt.Errorf("superset: create workspace %q returned an empty workspace id", spec.Name)
	}
	// Superset reuses the returned workspace when alreadyExists is true; its id
	// remains authoritative. Re-read it by id for the worktree path superset owns.
	workspace, err := b.getWorkspace(ctx, created.Workspace.ID)
	if err != nil {
		return WorkstreamHandle{}, err
	}
	if workspace.WorktreePath == "" {
		return WorkstreamHandle{}, fmt.Errorf("superset: workspace %s has an empty worktree path", workspace.ID)
	}
	return WorkstreamHandle{
		Backend:  b.Name(),
		ID:       workspace.ID,
		Name:     spec.Name,
		Cwd:      spec.Cwd,
		Worktree: workspace.WorktreePath,
	}, nil
}

// resolveProjectID finds the superset project for cwd, importing one named after
// cwd's basename (the project's own directory, not the workstream) when none exists.
func (b superset) resolveProjectID(ctx context.Context, cwd string) (string, error) {
	projects, err := b.listSetupProjects(ctx)
	if err != nil {
		return "", err
	}
	if id := matchProjectID(projects, cwd); id != "" {
		return id, nil
	}
	name := filepath.Base(cwd)
	out, err := b.run(ctx, supersetBin, "projects", "create", "--local", "--import", cwd, "--name", name, "--json")
	if err != nil {
		return "", fmt.Errorf("superset: create project %q: %w", name, err)
	}
	created, err := decodeJSON[supersetProjectCreateResult](out, "superset", "project")
	if err != nil {
		return "", err
	}
	if created.ProjectID == "" {
		return "", fmt.Errorf("superset: create project %q returned an empty project id", name)
	}
	return created.ProjectID, nil
}

// getWorkspace re-reads a single workspace by id via `workspaces get`, which reports
// the worktree path superset assigns; unlike the list commands it takes no --local.
func (b superset) getWorkspace(ctx context.Context, id string) (supersetWorkspace, error) {
	out, err := b.run(ctx, supersetBin, "workspaces", "get", id, "--json")
	if err != nil {
		return supersetWorkspace{}, fmt.Errorf("superset: get workspace %s: %w", id, err)
	}
	return decodeJSON[supersetWorkspace](out, "superset", "workspace")
}

func (b superset) listSetupProjects(ctx context.Context) ([]supersetProject, error) {
	out, err := b.run(ctx, supersetBin, "projects", "list", "--local", "--json")
	if err != nil {
		return nil, fmt.Errorf("superset: list projects: %w", err)
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

func (b superset) listWorkspaces(ctx context.Context) ([]supersetWorkspace, error) {
	out, err := b.run(ctx, supersetBin, "workspaces", "list", "--local", "--json")
	if err != nil {
		return nil, fmt.Errorf("superset: list workspaces: %w", err)
	}
	workspaces, err := decodeJSON[[]supersetWorkspace](out, "superset", "workspaces")
	if err != nil {
		return nil, err
	}
	return workspaces, nil
}

func (b superset) ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error) {
	workspaces, err := b.listWorkspaces(ctx)
	if err != nil {
		return nil, err
	}
	workstreams := make([]WorkstreamHandle, len(workspaces))
	for i, w := range workspaces {
		workstreams[i] = WorkstreamHandle{Backend: b.Name(), ID: w.ID, Name: w.Name, Worktree: w.WorktreePath}
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

// ListAgents reports the superset agents whose PTY child is still alive, read from
// the host's pty-daemon supervisor (its hello/list control socket). The daemon's
// list is host-wide and keyed by the same terminalId Spawn returns — cc-orchestrate's
// terminal handle — and the supervisor diff only tests membership of the handles it
// already tracks, so the host-wide live set is the signal it needs: an agent whose
// claude exited drops out (Alive=false) and is resumed. A dead or unreachable daemon
// yields an error, which the supervisor treats as "no signal" and skips, never "all
// agents gone".
func (b superset) ListAgents(ctx context.Context, _ WorkstreamHandle) ([]AgentHandle, error) {
	socket, err := supersetDaemonSocketPath()
	if err != nil {
		return nil, err
	}
	sessions, err := listSupersetSessions(ctx, socket)
	if err != nil {
		return nil, err
	}
	handles := []AgentHandle{}
	for _, s := range sessions {
		if s.Alive {
			handles = append(handles, AgentHandle{Backend: b.Name(), ID: s.ID})
		}
	}
	return handles, nil
}

// Kill terminates the agent by its claude --session-id, since superset exposes no
// per-terminal kill. The pkill pattern is passed after `--` because it begins
// with dashes that pkill would otherwise read as options. pkill exits 1 when no
// process matches, which is success here — the agent is already gone — so it is
// not surfaced as an error; only a real failure (exit 2/3) propagates.
func (b superset) Kill(ctx context.Context, agent AgentHandle) error {
	if agent.SessionID == "" {
		return errors.New("superset: cannot kill agent without a session id")
	}
	if _, err := b.run(ctx, "pkill", "-f", "--", "--session-id "+agent.SessionID); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return nil
		}
		return err
	}
	return nil
}

// KillWorkstream deletes the workspace, which SIGHUP→SIGKILLs all of its terminals.
func (b superset) KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error {
	_, err := b.run(ctx, supersetBin, "workspaces", "delete", workstream.ID, "--local", "--json")
	return err
}
