package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// supersetBin is both the registry key and the CLI the superset backend drives.
const supersetBin = "superset"

// shellSafe is the set of characters a token may contain and still pass through
// a POSIX shell unquoted; it mirrors Python's shlex.quote allowlist.
const shellSafe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@%+=:,./-_"

// superset places workspaces and spawns terminals through the superset CLI. It is
// spawn-only: there is no send-text, capture, or per-terminal kill CLI, so kills
// happen by claude --session-id identity and projects/agents are tracked upstream.
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
// a project handle wraps.
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

func (b superset) Name() string { return supersetBin }

func (b superset) Available() bool { return installed(supersetBin) }

func (b superset) Caps() Caps { return Caps{} }

// EnsureReady verifies the host service is running and healthy and that a user is
// authenticated; it never logs in. The two checks fail loud with the command to run.
func (b superset) EnsureReady(ctx context.Context) error {
	out, err := b.run(ctx, supersetBin, "status", "--json")
	if err != nil {
		return fmt.Errorf("superset: host service unreachable (run `superset start`): %w", err)
	}
	var status supersetStatus
	if err := json.Unmarshal(out, &status); err != nil {
		return fmt.Errorf("superset: cannot parse status: %w", err)
	}
	if !status.Running || !status.Healthy {
		return fmt.Errorf("superset: host service not ready (running=%t healthy=%t); run `superset start`", status.Running, status.Healthy)
	}
	out, err = b.run(ctx, supersetBin, "auth", "whoami", "--json")
	if err != nil {
		return fmt.Errorf("superset: not authenticated (run `superset auth login`): %w", err)
	}
	var who supersetIdentity
	if err := json.Unmarshal(out, &who); err != nil {
		return fmt.Errorf("superset: cannot parse identity: %w", err)
	}
	if who.UserID == "" && who.Email == "" {
		return errors.New("superset: no authenticated identity; run `superset auth login`")
	}
	return nil
}

// CreateProject resolves (or imports) the superset project for spec.Cwd, then
// forks a local workspace for it on the directory's git branch. The workspace id
// is the project handle other calls address.
func (b superset) CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error) {
	projectID, err := b.resolveProjectID(ctx, spec.Cwd)
	if err != nil {
		return ProjectHandle{}, err
	}
	branch := b.gitBranch(ctx, spec.Cwd)
	out, err := b.run(ctx, supersetBin, "workspaces", "create", "--local",
		"--project", projectID, "--branch", branch, "--name", spec.Name, "--json")
	if err != nil {
		return ProjectHandle{}, err
	}
	var ws supersetWorkspace
	if err := json.Unmarshal(out, &ws); err != nil {
		return ProjectHandle{}, fmt.Errorf("superset: cannot parse workspace: %w", err)
	}
	return ProjectHandle{Backend: b.Name(), ID: ws.ID, Name: spec.Name, Cwd: spec.Cwd}, nil
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
	var projects []supersetProject
	if err := json.Unmarshal(out, &projects); err != nil {
		return nil, fmt.Errorf("superset: cannot parse projects: %w", err)
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

func (b superset) gitBranch(ctx context.Context, cwd string) string {
	out, err := b.run(ctx, "git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "main"
	}
	if branch := strings.TrimSpace(string(out)); branch != "" && branch != "HEAD" {
		return branch
	}
	return "main"
}

func (b superset) ListProjects(ctx context.Context) ([]ProjectHandle, error) {
	out, err := b.run(ctx, supersetBin, "workspaces", "list", "--local", "--json")
	if err != nil {
		return nil, err
	}
	var workspaces []supersetWorkspace
	if err := json.Unmarshal(out, &workspaces); err != nil {
		return nil, fmt.Errorf("superset: cannot parse workspaces: %w", err)
	}
	projects := make([]ProjectHandle, len(workspaces))
	for i, w := range workspaces {
		projects[i] = ProjectHandle{Backend: b.Name(), ID: w.ID, Name: w.Name}
	}
	return projects, nil
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
		"--workspace", spec.Project.ID, "--cwd", spec.Cwd,
		"--command", wrapBashLogin(command), "--json")
	if err != nil {
		return AgentHandle{}, err
	}
	var term supersetTerminal
	if err := json.Unmarshal(out, &term); err != nil {
		return AgentHandle{}, fmt.Errorf("superset: cannot parse terminal: %w", err)
	}
	return AgentHandle{
		Backend:   b.Name(),
		ID:        term.TerminalID,
		ProjectID: spec.Project.ID,
		Name:      spec.Name,
		SessionID: spec.SessionID,
	}, nil
}

// ListAgents always returns empty: superset has no running-agent CLI, so the
// orchestrate agents table is the source of truth.
func (b superset) ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error) {
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

// KillProject deletes the workspace, which SIGHUP→SIGKILLs all of its terminals.
func (b superset) KillProject(ctx context.Context, project ProjectHandle) error {
	_, err := b.run(ctx, supersetBin, "workspaces", "delete", project.ID, "--local", "--json")
	return err
}

// wrapBashLogin renders command as a single `bash -lc <line>` string for the
// superset terminal's --command. Two shells parse it: the terminal's login shell
// (fish) parses the whole string and must receive the inner line fish-quoted,
// while bash -lc reparses that line and needs each token POSIX-quoted.
func wrapBashLogin(command []string) string {
	quoted := make([]string, len(command))
	for i, tok := range command {
		quoted[i] = shellQuote(tok)
	}
	return "bash -lc " + fishQuote(strings.Join(quoted, " "))
}

// shellQuote renders s as a single POSIX-shell token, escaping each embedded
// single quote by closing the quote, emitting an escaped quote, and reopening.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	for _, r := range s {
		if !strings.ContainsRune(shellSafe, r) {
			return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
		}
	}
	return s
}

// fishQuote renders s as a single fish token. fish single quotes treat \\ and \'
// as escapes (unlike POSIX), so backslashes and quotes are backslash-escaped in
// place; backslashes are escaped first so the quote escapes are not doubled.
func fishQuote(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
}
