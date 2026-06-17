// Package backend defines the placement-and-spawn abstraction over the agent
// runtimes cc-orchestrate targets (herd, superset, cmux, zellij, tmux), plus the
// registry that resolves which backend to use. Everything interactive rides the
// cc-interact event plane; a backend only places workspaces and spawns commands.
package backend

import (
	"context"
	"fmt"
	"slices"
)

// Precedence is the default backend resolution order: the first Available one
// wins unless the user selects another.
var Precedence = []string{"herd", "superset", "cmux", "zellij", "tmux"}

// ProjectSpec describes a project to create on a backend.
type ProjectSpec struct {
	Name string
	Cwd  string
}

// SpawnSpec describes an agent to spawn into a project. Command is the full argv
// the backend runs in a placed terminal (typically a claude invocation);
// SessionID is that child's deterministic --session-id, which backends without a
// per-terminal kill (superset) use to terminate the process by identity.
type SpawnSpec struct {
	Project   ProjectHandle
	Name      string
	Cwd       string
	Command   []string
	SessionID string
}

// ProjectHandle identifies a backend workspace.
type ProjectHandle struct {
	Backend string
	ID      string
	Name    string
	Cwd     string
}

// AgentHandle identifies a spawned agent's backend terminal. SessionID carries the
// child's claude --session-id so a backend that can't address its terminal (superset)
// can still kill the process by identity.
type AgentHandle struct {
	Backend   string
	ID        string
	ProjectID string
	Name      string
	SessionID string
}

// Caps reports the optional fast paths a backend supports beyond the event plane.
type Caps struct {
	SendText bool
	Capture  bool
}

// Backend is one agent placement+spawn runtime.
type Backend interface {
	Name() string
	Available() bool
	EnsureReady(ctx context.Context) error
	CreateProject(ctx context.Context, spec ProjectSpec) (ProjectHandle, error)
	ListProjects(ctx context.Context) ([]ProjectHandle, error)
	Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error)
	ListAgents(ctx context.Context, project ProjectHandle) ([]AgentHandle, error)
	Kill(ctx context.Context, agent AgentHandle) error
	KillProject(ctx context.Context, project ProjectHandle) error
	Caps() Caps
}

// registry holds the registered backends keyed by Name.
var registry = map[string]Backend{}

// Register adds a backend to the registry. Drivers call it from an init function.
func Register(b Backend) { registry[b.Name()] = b }

// Get returns the registered backend with the given name.
func Get(name string) (Backend, bool) {
	b, ok := registry[name]
	return b, ok
}

// ValidateBackend returns an error unless name is a known backend (present in
// Precedence and registered) whose runtime is installed. Callers add their own
// surface-specific hint (e.g. how to list backends) by wrapping the result.
func ValidateBackend(name string) error {
	b, ok := Get(name)
	if !slices.Contains(Precedence, name) || !ok || !b.Available() {
		return fmt.Errorf("backend %q is not an available backend", name)
	}
	return nil
}

// Available returns the registered backends, in precedence order, whose runtime
// is installed.
func Available() []Backend {
	out := []Backend{}
	for _, name := range Precedence {
		if b, ok := registry[name]; ok && b.Available() {
			out = append(out, b)
		}
	}
	return out
}

// Select returns the first available backend in precedence order.
func Select() (Backend, bool) {
	if avail := Available(); len(avail) > 0 {
		return avail[0], true
	}
	return nil, false
}
