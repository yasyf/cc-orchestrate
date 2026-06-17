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

// BackendName is a backend's registry identity. It is a named type so a backend
// name can never be silently mixed with an arbitrary string.
type BackendName string

// Precedence is the default backend resolution order: the first Available one
// wins unless the user selects another.
var Precedence = []BackendName{"herd", "superset", "cmux", "zellij", "tmux"}

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
	Backend BackendName
	ID      string
	Name    string
	Cwd     string
}

// AgentHandle identifies a spawned agent's backend terminal. SessionID carries the
// child's claude --session-id so a backend that can't address its terminal (superset)
// can still kill the process by identity.
type AgentHandle struct {
	Backend   BackendName
	ID        string
	ProjectID string
	Name      string
	SessionID string
}

// Capability is one native fast path a backend can perform itself instead of
// falling back to the cc-interact event plane (the LCD, lowest common
// denominator). It is the dispatch key: a caller checks Caps.Has(cap) to decide
// native-vs-LCD.
type Capability uint

const (
	// CanSendText delivers a message by typing it into the agent's terminal.
	CanSendText Capability = 1 << iota
	// CanCapture reads the agent terminal's screen/scrollback. Vocabulary only:
	// no backend advertises it and no Capturer interface exists until a consumer
	// does.
	CanCapture
	// CanEnumerate means ListAgents returns the live agent set, so boot reconcile
	// may prune DB rows the backend no longer reports.
	CanEnumerate
)

// Caps is the set of capabilities a backend supports. The zero value supports
// nothing — the pure-LCD backend (superset).
type Caps struct{ set Capability }

// Capabilities builds a Caps advertising the given capabilities.
func Capabilities(caps ...Capability) Caps {
	var c Caps
	for _, want := range caps {
		c.set |= want
	}
	return c
}

// Has reports whether the backend can perform want natively.
func (c Caps) Has(want Capability) bool { return c.set&want != 0 }

// Backend is one agent placement+spawn runtime.
type Backend interface {
	Name() BackendName
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

// Sender is a Backend that can deliver a message by typing it into a running
// agent's terminal, instead of routing it over the cc-interact event plane (the
// LCD). A backend implements Sender exactly when its Caps has CanSendText; the
// registry invariant test enforces that correspondence, so a capability never
// advertises a path the driver cannot take.
type Sender interface {
	SendText(ctx context.Context, agent AgentHandle, text string) error
}

// registry holds the registered backends keyed by Name.
var registry = map[BackendName]Backend{}

// Register adds a backend to the registry. Drivers call it from an init function.
func Register(b Backend) { registry[b.Name()] = b }

// Get returns the registered backend with the given name.
func Get(name BackendName) (Backend, bool) {
	b, ok := registry[name]
	return b, ok
}

// ValidateBackend returns an error unless name is a known backend (present in
// Precedence and registered) whose runtime is installed. Callers add their own
// surface-specific hint (e.g. how to list backends) by wrapping the result.
func ValidateBackend(name BackendName) error {
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
