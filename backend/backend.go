// Package backend defines the placement-and-spawn abstraction over the agent
// runtimes cc-orchestrate targets (herd, superset, cmux, zellij, tmux), plus the
// registry that resolves which backend to use. Everything interactive rides the
// cc-interact event plane; a backend only places workspaces and spawns commands.
package backend

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Name is a backend's registry identity. It is a named type so a backend
// name can never be silently mixed with an arbitrary string.
type Name string

// Precedence is the default backend resolution order: the first Available one
// wins unless the user selects another.
var Precedence = []Name{"herd", "superset", "cmux", "zellij", "tmux"}

// WorkstreamSpec describes a workstream to create on a backend. A workstream is
// the cwd-bearing unit of isolation: one git worktree on its own branch. Cwd is
// the worktree path cc-orchestrate hands a backend that does not fork its own;
// RepoCwd is the repo root that worktree derives from, and Branch is the branch it
// tracks. A backend that advertises ManagesWorktree forks the worktree itself off
// Branch and reports the path it used in WorkstreamHandle.Worktree.
type WorkstreamSpec struct {
	Name    string
	Cwd     string
	Branch  string
	RepoCwd string
}

// SpawnSpec describes an agent to spawn into a workstream. Command is the full
// argv the backend runs in a placed terminal (typically a claude invocation);
// SessionID is that child's deterministic --session-id, which backends without a
// per-terminal kill (superset) use to terminate the process by identity.
type SpawnSpec struct {
	Workstream WorkstreamHandle
	Name       string
	Cwd        string
	Command    []string
	SessionID  string
}

// WorkstreamHandle identifies the backend workspace backing one workstream.
// Worktree is the git worktree path the backend launched in: the Cwd
// cc-orchestrate handed it, or — for a backend that advertises ManagesWorktree —
// the path the backend forked and now owns, which cc-orchestrate adopts.
type WorkstreamHandle struct {
	Backend  Name
	ID       string
	Name     string
	Cwd      string
	Worktree string
}

// AgentHandle identifies a spawned agent's backend terminal. SessionID carries the
// child's claude --session-id so a backend that can't address its terminal (superset)
// can still kill the process by identity.
type AgentHandle struct {
	Backend      Name
	ID           string
	WorkstreamID string
	Name         string
	SessionID    string
}

// Capability is one native fast path a backend can perform itself instead of
// falling back to the cc-interact event plane (the LCD, lowest common
// denominator). It is the dispatch key: a caller checks Caps.Has(cap) to decide
// native-vs-LCD.
type Capability uint

const (
	// CanSendText lets the startup prober answer a blocking prompt by typing into
	// the agent's terminal.
	CanSendText Capability = 1 << iota
	// CanCapture reads the agent terminal's screen/scrollback natively. A backend
	// implements Capturer exactly when its Caps has CanCapture; the registry
	// invariant test enforces that correspondence.
	CanCapture
	// CanEnumerate means ListAgents returns the live agent set, so boot reconcile
	// may prune DB rows the backend no longer reports.
	CanEnumerate
	// ManagesWorktree means the backend forks and owns its own git worktree per
	// workstream (cc-orchestrate adopts the returned WorkstreamHandle.Worktree);
	// its absence means cc-orchestrate creates the worktree itself and passes it
	// as WorkstreamSpec.Cwd. Like CanEnumerate it is a trait capability with no
	// corresponding optional interface, so it stays out of the caps_test Sender
	// invariant.
	ManagesWorktree
)

// Caps is the set of capabilities a backend supports. The zero value supports
// nothing — a pure-LCD backend that rides the cc-interact event plane for everything.
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
	Name() Name
	Available() bool
	EnsureReady(ctx context.Context) error
	CreateWorkstream(ctx context.Context, spec WorkstreamSpec) (WorkstreamHandle, error)
	ListWorkstreams(ctx context.Context) ([]WorkstreamHandle, error)
	Spawn(ctx context.Context, spec SpawnSpec) (AgentHandle, error)
	ListAgents(ctx context.Context, workstream WorkstreamHandle) ([]AgentHandle, error)
	Kill(ctx context.Context, agent AgentHandle) error
	KillWorkstream(ctx context.Context, workstream WorkstreamHandle) error
	Caps() Caps
}

// Sender is a Backend whose startup prober can answer a blocking terminal prompt.
// Messages to running agents travel over the cc-interact event plane. A backend
// implements Sender exactly when its Caps has CanSendText.
type Sender interface {
	SendText(ctx context.Context, agent AgentHandle, text string) error
}

// Capturer is a Backend that can read a running agent's rendered terminal screen
// natively, instead of cc-orchestrate hosting the PTY itself. A backend implements
// Capturer exactly when its Caps has CanCapture; the registry invariant test
// enforces that correspondence, so the prober never asks a backend to capture down a
// path its driver cannot take.
type Capturer interface {
	Capture(ctx context.Context, agent AgentHandle) (string, error)
}

// AgentProber is a Backend that can report whether a spawned agent's process is
// still alive in its terminal — distinct from whether the terminal still exists. A
// backend whose terminal can outlive its child (a tmux pane under remain-on-exit
// lingers as a dead pane that ListAgents still reports) implements it so the
// supervisor can corroborate a transcript-staleness death signal before resuming a
// stale agent. Unlike Sender/Capturer it carries no Caps bit: it is a best-effort
// corroboration, so a backend that cannot answer it simply opts out of
// staleness-driven resume rather than promising a path it lacks.
type AgentProber interface {
	AgentAlive(ctx context.Context, agent AgentHandle) (bool, error)
}

// Attacher is a Backend that can hand this process's terminal to a running agent's
// backend session, so a human takes over the agent's live terminal. AttachArgv runs
// the backend's own focus pre-steps — selecting the window and pane holding the agent
// — through the driver's run seam in-package, then returns the argv of the
// multiplexer's foreground attach client. The TTY takeover never happens here: the
// caller execs the returned argv to replace its own process, so the multiplexer
// client owns the TTY, signals, and exit code. Like AgentProber it carries no Caps
// bit — there is no LCD fallback for a terminal takeover, so the caller type-asserts
// the interface directly rather than dispatching off a capability.
type Attacher interface {
	AttachArgv(ctx context.Context, agent AgentHandle) ([]string, error)
}

// registry holds the registered backends keyed by Name. registryMu guards it: drivers
// Register from init, but Get is read concurrently from many tailer/prober goroutines,
// so the map needs synchronization like the stdlib's sql.Register / image registries.
var (
	registryMu sync.RWMutex
	registry   = map[Name]Backend{}
)

// Register adds a backend to the registry. Drivers call it from an init function.
func Register(b Backend) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[b.Name()] = b
}

// Get returns the registered backend with the given name.
func Get(name Name) (Backend, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	b, ok := registry[name]
	return b, ok
}

// ValidateBackend returns an error unless name is a known backend (present in
// Precedence and registered) whose runtime is installed. Callers add their own
// surface-specific hint (e.g. how to list backends) by wrapping the result.
func ValidateBackend(name Name) error {
	b, ok := Get(name)
	if !slices.Contains(Precedence, name) || !ok || !b.Available() {
		return fmt.Errorf("backend %q is not an available backend", name)
	}
	return nil
}

// Available returns the registered backends, in precedence order, whose runtime
// is installed.
func Available() []Backend {
	registryMu.RLock()
	defer registryMu.RUnlock()
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
