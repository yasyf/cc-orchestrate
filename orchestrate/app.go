// Package orchestrate is the cc-orchestrate composition root: it wires the
// cc-interact substrate (daemon, channel, lifecycle hooks) into a cobra command
// tree and layers the agent-fleet domain commands on top.
package orchestrate

import (
	"context"
	"path/filepath"

	"github.com/yasyf/cc-interact/channelsetup"
	"github.com/yasyf/cc-interact/cmd"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/procs"
	"github.com/yasyf/daemonkit/paths"
	"github.com/yasyf/daemonkit/version"
)

const (
	// AppName labels the daemon and user-facing messages.
	AppName = "cc-orchestrate"

	// appDir is the v1 state-directory basename under the user's home.
	appDir = ".cc-orchestrate-v1"
)

var channelPlugin = channelsetup.Plugin{Marketplace: "cc-orchestrate", Name: "cc-orchestrate"}

const channelServer = "cc-orchestrate"

// Version is the binary's build version, advertised in the channel handshake. It is
// a var (not const) so the release build can inject the tag via
// -ldflags "-X github.com/yasyf/cc-orchestrate/orchestrate.Version=<tag>".
var Version = "dev"

// buildVersion is the memoized build version for daemon eviction: a stamped release
// passes through, an unstamped dev build resolves to "9999.<mtime-nanos>.0-dev".
func buildVersion() string { return version.Running(Version) }

// Domain event types appended to a subject's cc-interact event log.
const (
	EventSpawned = "orchestrate.spawned" // a child agent was spawned
	EventExited  = "orchestrate.exited"  // a child agent exited (terminal)
	EventStatus  = "orchestrate.status"  // transcript-derived status update
	EventMessage = "orchestrate.message" // orchestrator → agent message
	EventReport  = "orchestrate.report"  // agent → orchestrator report

	EventRestarted = "orchestrate.restarted" // supervisor/manual re-spawn of a vanished terminal (non-terminal)
	EventAbandoned = "orchestrate.abandoned" // restart budget exhausted; precedes the terminal EventExited (non-terminal)
	EventAdopted   = "orchestrate.adopted"   // a hand-started session was adopted into the fleet (non-terminal)
)

// LifecycleStatus is the lifecycle state stored on a repo or agent row. It is a
// named type so a status field can never be assigned an arbitrary string.
type LifecycleStatus string

// StatusActive and the terminal StatusExited/StatusKilled are the lifecycle states a
// repo or agent row can hold.
const (
	StatusActive LifecycleStatus = "active" // running; matches daemon.Config.ActiveStatuses
	StatusExited LifecycleStatus = "exited" // terminal: the agent's process is gone
	StatusKilled LifecycleStatus = "killed" // terminal: the repo and its workspace were torn down
)

func appPaths() paths.Paths { return paths.Paths{App: appDir} }

// worktreesBase is the root under which non-primary workstream worktrees live,
// one subdirectory per repo: ~/.cc-orchestrate-v1/worktrees/<repo-id>/<name>.
func worktreesBase() string { return filepath.Join(appPaths().StateDir(), "worktrees") }

func newClient(ctx context.Context) (*daemon.Client, error) {
	l, err := launcher()
	if err != nil {
		return nil, err
	}
	return l.NewClient(ctx)
}

func launcher() (daemon.Launcher, error) {
	agent, err := appAgent()
	if err != nil {
		return daemon.Launcher{}, err
	}
	return daemon.Launcher{
		Paths: appPaths(), WireBuild: daemon.WireBuild, RuntimeBuild: buildVersion(),
		Agent: agent, Roles: appRoles(),
	}, nil
}

// deps builds the substrate wiring every cc-interact command shares: the state
// paths, version, control client, lazy daemon launch, window identity, terminal
// event predicate, the daemon entry point, and the child channel tools.
func deps() cmd.Deps {
	return cmd.Deps{
		Paths:                  appPaths(),
		Version:                buildVersion(),
		NewClient:              newClient,
		EnsureCurrent:          ensureCurrent,
		EnsureCurrentIfRunning: ensureCurrentIfRunning,
		Stop:                   stop,
		ClaudePID:              procs.ClaudePID,
		WindowAlive:            procs.LiveClaude,
		TerminalEvent:          func(t string) bool { return t == EventExited },
		Serve:                  serve,
		ChannelTools:           channelTools,
	}
}

func ensureCurrent(ctx context.Context) error {
	l, err := launcher()
	if err != nil {
		return err
	}
	return l.EnsureCurrent(ctx, daemon.UpgradeTimeout)
}

func ensureCurrentIfRunning(ctx context.Context) error {
	l, err := launcher()
	if err != nil {
		return err
	}
	return l.EnsureCurrentIfRunning(ctx)
}

func stop(ctx context.Context) error {
	l, err := launcher()
	if err != nil {
		return err
	}
	return l.Stop(ctx, daemon.UpgradeTimeout)
}
