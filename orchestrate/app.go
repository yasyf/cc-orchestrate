// Package orchestrate is the cc-orchestrate composition root: it wires the
// cc-interact substrate (daemon, channel, lifecycle hooks) into a cobra command
// tree and layers the agent-fleet domain commands on top.
package orchestrate

import (
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/yasyf/cc-interact/cmd"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/paths"
)

const (
	// AppName labels the daemon and user-facing messages.
	AppName = "cc-orchestrate"

	// appDir is the state-directory basename under the user's home (~/.cc-orchestrate).
	appDir = ".cc-orchestrate"
)

// Version is the binary's build version, advertised in the channel handshake. It is
// a var (not const) so the release build can inject the tag via
// -ldflags "-X github.com/yasyf/cc-orchestrate/orchestrate.Version=<tag>".
var Version = "0.2.0-dev"

// Domain event types appended to a subject's cc-interact event log.
const (
	EventSpawned = "orchestrate.spawned" // a child agent was spawned
	EventExited  = "orchestrate.exited"  // a child agent exited (terminal)
	EventStatus  = "orchestrate.status"  // transcript-derived status update
	EventMessage = "orchestrate.message" // orchestrator → agent message
	EventReport  = "orchestrate.report"  // agent → orchestrator report
)

// LifecycleStatus is the lifecycle state stored on a project or agent row. It is a
// named type so a status field can never be assigned an arbitrary string.
type LifecycleStatus string

const (
	StatusActive LifecycleStatus = "active" // running; matches daemon.Config.ActiveStatuses
	StatusExited LifecycleStatus = "exited" // terminal: the agent's process is gone
	StatusKilled LifecycleStatus = "killed" // terminal: the project and its workspace were torn down
)

func appPaths() paths.Paths { return paths.Paths{App: appDir} }

func newClient() *daemon.Client { return daemon.NewClient(appPaths().SocketPath()) }

func launcher() daemon.Launcher {
	return daemon.Launcher{Paths: appPaths(), Version: Version, Args: []string{"daemon"}}
}

// deps builds the substrate wiring every cc-interact command shares: the state
// paths, version, control client, lazy daemon launch, window identity, terminal
// event predicate, the daemon entry point, and the child channel tools.
func deps() cmd.Deps {
	return cmd.Deps{
		Paths:                  appPaths(),
		Version:                Version,
		NewClient:              newClient,
		EnsureCurrent:          func(context.Context) error { return launcher().EnsureCurrent(daemon.UpgradeTimeout) },
		EnsureCurrentIfRunning: func() error { return launcher().EnsureCurrentIfRunning() },
		ClaudePID:              os.Getpid,
		WindowAlive:            windowAlive,
		TerminalEvent:          func(t string) bool { return t == EventExited },
		Serve:                  serve,
		ChannelTools:           channelTools,
	}
}

// windowAlive reports whether pid names a live process, so a pid-bound watch or
// channel consumer self-terminates once its agent's claude window dies. EPERM
// means the process exists under another owner — still alive.
func windowAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
