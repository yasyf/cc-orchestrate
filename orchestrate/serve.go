package orchestrate

import (
	"context"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"
)

// Domain control-plane ops the daemon routes to the handlers registered in serve.
// The values are namespaced so they never collide with cc-interact's reserved
// core ops (health, shutdown, resolve, session-record, guard-edit, channel-ack,
// status).
const (
	opSpawn       daemon.Op = "agent-spawn"
	opSendMessage daemon.Op = "agent-send-message"
	opStatus      daemon.Op = "agent-status"
	opList        daemon.Op = "agent-list"
)

// serve runs the long-lived daemon: it builds the cc-interact daemon with the
// orchestrate schema and the channel presence lifecycle, registers the domain
// ops, then serves control RPCs until ctx is cancelled.
func serve(ctx context.Context) error {
	c := channel.Connectivity{}
	s, err := daemon.New(daemon.Config{
		AppName:        AppName,
		Paths:          appPaths(),
		Version:        Version,
		ActiveStatuses: []string{"active"},
		WindowAlive:    func(int) bool { return true },
		Migrate:        migrate,
		// c.Type() (not c.EventType) so the SSE plane filters the same presence
		// type these hooks emit, correct even for the Connectivity zero value.
		PresenceEventType: c.Type(),
		OnPresenceChange:  c.OnPresenceChange,
		BootReconcile:     c.BootReconcile,
	})
	if err != nil {
		return err
	}
	for _, op := range []daemon.Op{opSpawn, opSendMessage, opStatus, opList} {
		s.Register(op, handleStub)
	}
	return s.Serve(ctx)
}

// handleStub answers a domain op with a bare success until a later phase supplies
// the real handler.
func handleStub(daemon.HandlerCtx) daemon.Reply { return daemon.Reply{OK: true} }
