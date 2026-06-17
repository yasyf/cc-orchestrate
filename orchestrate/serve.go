package orchestrate

import (
	"context"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
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
	opConfigGet   daemon.Op = "config-get"
	opConfigSet   daemon.Op = "config-set"
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
		// Run the channel boot reconcile, then resume a transcript tailer for every
		// agent still active across the restart.
		BootReconcile: func(ctx context.Context, s *daemon.Server) error {
			if err := c.BootReconcile(ctx, s); err != nil {
				return err
			}
			agents, err := listActiveAgents(ctx, s.DB())
			if err != nil {
				return err
			}
			for _, ag := range agents {
				startTailer(ctx, s, ag)
			}
			return nil
		},
	})
	if err != nil {
		return err
	}
	s.Register(opSpawn, handleStub)
	s.Register(opSendMessage, handleSendMessage)
	s.Register(opStatus, handleStatus)
	s.Register(opList, handleList)
	s.Register(opConfigGet, handleConfigGet)
	s.Register(opConfigSet, handleConfigSet)
	return s.Serve(ctx)
}

// startTailer launches a background transcript tailer for an agent, persisting
// each derived Status to its row and mirroring it onto the subject log as an
// OriginSystem EventStatus. An agent with no session id has no transcript to
// tail, so it is skipped.
func startTailer(ctx context.Context, s *daemon.Server, ag agentRow) {
	if ag.SessionID == "" {
		return
	}
	go runTailer(ctx, ag.SessionID, ag.Scope, func(st Status) error {
		applyStatus(ctx, s.DB(), ag.ID, st)
		_, err := s.Append(ctx, &event.Event{
			SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventStatus, Payload: jsonStatus(st),
		})
		return err
	})
}

// handleStub answers a domain op with a bare success until a later phase supplies
// the real handler.
func handleStub(daemon.HandlerCtx) daemon.Reply { return daemon.Reply{OK: true} }
