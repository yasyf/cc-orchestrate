package orchestrate

import (
	"context"
	"database/sql"
	"sync"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
)

// Domain control-plane ops the daemon routes to the handlers registered in serve.
// The values are namespaced so they never collide with cc-interact's reserved
// core ops (health, shutdown, resolve, session-record, guard-edit, channel-ack,
// status).
const (
	opSpawn           daemon.Op = "agent-spawn"
	opSendMessage     daemon.Op = "agent-send-message"
	opReport          daemon.Op = "agent-report"
	opStatus          daemon.Op = "agent-status"
	opList            daemon.Op = "agent-list"
	opAgentKill       daemon.Op = "agent-kill"
	opProjectCreate   daemon.Op = "project-create"
	opProjectList     daemon.Op = "project-list"
	opProjectActivate daemon.Op = "project-activate"
	opConfigGet       daemon.Op = "config-get"
	opConfigSet       daemon.Op = "config-set"
)

// tailers is the daemon-lifetime transcript-tailer manager, bound to the serve
// context so a tailer outlives the per-request handler context that spawned it.
var tailers *tailerManager

// serve runs the long-lived daemon: it builds the cc-interact daemon with the
// orchestrate schema and the channel presence lifecycle, registers the domain
// ops, then serves control RPCs until ctx is cancelled.
func serve(ctx context.Context) error {
	tailers = newTailerManager(ctx)
	c := channel.Connectivity{}
	s, err := daemon.New(daemon.Config{
		AppName:        AppName,
		Paths:          appPaths(),
		Version:        Version,
		ActiveStatuses: []string{"active"},
		WindowAlive:    func(int) bool { return true },
		// c.Type() (not c.EventType) so the SSE plane filters the same presence
		// type these hooks emit, correct even for the Connectivity zero value.
		PresenceEventType: c.Type(),
		OnPresenceChange:  c.OnPresenceChange,
		Migrate:           migrate,
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
				tailers.start(s.DB(), s.Append, ag)
			}
			return nil
		},
	})
	if err != nil {
		return err
	}
	s.Register(opSpawn, handleSpawn)
	s.Register(opSendMessage, handleSendMessage)
	s.Register(opReport, handleReport)
	s.Register(opStatus, handleStatus)
	s.Register(opList, handleList)
	s.Register(opAgentKill, handleAgentKill)
	s.Register(opProjectCreate, handleProjectCreate)
	s.Register(opProjectList, handleProjectList)
	s.Register(opProjectActivate, handleProjectActivate)
	s.Register(opConfigGet, handleConfigGet)
	s.Register(opConfigSet, handleConfigSet)
	return s.Serve(ctx)
}

// tailerManager owns every running transcript tailer for the daemon's lifetime.
// Each tailer's context derives from base (the serve context), not from the
// per-request handler context that started it, so a tailer survives the RPC that
// spawned the agent.
type tailerManager struct {
	base    context.Context
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func newTailerManager(ctx context.Context) *tailerManager {
	return &tailerManager{base: ctx, cancels: map[string]context.CancelFunc{}}
}

// start launches a background transcript tailer for an agent, persisting each
// derived Status to its row and mirroring it onto the subject log as an
// OriginSystem EventStatus. An agent with no session id has no transcript to
// tail, so it is skipped. A tailer already running for the agent id is cancelled
// and replaced.
func (m *tailerManager) start(db *sql.DB, appendFn daemon.AppendFunc, ag agentRow) {
	if ag.SessionID == "" {
		return
	}
	cctx, cancel := context.WithCancel(m.base)
	m.mu.Lock()
	if prev, ok := m.cancels[ag.ID]; ok {
		prev()
	}
	m.cancels[ag.ID] = cancel
	m.mu.Unlock()
	go runTailer(cctx, ag.SessionID, ag.Scope, func(st Status) error {
		applyStatus(cctx, db, ag.ID, st)
		_, err := appendFn(cctx, &event.Event{
			SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventStatus, Payload: jsonStatus(st),
		})
		return err
	})
}

// stop cancels an agent's tailer and forgets it. It is a no-op for an agent with
// no running tailer.
func (m *tailerManager) stop(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cancel, ok := m.cancels[id]; ok {
		cancel()
		delete(m.cancels, id)
	}
}
