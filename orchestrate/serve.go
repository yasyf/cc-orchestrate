package orchestrate

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
)

// Domain control-plane ops the daemon routes to the handlers registered in serve.
// The values are namespaced so they never collide with cc-interact's reserved
// core ops (health, shutdown, resolve, session-record, guard-edit, channel-ack,
// status).
const (
	opSpawn              daemon.Op = "agent-spawn"
	opSendMessage        daemon.Op = "agent-send-message"
	opReport             daemon.Op = "agent-report"
	opStatus             daemon.Op = "agent-status"
	opList               daemon.Op = "agent-list"
	opAgentKill          daemon.Op = "agent-kill"
	opRepoCreate         daemon.Op = "repo-create"
	opRepoList           daemon.Op = "repo-list"
	opRepoActivate       daemon.Op = "repo-activate"
	opRepoKill           daemon.Op = "repo-kill"
	opWorkstreamCreate   daemon.Op = "workstream-create"
	opWorkstreamList     daemon.Op = "workstream-list"
	opWorkstreamActivate daemon.Op = "workstream-activate"
	opWorkstreamKill     daemon.Op = "workstream-kill"
	opSprintCreate       daemon.Op = "sprint-create"
	opSprintList         daemon.Op = "sprint-list"
	opSprintActivate     daemon.Op = "sprint-activate"
	opConfigGet          daemon.Op = "config-get"
	opConfigSet          daemon.Op = "config-set"
	opSerialize          daemon.Op = "serialize"
	opRestore            daemon.Op = "restore"
)

// tailers is the daemon-lifetime transcript-tailer manager, bound to the serve
// context so a tailer outlives the per-request handler context that spawned it.
var tailers *tailerManager

// supervisorRunner is the daemon-lifetime keep-alive supervisor, started once after
// boot reconcile settles and bound to the serve context so its goroutine tears down
// on daemon shutdown.
var supervisorRunner *supervisor

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
		ActiveStatuses: []string{string(StatusActive)},
		WindowAlive:    func(int) bool { return true },
		// c.Type() (not c.EventType) so the SSE plane filters the same presence
		// type these hooks emit, correct even for the Connectivity zero value.
		PresenceEventType: c.Type(),
		OnPresenceChange:  c.OnPresenceChange,
		Migrate:           migrate,
		// Run the channel boot reconcile, repair DB rows whose backend workspace or
		// terminal vanished while the daemon was down, then resume a transcript
		// tailer for every agent still active across the restart (the post-reconcile
		// active set).
		BootReconcile: func(ctx context.Context, s *daemon.Server) error {
			if err := c.BootReconcile(ctx, s); err != nil {
				return err
			}
			db := s.DB()
			if err := reconcileWorkstreams(ctx, db, s.Append); err != nil {
				return err
			}
			if err := reconcileAgents(ctx, db, s.Append); err != nil {
				return err
			}
			agents, err := listActiveAgents(ctx, db)
			if err != nil {
				return err
			}
			for _, ag := range agents {
				tailers.start(db, s.Append, ag)
			}
			// Start the keep-alive supervisor only after the boot prune+resume has
			// settled, on the daemon-lifetime ctx so it tears down on shutdown.
			supervisorRunner = newSupervisor()
			go supervisorRunner.run(ctx, db, s.Append)
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
	s.Register(opRepoCreate, handleRepoCreate)
	s.Register(opRepoList, handleRepoList)
	s.Register(opRepoActivate, handleRepoActivate)
	s.Register(opRepoKill, handleRepoKill)
	s.Register(opWorkstreamCreate, handleWorkstreamCreate)
	s.Register(opWorkstreamList, handleWorkstreamList)
	s.Register(opWorkstreamActivate, handleWorkstreamActivate)
	s.Register(opWorkstreamKill, handleWorkstreamKill)
	s.Register(opSprintCreate, handleSprintCreate)
	s.Register(opSprintList, handleSprintList)
	s.Register(opSprintActivate, handleSprintActivate)
	s.Register(opConfigGet, handleConfigGet)
	s.Register(opConfigSet, handleConfigSet)
	s.Register(opSerialize, handleSerialize)
	s.Register(opRestore, handleRestore)
	return s.Serve(ctx)
}

// tailerManager owns every running transcript tailer for the daemon's lifetime.
// Each tailer's context derives from base (the serve context), not from the
// per-request handler context that started it, so a tailer survives the RPC that
// spawned the agent.
type tailerManager struct {
	base     context.Context
	interval time.Duration
	grace    time.Duration
	mu       sync.Mutex
	cancels  map[string]*tailerCancel
}

// tailerCancel wraps a running tailer's CancelFunc so the manager can tell one
// tailer from a later replacement by pointer identity: a finished tailer removes
// its own entry only, never a successor that already took its agent id.
type tailerCancel struct{ cancel context.CancelFunc }

func newTailerManager(ctx context.Context) *tailerManager {
	return &tailerManager{base: ctx, interval: pollInterval, grace: probeGrace, cancels: map[string]*tailerCancel{}}
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
	tc := &tailerCancel{cancel: cancel}
	m.mu.Lock()
	if prev, ok := m.cancels[ag.ID]; ok {
		prev.cancel()
	}
	m.cancels[ag.ID] = tc
	m.mu.Unlock()
	go func() {
		defer m.finish(ag.ID, tc)
		// emit persists a derived Status to the row and mirrors it onto the subject
		// log. Both the prober and the tailer's replay reach it; only the live tailer
		// path (onStatus) layers the restart-budget reset on top.
		emit := func(st Status) error {
			if err := applyStatus(cctx, db, ag.ID, st); err != nil {
				return err
			}
			_, err := appendFn(cctx, &event.Event{
				SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventStatus, Payload: jsonStatus(st),
			})
			return err
		}
		// onStatus is the tailer's status sink. It resets the restart budget only on a
		// genuinely-new healthy state (live) — never on the pre-crash state the tailer
		// replays from history on every (re)start — so a crash-looping agent whose last
		// transcript line was healthy still accrues its budget to abandonment instead of
		// resetting to zero each respawn. Gated on the start snapshot's RestartCount > 0
		// so a never-restarted agent takes no spurious write.
		onStatus := func(st Status, live bool) error {
			if live && ag.RestartCount > 0 && healthyState(st.State) {
				if err := resetRestart(cctx, db, ag.ID); err != nil {
					return err
				}
			}
			return emit(st)
		}
		// Grace-period interactive-prompt driver: before any transcript exists, probe
		// the agent's screen and drive a known blocking prompt (e.g. the trust dialog)
		// to completion, so a blocked agent is never silently invisible. It runs to
		// completion before the tailer, then the tailer's first real status overwrites
		// the transient blocked state.
		runProber(cctx, db, ag, emit, m.interval, m.grace)
		err := runTailer(cctx, ag.SessionID, ag.Scope, m.interval, onStatus,
			func(text string) error {
				if text == ag.Prompt {
					return nil // the spawn prompt is already recorded by EventSpawned
				}
				_, err := appendFn(cctx, &event.Event{
					SubjectID: ag.SubjectID, Origin: event.OriginSystem, Type: EventInbound, Payload: inboundPayload(text),
				})
				return err
			})
		if err != nil {
			log.Printf("cc-orchestrate: tailer for agent %s stopped: %v", ag.ID, err)
		}
	}()
}

// stop cancels an agent's tailer and forgets it. It is a no-op for an agent with
// no running tailer.
func (m *tailerManager) stop(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tc, ok := m.cancels[id]; ok {
		tc.cancel()
		delete(m.cancels, id)
	}
}

// finish releases a self-exited tailer's context and drops its map entry, unless a
// later start already replaced it — so a finishing tailer never clears its
// successor and the map does not accumulate stale entries over the daemon's life.
func (m *tailerManager) finish(id string, tc *tailerCancel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc.cancel()
	if m.cancels[id] == tc {
		delete(m.cancels, id)
	}
}
