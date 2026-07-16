package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"
)

// The fleet subject is a single dedicated cc-interact subject every fleet frame is
// appended to, addressable by the realtime plane as GET /events?session=fleet: the
// daemon resolves that ref through `SELECT id FROM subjects WHERE id=? OR slug=?`, so a
// subject created with slug "fleet" answers the URL the catalog publishes. Its
// (session, scope) pair is fixed and its window carries no claude pid, so a fresh=false
// Start resumes the same subject — and its per-subject seq — across daemon swaps rather
// than minting a new one each boot.
const (
	fleetSession = "fleet"
	fleetScope   = "fleet"
	fleetSlug    = "fleet"
)

// coalesceWindow bounds token-only fleet.agent.status frames to at most one per agent
// per window; a state/tool/target transition is never throttled.
const coalesceWindow = 3 * time.Second

// The fleet.agent.exited reasons: an operator or a container-kill cascade terminated
// the agent (killed) versus the agent's own process ending — a supervisor abandonment
// (exited).
const (
	reasonKilled = "killed"
	reasonExited = "exited"
)

// The fleet frame type strings: the `type` field a stream consumer reads off each SSE
// payload, and the keys fleetEventSchemas advertises under the catalog's events.types.
const (
	FrameAgentSpawned   = "fleet.agent.spawned"
	FrameAgentStatus    = "fleet.agent.status"
	FrameAgentMessage   = "fleet.agent.message"
	FrameAgentReport    = "fleet.agent.report"
	FrameAgentExited    = "fleet.agent.exited"
	FrameAgentRestarted = "fleet.agent.restarted"
	FrameAgentAbandoned = "fleet.agent.abandoned"

	FrameRepoCreated   = "fleet.repo.created"
	FrameRepoActivated = "fleet.repo.activated"
	FrameRepoKilled    = "fleet.repo.killed"

	FrameWorkstreamCreated   = "fleet.workstream.created"
	FrameWorkstreamActivated = "fleet.workstream.activated"
	FrameWorkstreamKilled    = "fleet.workstream.killed"

	FrameSprintCreated   = "fleet.sprint.created"
	FrameSprintActivated = "fleet.sprint.activated"
	FrameSprintKilled    = "fleet.sprint.killed"

	FrameSerialized = "fleet.serialized"
	FrameRestored   = "fleet.restored"
)

// fleetFrame is one typed fleet-stream frame. frameType is both the event Type the
// frame is appended under and the `type` field inside its JSON payload; the coalescer
// reads it and buildCatalog keys the frame's schema by it.
type fleetFrame interface{ frameType() string }

// fleetAgentSpawned announces a new (or restored) agent on the fleet. Subject is the
// per-agent subject id — the correlation key a TUI opens GET /events?session=<subject>
// with — so a late consumer can attach to the agent's own stream without a replay.
type fleetAgentSpawned struct {
	Type     string `json:"type"`
	TS       string `json:"ts"`
	AgentID  string `json:"agent_id"`
	Name     string `json:"name"`
	SprintID string `json:"sprint_id"`
	Backend  string `json:"backend"`
	Subject  string `json:"subject"`
}

func (f fleetAgentSpawned) frameType() string { return f.Type }

// fleetAgentStatus is a transcript-derived status snapshot. It is the only coalesced
// frame: a state/tool/target transition emits immediately, a token-only change at most
// once per coalesceWindow — the exact live token counts stay on the per-agent stream.
type fleetAgentStatus struct {
	Type    string `json:"type"`
	TS      string `json:"ts"`
	AgentID string `json:"agent_id"`
	State   string `json:"state"`
	Tool    string `json:"tool"`
	Target  string `json:"target"`
	Tokens  int    `json:"tokens"`
}

func (f fleetAgentStatus) frameType() string { return f.Type }

// fleetAgentMessage marks an orchestrator → agent message delivery.
type fleetAgentMessage struct {
	Type    string `json:"type"`
	TS      string `json:"ts"`
	AgentID string `json:"agent_id"`
}

func (f fleetAgentMessage) frameType() string { return f.Type }

// fleetAgentReport mirrors an agent → orchestrator report and its optional run state.
type fleetAgentReport struct {
	Type    string `json:"type"`
	TS      string `json:"ts"`
	AgentID string `json:"agent_id"`
	State   string `json:"state,omitempty"`
}

func (f fleetAgentReport) frameType() string { return f.Type }

// fleetAgentExited is an agent's terminal frame; Reason is killed (an operator or a
// container-kill cascade) or exited (a supervisor abandonment).
type fleetAgentExited struct {
	Type    string `json:"type"`
	TS      string `json:"ts"`
	AgentID string `json:"agent_id"`
	Reason  string `json:"reason"`
}

func (f fleetAgentExited) frameType() string { return f.Type }

// fleetAgentRestarted marks a non-terminal respawn — a supervisor budget restart or a
// manual cco.agent.respawn — carrying the restart attempt count.
type fleetAgentRestarted struct {
	Type    string `json:"type"`
	TS      string `json:"ts"`
	AgentID string `json:"agent_id"`
	Attempt int    `json:"attempt"`
}

func (f fleetAgentRestarted) frameType() string { return f.Type }

// fleetAgentAbandoned marks the supervisor giving up after Attempts restarts, just
// before the terminal fleet.agent.exited.
type fleetAgentAbandoned struct {
	Type     string `json:"type"`
	TS       string `json:"ts"`
	AgentID  string `json:"agent_id"`
	Attempts int    `json:"attempts"`
}

func (f fleetAgentAbandoned) frameType() string { return f.Type }

// fleetContainer is the shared shape of every repo/workstream/sprint lifecycle frame —
// the container's id and name under a created/activated/killed Type.
type fleetContainer struct {
	Type string `json:"type"`
	TS   string `json:"ts"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (f fleetContainer) frameType() string { return f.Type }

// fleetBundle is the shared shape of the serialize/restore frames: the bundle path and
// the number of agents it covers.
type fleetBundle struct {
	Type  string `json:"type"`
	TS    string `json:"ts"`
	Path  string `json:"path"`
	Count int    `json:"count"`
}

func (f fleetBundle) frameType() string { return f.Type }

func spawnedFrame(ag agentRow) fleetAgentSpawned {
	return fleetAgentSpawned{
		Type: FrameAgentSpawned, TS: nowStamp(), AgentID: ag.ID, Name: ag.Name,
		SprintID: ag.SprintID, Backend: string(ag.Backend), Subject: ag.SubjectID,
	}
}

func agentStatusFrame(agentID string, st Status) fleetAgentStatus {
	return fleetAgentStatus{
		Type: FrameAgentStatus, TS: nowStamp(), AgentID: agentID,
		State: string(st.State), Tool: st.Tool, Target: st.Target, Tokens: st.Tokens,
	}
}

func messageFrame(agentID string) fleetAgentMessage {
	return fleetAgentMessage{Type: FrameAgentMessage, TS: nowStamp(), AgentID: agentID}
}

func reportFrame(agentID, state string) fleetAgentReport {
	return fleetAgentReport{Type: FrameAgentReport, TS: nowStamp(), AgentID: agentID, State: state}
}

func exitedFrame(agentID, reason string) fleetAgentExited {
	return fleetAgentExited{Type: FrameAgentExited, TS: nowStamp(), AgentID: agentID, Reason: reason}
}

func restartedFrame(agentID string, attempt int) fleetAgentRestarted {
	return fleetAgentRestarted{Type: FrameAgentRestarted, TS: nowStamp(), AgentID: agentID, Attempt: attempt}
}

func abandonedFrame(agentID string, attempts int) fleetAgentAbandoned {
	return fleetAgentAbandoned{Type: FrameAgentAbandoned, TS: nowStamp(), AgentID: agentID, Attempts: attempts}
}

func containerFrame(frameType, id, name string) fleetContainer {
	return fleetContainer{Type: frameType, TS: nowStamp(), ID: id, Name: name}
}

func bundleFrame(frameType, path string, count int) fleetBundle {
	return fleetBundle{Type: frameType, TS: nowStamp(), Path: path, Count: count}
}

// fleetLog is the daemon-lifetime fleet event stream: the single write codepath every
// fleet frame flows through. It is package-level state initialized once by
// startFleetStream in BootReconcile, mirroring how tailers and supervisorRunner are
// held, because the domain handlers that emit frames are free functions with nowhere to
// thread it. A nil fleetLog — a handler exercised in a focused unit test with no daemon
// booted — makes every emit a no-op, so such a test needs no fleet bootstrap.
var fleetLog *fleetStream

// fleetStream appends every fleet frame to the fleet subject under one lock, coalescing
// token-only status frames and tracking the subject's high-water seq for the atomic
// bootstrap cursor cco.fleet.status hands a reconnecting TUI.
type fleetStream struct {
	subjectID string
	append    daemon.AppendFunc
	now       func() time.Time

	mu       sync.Mutex
	lastSeq  int64
	coalesce map[string]statusCoalesce
}

// statusCoalesce is the last emitted state/tool/target for one agent plus the time of
// its last emitted status — the reference the token-only throttle measures against.
type statusCoalesce struct {
	state    string
	tool     string
	target   string
	lastEmit time.Time
}

// emit is the single fleet write codepath. Frames mirror durable state — row and
// subject-log mutations — never teardown success: a caller emits only after the durable
// mutation a frame reflects has committed, and never gates a frame on a backend teardown.
// It coalesces a token-only status frame, clears an agent's coalesce entry on its
// terminal or (re)spawn frame, appends the frame to the fleet subject, and advances the
// high-water seq. It never fails a caller's already-completed op — an append failure is
// logged (matching the tailer and supervisor async paths) rather than propagated — and is
// a no-op on a nil stream so a focused handler test needs no fleet bootstrap.
func (f *fleetStream) emit(ctx context.Context, fr fleetFrame) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if st, ok := fr.(fleetAgentStatus); ok && !f.admitStatus(st.AgentID, st.State, st.Tool, st.Target) {
		return
	}
	f.resetCoalesceOn(fr)
	payload, err := json.Marshal(fr)
	if err != nil {
		log.Printf("cc-orchestrate: fleet %s marshal: %v", fr.frameType(), err)
		return
	}
	seq, err := f.append(ctx, &event.Event{
		SubjectID: f.subjectID, Origin: event.OriginSystem, Type: fr.frameType(), Payload: payload,
	})
	if err != nil {
		log.Printf("cc-orchestrate: fleet %s append: %v", fr.frameType(), err)
		return
	}
	f.lastSeq = seq
}

// admitStatus applies the status coalescing policy under f.mu: a state/tool/target
// transition (or an agent's first status) is always admitted and refreshes the throttle
// reference, so no transition is ever dropped; a token-only change is admitted only once
// its window has elapsed since the last emitted status.
func (f *fleetStream) admitStatus(agentID, state, tool, target string) bool {
	prev, seen := f.coalesce[agentID]
	now := f.now()
	if !seen || prev.state != state || prev.tool != tool || prev.target != target {
		f.coalesce[agentID] = statusCoalesce{state: state, tool: tool, target: target, lastEmit: now}
		return true
	}
	if now.Sub(prev.lastEmit) >= coalesceWindow {
		prev.lastEmit = now
		f.coalesce[agentID] = prev
		return true
	}
	return false
}

// resetCoalesceOn drops an agent's coalescing state on its terminal (exited) or (re)spawn
// frame, so a respawned agent's first status always emits (never suppressed by the prior
// life's window) and the coalesce map never grows without bound. Held under f.mu by emit.
func (f *fleetStream) resetCoalesceOn(fr fleetFrame) {
	switch t := fr.(type) {
	case fleetAgentExited:
		delete(f.coalesce, t.AgentID)
	case fleetAgentRestarted:
		delete(f.coalesce, t.AgentID)
	case fleetAgentSpawned:
		delete(f.coalesce, t.AgentID)
	}
}

// subject returns the fleet subject id (empty on a nil stream).
func (f *fleetStream) subject() string {
	if f == nil {
		return ""
	}
	return f.subjectID
}

// seq returns the fleet subject's current high-water seq (0 on a nil stream) — the
// resume cursor cco.fleet.status hands a connecting TUI.
func (f *fleetStream) seq() int64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSeq
}

// emitReport mirrors an agent's report to the fleet stream. The report handler resolves
// the reporting subject, not the agent id its frame carries, so this maps back from the
// subject to the agent. A lookup failure is logged, never propagated — the report itself
// has already succeeded and must not fail because its fleet mirror could not correlate.
func emitReport(ctx context.Context, db *sql.DB, subjectID, state string) {
	ag, err := getAgentBySubject(ctx, db, subjectID)
	if err != nil {
		log.Printf("cc-orchestrate: fleet report mirror: %v", err)
		return
	}
	fleetLog.emit(ctx, reportFrame(ag.ID, state))
}

// startFleetStream resolves the dedicated fleet subject — creating it on first boot,
// resuming it on every restart — and returns the stream every fleet frame flows through.
// It builds its own subject.Resolver over the daemon's subjects table, the same store
// the daemon and its realtime plane read, so the subject it creates is exactly the one
// GET /events?session=fleet resolves. fresh=false with a fixed, pid-less (session,
// scope) pair resumes the same subject and its per-subject seq across daemon swaps.
func startFleetStream(ctx context.Context, s *daemon.Server) (*fleetStream, error) {
	resolver := subject.Resolver{
		Store:  store.NewSubjectStore(s.DB()),
		Policy: subject.Policy{Active: func(sub subject.Subject) bool { return sub.Status == string(StatusActive) }},
	}
	sub, _, err := resolver.Start(ctx, subject.Window{Session: fleetSession}, fleetScope, fleetSlug, lifecycle, false)
	if err != nil {
		return nil, fmt.Errorf("start fleet subject: %w", err)
	}
	seq, err := currentFleetSeq(ctx, s, sub.ID)
	if err != nil {
		return nil, err
	}
	return &fleetStream{
		subjectID: sub.ID, append: s.Append, now: time.Now,
		lastSeq: seq, coalesce: map[string]statusCoalesce{},
	}, nil
}

// currentFleetSeq reads the fleet subject's high-water seq so a daemon restart resumes
// the bootstrap cursor rather than replaying every consumer from zero. It reads through
// the daemon's own EventsSince — the sanctioned event-log read path — whose rows are
// ordered oldest-first, so the last one carries the high-water seq.
func currentFleetSeq(ctx context.Context, s *daemon.Server, subjectID string) (int64, error) {
	evs, err := s.EventsSince(ctx, subjectID, 0, "")
	if err != nil {
		return 0, fmt.Errorf("read fleet seq: %w", err)
	}
	if len(evs) == 0 {
		return 0, nil
	}
	return evs[len(evs)-1].Seq, nil
}

// fleetStatusRequest takes no arguments — the fleet status is the whole current view.
type fleetStatusRequest struct{}

// fleetStatusResult is the atomic bootstrap a TUI reads once before connecting the SSE
// stream: the fleet subject and its current seq (the last_event_id to resume from, so
// the stream is never replayed from zero), the live HTTP port, and the full current
// repo/workstream/sprint/agent views (every status, unfiltered).
type fleetStatusResult struct {
	FleetSubject string           `json:"fleet_subject"`
	Seq          int64            `json:"seq"`
	HTTPPort     int              `json:"http_port"`
	Repos        []repoView       `json:"repos"`
	Workstreams  []workstreamView `json:"workstreams"`
	Sprints      []sprintView     `json:"sprints"`
	Agents       []agentView      `json:"agents"`
}

// fleetStatusMidRead is a test seam fired between the cursor read and the views read, so
// a test can inject an emit into that window and prove the cursor lags the snapshot. A
// no-op in production.
var fleetStatusMidRead = func() {}

// handleFleetStatus answers cco.fleet.status: the current views plus the fleet subject
// and its seq. The seq is read before the views, so the resume cursor can only lag the
// snapshot, never lead it — a frame emitted between the two reads is already reflected in
// the views yet is re-delivered on the stream (an idempotent repaint), never dropped.
func handleFleetStatus(hc daemon.HandlerCtx, _ fleetStatusRequest) (fleetStatusResult, error) {
	// Cursor before views, so it can only lag the snapshot, never lead it.
	fleetSubject := fleetLog.subject()
	seq := fleetLog.seq()
	fleetStatusMidRead()
	repos, err := listRepos(hc.Ctx, hc.DB, "")
	if err != nil {
		return fleetStatusResult{}, err
	}
	workstreams, err := listWorkstreams(hc.Ctx, hc.DB, "", "")
	if err != nil {
		return fleetStatusResult{}, err
	}
	sprints, err := listSprints(hc.Ctx, hc.DB, "", "")
	if err != nil {
		return fleetStatusResult{}, err
	}
	agents, err := listAgents(hc.Ctx, hc.DB, "", "")
	if err != nil {
		return fleetStatusResult{}, err
	}
	res := fleetStatusResult{
		FleetSubject: fleetSubject,
		Seq:          seq,
		HTTPPort:     hc.HTTPPort,
		Repos:        make([]repoView, len(repos)),
		Workstreams:  make([]workstreamView, len(workstreams)),
		Sprints:      make([]sprintView, len(sprints)),
		Agents:       make([]agentView, len(agents)),
	}
	for i, p := range repos {
		res.Repos[i] = newRepoView(p)
	}
	for i, w := range workstreams {
		res.Workstreams[i] = newWorkstreamView(w)
	}
	for i, sp := range sprints {
		res.Sprints[i] = newSprintView(sp)
	}
	for i, ag := range agents {
		res.Agents[i] = newAgentView(ag)
	}
	return res, nil
}

// init populates the catalog's fleet frame schemas (the fleetEventSchemas var declared
// in xrpc.go) by reflecting each frame struct under its type string, so
// cco.server.describe serves the fleet taxonomy under events.types. The container and
// bundle frames share one struct each, registered under every type string they carry.
func init() {
	for frameType, sample := range map[string]fleetFrame{
		FrameAgentSpawned:   fleetAgentSpawned{},
		FrameAgentStatus:    fleetAgentStatus{},
		FrameAgentMessage:   fleetAgentMessage{},
		FrameAgentReport:    fleetAgentReport{},
		FrameAgentExited:    fleetAgentExited{},
		FrameAgentRestarted: fleetAgentRestarted{},
		FrameAgentAbandoned: fleetAgentAbandoned{},

		FrameRepoCreated:   fleetContainer{},
		FrameRepoActivated: fleetContainer{},
		FrameRepoKilled:    fleetContainer{},

		FrameWorkstreamCreated:   fleetContainer{},
		FrameWorkstreamActivated: fleetContainer{},
		FrameWorkstreamKilled:    fleetContainer{},

		FrameSprintCreated:   fleetContainer{},
		FrameSprintActivated: fleetContainer{},
		FrameSprintKilled:    fleetContainer{},

		FrameSerialized: fleetBundle{},
		FrameRestored:   fleetBundle{},
	} {
		fleetEventSchemas[frameType] = schemaFor(reflect.TypeOf(sample))
	}
}
