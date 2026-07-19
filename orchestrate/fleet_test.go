package orchestrate

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
)

// fakeClock is an injectable, advanceable clock for the fleet coalescer, so the
// coalescing tests assert the window boundary without ever sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// installTestFleet points the package fleetLog at a stream backed by a fresh eventLog
// and fake clock, restoring the previous stream when the test ends.
func installTestFleet(t *testing.T) (*eventLog, *fakeClock) {
	t.Helper()
	log := &eventLog{}
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0).UTC()}
	prev := fleetLog
	fleetLog = &fleetStream{
		subjectID: "fleet-subject", append: log.append, now: clk.Now,
		coalesce: map[string]statusCoalesce{},
	}
	t.Cleanup(func() { fleetLog = prev })
	return log, clk
}

// framePayload decodes the first frame of frameType in the log, failing when none was
// recorded.
func framePayload(t *testing.T, log *eventLog, frameType string) map[string]any {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, e := range log.events {
		if e.Type == frameType {
			var m map[string]any
			if err := json.Unmarshal(e.Payload, &m); err != nil {
				t.Fatalf("decode %s payload: %v", frameType, err)
			}
			return m
		}
	}
	t.Fatalf("no %s frame recorded; got %v", frameType, log.types())
	return nil
}

// exitedReasons returns the reason of every fleet.agent.exited frame, in recorded order.
func exitedReasons(t *testing.T, log *eventLog) []string {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	var out []string
	for _, e := range log.events {
		if e.Type == FrameAgentExited {
			var m map[string]any
			if err := json.Unmarshal(e.Payload, &m); err != nil {
				t.Fatalf("decode exited payload: %v", err)
			}
			out = append(out, m["reason"].(string))
		}
	}
	return out
}

// TestFleetSubjectSSEAddressable is the critical contract: the bootstrapped fleet
// subject must answer the exact stream URL the catalog publishes. It builds a real
// daemon, bootstraps the fleet subject through startFleetStream (as BootReconcile does),
// then a GET on /events?session=fleet must resolve it (200 + the SSE preamble), never a
// 404 as an unknown ref would.
func TestFleetSubjectSSEAddressable(t *testing.T) {
	s, ts := newXRPCServer(t)
	fleet, err := startFleetStream(context.Background(), s)
	if err != nil {
		t.Fatalf("startFleetStream: %v", err)
	}
	prev := fleetLog
	fleetLog = fleet
	t.Cleanup(func() { fleetLog = prev })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?session=fleet", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events?session=fleet: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the fleet subject must resolve the published stream URL)", resp.StatusCode)
	}
	buf := make([]byte, 256)
	if n, _ := resp.Body.Read(buf); n == 0 {
		t.Fatal("no SSE preamble read; the fleet stream did not open on the subject")
	}
}

func TestFleetCoalescing(t *testing.T) {
	log, clk := installTestFleet(t)
	ctx := context.Background()
	emitStatus := func(state State, tokens int) {
		fleetLog.emit(ctx, agentStatusFrame("a1", Status{State: state, Tool: "Bash", Target: "go test", Tokens: tokens}))
	}

	emitStatus(StateWorking, 10) // first status: a transition, always emits
	if n := log.count(FrameAgentStatus); n != 1 {
		t.Fatalf("first status: %d frames, want 1", n)
	}

	clk.advance(1 * time.Second)
	emitStatus(StateWorking, 20) // token-only within the window: suppressed
	if n := log.count(FrameAgentStatus); n != 1 {
		t.Fatalf("token-only within window: %d frames, want 1 (suppressed)", n)
	}

	emitStatus(StateIdle, 25) // a transition during the suppression window still emits
	if n := log.count(FrameAgentStatus); n != 2 {
		t.Fatalf("state transition in window: %d frames, want 2 (emitted)", n)
	}

	clk.advance(coalesceWindow)
	emitStatus(StateIdle, 40) // token-only past the window: emits
	if n := log.count(FrameAgentStatus); n != 3 {
		t.Fatalf("token-only past window: %d frames, want 3 (emitted)", n)
	}

	clk.advance(1 * time.Second)
	emitStatus(StateIdle, 55) // token-only within the reopened window: suppressed
	if n := log.count(FrameAgentStatus); n != 3 {
		t.Fatalf("token-only within reopened window: %d frames, want 3 (suppressed)", n)
	}
}

// TestFleetCoalescingIsPerAgent proves the coalescer keys on agent id: a second agent's
// first status is never suppressed by a first agent's window.
func TestFleetCoalescingIsPerAgent(t *testing.T) {
	log, _ := installTestFleet(t)
	ctx := context.Background()
	fleetLog.emit(ctx, agentStatusFrame("a1", Status{State: StateWorking, Tokens: 1}))
	fleetLog.emit(ctx, agentStatusFrame("a2", Status{State: StateWorking, Tokens: 1}))
	if n := log.count(FrameAgentStatus); n != 2 {
		t.Fatalf("two agents' first statuses: %d frames, want 2", n)
	}
}

func TestFleetSeqMonotonic(t *testing.T) {
	installTestFleet(t)
	ctx := context.Background()
	frames := []fleetFrame{
		spawnedFrame(agentRow{ID: "a1", SubjectID: "s1"}),
		messageFrame("a1"),
		containerFrame(FrameSprintCreated, "sp1", "main"),
		reportFrame("a1", "working"),
		exitedFrame("a1", reasonKilled),
	}
	var prev int64
	for i, fr := range frames {
		fleetLog.emit(ctx, fr)
		got := fleetLog.seq()
		if got <= prev {
			t.Fatalf("frame %d (%s): seq %d not strictly greater than prev %d", i, fr.frameType(), got, prev)
		}
		prev = got
	}
}

func TestFleetEventSchemas(t *testing.T) {
	known := map[string]bool{
		FrameAgentSpawned: true, FrameAgentStatus: true, FrameAgentMessage: true,
		FrameAgentReport: true, FrameAgentExited: true, FrameAgentRestarted: true, FrameAgentAbandoned: true,
		FrameRepoCreated: true, FrameRepoActivated: true, FrameRepoKilled: true,
		FrameWorkstreamCreated: true, FrameWorkstreamActivated: true, FrameWorkstreamKilled: true,
		FrameSprintCreated: true, FrameSprintActivated: true, FrameSprintKilled: true,
		FrameSerialized: true, FrameRestored: true,
	}
	if len(fleetEventSchemas) == 0 {
		t.Fatal("fleetEventSchemas is empty; the catalog would advertise no fleet frame types")
	}
	if len(fleetEventSchemas) != len(known) {
		t.Errorf("fleetEventSchemas has %d keys, want %d", len(fleetEventSchemas), len(known))
	}
	for key, schema := range fleetEventSchemas {
		if !known[key] {
			t.Errorf("fleetEventSchemas has unexpected key %q", key)
		}
		m, ok := schema.(map[string]any)
		if !ok || m["type"] != "object" {
			t.Errorf("schema for %q = %v, want an object schema", key, schema)
		}
	}
	for key := range known {
		if _, ok := fleetEventSchemas[key]; !ok {
			t.Errorf("fleetEventSchemas missing %q", key)
		}
	}
}

func TestFleetStatus(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)
	installTestFleet(t)

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/s", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "tmux", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/s", SessionID: "sess-1",
		SubjectID: "subj-a1", Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})

	// Advance the fleet seq so the reported cursor is a live, non-zero high-water mark.
	fleetLog.emit(ctx, containerFrame(FrameRepoCreated, "p1", "alpha"))
	fleetLog.emit(ctx, spawnedFrame(agentRow{ID: "a1", SubjectID: "subj-a1"}))
	wantSeq := fleetLog.seq()
	if wantSeq == 0 {
		t.Fatal("fleet seq is 0 after emits; the bootstrap cursor would force a from-zero replay")
	}

	res, err := handleFleetStatus(daemon.HandlerCtx{Ctx: ctx, DB: db, HTTPPort: 4321}, fleetStatusRequest{})
	if err != nil {
		t.Fatalf("handleFleetStatus: %v", err)
	}
	if res.FleetSubject != "fleet-subject" {
		t.Errorf("fleet_subject = %q, want fleet-subject", res.FleetSubject)
	}
	if res.Seq != wantSeq {
		t.Errorf("seq = %d, want the live high-water %d", res.Seq, wantSeq)
	}
	if res.HTTPPort != 4321 {
		t.Errorf("http_port = %d, want 4321", res.HTTPPort)
	}
	if len(res.Repos) != 1 || len(res.Workstreams) != 1 || len(res.Sprints) != 1 || len(res.Agents) != 1 {
		t.Fatalf("views = %d repos, %d workstreams, %d sprints, %d agents; want 1 each",
			len(res.Repos), len(res.Workstreams), len(res.Sprints), len(res.Agents))
	}
	if res.Agents[0].SubjectID != "subj-a1" {
		t.Errorf("agent view subject_id = %q, want subj-a1", res.Agents[0].SubjectID)
	}

	// The agent view carries subject_id on the wire so a late TUI can correlate.
	raw, err := json.Marshal(res.Agents[0])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if got, ok := m["subject_id"]; !ok || got != "subj-a1" {
		t.Errorf("marshaled agent view subject_id = %v (present %v), want subj-a1", got, ok)
	}
}

func TestFleetSpawnFrame(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	oldLookup := lookupPath
	lookupPath = func(string) (string, error) { return "", exec.ErrNotFound }
	t.Cleanup(func() { lookupPath = oldLookup })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	backend.Register(spawnBackend{spec: &backend.SpawnSpec{}})

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), initializeDatabaseSchema)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: "/tmp/alpha", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/alpha", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}

	hc := daemon.HandlerCtx{
		Ctx: ctx, Env: daemon.Envelope{Body: mustJSON(t, map[string]string{"repo": "p1", "name": "worker", "prompt": "fix it"})},
		Window: subject.Window{Session: "parent", ClaudePID: 4242}, Scope: "/parent",
		Subjects: subject.Resolver{Store: store.NewSubjectStore(db)}, DB: db,
		Append: (&eventLog{}).append,
	}
	reply := runTyped(handleSpawn, hc)
	if !reply.OK {
		t.Fatalf("spawn failed: %s", reply.Error)
	}
	var out agentSpawnResult
	if err := json.Unmarshal(reply.Body, &out); err != nil {
		t.Fatal(err)
	}

	if log.count(FrameAgentSpawned) != 1 {
		t.Fatalf("fleet.agent.spawned count = %d, want 1; frames=%v", log.count(FrameAgentSpawned), log.types())
	}
	pl := framePayload(t, log, FrameAgentSpawned)
	if pl["subject"] == "" || pl["subject"] != out.SubjectID {
		t.Fatalf("spawned frame subject = %v, want the non-empty per-agent subject %q", pl["subject"], out.SubjectID)
	}
	if pl["agent_id"] != out.ID || pl["backend"] != "spawntest" {
		t.Fatalf("spawned frame = %v, want agent_id %q backend spawntest", pl, out.ID)
	}
}

func TestFleetAgentKillFrame(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	db := newTestDB(ctx, t)

	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "optest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", Name: "worker", SubjectID: "subj-1",
		Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})
	backend.Register(opBackend{})

	if reply := runTyped(handleAgentKill, opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("kill failed: %s", reply.Error)
	}
	if got := exitedReasons(t, log); len(got) != 1 || got[0] != reasonKilled {
		t.Fatalf("exited frames = %v, want exactly one reason=killed", got)
	}
	if pl := framePayload(t, log, FrameAgentExited); pl["agent_id"] != "a1" {
		t.Fatalf("exited frame agent_id = %v, want a1", pl["agent_id"])
	}

	// An idempotent re-kill of the now-exited agent emits no second exited frame.
	if reply := runTyped(handleAgentKill, opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("re-kill failed: %s", reply.Error)
	}
	if n := log.count(FrameAgentExited); n != 1 {
		t.Fatalf("exited frame count after re-kill = %d, want 1 (no duplicate)", n)
	}
}

func TestFleetSprintKillFrames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	db := newTestDB(ctx, t)

	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "sprintA", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	for _, a := range []agentRow{
		{ID: "a1", SprintID: "s1", Backend: "optest", TerminalHandle: "term-1", SessionID: "sess-1", Scope: "/s", SubjectID: "subj-1", Status: StatusActive, State: StateWorking, CreatedAt: "t0"},
		{ID: "a2", SprintID: "s1", Backend: "optest", TerminalHandle: "term-2", SessionID: "sess-2", Scope: "/s", SubjectID: "subj-2", Status: StatusActive, State: StateWorking, CreatedAt: "t1"},
		{ID: "a3", SprintID: "s1", Backend: "optest", TerminalHandle: "term-3", SessionID: "sess-3", Scope: "/s", SubjectID: "subj-3", Status: StatusExited, State: StateIdle, CreatedAt: "t2"},
	} {
		mustInsertAgent(ctx, t, db, a)
	}
	backend.Register(opBackend{})

	if reply := runTyped(handleSprintKill, opCtx(db, mustJSON(t, map[string]string{"id": "s1"}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("sprint kill failed: %s", reply.Error)
	}

	if n := log.count(FrameSprintKilled); n != 1 {
		t.Fatalf("fleet.sprint.killed count = %d, want 1", n)
	}
	// Only the two formerly-active agents get an exited(killed) frame; the pre-exited a3
	// does not.
	if got := exitedReasons(t, log); len(got) != 2 || got[0] != reasonKilled || got[1] != reasonKilled {
		t.Fatalf("exited reasons = %v, want exactly two killed (a1, a2)", got)
	}
	if sp := framePayload(t, log, FrameSprintKilled); sp["id"] != "s1" || sp["name"] != "sprintA" {
		t.Fatalf("sprint.killed frame = %v, want id s1 name sprintA", sp)
	}
}

func TestFleetWorkstreamKillFrames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	db := newTestDB(ctx, t)

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "optest", Cwd: "/s", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	// A primary workstream so tearDownWorkstream removes no git worktree (none to seed).
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "optest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/s", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "optest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", SubjectID: "subj-1", Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})
	backend.Register(opBackend{})

	if reply := runTyped(handleWorkstreamKill, opCtx(db, mustJSON(t, map[string]string{"id": "w1"}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("workstream kill failed: %s", reply.Error)
	}
	if n := log.count(FrameWorkstreamKilled); n != 1 {
		t.Fatalf("fleet.workstream.killed count = %d, want 1", n)
	}
	if got := exitedReasons(t, log); len(got) != 1 || got[0] != reasonKilled {
		t.Fatalf("exited reasons = %v, want one killed (a1)", got)
	}
	if ws := framePayload(t, log, FrameWorkstreamKilled); ws["id"] != "w1" || ws["name"] != "main" {
		t.Fatalf("workstream.killed frame = %v, want id w1 name main", ws)
	}
}

func TestFleetRespawnFrame(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	db := newTestDB(ctx, t)
	backend.Register(spawnBackend{spec: &backend.SpawnSpec{}})

	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: "/tmp/a", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	if err := insertWorkstream(ctx, db, workstreamRow{
		ID: "w1", RepoID: "p1", Name: "main", Backend: "spawntest", WorkspaceHandle: "ws-1",
		Branch: "main", Worktree: "/tmp/a", IsPrimary: true, Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertSprint(ctx, db, sprintRow{ID: "s1", WorkstreamID: "w1", Name: "main", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "s1", Backend: "spawntest", TerminalHandle: "term-old",
		SessionID: "sess-1", Scope: "/tmp/a", Name: "worker", SubjectID: "subj-1",
		Status: StatusExited, State: StateIdle, CreatedAt: "t0", RestartCount: 2, LastRestartAt: "2026-06-01T00:00:00Z",
	})

	if reply := runTyped(handleAgentRespawn, opCtx(db, mustJSON(t, map[string]any{"agent_id": "a1"}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("respawn failed: %s", reply.Error)
	}
	if log.count(FrameAgentRestarted) != 1 {
		t.Fatalf("fleet.agent.restarted count = %d, want 1; frames=%v", log.count(FrameAgentRestarted), log.types())
	}
	pl := framePayload(t, log, FrameAgentRestarted)
	if pl["agent_id"] != "a1" {
		t.Fatalf("restarted frame agent_id = %v, want a1", pl["agent_id"])
	}
	// A manual respawn resets the budget, so it reports attempt 0.
	if pl["attempt"].(float64) != 0 {
		t.Fatalf("restarted frame attempt = %v, want 0 (budget reset)", pl["attempt"])
	}
}

func TestFleetSerializeRestoreFrames(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	log, _ := installTestFleet(t)
	db := newTestDB(ctx, t)
	backend.Register(serializeTestBackend{})
	seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
		Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})

	out := filepath.Join(t.TempDir(), "bundle.json")
	if reply := runTyped(handleSerialize, opCtx(db, mustJSON(t, map[string]string{"out": out}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("serialize failed: %s", reply.Error)
	}
	if log.count(FrameSerialized) != 1 {
		t.Fatalf("fleet.serialized count = %d, want 1", log.count(FrameSerialized))
	}
	if ser := framePayload(t, log, FrameSerialized); ser["path"] != out || ser["count"].(float64) != 1 {
		t.Fatalf("serialized frame = %v, want path %q count 1", ser, out)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM agents`); err != nil {
		t.Fatal(err)
	}
	if reply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": out}), (&eventLog{}).append)); !reply.OK {
		t.Fatalf("restore failed: %s", reply.Error)
	}
	// Restore emits one spawned frame per revived agent (the identity + subject a
	// reconnecting TUI needs) plus one bundle-level restored frame.
	if log.count(FrameAgentSpawned) != 1 {
		t.Fatalf("restore fleet.agent.spawned count = %d, want 1 per revived agent", log.count(FrameAgentSpawned))
	}
	if rev := framePayload(t, log, FrameAgentSpawned); rev["agent_id"] != "a1" || rev["subject"] != "subj-a1" {
		t.Fatalf("restore spawned frame = %v, want agent_id a1 subject subj-a1", rev)
	}
	if log.count(FrameRestored) != 1 {
		t.Fatalf("fleet.restored count = %d, want 1", log.count(FrameRestored))
	}
	if res := framePayload(t, log, FrameRestored); res["path"] != out || res["count"].(float64) != 1 {
		t.Fatalf("restored frame = %v, want path %q count 1", res, out)
	}
}

// TestFleetAbandonFrameOrder asserts the abandon sequence — abandoned then a terminal
// exited(reason=exited) — that supervisor.respawnUnderBudget drives through the emit
// chokepoint.
func TestFleetAbandonFrameOrder(t *testing.T) {
	log, _ := installTestFleet(t)
	ctx := context.Background()
	fleetLog.emit(ctx, abandonedFrame("a1", restartBudget))
	fleetLog.emit(ctx, exitedFrame("a1", reasonExited))

	if log.count(FrameAgentAbandoned) != 1 {
		t.Fatalf("abandoned count = %d, want 1", log.count(FrameAgentAbandoned))
	}
	if got := exitedReasons(t, log); len(got) != 1 || got[0] != reasonExited {
		t.Fatalf("exited reasons = %v, want one exited (supervisor abandon)", got)
	}
	if got := log.types(); len(got) != 2 || got[0] != FrameAgentAbandoned || got[1] != FrameAgentExited {
		t.Fatalf("frame order = %v, want [abandoned exited]", got)
	}
}

// TestFleetStatusCursorLagsSnapshot proves the resume cursor is captured before the views
// (finding 6): a frame emitted between the two reads is already reflected in the snapshot
// yet carries a seq beyond the reported cursor, so resuming re-delivers it rather than
// skipping it. The window is driven deterministically via the fleetStatusMidRead seam.
func TestFleetStatusCursorLagsSnapshot(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)
	installTestFleet(t)
	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "tmux", Cwd: "/s", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	before := fleetLog.seq()

	prev := fleetStatusMidRead
	fleetStatusMidRead = func() {
		if err := insertRepo(ctx, db, repoRow{ID: "p2", Name: "beta", Backend: "tmux", Cwd: "/s2", Status: StatusActive, CreatedAt: "t1"}); err != nil {
			t.Fatal(err)
		}
		fleetLog.emit(ctx, containerFrame(FrameRepoCreated, "p2", "beta"))
	}
	t.Cleanup(func() { fleetStatusMidRead = prev })

	res, err := handleFleetStatus(daemon.HandlerCtx{Ctx: ctx, DB: db, HTTPPort: 1}, fleetStatusRequest{})
	if err != nil {
		t.Fatalf("handleFleetStatus: %v", err)
	}
	// The mid-read repo is in the snapshot (views read after the emit).
	present := false
	for _, r := range res.Repos {
		if r.ID == "p2" {
			present = true
		}
	}
	if !present {
		t.Fatalf("views = %+v, want the mid-read repo p2 present", res.Repos)
	}
	// The cursor was captured before that emit, so it lags — never leads — the snapshot.
	if res.Seq != before {
		t.Fatalf("seq = %d, want %d (cursor captured before the mid-read emit)", res.Seq, before)
	}
	if fleetLog.seq() <= res.Seq {
		t.Fatalf("post-call seq %d must exceed the reported cursor %d (the p2 frame is re-delivered)", fleetLog.seq(), res.Seq)
	}
}

// TestFleetRepoCreateSprintFrameId proves the default-sprint frame carries the created
// sprint row's actual id (finding 7), not a second random slug that matches nothing.
func TestFleetRepoCreateSprintFrameId(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)
	log, _ := installTestFleet(t)
	repo := gitRepo(ctx, t, "main")
	backend.Register(workstreamBackend{manages: true, worktree: t.TempDir()})

	reply := runTyped(handleRepoCreate, opCtx(db, mustJSON(t, map[string]string{"name": "demo", "backend": "wstest", "cwd": repo}), nil))
	if !reply.OK {
		t.Fatalf("repo create failed: %s", reply.Error)
	}
	pl := framePayload(t, log, FrameSprintCreated)
	sprintID, _ := pl["id"].(string)
	if sprintID == "" {
		t.Fatalf("sprint.created frame has no id: %v", pl)
	}
	if _, err := getSprint(ctx, db, sprintID, ""); err != nil {
		t.Fatalf("sprint.created frame id %q is not a queryable sprint row: %v", sprintID, err)
	}
}

// TestFleetCoalescePrunedOnExit proves a terminal frame clears an agent's coalesce entry
// (finding 11): after an exit, a respawn-identical status re-emits despite still being
// inside the 3s coalesce window.
func TestFleetCoalescePrunedOnExit(t *testing.T) {
	log, clk := installTestFleet(t)
	ctx := context.Background()
	st := Status{State: StateWorking, Tool: "Bash", Target: "go test", Tokens: 1}

	fleetLog.emit(ctx, agentStatusFrame("a1", st)) // first status: emits
	if n := log.count(FrameAgentStatus); n != 1 {
		t.Fatalf("first status: %d, want 1", n)
	}
	clk.advance(1 * time.Second)
	fleetLog.emit(ctx, agentStatusFrame("a1", st)) // identical, in window: suppressed
	if n := log.count(FrameAgentStatus); n != 1 {
		t.Fatalf("identical status in window: %d, want 1 (suppressed)", n)
	}
	fleetLog.emit(ctx, exitedFrame("a1", reasonKilled)) // prunes a1's coalesce entry
	fleetLog.emit(ctx, agentStatusFrame("a1", st))      // re-emits despite the window
	if n := log.count(FrameAgentStatus); n != 2 {
		t.Fatalf("status after exit: %d, want 2 (coalesce entry pruned on exit)", n)
	}
}
