package orchestrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// superviseBackend is a registered test backend for the keep-alive supervisor: its
// live agent set is controllable, every Spawn hands back a fresh terminal id (so a
// re-spawn is observable as a new backend_terminal_handle), and its CanEnumerate is
// toggleable so the "unsupervised backend" case is injectable. It advertises
// CanCapture too, so respawnAgent's wrapForCapture returns the argv unchanged and the
// test needs no real claude on PATH. Its name is outside backend.Precedence.
type superviseBackend struct {
	agents    []backend.AgentHandle
	enumerate bool
	mu        *sync.Mutex
	spawns    *int
	nextTerm  *string
	killed    *[]string // when set, records each Kill target's handle id
}

func (superviseBackend) Name() backend.Name                { return "supervisetest" }
func (superviseBackend) Available() bool                   { return true }
func (superviseBackend) EnsureReady(context.Context) error { return nil }
func (superviseBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}

func (superviseBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}

func (b superviseBackend) Spawn(_ context.Context, spec backend.SpawnSpec) (backend.AgentHandle, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	*b.spawns++
	return backend.AgentHandle{Backend: "supervisetest", ID: *b.nextTerm, SessionID: spec.SessionID}, nil
}

func (b superviseBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return b.agents, nil
}

func (b superviseBackend) Kill(_ context.Context, agent backend.AgentHandle) error {
	if b.killed != nil {
		b.mu.Lock()
		*b.killed = append(*b.killed, agent.ID)
		b.mu.Unlock()
	}
	return nil
}
func (superviseBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (superviseBackend) Capture(context.Context, backend.AgentHandle) (string, error)   { return "", nil }
func (b superviseBackend) Caps() backend.Caps {
	if b.enumerate {
		return backend.Capabilities(backend.CanEnumerate, backend.CanCapture)
	}
	return backend.Capabilities(backend.CanCapture)
}

// eventLog collects appended events under a mutex so the supervisor's writes and the
// background tailer goroutines never race the test reads.
type eventLog struct {
	mu     sync.Mutex
	events []*event.Event
}

func (l *eventLog) append(_ context.Context, e *event.Event) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	return int64(len(l.events)), nil
}

func (l *eventLog) types() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	for i, e := range l.events {
		out[i] = e.Type
	}
	return out
}

func (l *eventLog) count(t string) int {
	n := 0
	for _, et := range l.types() {
		if et == t {
			n++
		}
	}
	return n
}

func assertRestartCount(ctx context.Context, t *testing.T, db *sql.DB, id string, want int) {
	t.Helper()
	a, err := getAgent(ctx, db, id)
	if err != nil {
		t.Fatalf("getAgent %s: %v", id, err)
	}
	if a.RestartCount != want {
		t.Fatalf("agent %s restart_count = %d, want %d", id, a.RestartCount, want)
	}
}

func assertTerminalHandle(ctx context.Context, t *testing.T, db *sql.DB, id, want string) {
	t.Helper()
	a, err := getAgent(ctx, db, id)
	if err != nil {
		t.Fatalf("getAgent %s: %v", id, err)
	}
	if a.TerminalHandle != want {
		t.Fatalf("agent %s terminal handle = %q, want %q", id, a.TerminalHandle, want)
	}
}

func TestSupervisorTick(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	t.Run("vanish under budget re-spawns and increments", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)
		db := newTestDB(ctx, t)

		var mu sync.Mutex
		spawns := 0
		nextTerm := "term-2"
		backend.Register(superviseBackend{
			agents:    nil, // term-1 vanished
			enumerate: true,
			mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
		})
		seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
		seedAgent(ctx, t, db, "a1", "w1", "supervisetest", "term-1")

		log := &eventLog{}
		if err := newSupervisor().tick(ctx, db, log.append); err != nil {
			t.Fatal(err)
		}

		assertAgentStatus(ctx, t, db, "a1", StatusActive)
		assertRestartCount(ctx, t, db, "a1", 1)
		assertTerminalHandle(ctx, t, db, "a1", "term-2")
		if log.count(EventRestarted) != 1 {
			t.Fatalf("EventRestarted count = %d, want 1; events=%v", log.count(EventRestarted), log.types())
		}
		if log.count(EventExited) != 0 || log.count(EventAbandoned) != 0 {
			t.Fatalf("under budget must not abandon/exit; events=%v", log.types())
		}
		mu.Lock()
		gotSpawns := spawns
		mu.Unlock()
		if gotSpawns != 1 {
			t.Fatalf("backend Spawn calls = %d, want 1", gotSpawns)
		}
	})

	t.Run("at budget abandons then exits, no re-spawn", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)
		db := newTestDB(ctx, t)

		var mu sync.Mutex
		spawns := 0
		nextTerm := "term-x"
		backend.Register(superviseBackend{
			agents:    nil,
			enumerate: true,
			mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
		})
		seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
		mustInsertAgent(ctx, t, db, agentRow{
			ID: "a1", SprintID: "w1-s", Backend: "supervisetest", TerminalHandle: "term-1",
			SubjectID: "subj-a1", Status: StatusActive, State: StateWorking,
			RestartCount: restartBudget, CreatedAt: "t0",
		})

		log := &eventLog{}
		if err := newSupervisor().tick(ctx, db, log.append); err != nil {
			t.Fatal(err)
		}

		assertAgentStatus(ctx, t, db, "a1", StatusExited)
		if log.count(EventAbandoned) != 1 || log.count(EventExited) != 1 {
			t.Fatalf("at budget want one Abandoned then one Exited; events=%v", log.types())
		}
		if log.count(EventRestarted) != 0 {
			t.Fatalf("at budget must not restart; events=%v", log.types())
		}
		// EventAbandoned must precede the terminal EventExited.
		ts := log.types()
		ai, ei := -1, -1
		for i, ty := range ts {
			if ty == EventAbandoned {
				ai = i
			}
			if ty == EventExited {
				ei = i
			}
		}
		if ai < 0 || ei < 0 || ai > ei {
			t.Fatalf("EventAbandoned must precede EventExited; events=%v", ts)
		}
		mu.Lock()
		gotSpawns := spawns
		mu.Unlock()
		if gotSpawns != 0 {
			t.Fatalf("backend Spawn calls = %d, want 0 at budget", gotSpawns)
		}
	})

	t.Run("non-enumerable backend is never supervised", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)
		db := newTestDB(ctx, t)

		var mu sync.Mutex
		spawns := 0
		nextTerm := "term-2"
		backend.Register(superviseBackend{
			agents:    nil, // would look "all gone" — but the backend cannot enumerate
			enumerate: false,
			mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
		})
		seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
		seedAgent(ctx, t, db, "a1", "w1", "supervisetest", "term-1")

		log := &eventLog{}
		if err := newSupervisor().tick(ctx, db, log.append); err != nil {
			t.Fatal(err)
		}

		assertAgentStatus(ctx, t, db, "a1", StatusActive)
		assertRestartCount(ctx, t, db, "a1", 0)
		assertTerminalHandle(ctx, t, db, "a1", "term-1")
		if len(log.types()) != 0 {
			t.Fatalf("non-enumerable backend must emit nothing; events=%v", log.types())
		}
		mu.Lock()
		gotSpawns := spawns
		mu.Unlock()
		if gotSpawns != 0 {
			t.Fatalf("backend Spawn calls = %d, want 0 (unsupervised)", gotSpawns)
		}
	})

	t.Run("a kill racing a restart yields exactly one terminal outcome", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)
		db := newTestDB(ctx, t)

		var mu sync.Mutex
		spawns := 0
		nextTerm := "term-2"
		backend.Register(superviseBackend{
			agents:    nil,
			enumerate: true,
			mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
		})
		seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
		seedAgent(ctx, t, db, "a1", "w1", "supervisetest", "term-1")

		// Take the agent lock the way handleAgentKill does, flip the row to exited
		// under it, then release — modelling a kill that wins the race. reconcileVanished
		// must block on the same lock, re-read the now-exited row, and skip.
		mu2 := agentLock("a1")
		mu2.Lock()
		if err := setAgentLifecycle(ctx, db, "a1", StatusExited); err != nil {
			t.Fatal(err)
		}

		log := &eventLog{}
		done := make(chan error, 1)
		go func() { done <- newSupervisor().tick(ctx, db, log.append) }()

		// The tick is now parked on agentLock("a1"); releasing lets it observe the
		// killed row.
		mu2.Unlock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}

		assertAgentStatus(ctx, t, db, "a1", StatusExited)
		assertRestartCount(ctx, t, db, "a1", 0)
		if log.count(EventRestarted) != 0 {
			t.Fatalf("a killed agent must never be resurrected; events=%v", log.types())
		}
		mu.Lock()
		gotSpawns := spawns
		mu.Unlock()
		if gotSpawns != 0 {
			t.Fatalf("backend Spawn calls = %d, want 0 (agent was killed)", gotSpawns)
		}
	})

	t.Run("a kill racing a supervisor restart targets the fresh terminal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tailers = newTailerManager(ctx)
		db := newTestDB(ctx, t)

		var mu sync.Mutex
		spawns := 0
		nextTerm := "term-2"
		var killed []string
		backend.Register(superviseBackend{
			agents:    nil, // term-1 vanished
			enumerate: true,
			mu:        &mu, spawns: &spawns, nextTerm: &nextTerm, killed: &killed,
		})
		seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
		seedAgent(ctx, t, db, "a1", "w1", "supervisetest", "term-1")

		// Hold the agent lock the way a supervisor respawn does, so agent-kill must
		// block on it before it can read the row. A handler that re-reads under the
		// lock observes the respawned term-2; a regressed one that read term-1 before
		// the lock would kill the stale, vanished terminal and orphan the fresh one.
		hold := agentLock("a1")
		hold.Lock()

		log := &eventLog{}
		var reply daemon.Reply
		done := make(chan struct{})
		go func() {
			reply = handleAgentKill(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), log.append))
			close(done)
		}()

		// Give a regressed pre-lock read time to observe term-1, then land the respawn's
		// effect under the lock: a fresh terminal term-2, row still active. (With the
		// fix agent-kill reads nothing until the lock, so the delay never makes it flaky.)
		time.Sleep(30 * time.Millisecond)
		if err := setAgentTerminalHandle(ctx, db, "a1", "term-2"); err != nil {
			t.Fatal(err)
		}
		hold.Unlock()

		<-done
		if !reply.OK {
			t.Fatalf("agent-kill failed: %s", reply.Error)
		}
		assertAgentStatus(ctx, t, db, "a1", StatusExited)
		mu.Lock()
		gotKilled := append([]string(nil), killed...)
		mu.Unlock()
		if len(gotKilled) != 1 || gotKilled[0] != "term-2" {
			t.Fatalf("agent-kill targeted %v, want exactly [term-2] (the fresh terminal, not the stale handle)", gotKilled)
		}
		if log.count(EventExited) != 1 {
			t.Fatalf("agent-kill must append exactly one EventExited; events=%v", log.types())
		}
	})
}

// TestTailerResetsRestartBudgetOnLiveHealthOnly drives the real tailerManager.start
// path and proves the budget reset keys off genuinely-new work, not replayed history:
// the tailer first replays a healthy (idle) line from the transcript — which must NOT
// reset the budget, since it is stale pre-crash state every (re)start re-derives — then
// observes a healthy state reached by a line appended while live, which is the real
// recovery signal and the only thing that may reset. A never-restarted agent
// (RestartCount == 0) takes no reset write even on live healthy activity.
func TestTailerResetsRestartBudgetOnLiveHealthOnly(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	for _, tc := range []struct {
		name      string
		startWith int
		wantReset bool
	}{
		{"restarted agent resets only on live recovery", 2, true},
		{"never-restarted agent never resets", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("CLAUDE_CONFIG_DIR", "")
			dir := filepath.Join(home, ".claude", "projects", "p")
			if err := os.MkdirAll(dir, 0o750); err != nil {
				t.Fatal(err)
			}
			session := "sess-reset-" + tc.name
			path := filepath.Join(dir, session+".jsonl")
			// Replayed history: a healthy end_turn line the tailer rebuilds status from
			// on start. It is stale pre-crash work and must NOT reset the budget.
			if err := os.WriteFile(path, []byte(lineText+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tailers = newTailerManager(ctx)
			db := newTestDB(ctx, t)
			seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
			mustInsertAgent(ctx, t, db, agentRow{
				ID: "a1", SprintID: "w1-s", Backend: "supervisetest", TerminalHandle: "term-1",
				SessionID: session, Scope: "/s", SubjectID: "subj-a1", Status: StatusActive, State: StateUnknown,
				RestartCount: tc.startWith, LastRestartAt: "t1", CreatedAt: "t0",
			})

			log := &eventLog{}
			tailers.start(db, log.append, agentRow{
				ID: "a1", SessionID: session, Scope: "/s", SubjectID: "subj-a1", RestartCount: tc.startWith,
			})

			// Replay caught up once the first status frame lands; the tailer is now live.
			// The replayed healthy state must have left the budget untouched.
			waitUntil(t, "replay status", func() bool { return log.count(EventStatus) > 0 })
			if got, err := getAgent(ctx, db, "a1"); err != nil {
				t.Fatal(err)
			} else if got.RestartCount != tc.startWith {
				t.Fatalf("replayed history must not reset the budget; count=%d want %d", got.RestartCount, tc.startWith)
			}

			// Genuinely-new live activity reaching a healthy (working) state: the real
			// recovery signal, the only thing that may reset the budget.
			appendLine(t, path, lineBash+"\n")
			waitUntil(t, "live status", func() bool { return log.count(EventStatus) > 1 })

			got, err := getAgent(ctx, db, "a1")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantReset {
				if got.RestartCount != 0 || got.LastRestartAt != "" {
					t.Fatalf("live recovery must reset to 0/'', got count=%d at=%q", got.RestartCount, got.LastRestartAt)
				}
			} else if got.RestartCount != tc.startWith {
				t.Fatalf("never-restarted agent must stay at %d, got %d", tc.startWith, got.RestartCount)
			}
		})
	}
}

// TestSupervisorFirstRestartResetsBudgetViaTailer drives a FIRST restart through the
// real reconcileVanished -> respawnAgent -> tailers.start wiring: a StatusActive agent
// whose terminal has vanished starts at RestartCount 0, so the tick re-spawns it
// (0->1) and starts the tailer with the incremented snapshot. The resumed agent's
// tailer first replays the pre-crash transcript — which must NOT reset the budget —
// then observes a healthy state reached by genuinely-new activity, so the reset hook
// (gated on the snapshot's RestartCount > 0) fires only on that real recovery and
// clears the budget to 0. It is the live-wiring complement to
// TestTailerResetsRestartBudgetOnLiveHealthOnly: here the count has to reach the tailer
// snapshot through the supervisor's persist-then-respawn order, so a snapshot that
// lagged by one would leave the count stuck at 1 and fail this test.
func TestSupervisorFirstRestartResetsBudgetViaTailer(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	dir := filepath.Join(home, ".claude", "projects", "p")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	session := "sess-live-restart"
	path := filepath.Join(dir, session+".jsonl")
	// Pre-crash history ending healthy: the resumed agent's tailer replays this idle
	// line on start, but replay alone must not reset the budget.
	if err := os.WriteFile(path, []byte(lineText+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)
	db := newTestDB(ctx, t)

	var mu sync.Mutex
	spawns := 0
	nextTerm := "term-2"
	backend.Register(superviseBackend{
		agents:    nil, // term-1 vanished
		enumerate: true,
		mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
	})
	seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "supervisetest", TerminalHandle: "term-1",
		SessionID: session, Scope: "/s", SubjectID: "subj-a1", Status: StatusActive, State: StateWorking,
		RestartCount: 0, CreatedAt: "t0",
	})

	log := &eventLog{}
	if err := newSupervisor().tick(ctx, db, log.append); err != nil {
		t.Fatal(err)
	}

	// The tick re-spawned into a fresh terminal and counted the attempt.
	assertTerminalHandle(ctx, t, db, "a1", "term-2")
	if log.count(EventRestarted) != 1 {
		t.Fatalf("EventRestarted count = %d, want 1; events=%v", log.count(EventRestarted), log.types())
	}

	// Replay caught up once the first status frame lands; the tailer is now live. The
	// replayed pre-crash idle state must have left the just-counted attempt intact.
	waitUntil(t, "replay status", func() bool { return log.count(EventStatus) > 0 })
	assertRestartCount(ctx, t, db, "a1", 1)

	// The resumed agent now does genuinely-new work reaching a healthy state: the live
	// recovery signal that clears the budget.
	appendLine(t, path, lineBash+"\n")
	waitUntil(t, "budget reset", func() bool {
		a, err := getAgent(ctx, db, "a1")
		return err == nil && a.RestartCount == 0
	})

	got, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if got.RestartCount != 0 || got.LastRestartAt != "" {
		t.Fatalf("first restart reaching live healthy must reset budget to 0/'', got count=%d at=%q", got.RestartCount, got.LastRestartAt)
	}
	assertAgentStatus(ctx, t, db, "a1", StatusActive)
}

// TestSupervisorCrashLoopAbandonsAtBudget proves the headline guarantee: an agent that
// crash-loops — its terminal vanishes each tick and the resumed process does NO new
// work — accrues its restart budget to abandonment instead of resetting it. Each
// respawn's tailer only replays the same pre-crash healthy line (no live activity), so
// the budget can never reset; after restartBudget respawns the next tick abandons and
// terminally exits the agent. Before the live-gated reset fix the replayed healthy
// state reset the budget to 0 every tick, so the loop ran forever.
func TestSupervisorCrashLoopAbandonsAtBudget(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	dir := filepath.Join(home, ".claude", "projects", "p")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	session := "sess-crashloop"
	// Pre-crash history ending healthy: every respawn's tailer replays this same idle
	// line, but the resumed process does no NEW work, so the budget must accrue.
	if err := os.WriteFile(filepath.Join(dir, session+".jsonl"), []byte(lineText+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTailerManager(ctx)
	db := newTestDB(ctx, t)

	var mu sync.Mutex
	spawns := 0
	nextTerm := "term-2"
	backend.Register(superviseBackend{
		agents:    nil, // always vanished
		enumerate: true,
		mu:        &mu, spawns: &spawns, nextTerm: &nextTerm,
	})
	seedWorkstream(ctx, t, db, "w1", "p1", "supervisetest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "supervisetest", TerminalHandle: "term-1",
		SessionID: session, Scope: "/s", SubjectID: "subj-a1", Status: StatusActive, State: StateWorking,
		RestartCount: 0, CreatedAt: "t0",
	})

	log := &eventLog{}
	sup := newSupervisor()
	for i := 0; i < restartBudget+1; i++ {
		if err := sup.tick(ctx, db, log.append); err != nil {
			t.Fatal(err)
		}
		// Let the respawn's tailer replay to its healthy state before the next tick. A
		// replay-driven reset (the bug) would have zeroed the count here, so the loop
		// would never reach budget and this test would hang/fail; the live gate keeps
		// the replay from resetting, so the count accrues across ticks.
		if i < restartBudget {
			waitUntil(t, "respawn replay", func() bool { return log.count(EventStatus) > i })
		}
	}

	assertAgentStatus(ctx, t, db, "a1", StatusExited)
	if log.count(EventRestarted) != restartBudget {
		t.Fatalf("EventRestarted count = %d, want %d; events=%v", log.count(EventRestarted), restartBudget, log.types())
	}
	if log.count(EventAbandoned) != 1 || log.count(EventExited) != 1 {
		t.Fatalf("crash loop must abandon then exit exactly once; events=%v", log.types())
	}
}

func waitUntil(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !pred() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
