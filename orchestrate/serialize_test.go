package orchestrate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ptyhost"
)

// serializeTestBackend is a registered test backend for serialize/restore: it
// advertises CanCapture (so resolveScreen takes the native path and respawnAgent's
// wrapForCapture short-circuits — no real claude needed) and CanSendText (so the
// native path also resolves a Sender). Capture returns a canned, session-keyed screen,
// and Spawn hands back a fresh, session-keyed terminal id so a restored agent's new
// backend_terminal_handle is observable. Its name is outside backend.Precedence.
type serializeTestBackend struct {
	killed *[]string // when set, records each Kill target's handle id
}

func (serializeTestBackend) Name() backend.Name                { return "sertest" }
func (serializeTestBackend) Available() bool                   { return true }
func (serializeTestBackend) EnsureReady(context.Context) error { return nil }
func (serializeTestBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}

func (serializeTestBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}

func (serializeTestBackend) Spawn(_ context.Context, spec backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{Backend: "sertest", ID: "restored-" + spec.SessionID, SessionID: spec.SessionID}, nil
}

func (serializeTestBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}

func (b serializeTestBackend) Kill(_ context.Context, agent backend.AgentHandle) error {
	if b.killed != nil {
		*b.killed = append(*b.killed, agent.ID)
	}
	return nil
}

func (serializeTestBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error {
	return nil
}

func (serializeTestBackend) Capture(_ context.Context, agent backend.AgentHandle) (string, error) {
	return "screen:" + agent.SessionID, nil
}
func (serializeTestBackend) SendText(context.Context, backend.AgentHandle, string) error { return nil }
func (serializeTestBackend) Caps() backend.Caps {
	return backend.Capabilities(backend.CanCapture, backend.CanSendText)
}

// writeBundle marshals a bundle to a JSON file in dir and returns its path.
func writeBundle(t *testing.T, dir string, bundle serializeBundle) string {
	t.Helper()
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		t.Fatalf("marshal bundle: %v", err)
	}
	path := filepath.Join(dir, "bundle.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return path
}

func TestSerializeRestoreRoundTrip(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	backend.Register(serializeTestBackend{})
	seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")

	sids := map[string]string{"a1": "sess-1", "a2": "sess-2"}
	for id, sid := range sids {
		mustInsertAgent(ctx, t, db, agentRow{
			ID: id, SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-" + id,
			SessionID: sid, Scope: "/s", Name: id, Prompt: "do " + id, SubjectID: "subj-" + id,
			Status: StatusActive, State: StateWorking, CreatedAt: "t0",
		})
	}

	out := filepath.Join(t.TempDir(), "bundle.json")
	serLog := &eventLog{}
	reply := runTyped(handleSerialize, opCtx(db, mustJSON(t, map[string]string{"out": out}), serLog.append))
	if !reply.OK {
		t.Fatalf("serialize failed: %s", reply.Error)
	}
	var serRes struct {
		Path  string `json:"path"`
		Count int    `json:"count"`
	}
	if err := json.Unmarshal(reply.Body, &serRes); err != nil {
		t.Fatal(err)
	}
	if serRes.Path != out || serRes.Count != len(sids) {
		t.Fatalf("serialize reply = %+v, want path %q count %d", serRes, out, len(sids))
	}
	if serLog.count(EventSerialized) != len(sids) {
		t.Fatalf("EventSerialized count = %d, want %d; events=%v", serLog.count(EventSerialized), len(sids), serLog.types())
	}

	bundle, err := readBundle(out)
	if err != nil {
		t.Fatalf("readBundle: %v", err)
	}
	if bundle.Version != serializeBundleVersion {
		t.Fatalf("bundle version = %d, want %d", bundle.Version, serializeBundleVersion)
	}
	if len(bundle.Agents) != len(sids) {
		t.Fatalf("bundle has %d agents, want %d", len(bundle.Agents), len(sids))
	}
	for _, sa := range bundle.Agents {
		wantSID, ok := sids[sa.ID]
		if !ok {
			t.Fatalf("bundle has unexpected agent %q", sa.ID)
		}
		if sa.SessionID != wantSID {
			t.Fatalf("agent %q session id = %q, want %q", sa.ID, sa.SessionID, wantSID)
		}
		if sa.Screen != "screen:"+wantSID {
			t.Fatalf("agent %q screen = %q, want %q", sa.ID, sa.Screen, "screen:"+wantSID)
		}
	}

	// Wipe: delete the agent rows (the recreate-from-scratch path), leaving the
	// workstream and sprint intact so respawnAgent can resolve them.
	if _, err := db.ExecContext(ctx, `DELETE FROM agents`); err != nil {
		t.Fatal(err)
	}
	if active, err := listActiveAgents(ctx, db); err != nil || len(active) != 0 {
		t.Fatalf("after wipe listActiveAgents = %v (err %v), want empty", active, err)
	}

	resLog := &eventLog{}
	rReply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": out}), resLog.append))
	if !rReply.OK {
		t.Fatalf("restore failed: %s", rReply.Error)
	}
	var rRes struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rReply.Body, &rRes); err != nil {
		t.Fatal(err)
	}
	if rRes.Count != len(sids) {
		t.Fatalf("restore count = %d, want %d", rRes.Count, len(sids))
	}
	for id, sid := range sids {
		ag, err := getAgent(ctx, db, id)
		if err != nil {
			t.Fatalf("getAgent %q after restore: %v", id, err)
		}
		if ag.SessionID != sid {
			t.Fatalf("restored agent %q session id = %q, want %q", id, ag.SessionID, sid)
		}
		if ag.Status != StatusActive {
			t.Fatalf("restored agent %q status = %q, want active", id, ag.Status)
		}
		if ag.TerminalHandle != "restored-"+sid {
			t.Fatalf("restored agent %q terminal = %q, want %q", id, ag.TerminalHandle, "restored-"+sid)
		}
	}
	if resLog.count(EventRestored) != len(sids) {
		t.Fatalf("EventRestored count = %d, want %d; events=%v", resLog.count(EventRestored), len(sids), resLog.types())
	}
}

// TestRestoreIntoWipedDB exercises the documented recreate-from-scratch path: a real
// ~/.cc-orchestrate wipe drops the single SQLite DB with every table, so restore must
// recreate the repo → workstream → sprint hierarchy from the bundle before respawnAgent
// can resolve the parents it resumes an agent into. Without the hierarchy in the bundle
// restore aborts at "sprint not found"; with it, the whole tree and the agent come back.
func TestRestoreIntoWipedDB(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	backend.Register(serializeTestBackend{})
	if err := insertRepo(ctx, db, repoRow{
		ID: "p1", Name: "alpha", Backend: "sertest", Cwd: "/tmp/p1", Status: StatusActive, CreatedAt: "t0",
	}); err != nil {
		t.Fatal(err)
	}
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

	// A full wipe: drop every consumer table, the way a ~/.cc-orchestrate wipe would.
	for _, table := range []string{"agents", "sprints", "workstreams", "repos", "config"} {
		if _, err := db.ExecContext(ctx, `DELETE FROM `+table); err != nil { //nolint:gosec // G202: table names are hardcoded test fixtures, not user input
			t.Fatalf("wipe %s: %v", table, err)
		}
	}

	log := &eventLog{}
	reply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": out}), log.append))
	if !reply.OK {
		t.Fatalf("restore into a wiped DB failed: %s", reply.Error)
	}

	// The hierarchy respawnAgent resolves is recreated from the bundle.
	if _, err := getRepo(ctx, db, "p1"); err != nil {
		t.Fatalf("repo not recreated: %v", err)
	}
	if _, err := getWorkstream(ctx, db, "w1", ""); err != nil {
		t.Fatalf("workstream not recreated: %v", err)
	}
	if _, err := getSprint(ctx, db, "w1-s", ""); err != nil {
		t.Fatalf("sprint not recreated: %v", err)
	}
	ag, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatalf("agent not recreated: %v", err)
	}
	if ag.Status != StatusActive {
		t.Fatalf("restored agent status = %q, want active", ag.Status)
	}
	if ag.TerminalHandle != "restored-sess-1" {
		t.Fatalf("restored agent terminal = %q, want %q", ag.TerminalHandle, "restored-sess-1")
	}
	if log.count(EventRestored) != 1 {
		t.Fatalf("EventRestored count = %d, want 1; events=%v", log.count(EventRestored), log.types())
	}
}

func TestSerializeWriteFailureAppendsNoEvent(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	backend.Register(serializeTestBackend{})
	seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-a1",
		SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
		Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})

	// Point --out at an existing directory so WriteFile fails after the screen is
	// captured: EventSerialized claims a bundle was written, so a failed write must
	// append none of them.
	out := filepath.Join(t.TempDir(), "is-a-dir")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}

	log := &eventLog{}
	reply := runTyped(handleSerialize, opCtx(db, mustJSON(t, map[string]string{"out": out}), log.append))
	if reply.OK {
		t.Fatalf("serialize to a directory path returned OK, want failure")
	}
	if log.count(EventSerialized) != 0 {
		t.Fatalf("a failed serialize must append no EventSerialized; events=%v", log.types())
	}
}

func TestRestoreStrictParseFailsLoud(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	for _, tc := range []struct {
		name string
		body string
	}{
		{
			name: "unknown field",
			body: `{"version":1,"created_at":"2026-06-19T00:00:00Z","agents":[` +
				`{"id":"a1","sprint_id":"w1-s","backend":"sertest","terminal_handle":"t",` +
				`"session_id":"sess-1","scope":"/s","name":"","prompt":"","subject_id":"subj-a1",` +
				`"ccnotes_task":"","restart_count":0,"last_restart_at":"","screen":"x","bogus":true}]}`,
		},
		{
			name: "malformed json",
			body: `{"version":1,"agents":[ not json`,
		},
		{
			name: "duplicate session id",
			body: `{"version":1,"created_at":"2026-06-19T00:00:00Z","agents":[` +
				`{"id":"a1","sprint_id":"w1-s","backend":"sertest","terminal_handle":"t1",` +
				`"session_id":"sess-1","scope":"/s","name":"","prompt":"","subject_id":"subj-a1",` +
				`"ccnotes_task":"","restart_count":0,"last_restart_at":"","screen":"x"},` +
				`{"id":"a2","sprint_id":"w1-s","backend":"sertest","terminal_handle":"t2",` +
				`"session_id":"sess-1","scope":"/s","name":"","prompt":"","subject_id":"subj-a2",` +
				`"ccnotes_task":"","restart_count":0,"last_restart_at":"","screen":"y"}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tailers = newTestTailerManager(ctx)
			db := newTestDB(ctx, t)
			backend.Register(serializeTestBackend{})
			seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")

			path := filepath.Join(t.TempDir(), "bundle.json")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}

			log := &eventLog{}
			reply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": path}), log.append))
			if reply.OK {
				t.Fatalf("restore of a %s bundle returned OK, want failure", tc.name)
			}
			if active, err := listActiveAgents(ctx, db); err != nil || len(active) != 0 {
				t.Fatalf("a failed restore must insert nothing; listActiveAgents = %v (err %v)", active, err)
			}
			if log.count(EventRestored) != 0 {
				t.Fatalf("a failed restore must append no EventRestored; events=%v", log.types())
			}
		})
	}
}

func TestRestorePresentRowRewritesHandle(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	var killed []string
	backend.Register(serializeTestBackend{killed: &killed})
	seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-orig",
		SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
		Status: StatusActive, State: StateWorking, CreatedAt: "t0",
	})

	path := writeBundle(t, t.TempDir(), serializeBundle{
		Version:   serializeBundleVersion,
		CreatedAt: "2026-06-19T00:00:00Z",
		Agents: []serializedAgent{{
			ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-orig",
			SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
			Screen: "screen:sess-1",
		}},
	})

	log := &eventLog{}
	reply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": path}), log.append))
	if !reply.OK {
		t.Fatalf("restore failed: %s", reply.Error)
	}
	active, err := listActiveAgents(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 {
		t.Fatalf("present-row restore must not duplicate the row; listActiveAgents = %d rows", len(active))
	}
	ag, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ag.TerminalHandle != "restored-sess-1" {
		t.Fatalf("present-row restore terminal = %q, want %q", ag.TerminalHandle, "restored-sess-1")
	}
	// The prior live terminal must be killed before the re-attach, never leaked.
	if len(killed) != 1 || killed[0] != "term-orig" {
		t.Fatalf("present-active restore killed %v, want exactly [term-orig] (the leaked predecessor)", killed)
	}
	if log.count(EventRestored) != 1 {
		t.Fatalf("EventRestored count = %d, want 1; events=%v", log.count(EventRestored), log.types())
	}
}

// TestRestorePresentExitedRowReactivates proves restore revives a present exited row
// (the natural post-abandon state) rather than leaving it exited while it owns a fresh
// live terminal: the row must come back StatusActive with the rewritten handle so the
// supervisor and agent-kill manage it again. A present exited row's prior terminal is
// already gone, so restore must not try to kill it.
func TestRestorePresentExitedRowReactivates(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
	t.Setenv("HOME", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tailers = newTestTailerManager(ctx)
	db := newTestDB(ctx, t)
	var killed []string
	backend.Register(serializeTestBackend{killed: &killed})
	seedWorkstream(ctx, t, db, "w1", "p1", "sertest", "ws-1")
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-dead",
		SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
		Status: StatusExited, State: StateIdle, CreatedAt: "t0",
	})

	path := writeBundle(t, t.TempDir(), serializeBundle{
		Version:   serializeBundleVersion,
		CreatedAt: "2026-06-19T00:00:00Z",
		Agents: []serializedAgent{{
			ID: "a1", SprintID: "w1-s", Backend: "sertest", TerminalHandle: "term-dead",
			SessionID: "sess-1", Scope: "/s", Name: "a1", Prompt: "do a1", SubjectID: "subj-a1",
			Screen: "screen:sess-1",
		}},
	})

	log := &eventLog{}
	reply := runTyped(handleRestore, opCtx(db, mustJSON(t, map[string]string{"path": path}), log.append))
	if !reply.OK {
		t.Fatalf("restore failed: %s", reply.Error)
	}
	ag, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if ag.Status != StatusActive {
		t.Fatalf("restored exited agent status = %q, want active (else an unkillable orphan)", ag.Status)
	}
	if ag.TerminalHandle != "restored-sess-1" {
		t.Fatalf("restored exited agent terminal = %q, want %q", ag.TerminalHandle, "restored-sess-1")
	}
	if len(killed) != 0 {
		t.Fatalf("a present exited row's terminal is already gone; restore must not kill, killed=%v", killed)
	}
	if log.count(EventRestored) != 1 {
		t.Fatalf("EventRestored count = %d, want 1; events=%v", log.count(EventRestored), log.types())
	}
}

// TestCaptureAfterRespawnDialsCurrentIncarnation pins captureScreenText's under-lock
// re-read: the caller's row snapshot can predate a concurrent respawn that rotated
// the incarnation nonce, and resolving the screen from that snapshot would dial the
// OLD incarnation's pty socket. Two live pty-hosts serve the old and new
// incarnations' sockets with distinct screens; a capture handed the stale snapshot
// must return the NEW incarnation's screen.
func TestCaptureAfterRespawnDialsCurrentIncarnation(t *testing.T) {
	shortHome(t) // the pty socket paths must stay inside the OS sun_path limit
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db := newTestDB(ctx, t)
	// A non-capturing backend routes resolveScreen through the pty-host socket, the
	// path derived from the row's spawn nonce.
	backend.Register(nonCapturingSpawnBackend{spawnBackend{spec: &backend.SpawnSpec{}}})
	if err := insertRepo(ctx, db, repoRow{ID: "p1", Name: "alpha", Backend: "spawntest", Cwd: "/s", Status: StatusActive, CreatedAt: "t0"}); err != nil {
		t.Fatal(err)
	}
	seedWorkstream(ctx, t, db, "w1", "p1", "spawntest", "ws-1")

	if err := os.MkdirAll(filepath.Dir(ptySocketPath("sc", "n-old")), 0o700); err != nil {
		t.Fatal(err)
	}
	startHost := func(nonce, text string) {
		t.Helper()
		sock := ptySocketPath("sc", nonce)
		hctx, hcancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- ptyhost.Run(hctx, ptyhost.Options{Socket: sock, Argv: []string{"sh", "-c", "printf " + text + "; while :; do sleep 0.1; done"}})
		}()
		t.Cleanup(func() {
			hcancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		})
		cl := ptyhost.Dial(sock)
		waitUntil(t, text+" on the "+nonce+" screen", func() bool {
			s, err := cl.Capture(ctx)
			return err == nil && strings.Contains(s, text)
		})
	}
	startHost("n-old", "OLD-INCARNATION")
	startHost("n-new", "NEW-INCARNATION")

	// The row's CURRENT incarnation is n-new; the caller's snapshot is the stale n-old.
	mustInsertAgent(ctx, t, db, agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "spawntest", TerminalHandle: "term-2",
		SessionID: "sc", Scope: "/s", SubjectID: "subj-a1", Status: StatusActive, State: StateWorking,
		CreatedAt: "t0", SpawnNonce: "n-new",
	})
	stale := agentRow{
		ID: "a1", SprintID: "w1-s", Backend: "spawntest", TerminalHandle: "term-1",
		SessionID: "sc", Scope: "/s", SubjectID: "subj-a1", Status: StatusActive, State: StateWorking,
		CreatedAt: "t0", SpawnNonce: "n-old",
	}
	text, err := captureScreenText(ctx, db, stale)
	if err != nil {
		t.Fatalf("captureScreenText: %v", err)
	}
	if !strings.Contains(text, "NEW-INCARNATION") || strings.Contains(text, "OLD-INCARNATION") {
		t.Fatalf("capture = %q, want the NEW incarnation's screen only", text)
	}
}
