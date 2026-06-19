package orchestrate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/event"
)

func appendLine(t *testing.T, path, data string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(data); err != nil {
		t.Fatal(err)
	}
}

func recvStatus(t *testing.T, ch <-chan Status) Status {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a status")
		return Status{}
	}
}

func expectNoStatus(t *testing.T, ch <-chan Status) {
	t.Helper()
	select {
	case s := <-ch:
		t.Fatalf("unexpected status emitted: %+v", s)
	case <-time.After(60 * time.Millisecond):
	}
}

// TestRunTailerStreamsStatuses exercises the appears-later poll, offset tailing
// across incremental appends, partial-line buffering, and change-deduped emission.
func TestRunTailerStreamsStatuses(t *testing.T) {
	interval := 5 * time.Millisecond

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	dir := filepath.Join(home, ".claude", "projects", "test-proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	session := "sess-123"
	path := filepath.Join(dir, session+".jsonl")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Status, 16)
	done := make(chan error, 1)
	go func() {
		done <- runTailer(ctx, session, "scope", interval, func(s Status, _ bool) error {
			got <- s
			return nil
		}, func(string) error { return nil })
	}()

	// Transcript appears after the tailer starts: the wait loop must poll it in.
	appendLine(t, path, lineBash+"\n")
	if s := recvStatus(t, got); s != (Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12}) {
		t.Fatalf("first status = %+v", s)
	}

	// Offset tailing: a complete tool_result clears pending; status is unchanged
	// (still mid-turn working), so nothing is emitted. The trailing Edit line is
	// written without its newline and must stay buffered, emitting nothing.
	appendLine(t, path, lineResultBash+"\n"+lineEdit)
	expectNoStatus(t, got)

	// Completing the buffered line surfaces the Edit.
	appendLine(t, path, "\n")
	if s := recvStatus(t, got); s != (Status{State: StateWorking, Tool: "Edit", Target: "/tmp/x.go", Tokens: 19}) {
		t.Fatalf("second status = %+v", s)
	}

	// Clearing the edit then an end_turn text turn drops to idle.
	appendLine(t, path, lineResultEdit+"\n"+lineText+"\n")
	if s := recvStatus(t, got); s != (Status{State: StateIdle, Tool: "Edit", Target: "/tmp/x.go", LastText: "All done.", Tokens: 49}) {
		t.Fatalf("third status = %+v", s)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runTailer returned %v, want nil on cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runTailer did not return after cancel")
	}
}

// TestRunTailerEmitsInboundLiveOnly proves the tailer emits an inbound audit turn
// only for turns appended while live, never for the historical turns it replays on
// start — otherwise every daemon restart would duplicate audit frames.
func TestRunTailerEmitsInboundLiveOnly(t *testing.T) {
	interval := 5 * time.Millisecond

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	dir := filepath.Join(home, ".claude", "projects", "test-proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	session := "sess-inbound"
	path := filepath.Join(dir, session+".jsonl")

	// Pre-existing transcript: a prior inbound turn plus an assistant turn. Both
	// are replayed on start; the inbound turn must not be re-emitted, while the
	// assistant turn's status emission signals that replay caught up and the tailer
	// is live.
	if err := os.WriteFile(path, []byte(lineUserPrompt+"\n"+lineText+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status := make(chan Status, 8)
	inbound := make(chan string, 8)
	done := make(chan error, 1)
	go func() {
		done <- runTailer(ctx, session, "scope", interval,
			func(s Status, _ bool) error { status <- s; return nil },
			func(text string) error { inbound <- text; return nil })
	}()

	// The replayed assistant status confirms the first read caught up; the tailer
	// is now live.
	if s := recvStatus(t, status); s.State != StateIdle {
		t.Fatalf("replay status = %+v, want idle", s)
	}

	// A user turn appended while live surfaces as an inbound audit frame.
	appendLine(t, path, lineUserPrompt2+"\n")
	select {
	case got := <-inbound:
		if got != "second prompt" {
			t.Fatalf("inbound = %q, want %q", got, "second prompt")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a live inbound turn")
	}

	// The replayed pre-existing turn must not also surface.
	select {
	case extra := <-inbound:
		t.Fatalf("replayed inbound turn was re-emitted: %q", extra)
	case <-time.After(60 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runTailer returned %v, want nil on cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runTailer did not return after cancel")
	}
}

// TestTailerManagerEmitsInboundEvent exercises tailerManager.start's inbound
// branch end to end: a live user turn becomes an OriginSystem EventInbound, while
// a turn matching the agent's spawn prompt is deduped (already in EventSpawned).
func TestTailerManagerEmitsInboundEvent(t *testing.T) {
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	dir := filepath.Join(home, ".claude", "projects", "p")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	session := "sess-mgr"
	path := filepath.Join(dir, session+".jsonl")
	// A replayed assistant turn: its status emission signals the tailer is live.
	if err := os.WriteFile(path, []byte(lineText+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := newTailerManager(ctx)
	db := newTestDB(t)

	var mu sync.Mutex
	var events []*event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		return 1, nil
	}
	countType := func(typ string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, e := range events {
			if e.Type == typ {
				n++
			}
		}
		return n
	}
	firstOf := func(typ string) *event.Event {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.Type == typ {
				return e
			}
		}
		return nil
	}
	waitUntil := func(what string, pred func() bool) {
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

	m.start(db, appendFn, agentRow{ID: "a1", SessionID: session, Scope: "/s", SubjectID: "subj-1", Prompt: "the spawn prompt"})

	// Replay caught up once a status frame lands; the tailer is now live.
	waitUntil("replay status", func() bool { return countType(EventStatus) > 0 })

	// A live turn matching the spawn prompt is deduped; a different one is emitted.
	appendLine(t, path, `{"type":"user","isSidechain":false,"message":{"role":"user","content":"the spawn prompt"}}`+"\n")
	appendLine(t, path, lineUserPrompt+"\n")
	waitUntil("inbound event", func() bool { return countType(EventInbound) > 0 })

	if n := countType(EventInbound); n != 1 {
		t.Fatalf("EventInbound count = %d, want 1 (spawn prompt must be deduped)", n)
	}
	in := firstOf(EventInbound)
	if in.Origin != event.OriginSystem || in.SubjectID != "subj-1" {
		t.Fatalf("inbound event = origin %q subject %q, want system/subj-1", in.Origin, in.SubjectID)
	}
	var pl struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(in.Payload, &pl); err != nil {
		t.Fatalf("inbound payload: %v", err)
	}
	if pl.Type != EventInbound || pl.Text != "a plain human prompt" {
		t.Fatalf("inbound payload = %+v, want type=%s text=%q", pl, EventInbound, "a plain human prompt")
	}
}

func TestFindTranscriptPicksNewest(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	session := "dup-sess"
	older := filepath.Join(home, ".claude", "projects", "alpha")
	newer := filepath.Join(home, ".claude", "projects", "beta")
	for _, d := range []string{older, newer} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	oldPath := filepath.Join(older, session+".jsonl")
	newPath := filepath.Join(newer, session+".jsonl")
	for _, p := range []string{oldPath, newPath} {
		if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stale := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldPath, stale, stale); err != nil {
		t.Fatal(err)
	}

	if got, ok, err := findTranscript(session); err != nil || !ok || got != newPath {
		t.Fatalf("findTranscript() = %q, %v, %v; want %q, true, nil", got, ok, err, newPath)
	}
}

func TestFindTranscriptMissing(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	if got, ok, err := findTranscript("nope"); ok || err != nil {
		t.Fatalf("findTranscript() = %q, %v, %v; want \"\", false, nil", got, ok, err)
	}
}

// TestFindTranscriptHonorsConfigDir proves the tailer resolves transcripts under
// $CLAUDE_CONFIG_DIR/projects, taking precedence over ~/.claude — claude writes a
// relocated child's transcript there, so a tailer reading only ~/.claude would
// never find it.
func TestFindTranscriptHonorsConfigDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)

	session := "cfg-sess"
	decoy := filepath.Join(home, ".claude", "projects", "decoy")
	real := filepath.Join(cfg, "projects", "real")
	for _, d := range []string{decoy, real} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A ~/.claude match must be ignored entirely when CLAUDE_CONFIG_DIR is set.
	if err := os.WriteFile(filepath.Join(decoy, session+".jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(real, session+".jsonl")
	if err := os.WriteFile(want, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got, ok, err := findTranscript(session); err != nil || !ok || got != want {
		t.Fatalf("findTranscript() = %q, %v, %v; want %q, true, nil", got, ok, err, want)
	}
}

// TestFindTranscriptHomeError proves an unresolvable home directory surfaces as an
// error instead of an empty path that would make the tailer poll forever.
func TestFindTranscriptHomeError(t *testing.T) {
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	t.Setenv("HOME", "")
	if _, ok, err := findTranscript("any"); err == nil || ok {
		t.Fatalf("findTranscript() = ok %v, err %v; want false and a wrapped home error", ok, err)
	}
}

// TestTailerManagerFinish proves a self-exited tailer drops its own entry but never
// a successor that already replaced it under the same agent id.
func TestTailerManagerFinish(t *testing.T) {
	m := newTailerManager(context.Background())

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := &tailerCancel{cancel: cancel}
	m.cancels["a1"] = tc
	m.finish("a1", tc)
	if _, ok := m.cancels["a1"]; ok {
		t.Fatal("finish did not drop the finished tailer's entry")
	}

	// A finishing predecessor must not clear the successor that took the same id.
	_, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	tc2 := &tailerCancel{cancel: cancel2}
	m.cancels["a2"] = tc2
	m.finish("a2", &tailerCancel{cancel: func() {}})
	if m.cancels["a2"] != tc2 {
		t.Fatal("finish cleared a successor's entry")
	}
}
