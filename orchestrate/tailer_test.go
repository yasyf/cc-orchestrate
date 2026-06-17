package orchestrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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
		done <- runTailer(ctx, session, "scope", interval, func(s Status) error {
			got <- s
			return nil
		})
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
