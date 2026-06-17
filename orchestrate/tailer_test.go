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
	old := pollInterval
	pollInterval = 5 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	home := t.TempDir()
	t.Setenv("HOME", home)
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
		done <- runTailer(ctx, session, "scope", func(s Status) error {
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

	if got, ok := findTranscript(session); !ok || got != newPath {
		t.Fatalf("findTranscript() = %q, %v; want %q, true", got, ok, newPath)
	}
}

func TestFindTranscriptMissing(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if got, ok := findTranscript("nope"); ok {
		t.Fatalf("findTranscript() = %q, true; want \"\", false", got)
	}
}
