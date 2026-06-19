package ptyhost

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitFor polls cond up to ~3s, returning whether it became true.
func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// TestHostRoundTrip drives the full loop against a real PTY with `cat` as the child:
// inject "hello"+Enter, then assert CAPTURE shows the echoed line. No claude needed.
func TestHostRoundTrip(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "pty.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{Socket: sock, Argv: []string{"sh", "-c", "cat"}}) }()

	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}

	cl := Dial(sock)
	if err := cl.SendKeys(ctx, "hello", "Enter"); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}

	var screen string
	if !waitFor(func() bool {
		s, err := cl.Capture(ctx)
		if err != nil {
			return false
		}
		screen = s
		return strings.Contains(s, "hello")
	}) {
		t.Fatalf("screen never showed the echoed keys: %q", screen)
	}

	cancel() // terminate the child
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestHostDrainsQueryReplies guards the deadlock where the vt emulator answers a
// terminal query (here a cursor-position report, ESC[6n) by writing into an unbuffered
// pipe: if the host does not drain that pipe back to the child, Feed blocks forever
// holding the grid lock and the screen never advances. The child emits the query then
// visible text; the text must appear, proving the reply was drained and parsing continued.
func TestHostDrainsQueryReplies(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "pty.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{Socket: sock, Argv: []string{"sh", "-c", `printf '\033[6n'; printf 'AFTERQUERY'; sleep 5`}})
	}()

	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}
	cl := Dial(sock)
	var screen string
	if !waitFor(func() bool {
		s, err := cl.Capture(ctx)
		if err != nil {
			return false
		}
		screen = s
		return strings.Contains(s, "AFTERQUERY")
	}) {
		t.Fatalf("screen never advanced past the query (drain deadlock?): %q", screen)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestEncodeKeys covers the named-key vs literal-text split.
func TestEncodeKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want []byte
	}{
		{"named enter", []string{"Enter"}, []byte{'\r'}},
		{"down then enter", []string{"Down", "Enter"}, []byte{0x1b, '[', 'B', '\r'}},
		{"literal text falls through", []string{"yes"}, []byte("yes")},
		{"literal then named", []string{"1", "Enter"}, []byte{'1', '\r'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := encodeKeys(tc.in); string(got) != string(tc.want) {
				t.Fatalf("encodeKeys(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
