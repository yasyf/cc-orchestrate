package ptyhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

// shortSock returns a unix socket path under a short-named temp dir, cleaned up on test
// end. t.TempDir() embeds the (here long) test name, which can push a socket path past
// the OS sun_path limit (~104 bytes on darwin) and fail bind with EINVAL.
func shortSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pty")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

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

func testOptions(t *testing.T, socket string, argv []string) Options {
	t.Helper()
	return Options{
		Socket: socket, ProcessStore: filepath.Join(filepath.Dir(socket), "processes.db"),
		Argv: argv, RuntimeBuild: "1.0.0",
	}
}

func socketUnavailable(path string) bool {
	conn, err := net.DialTimeout("unix", path, 20*time.Millisecond)
	if err != nil {
		return true
	}
	_ = conn.Close()
	return false
}

// TestHostRoundTrip drives the full loop against a real PTY with `cat` as the child:
// inject "hello"+Enter, then assert CAPTURE shows the echoed line. No claude needed.
func TestHostRoundTrip(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "pty.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	opts := testOptions(t, sock, []string{"sh", "-c", "cat"})
	go func() { done <- Run(ctx, opts) }()

	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}

	cl := Dial(sock)
	defer func() { _ = cl.Close() }()
	if err := cl.SendKeys(ctx, "hello", "Enter"); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}
	cl.mu.Lock()
	firstSession := cl.session
	cl.mu.Unlock()
	if firstSession == nil {
		t.Fatal("SendKeys did not establish a persistent session")
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
	cl.mu.Lock()
	secondSession := cl.session
	cl.mu.Unlock()
	if secondSession != firstSession {
		t.Fatal("Capture replaced the live persistent session")
	}

	cancel() // terminate the child
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	if _, err := cl.Capture(callCtx); err == nil {
		t.Fatal("Capture succeeded after runtime settlement")
	}
	cl.mu.Lock()
	retired := cl.session == nil
	cl.mu.Unlock()
	if !retired {
		t.Fatal("failed persistent session was not retired")
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
	opts := testOptions(t, sock, []string{"sh", "-c", `printf '\033[6n'; printf 'AFTERQUERY'; sleep 5`})
	go func() {
		done <- Run(ctx, opts)
	}()

	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}
	cl := Dial(sock)
	defer func() { _ = cl.Close() }()
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

func TestWireBuildMismatchIsRejectedBeforeDispatch(t *testing.T) {
	sock := shortSock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := testOptions(t, sock, []string{"sh", "-c", "cat"})
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}

	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:      wire.UnixDialer(sock),
		WireBuild: "cc-orchestrate.pty.v0",
		Role:      trust.UnprotectedRole,
	})
	if !errors.Is(err, wire.ErrBuildMismatch) {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("mismatched wire handshake error = %v, want %v", err, wire.ErrBuildMismatch)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestOnChildExitFiresOnNaturalExit proves the exit hook fires exactly once when the
// hosted child exits on its own — the signal the pty-host turns into its daemon report
// — and only after the control endpoint is no longer dialable: the hook hands
// control to the daemon, so the wrapper's daemonkit-owned listener must be fully
// settled before it runs.
func TestOnChildExitFiresOnNaturalExit(t *testing.T) {
	sock := shortSock(t)
	var fired, sockPresent int32
	done := make(chan error, 1)
	opts := testOptions(t, sock, []string{"sh", "-c", "exit 0"})
	opts.OnChildExit = func() {
		atomic.AddInt32(&fired, 1)
		if !socketUnavailable(sock) {
			atomic.StoreInt32(&sockPresent, 1)
		}
	}
	go func() {
		done <- Run(context.Background(), opts)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the child exited")
	}
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("OnChildExit fired %d times, want exactly 1 on a natural child exit", got)
	}
	if atomic.LoadInt32(&sockPresent) != 0 {
		t.Fatal("OnChildExit ran with the control endpoint still dialable")
	}
}

// TestChildExitTeardownSparesReplacementSocket simulates the kill-driven respawn race
// at the host level: a replacement pty-host has already bound its own per-incarnation
// socket while the old wrapper is still tearing down. The old wrapper's teardown must
// settle only its own listener — the replacement's stays dialable. Socket paths are
// per incarnation, and daemonkit owns stale-socket removal under its permanent lock.
func TestChildExitTeardownSparesReplacementSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "pty")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	oldSock := filepath.Join(dir, "sid-n1.sock")
	replacement := filepath.Join(dir, "sid-n2.sock")
	ln, err := net.Listen("unix", replacement)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	var fired int32
	done := make(chan error, 1)
	opts := testOptions(t, oldSock, []string{"sh", "-c", "exit 0"})
	opts.OnChildExit = func() { atomic.AddInt32(&fired, 1) }
	go func() {
		done <- Run(context.Background(), opts)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the child exited")
	}

	if !socketUnavailable(oldSock) {
		t.Fatal("old wrapper's control endpoint remained dialable after teardown")
	}
	if _, err := os.Stat(replacement); err != nil {
		t.Fatalf("replacement's socket was disturbed by the old wrapper's teardown: %v", err)
	}
	conn, err := net.Dial("unix", replacement)
	if err != nil {
		t.Fatalf("replacement's socket not dialable after the old wrapper's teardown: %v", err)
	}
	_ = conn.Close()
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("OnChildExit fired %d times, want 1", got)
	}
}

// TestChildExitNotBlockedByWedgedClient proves a client that connects to the control
// socket and never completes its request cannot stall the wrapper's teardown: Close
// expires the wedged connection's deadline instead of trusting the client, so the
// child's exit still drains the server, closes the endpoint, and fires OnChildExit
// promptly — the exit report is never held hostage.
func TestChildExitNotBlockedByWedgedClient(t *testing.T) {
	sock := shortSock(t)
	flag := filepath.Join(filepath.Dir(sock), "exit-flag")
	var fired int32
	done := make(chan error, 1)
	opts := testOptions(t, sock, []string{"sh", "-c", "while [ ! -e " + flag + " ]; do sleep 0.05; done"})
	opts.OnChildExit = func() { atomic.AddInt32(&fired, 1) }
	go func() {
		done <- Run(context.Background(), opts)
	}()
	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}

	// Wedge: connect, send nothing, and hold the connection open across the exit.
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := os.WriteFile(flag, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run blocked past the child's exit behind a wedged client")
	}
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("OnChildExit fired %d times, want 1", got)
	}
	if !socketUnavailable(sock) {
		t.Fatal("control endpoint remained dialable after teardown")
	}
}

// TestOnChildExitSkippedOnRealSignal delivers an actual SIGTERM to the wrapper process
// — the way a daemon kill or terminal teardown reaches it, unlike the direct context
// cancellation TestOnChildExitSkippedOnSignalTeardown drives — and proves the hook
// stays silent: the signal cancels ctx, ctx cancellation kills the child, and the
// ctx.Err() gate skips the report. Run's NotifyContext is registered before the socket
// binds, so the wait on the socket also guarantees the test binary survives the signal.
func TestOnChildExitSkippedOnRealSignal(t *testing.T) {
	sock := shortSock(t)
	var fired int32
	done := make(chan error, 1)
	opts := testOptions(t, sock, []string{"sh", "-c", "sleep 5"})
	opts.OnChildExit = func() { atomic.AddInt32(&fired, 1) }
	go func() {
		done <- Run(context.Background(), opts)
	}()
	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
	if got := atomic.LoadInt32(&fired); got != 0 {
		t.Fatalf("OnChildExit fired %d times, want 0 on a real SIGTERM teardown", got)
	}
}

// TestOnChildExitSkippedOnSignalTeardown proves the hook does NOT fire when a parent
// cancellation (the signal-teardown path) kills the child, so a deliberately torn-down
// wrapper never reports a spurious child-exit — that window is left to the supervisor
// fallbacks.
func TestOnChildExitSkippedOnSignalTeardown(t *testing.T) {
	sock := shortSock(t)
	ctx, cancel := context.WithCancel(context.Background())
	var fired int32
	done := make(chan error, 1)
	opts := testOptions(t, sock, []string{"sh", "-c", "sleep 5"})
	opts.OnChildExit = func() { atomic.AddInt32(&fired, 1) }
	go func() {
		done <- Run(ctx, opts)
	}()
	if !waitFor(func() bool { _, err := os.Stat(sock); return err == nil }) {
		t.Fatal("control socket never appeared")
	}
	cancel() // parent teardown, as a SIGINT/SIGTERM/SIGHUP would
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := atomic.LoadInt32(&fired); got != 0 {
		t.Fatalf("OnChildExit fired %d times, want 0 on a signal teardown", got)
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

func TestPTYSchemaFingerprint(t *testing.T) {
	sum := sha256.Sum256([]byte(ptySchemaV1))
	if got := hex.EncodeToString(sum[:]); got != ptySchemaDigest {
		t.Fatalf("pty schema digest = %s, want %s", got, ptySchemaDigest)
	}
	if got := ptyWireBuild; got != "cc-orchestrate.pty.v1."+ptySchemaDigest {
		t.Fatalf("pty wire build = %q", got)
	}
}

func TestDecodeMessageRejectsUnknownAndTrailingFields(t *testing.T) {
	for _, payload := range []string{
		`{"data":"aGVsbG8=","extra":true}`,
		`{"data":"aGVsbG8="}{}`,
	} {
		var request keysRequest
		if err := decodeMessage([]byte(payload), &request); err == nil {
			t.Fatalf("decodeMessage(%q) succeeded", payload)
		}
	}
}

func TestProtocolMessagesEncodeExactly(t *testing.T) {
	payload, err := encodeMessage(keysRequest{Data: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(payload), `{"data":"aGVsbG8="}`; got != want {
		t.Fatalf("keys request = %s, want %s", got, want)
	}
	payload, err = encodeMessage(captureResponse{Text: "screen"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(payload), `{"text":"screen"}`; got != want {
		t.Fatalf("capture response = %s, want %s", got, want)
	}
}
