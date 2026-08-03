package ptyhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

// sunPathBytes is darwin's sun_path capacity including its terminating NUL.
const sunPathBytes = 104

// sandboxHome points daemonkit's home override at a short-named temp directory
// and returns it. daemonkit resolves the home through the passwd database and
// never reads HOME, so a test that sandboxes only HOME writes its sockets into
// the developer's real home. The base must be short: every pty daemon's socket
// is this home joined with its label and "/daemon.sock", and t.TempDir() embeds
// the test name in a path long enough on its own to push that past sun_path.
func sandboxHome(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pty")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("DAEMONKIT_HOME", dir)
	return dir
}

// stateDir is where one incarnation's socket and owner record live, derived
// from the same declaration the host serves and the client dials.
func stateDir(spawnNonce string) string {
	return filepath.Dir(ptyDaemon(spawnNonce).RecordPath())
}

func ptySocket(spawnNonce string) string {
	return filepath.Join(stateDir(spawnNonce), "daemon.sock")
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

func waitSocket(t *testing.T, spawnNonce string) {
	t.Helper()
	if !waitFor(func() bool { _, err := os.Stat(ptySocket(spawnNonce)); return err == nil }) {
		t.Fatal("control socket never appeared")
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

func dial(t *testing.T, spawnNonce string) *Client {
	t.Helper()
	client, err := Dial(spawnNonce)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestPTYSocketFitsSunPath pins the label budget the derivation spends. The
// label carries 64 bits of the nonce hash and nothing else precisely so the
// socket clears sun_path under a home far longer than any real one; a label
// that also embedded the session id would overflow on a long account name.
func TestPTYSocketFitsSunPath(t *testing.T) {
	t.Setenv("DAEMONKIT_HOME", "/Users/a-deliberately-long-account-name")
	socket := ptySocket("11111111-2222-3333-4444-555555555555")
	if len(socket) >= sunPathBytes {
		t.Fatalf("socket path is %d bytes, over the %d sun_path fits: %q", len(socket), sunPathBytes-1, socket)
	}
}

// TestPTYDaemonIsPerIncarnation proves two spawns of one session never share a
// path: the label derives from the spawn nonce alone, which is what makes a
// kill-driven respawn race-free.
func TestPTYDaemonIsPerIncarnation(t *testing.T) {
	first := ptyDaemon("nonce-one")
	second := ptyDaemon("nonce-two")
	if first.Label == second.Label {
		t.Fatalf("two incarnations share label %q", first.Label)
	}
	if first.Label != ptyDaemon("nonce-one").Label {
		t.Fatal("one nonce derived two labels")
	}
	if err := first.ValidateForServe(); err != nil {
		t.Fatalf("ValidateForServe: %v", err)
	}
	if err := first.ValidateForClient(); err != nil {
		t.Fatalf("ValidateForClient: %v", err)
	}
	if first.Schemas[0] != ptyWireBuild {
		t.Fatalf("schema = %q, want %q", first.Schemas[0], ptyWireBuild)
	}
	if first.Shutdown != ptyShutdown || first.Handshake != ptyHandshake {
		t.Fatalf("grace = shutdown %v handshake %v", first.Shutdown, first.Handshake)
	}
}

// TestHostRoundTrip drives the full loop against a real PTY with `cat` as the
// child: inject "hello"+Enter, then assert CAPTURE shows the echoed line. No
// claude needed.
func TestHostRoundTrip(t *testing.T) {
	sandboxHome(t)
	const nonce = "roundtrip"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{SpawnNonce: nonce, Argv: []string{"sh", "-c", "cat"}}) }()
	waitSocket(t, nonce)

	cl := dial(t, nonce)
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
	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	if _, err := cl.Capture(callCtx); err == nil {
		t.Fatal("Capture succeeded after the host settled")
	}
	if _, err := os.Stat(stateDir(nonce)); !os.IsNotExist(err) {
		t.Fatalf("state dir survived the host: %v", err)
	}
}

// TestHostDrainsQueryReplies guards the deadlock where the vt emulator answers
// a terminal query (here a cursor-position report, ESC[6n) by writing into an
// unbuffered pipe: if the host does not drain that pipe back to the child, Feed
// blocks forever holding the grid lock and the screen never advances. The child
// emits the query then visible text; the text must appear, proving the reply
// was drained and parsing continued.
func TestHostDrainsQueryReplies(t *testing.T) {
	sandboxHome(t)
	const nonce = "queryreplies"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	argv := []string{"sh", "-c", `printf '\033[6n'; printf 'AFTERQUERY'; sleep 5`}
	go func() { done <- Run(ctx, Options{SpawnNonce: nonce, Argv: argv}) }()
	waitSocket(t, nonce)

	cl := dial(t, nonce)
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

// TestSchemaMismatchIsRejectedBeforeDispatch proves a client speaking another
// era of the pty protocol is refused at the handshake rather than reaching a
// handler that would decode its payload as v1.
func TestSchemaMismatchIsRejectedBeforeDispatch(t *testing.T) {
	sandboxHome(t)
	const nonce = "schemamismatch"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{SpawnNonce: nonce, Argv: []string{"sh", "-c", "cat"}}) }()
	waitSocket(t, nonce)

	stale := ptyDaemon(nonce)
	stale.Schemas = []daemonkit.Schema{"cc-orchestrate.pty.v0"}
	client, err := daemonkit.Open(stale)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	callCtx, callCancel := context.WithTimeout(ctx, time.Second)
	defer callCancel()
	if _, err := client.Business().Call(callCtx, opCapture, nil); err == nil {
		t.Fatal("a mismatched schema was admitted")
	}

	cl := dial(t, nonce)
	if _, err := cl.Capture(ctx); err != nil {
		t.Fatalf("the matching schema was refused too: %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestOnChildExitFiresOnNaturalExit proves the exit hook fires exactly once
// when the hosted child exits on its own — the signal the pty-host turns into
// its daemon report — and only after the control endpoint is no longer
// dialable: the hook hands control to the daemon, so the wrapper's listener
// must be fully settled before it runs.
func TestOnChildExitFiresOnNaturalExit(t *testing.T) {
	sandboxHome(t)
	const nonce = "naturalexit"
	var fired, sockPresent int32
	done := make(chan error, 1)
	opts := Options{
		SpawnNonce: nonce,
		Argv:       []string{"sh", "-c", "exit 0"},
		OnChildExit: func() {
			atomic.AddInt32(&fired, 1)
			if !socketUnavailable(ptySocket(nonce)) {
				atomic.StoreInt32(&sockPresent, 1)
			}
		},
	}
	go func() { done <- Run(context.Background(), opts) }()
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

// TestNonZeroChildExitIsReported proves the wrapper mirrors its child's own
// failure instead of swallowing it, while still reporting the exit.
func TestNonZeroChildExitIsReported(t *testing.T) {
	sandboxHome(t)
	const nonce = "childfails"
	var fired int32
	done := make(chan error, 1)
	opts := Options{
		SpawnNonce:  nonce,
		Argv:        []string{"sh", "-c", "exit 3"},
		OnChildExit: func() { atomic.AddInt32(&fired, 1) },
	}
	go func() { done <- Run(context.Background(), opts) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run reported success for a child that exited 3")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the child exited")
	}
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("OnChildExit fired %d times, want 1", got)
	}
}

// TestChildExitTeardownSparesReplacementIncarnation simulates the kill-driven
// respawn race: a replacement pty-host has already taken its own label while
// the old wrapper is still tearing down. Every path the old wrapper settles or
// removes derives from its own label, so the replacement's socket and state
// survive untouched.
func TestChildExitTeardownSparesReplacementIncarnation(t *testing.T) {
	sandboxHome(t)
	const old, replacement = "incarnation-1", "incarnation-2"
	if err := os.MkdirAll(stateDir(replacement), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", ptySocket(replacement))
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
	opts := Options{
		SpawnNonce:  old,
		Argv:        []string{"sh", "-c", "exit 0"},
		OnChildExit: func() { atomic.AddInt32(&fired, 1) },
	}
	go func() { done <- Run(context.Background(), opts) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the child exited")
	}

	if !socketUnavailable(ptySocket(old)) {
		t.Fatal("old wrapper's control endpoint remained dialable after teardown")
	}
	if _, err := os.Stat(stateDir(old)); !os.IsNotExist(err) {
		t.Fatalf("old wrapper left its state dir behind: %v", err)
	}
	conn, err := net.Dial("unix", ptySocket(replacement))
	if err != nil {
		t.Fatalf("replacement's socket not dialable after the old wrapper's teardown: %v", err)
	}
	_ = conn.Close()
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("OnChildExit fired %d times, want 1", got)
	}
}

// TestChildExitNotBlockedByWedgedClient proves a client that connects to the
// control socket and never completes its handshake cannot stall the wrapper's
// teardown: the child's exit still drains the server, closes the endpoint, and
// fires OnChildExit promptly — the exit report is never held hostage.
func TestChildExitNotBlockedByWedgedClient(t *testing.T) {
	home := sandboxHome(t)
	const nonce = "wedgedclient"
	flag := filepath.Join(home, "exit-flag")
	var fired int32
	done := make(chan error, 1)
	opts := Options{
		SpawnNonce:  nonce,
		Argv:        []string{"sh", "-c", "while [ ! -e " + flag + " ]; do sleep 0.05; done"},
		OnChildExit: func() { atomic.AddInt32(&fired, 1) },
	}
	go func() { done <- Run(context.Background(), opts) }()
	waitSocket(t, nonce)

	// Wedge: connect, send nothing, and hold the connection open across the exit.
	var conn net.Conn
	if !waitFor(func() bool {
		var err error
		conn, err = net.DialTimeout("unix", ptySocket(nonce), 20*time.Millisecond)
		return err == nil
	}) {
		t.Fatal("control socket never accepted the wedged client")
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
	if !socketUnavailable(ptySocket(nonce)) {
		t.Fatal("control endpoint remained dialable after teardown")
	}
}

// TestOnChildExitSkippedOnRealSignal delivers an actual SIGTERM to the wrapper
// process — the way a daemon kill or terminal teardown reaches it — and proves
// the hook stays silent: Serve arms its drain signals before it binds, so the
// wait on the socket also guarantees the test binary survives the signal.
func TestOnChildExitSkippedOnRealSignal(t *testing.T) {
	sandboxHome(t)
	const nonce = "realsignal"
	var fired int32
	done := make(chan error, 1)
	opts := Options{
		SpawnNonce:  nonce,
		Argv:        []string{"sh", "-c", "sleep 5"},
		OnChildExit: func() { atomic.AddInt32(&fired, 1) },
	}
	go func() { done <- Run(context.Background(), opts) }()
	waitSocket(t, nonce)
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

// TestOnChildExitSkippedOnSignalTeardown proves the hook does NOT fire when a
// parent cancellation kills the child, so a deliberately torn-down wrapper
// never reports a spurious child-exit — that window is left to the supervisor
// fallbacks.
func TestOnChildExitSkippedOnSignalTeardown(t *testing.T) {
	sandboxHome(t)
	const nonce = "signalteardown"
	ctx, cancel := context.WithCancel(context.Background())
	var fired int32
	done := make(chan error, 1)
	opts := Options{
		SpawnNonce:  nonce,
		Argv:        []string{"sh", "-c", "sleep 5"},
		OnChildExit: func() { atomic.AddInt32(&fired, 1) },
	}
	go func() { done <- Run(ctx, opts) }()
	waitSocket(t, nonce)
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
	if got := ptyWireBuild; got != daemonkit.Schema("cc-orchestrate.pty.v1."+ptySchemaDigest) {
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
