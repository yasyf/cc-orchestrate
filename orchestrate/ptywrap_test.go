package orchestrate

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// shortHome returns a temp HOME anchored at /tmp, cleaned up on test end. The default
// temp root (darwin's /var/folders/...) plus .cc-orchestrate-v1/pty/<sid>-<16-hex>.sock
// overruns the OS sun_path limit (~104 bytes); a /tmp anchor leaves ample headroom.
func shortHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "cco")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	return home
}

func TestWrapForCapture(t *testing.T) {
	self := "/abs/cc-orchestrate"
	claudeCmd := []string{"claude", "--session-id", "sid-1", "--flag", "v"}
	ccpCmd := []string{"/opt/homebrew/bin/ccp", "run", "--session-id", "sid-1", "--flag", "v"}

	t.Run("capturing backend leaves claude argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", "nonce-1", nil, claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, claudeCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, claudeCmd)
		}
	})

	t.Run("capturing backend leaves ccp argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", "nonce-1", nil, ccpCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, ccpCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, ccpCmd)
		}
	})

	t.Run("capturing backend carries the launcher prefix verbatim", func(t *testing.T) {
		launcher := []string{"cc-runtime", "wrap", "--"}
		got, err := wrapForCapture(self, "sid-1", "nonce-1", launcher, claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := append(append([]string{}, launcher...), claudeCmd...)
		if !slices.Equal(got, want) {
			t.Fatalf("argv = %v, want %v", got, want)
		}
	})

	t.Run("empty launcher matches nil byte-for-byte", func(t *testing.T) {
		empty, err := wrapForCapture(self, "sid-1", "nonce-1", []string{}, claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(empty, claudeCmd) {
			t.Fatalf("argv = %v, want unchanged %v", empty, claudeCmd)
		}
	})

	t.Run("non-capturing wrap resolves claude under the pty-host", func(t *testing.T) {
		bin := t.TempDir()
		claudePath := filepath.Join(bin, "claude")
		//nolint:gosec // the fake claude must be executable for LookPath to resolve it
		if err := os.WriteFile(claudePath, []byte("#!/bin/sh\n"), 0o700); err != nil {
			t.Fatalf("write fake claude: %v", err)
		}
		t.Setenv("PATH", bin)
		t.Setenv("HOME", t.TempDir())

		got, err := wrapForCapture(self, "sid-1", "nonce-1", nil, claudeCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--spawn-nonce", "nonce-1", "--",
			claudePath, "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("non-capturing wrap resolves the launcher head and claude", func(t *testing.T) {
		bin := t.TempDir()
		for _, name := range []string{"claude", "cc-runtime"} {
			//nolint:gosec // the fakes must be executable for PATH resolution to find them
			if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"), 0o700); err != nil {
				t.Fatalf("write fake %s: %v", name, err)
			}
		}
		t.Setenv("PATH", bin)
		t.Setenv("HOME", t.TempDir())

		got, err := wrapForCapture(self, "sid-1", "nonce-1", []string{"cc-runtime", "wrap", "--"}, claudeCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--spawn-nonce", "nonce-1", "--",
			filepath.Join(bin, "cc-runtime"), "wrap", "--",
			filepath.Join(bin, "claude"), "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("non-capturing wrap fails loud on an unresolvable launcher", func(t *testing.T) {
		t.Setenv("PATH", "")
		if _, err := wrapForCapture(self, "sid-1", "nonce-1", []string{"cc-runtime", "wrap", "--"}, ccpCmd, backend.Capabilities()); err == nil {
			t.Fatal("wrapForCapture resolved a launcher missing from PATH, want error")
		}
	})

	t.Run("non-capturing wrap passes the resolved ccp path through unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", "nonce-1", nil, ccpCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--spawn-nonce", "nonce-1", "--",
			"/opt/homebrew/bin/ccp", "run", "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})
}

// TestReportChildExitToleratesUnreachableDaemon proves the pty-host's last-act report
// never blocks or fails the wrapper's own exit when the daemon is down: with no daemon
// listening on the derived socket, reportChildExit swallows the dial error and returns
// promptly, so the wrapper exits cleanly and the fallbacks cover the window.
func TestReportChildExitToleratesUnreachableDaemon(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // a socket path under a home with no daemon listening
	done := make(chan struct{})
	go func() {
		reportChildExit("sess-unreachable", "nonce-1")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(reportChildExitTimeout + 2*time.Second):
		t.Fatal("reportChildExit blocked past its timeout with the daemon unreachable")
	}
}

func startChildExitCapture(t *testing.T) <-chan daemon.Envelope {
	t.Helper()
	got := make(chan daemon.Envelope, 1)
	s, err := daemon.New(testDaemonConfig())
	if err != nil {
		t.Fatalf("daemon.New: %v", err)
	}
	s.Register(mAgentChildExited.op(), func(h daemon.HandlerCtx) daemon.Reply {
		got <- h.Env
		return daemon.Reply{OK: true}
	})
	startTestDaemon(t, s)
	return got
}

// TestReportChildExitReachesDaemon proves the positive path at the socket level: with
// a daemon listening on the derived control socket, reportChildExit delivers exactly
// one cco.agent.childExited envelope carrying the session id and spawn nonce, then
// returns once the reply lands.
func TestReportChildExitReachesDaemon(t *testing.T) {
	// t.TempDir embeds the (long) test name, which can push HOME/.cc-orchestrate-v1/
	// daemon.sock past the OS sun_path limit; a bare MkdirTemp stays short.
	home, err := os.MkdirTemp("", "cco")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)

	got := startChildExitCapture(t)

	reportChildExit("sess-live", "nonce-live")

	select {
	case env := <-got:
		if env.Op != mAgentChildExited.op() {
			t.Fatalf("op = %q, want %q", env.Op, mAgentChildExited.op())
		}
		var req agentChildExitedRequest
		if err := json.Unmarshal(env.Body, &req); err != nil {
			t.Fatalf("unmarshal report body: %v", err)
		}
		if req.SessionID != "sess-live" || req.SpawnNonce != "nonce-live" {
			t.Fatalf("report = %+v, want SessionID sess-live SpawnNonce nonce-live", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no childExited envelope reached the daemon socket")
	}
}

// TestPtyHostCmdReportsChildExitToDaemon drives the production wiring end to end at
// the wrapper level: the hidden pty-host command hosts a real child under a PTY,
// serves its per-incarnation control socket, and — once the child exits naturally —
// tears the socket down and delivers exactly one cco.agent.childExited envelope
// carrying its --session-id and --spawn-nonce to the daemon socket (a /tmp-anchored
// HOME keeps the per-incarnation socket inside the OS sun_path limit). Together with
// TestHandleChildExited (the daemon's side of that envelope) this covers the full
// OnChildExit → report → handler chain.
func TestPtyHostCmdReportsChildExitToDaemon(t *testing.T) {
	shortHome(t)

	got := startChildExitCapture(t)

	c := ptyHostCmd()
	c.SetArgs([]string{"--session-id", "sc", "--spawn-nonce", "n1chain0", "--", "sh", "-c", "exit 0"})
	if err := c.Execute(); err != nil {
		t.Fatalf("pty-host command: %v", err)
	}

	// The wrapper fully settled its daemonkit-owned per-incarnation endpoint.
	if _, err := os.Stat(ptySocketPath("sc", "n1chain0")); !os.IsNotExist(err) {
		t.Fatalf("per-incarnation control socket left behind: stat err = %v", err)
	}

	select {
	case env := <-got:
		if env.Op != mAgentChildExited.op() {
			t.Fatalf("op = %q, want %q", env.Op, mAgentChildExited.op())
		}
		var req agentChildExitedRequest
		if err := json.Unmarshal(env.Body, &req); err != nil {
			t.Fatalf("unmarshal report body: %v", err)
		}
		if req.SessionID != "sc" || req.SpawnNonce != "n1chain0" {
			t.Fatalf("report = %+v, want SessionID sc SpawnNonce n1chain0", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no childExited envelope reached the daemon socket")
	}
}

func TestPtySocketPathDeterministic(t *testing.T) {
	if a, b := ptySocketPath("sid-9", "nonce-abc"), ptySocketPath("sid-9", "nonce-abc"); a != b {
		t.Fatalf("ptySocketPath not deterministic: %q vs %q", a, b)
	}
	if ptySocketPath("a", "nonce-abc") == ptySocketPath("b", "nonce-abc") {
		t.Fatal("ptySocketPath collides across session ids")
	}
	// Per-incarnation: two nonces of the same session must derive distinct paths, or a
	// killed wrapper's listener settlement could disturb its replacement's socket.
	if ptySocketPath("sid-9", "11111111-aaaa") == ptySocketPath("sid-9", "22222222-bbbb") {
		t.Fatal("ptySocketPath collides across spawn nonces of the same session")
	}
	// The nonces here share their leading 8 chars deliberately: a truncated-nonce
	// suffix would collide, so this pins the derivation to the FULL nonce.
	if ptySocketPath("sid-9", "11111111-aaaa") == ptySocketPath("sid-9", "11111111-bbbb") {
		t.Fatal("ptySocketPath collides across spawn nonces sharing an 8-char prefix")
	}
	// The suffix carries 64 bits (16 hex chars), so two incarnations of one session
	// can never share a path in practice.
	base := filepath.Base(ptySocketPath("sid-9", "nonce-abc"))
	suffix := strings.TrimSuffix(strings.TrimPrefix(base, "sid-9-"), ".sock")
	if len(suffix) != 16 {
		t.Fatalf("socket suffix = %q (%d chars), want 16 hex chars (64 bits)", suffix, len(suffix))
	}
	if _, err := hex.DecodeString(suffix); err != nil {
		t.Fatalf("socket suffix %q is not hex: %v", suffix, err)
	}
	t.Run("empty nonce panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("ptySocketPath accepted an empty spawn nonce")
			}
		}()
		_ = ptySocketPath("sid-9", "")
	})
}

func TestPtyProcessStorePathIsStablePerSession(t *testing.T) {
	shortHome(t)
	path := ptyProcessStorePath("sid-9")
	if path != ptyProcessStorePath("sid-9") {
		t.Fatal("ptyProcessStorePath is not deterministic")
	}
	if path == ptyProcessStorePath("sid-10") {
		t.Fatal("ptyProcessStorePath collides across sessions")
	}
	base := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "process-"), ".db")
	if len(base) != 16 {
		t.Fatalf("process store suffix = %q (%d chars), want 16 hex chars", base, len(base))
	}
	if _, err := hex.DecodeString(base); err != nil {
		t.Fatalf("process store suffix %q is not hex: %v", base, err)
	}
	t.Run("empty session panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("ptyProcessStorePath accepted an empty session id")
			}
		}()
		_ = ptyProcessStorePath("")
	})
}
