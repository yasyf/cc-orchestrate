package orchestrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

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
		sandboxHome(t)

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
		sandboxHome(t)

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
	sandboxHome(t) // a socket path under a home with no daemon listening
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
	s, err := daemon.New(testDaemonConfig(t))
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
	sandboxHome(t)

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
// the wrapper level: the hidden pty-host command hosts a real child under a PTY and
// — once the child exits naturally — delivers exactly one cco.agent.childExited
// envelope carrying its --session-id and --spawn-nonce to the daemon socket.
// Together with TestHandleChildExited (the daemon's side of that envelope) this
// covers the full OnChildExit → report → handler chain.
func TestPtyHostCmdReportsChildExitToDaemon(t *testing.T) {
	sandboxHome(t)

	got := startChildExitCapture(t)

	c := ptyHostCmd()
	c.SetArgs([]string{"--session-id", "sc", "--spawn-nonce", "n1chain0", "--", "sh", "-c", "exit 0"})
	if err := c.Execute(); err != nil {
		t.Fatalf("pty-host command: %v", err)
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
