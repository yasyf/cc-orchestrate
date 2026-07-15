package orchestrate

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/cc-orchestrate/backend"
)

func TestWrapForCapture(t *testing.T) {
	self := "/abs/cc-orchestrate"
	claudeCmd := []string{"claude", "--session-id", "sid-1", "--flag", "v"}
	ccpCmd := []string{"/opt/homebrew/bin/ccp", "run", "--session-id", "sid-1", "--flag", "v"}

	t.Run("capturing backend leaves claude argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, claudeCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, claudeCmd)
		}
	})

	t.Run("capturing backend leaves ccp argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", ccpCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, ccpCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, ccpCmd)
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

		got, err := wrapForCapture(self, "sid-1", claudeCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--",
			claudePath, "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("non-capturing wrap passes the resolved ccp path through unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", ccpCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--",
			"/opt/homebrew/bin/ccp", "run", "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})
}

func TestPtySocketPathDeterministic(t *testing.T) {
	if a, b := ptySocketPath("sid-9"), ptySocketPath("sid-9"); a != b {
		t.Fatalf("ptySocketPath not deterministic: %q vs %q", a, b)
	}
	if ptySocketPath("a") == ptySocketPath("b") {
		t.Fatal("ptySocketPath collides across session ids")
	}
}
