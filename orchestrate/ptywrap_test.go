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
		got, err := wrapForCapture(self, "sid-1", nil, claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, claudeCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, claudeCmd)
		}
	})

	t.Run("capturing backend leaves ccp argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", nil, ccpCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, ccpCmd) {
			t.Fatalf("argv = %v, want unchanged %v", got, ccpCmd)
		}
	})

	t.Run("capturing backend carries the launcher prefix verbatim", func(t *testing.T) {
		launcher := []string{"cc-runtime", "wrap", "--"}
		got, err := wrapForCapture(self, "sid-1", launcher, claudeCmd, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := append(append([]string{}, launcher...), claudeCmd...)
		if !slices.Equal(got, want) {
			t.Fatalf("argv = %v, want %v", got, want)
		}
	})

	t.Run("empty launcher matches nil byte-for-byte", func(t *testing.T) {
		empty, err := wrapForCapture(self, "sid-1", []string{}, claudeCmd, backend.Capabilities(backend.CanCapture))
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

		got, err := wrapForCapture(self, "sid-1", nil, claudeCmd, backend.Capabilities())
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

		got, err := wrapForCapture(self, "sid-1", []string{"cc-runtime", "wrap", "--"}, claudeCmd, backend.Capabilities())
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--",
			filepath.Join(bin, "cc-runtime"), "wrap", "--",
			filepath.Join(bin, "claude"), "--session-id", "sid-1", "--flag", "v",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("wrapped argv =\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("non-capturing wrap fails loud on an unresolvable launcher", func(t *testing.T) {
		t.Setenv("PATH", "")
		if _, err := wrapForCapture(self, "sid-1", []string{"cc-runtime", "wrap", "--"}, ccpCmd, backend.Capabilities()); err == nil {
			t.Fatal("wrapForCapture resolved a launcher missing from PATH, want error")
		}
	})

	t.Run("non-capturing wrap passes the resolved ccp path through unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", nil, ccpCmd, backend.Capabilities())
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
