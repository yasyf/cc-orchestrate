package orchestrate

import (
	"slices"
	"testing"

	"github.com/yasyf/cc-orchestrate/backend"
)

func TestWrapForCapture(t *testing.T) {
	self := "/abs/cc-orchestrate"
	command := []string{"claude", "--session-id", "sid-1", "--flag", "v"}

	t.Run("capturing backend leaves argv unchanged", func(t *testing.T) {
		got, err := wrapForCapture(self, "sid-1", command, backend.Capabilities(backend.CanCapture))
		if err != nil {
			t.Fatalf("wrapForCapture: %v", err)
		}
		if !slices.Equal(got, command) {
			t.Fatalf("argv = %v, want unchanged %v", got, command)
		}
	})

	t.Run("non-capturing wrap resolves claude under the pty-host", func(t *testing.T) {
		got := wrapPTYHost(self, "sid-1", "/usr/bin/claude", command)
		want := []string{
			self, "pty-host", "--session-id", "sid-1", "--",
			"/usr/bin/claude", "--session-id", "sid-1", "--flag", "v",
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
