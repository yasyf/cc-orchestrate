package orchestrate

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-orchestrate/backend"
)

// attachBackend is a uniquely-named registered backend implementing Attacher, so
// execAttach's resolve-and-exec path is exercised without a real multiplexer.
type attachBackend struct {
	argv []string
	err  error
}

func (attachBackend) Name() backend.Name                { return "attachtest" }
func (attachBackend) Available() bool                   { return true }
func (attachBackend) EnsureReady(context.Context) error { return nil }
func (attachBackend) CreateWorkstream(context.Context, backend.WorkstreamSpec) (backend.WorkstreamHandle, error) {
	return backend.WorkstreamHandle{}, nil
}

func (attachBackend) ListWorkstreams(context.Context) ([]backend.WorkstreamHandle, error) {
	return nil, nil
}

func (attachBackend) Spawn(context.Context, backend.SpawnSpec) (backend.AgentHandle, error) {
	return backend.AgentHandle{}, nil
}

func (attachBackend) ListAgents(context.Context, backend.WorkstreamHandle) ([]backend.AgentHandle, error) {
	return nil, nil
}
func (attachBackend) Kill(context.Context, backend.AgentHandle) error                { return nil }
func (attachBackend) KillWorkstream(context.Context, backend.WorkstreamHandle) error { return nil }
func (attachBackend) Caps() backend.Caps                                             { return backend.Caps{} }

func (b attachBackend) AttachArgv(context.Context, backend.AgentHandle) ([]string, error) {
	return b.argv, b.err
}

// TestAgentAttachCommandTree asserts `agent attach` is reachable from Root() and
// demands exactly one id — validated before any daemon round trip. The tree is built
// from Root() without executing a RunE, so it needs no daemon.
func TestAgentAttachCommandTree(t *testing.T) {
	root := Root()
	sub, _, err := root.Find([]string{"agent", "attach"})
	if err != nil || sub.Name() != "attach" {
		t.Fatalf("Find(agent attach) = %v, %v", sub, err)
	}

	root.SetArgs([]string{"agent", "attach"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil {
		t.Fatal("agent attach with no id = nil, want an ExactArgs(1) error before any daemon call")
	}
}

// TestExecAttach drives execAttach with the execve seam stubbed: a registered Attacher
// backend resolves argv[0] on PATH and execs it with the original argv and the process
// environment, while a non-Attacher and an unregistered backend both fail fast.
func TestExecAttach(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "mux")
	//nolint:gosec // the fake multiplexer must be executable for LookPath to resolve it
	if err := os.WriteFile(binPath, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write fake mux: %v", err)
	}
	t.Setenv("PATH", dir)

	t.Run("resolves the argv and execs the multiplexer client", func(t *testing.T) {
		backend.Register(attachBackend{argv: []string{"mux", "attach", "sess-1"}})

		var gotPath string
		var gotArgv, gotEnv []string
		oldExecve := execve
		execve = func(p string, a, e []string) error {
			gotPath, gotArgv, gotEnv = p, a, e
			return nil
		}
		t.Cleanup(func() { execve = oldExecve })

		handle := backend.AgentHandle{Backend: "attachtest", ID: "term-1", WorkstreamID: "sess-1"}
		wantEnv := os.Environ()
		if err := execAttach(context.Background(), handle); err != nil {
			t.Fatalf("execAttach: %v", err)
		}
		if gotPath != binPath {
			t.Errorf("execve path = %q, want LookPath-resolved %q", gotPath, binPath)
		}
		wantArgv := []string{"mux", "attach", "sess-1"}
		if !slices.Equal(gotArgv, wantArgv) {
			t.Errorf("execve argv = %v, want %v", gotArgv, wantArgv)
		}
		if !slices.Equal(gotEnv, wantEnv) {
			t.Errorf("execve env = %v, want os.Environ() %v", gotEnv, wantEnv)
		}
	})

	t.Run("a non-Attacher backend errors", func(t *testing.T) {
		backend.Register(spawnBackend{})
		handle := backend.AgentHandle{Backend: "spawntest", ID: "term-1", WorkstreamID: "sess-1"}
		err := execAttach(context.Background(), handle)
		if err == nil || !strings.Contains(err.Error(), "backend spawntest does not support attach") {
			t.Fatalf("execAttach non-Attacher err = %v, want a does-not-support-attach error", err)
		}
	})

	t.Run("an unregistered backend errors", func(t *testing.T) {
		handle := backend.AgentHandle{Backend: "no-such-backend", ID: "term-1", WorkstreamID: "sess-1"}
		err := execAttach(context.Background(), handle)
		if err == nil || !strings.Contains(err.Error(), "backend no-such-backend does not support attach") {
			t.Fatalf("execAttach unregistered err = %v, want a does-not-support-attach error", err)
		}
	})
}
