package orchestrate

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestWrapScrubExec(t *testing.T) {
	self := "/abs/cc-orchestrate"
	for _, tc := range []struct {
		name    string
		command []string
		want    []string
	}{
		{
			name:    "plain claude argv",
			command: []string{"claude", "--session-id", "sid-1", "--flag", "v"},
			want:    []string{self, scrubExecCmdName, "--", "claude", "--session-id", "sid-1", "--flag", "v"},
		},
		{
			name:    "pty-host-wrapped argv is wrapped outermost",
			command: []string{self, "pty-host", "--session-id", "sid-1", "--", "/abs/claude", "--session-id", "sid-1"},
			want:    []string{self, scrubExecCmdName, "--", self, "pty-host", "--session-id", "sid-1", "--", "/abs/claude", "--session-id", "sid-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := wrapScrubExec(self, tc.command); !slices.Equal(got, tc.want) {
				t.Fatalf("wrapScrubExec =\n  %v\nwant\n  %v", got, tc.want)
			}
		})
	}
}

// runScrubExec drives the hidden scrub-exec command through a cobra root with execve
// stubbed, returning the path, argv, and environment the stub received.
func runScrubExec(t *testing.T, argv []string) (path string, gotArgv, env []string) {
	t.Helper()
	oldExecve := execve
	execve = func(p string, a, e []string) error {
		path, gotArgv, env = p, a, e
		return nil
	}
	t.Cleanup(func() { execve = oldExecve })

	root := &cobra.Command{Use: "cco", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(scrubExecCmd())
	root.SetArgs(append([]string{scrubExecCmdName, "--"}, argv...))
	if err := root.Execute(); err != nil {
		t.Fatalf("scrub-exec: %v", err)
	}
	return path, gotArgv, env
}

func writeFakeClaude(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "claude")
	//nolint:gosec // the fake claude must be executable for LookPath to resolve it
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	return p
}

// TestScrubExecCmd drives the hidden scrub-exec command with execve stubbed: it scrubs
// the inherited Claude session markers (leaving CLAUDE_CONFIG_DIR), resolves argv[0]
// with the child's PATH, and execs the resolved path with the full original argv and
// the scrubbed environment.
func TestScrubExecCmd(t *testing.T) {
	t.Run("scrubs markers and execs the resolved argv", func(t *testing.T) {
		claudePath := writeFakeClaude(t, t.TempDir())
		t.Setenv("PATH", filepath.Dir(claudePath))
		t.Setenv("CLAUDECODE", "1")
		t.Setenv("CLAUDE_CODE_CHILD_SESSION", "1")
		t.Setenv("CLAUDE_CODE_SESSION_ID", "parent-sid")
		t.Setenv("CLAUDE_CONFIG_DIR", "/home/x/.claude")

		argv := []string{"claude", "--session-id", "sid-1", "--flag", "v"}
		gotPath, gotArgv, env := runScrubExec(t, argv)

		for _, name := range []string{"CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ID"} {
			if v, ok := os.LookupEnv(name); ok {
				t.Errorf("%s = %q, want unset", name, v)
			}
		}
		if v, ok := os.LookupEnv("CLAUDE_CONFIG_DIR"); !ok || v != "/home/x/.claude" {
			t.Errorf("CLAUDE_CONFIG_DIR = %q, ok=%v, want /home/x/.claude untouched", v, ok)
		}
		// The environment handed to execve is the scrubbed one, not a stale
		// pre-scrub snapshot.
		configDirSeen := false
		for _, kv := range env {
			name, _, _ := strings.Cut(kv, "=")
			if name == "CLAUDECODE" || strings.HasPrefix(name, "CLAUDE_CODE_") {
				t.Errorf("execve env carries %s, want scrubbed", kv)
			}
			if name == "CLAUDE_CONFIG_DIR" {
				configDirSeen = true
			}
		}
		if !configDirSeen {
			t.Error("execve env lacks CLAUDE_CONFIG_DIR, want it untouched")
		}

		if gotPath != claudePath {
			t.Errorf("execve path = %q, want LookPath-resolved %q", gotPath, claudePath)
		}
		if !slices.Equal(gotArgv, argv) {
			t.Errorf("execve argv = %v, want full original %v", gotArgv, argv)
		}
	})

	t.Run("pty-host-wrapped argv passes through with its embedded delimiter", func(t *testing.T) {
		claudePath := writeFakeClaude(t, t.TempDir())
		argv := []string{claudePath, "pty-host", "--session-id", "sid-1", "--", claudePath, "--session-id", "sid-1"}
		gotPath, gotArgv, _ := runScrubExec(t, argv)
		if gotPath != claudePath {
			t.Errorf("execve path = %q, want %q", gotPath, claudePath)
		}
		if !slices.Equal(gotArgv, argv) {
			t.Errorf("execve argv = %v, want the nested vector intact %v", gotArgv, argv)
		}
	})

	t.Run("relative PATH entry resolves absolute", func(t *testing.T) {
		base := t.TempDir()
		relbin := filepath.Join(base, "relbin")
		if err := os.Mkdir(relbin, 0o750); err != nil {
			t.Fatal(err)
		}
		writeFakeClaude(t, relbin)
		t.Chdir(base)
		t.Setenv("PATH", "relbin")

		gotPath, _, _ := runScrubExec(t, []string{"claude"})
		wd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join(wd, "relbin", "claude"); gotPath != want {
			t.Errorf("execve path = %q, want ErrDot match resolved absolute %q", gotPath, want)
		}
	})
}
