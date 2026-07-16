package orchestrate

import (
	"os"
	"path/filepath"
	"slices"
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

// TestScrubExecCmd drives the hidden scrub-exec command with execve stubbed: it scrubs
// the inherited Claude session markers (leaving CLAUDE_CONFIG_DIR), resolves argv[0]
// with the child's PATH, and execs the LookPath-resolved absolute path with the full
// original argv.
func TestScrubExecCmd(t *testing.T) {
	bin := t.TempDir()
	claudePath := filepath.Join(bin, "claude")
	//nolint:gosec // the fake claude must be executable for LookPath to resolve it
	if err := os.WriteFile(claudePath, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write fake claude: %v", err)
	}
	t.Setenv("PATH", bin)

	t.Setenv("CLAUDECODE", "1")
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "1")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "parent-sid")
	t.Setenv("CLAUDE_CONFIG_DIR", "/home/x/.claude")

	var gotPath string
	var gotArgv []string
	oldExecve := execve
	execve = func(path string, argv []string, _ []string) error {
		gotPath, gotArgv = path, argv
		return nil
	}
	t.Cleanup(func() { execve = oldExecve })

	argv := []string{"claude", "--session-id", "sid-1", "--flag", "v"}
	root := &cobra.Command{Use: "cco", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(scrubExecCmd())
	root.SetArgs(append([]string{scrubExecCmdName, "--"}, argv...))
	if err := root.Execute(); err != nil {
		t.Fatalf("scrub-exec: %v", err)
	}

	for _, name := range []string{"CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ID"} {
		if v, ok := os.LookupEnv(name); ok {
			t.Errorf("%s = %q, want unset", name, v)
		}
	}
	if v, ok := os.LookupEnv("CLAUDE_CONFIG_DIR"); !ok || v != "/home/x/.claude" {
		t.Errorf("CLAUDE_CONFIG_DIR = %q, ok=%v, want /home/x/.claude untouched", v, ok)
	}

	if gotPath != claudePath {
		t.Errorf("execve path = %q, want LookPath-resolved %q", gotPath, claudePath)
	}
	if !slices.Equal(gotArgv, argv) {
		t.Errorf("execve argv = %v, want full original %v", gotArgv, argv)
	}
}
