package orchestrate

import (
	"os"
	"testing"
)

// TestScrubClaudeCodeEnv: see cc-notes note daemon-scrubs-claude-code-env.
func TestScrubClaudeCodeEnv(t *testing.T) {
	t.Setenv("CLAUDECODE", "1")
	t.Setenv("CLAUDE_CODE_CHILD_SESSION", "1")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "parent-sid")
	t.Setenv("CLAUDE_CODE_ENTRYPOINT", "cli")
	t.Setenv("CLAUDE_CONFIG_DIR", "/home/x/.claude")

	scrubClaudeCodeEnv()

	for _, name := range []string{"CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_ENTRYPOINT"} {
		if v, ok := os.LookupEnv(name); ok {
			t.Errorf("%s = %q, want unset", name, v)
		}
	}
	if v, ok := os.LookupEnv("CLAUDE_CONFIG_DIR"); !ok || v != "/home/x/.claude" {
		t.Errorf("CLAUDE_CONFIG_DIR = %q, ok=%v, want /home/x/.claude untouched", v, ok)
	}
}
