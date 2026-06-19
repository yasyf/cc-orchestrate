package orchestrate

import "testing"

// TestTerminalEventOnlyExited proves the daemon's terminal-event predicate closes a
// subject only on EventExited: every non-terminal lifecycle event (including the
// restart/abandon/serialize/restore additions) must leave the subject open.
func TestTerminalEventOnlyExited(t *testing.T) {
	isTerminal := deps().TerminalEvent
	for _, tc := range []struct {
		event string
		want  bool
	}{
		{EventExited, true},
		{EventRestarted, false},
		{EventAbandoned, false},
		{EventSerialized, false},
		{EventRestored, false},
		{EventStatus, false},
		{EventSpawned, false},
	} {
		t.Run(tc.event, func(t *testing.T) {
			if got := isTerminal(tc.event); got != tc.want {
				t.Fatalf("TerminalEvent(%q) = %v, want %v", tc.event, got, tc.want)
			}
		})
	}
}
