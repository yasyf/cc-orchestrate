package orchestrate

import (
	"testing"
	"time"

	"github.com/yasyf/cc-interact/version"
	dkversion "github.com/yasyf/daemonkit/version"
)

func TestBuildVersionDevFallback(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name         string
		stamped      string
		mtime        time.Time
		ok           bool
		want         string
		newerThan    []string
		tiesWithSelf bool
	}{
		{
			name:    "stamped build passthrough",
			stamped: "1.2.3",
			want:    "1.2.3",
		},
		{
			name:         "unstamped build uses mtime",
			stamped:      "dev",
			mtime:        mtime,
			ok:           true,
			want:         "9999.1700000000000000000.0-dev",
			newerThan:    []string{"0.9.0", "v0.9.0"},
			tiesWithSelf: true,
		},
		{
			name:         "same-second rebuild orders by nanosecond",
			stamped:      "dev",
			mtime:        mtime.Add(time.Nanosecond),
			ok:           true,
			want:         "9999.1700000000000000001.0-dev",
			newerThan:    []string{"9999.1700000000000000000.0-dev", "0.9.0"},
			tiesWithSelf: true,
		},
		{
			name:      "unstamped fallback",
			stamped:   "dev",
			want:      "dev",
			newerThan: []string{"0.9.0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dkversion.Resolve(tc.stamped, tc.mtime, tc.ok)
			if got != tc.want {
				t.Fatalf("version.Resolve(%q, %v, %v) = %q, want %q", tc.stamped, tc.mtime, tc.ok, got, tc.want)
			}
			for _, older := range tc.newerThan {
				if !version.Newer(got, older) {
					t.Errorf("version.Newer(%q, %q) = false, want true", got, older)
				}
			}
			if tc.tiesWithSelf {
				tie := dkversion.Resolve(tc.stamped, tc.mtime, tc.ok)
				if version.Newer(got, tie) {
					t.Errorf("version.Newer(%q, %q) = true, want false", got, tie)
				}
				if version.Newer(tie, got) {
					t.Errorf("version.Newer(%q, %q) = true, want false", tie, got)
				}
			}
		})
	}
}

// TestCrossComparatorSentinelOrdering pins that the daemonkit-shaped version strings
// cc-orchestrate now emits order identically under daemonkit's version.Newer and
// cc-interact's version.Newer: cc-orchestrate stamps via daemonkit, but the daemon
// eviction it feeds compares via cc-interact's comparator, so a dev sentinel must beat
// every release under both, and sentinels must order by nanosecond under both.
func TestCrossComparatorSentinelOrdering(t *testing.T) {
	const (
		devOld = "9999.1700000000000000000.0-dev"
		devNew = "9999.1700000000000000001.0-dev"
	)
	for _, tc := range []struct {
		name string
		a, b string
		want bool
	}{
		{"dev sentinel beats release", devOld, "1.2.3", true},
		{"dev sentinel beats v-release", devOld, "v9.9.9", true},
		{"release never beats dev sentinel", "v9.9.9", devOld, false},
		{"newer dev sentinel beats older", devNew, devOld, true},
		{"older dev sentinel loses to newer", devOld, devNew, false},
		{"dev sentinel ties itself", devOld, devOld, false},
		{"newer release beats older", "1.3.0", "1.2.9", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := version.Newer(tc.a, tc.b); got != tc.want {
				t.Errorf("cc-interact version.Newer(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			if got := dkversion.Newer(tc.a, tc.b); got != tc.want {
				t.Errorf("daemonkit version.Newer(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

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
		{EventAdopted, false},
	} {
		t.Run(tc.event, func(t *testing.T) {
			if got := isTerminal(tc.event); got != tc.want {
				t.Fatalf("TerminalEvent(%q) = %v, want %v", tc.event, got, tc.want)
			}
		})
	}
}
