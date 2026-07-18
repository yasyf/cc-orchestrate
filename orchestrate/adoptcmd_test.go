package orchestrate

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// TestAdoptCommandTree asserts `adopt` is reachable from Root() as a top-level command
// (not nested under `agent`), takes at most one positional arg, and carries exactly the
// flags the wire contract specifies. The tree is built from Root() without executing a
// RunE, so it needs no daemon.
func TestAdoptCommandTree(t *testing.T) {
	root := Root()
	sub, _, err := root.Find([]string{"adopt"})
	if err != nil || sub.Name() != "adopt" {
		t.Fatalf("Find(adopt) = %v, %v", sub, err)
	}
	if sub.Parent() != root {
		t.Fatalf("adopt parent = %v, want the root command (a top-level command)", sub.Parent())
	}

	wantFlags := []string{"cwd", "latest", "name", "relocate", "pid", "timeout"}
	for _, f := range wantFlags {
		if sub.Flags().Lookup(f) == nil {
			t.Errorf("adopt missing --%s flag", f)
		}
	}
	got := 0
	sub.Flags().VisitAll(func(*pflag.Flag) { got++ })
	if got != len(wantFlags) {
		t.Errorf("adopt flag count = %d, want %d (%v)", got, len(wantFlags), wantFlags)
	}

	if cwd := sub.Flags().Lookup("cwd"); cwd.DefValue != "." {
		t.Errorf("--cwd default = %q, want %q", cwd.DefValue, ".")
	}
	if timeout := sub.Flags().Lookup("timeout"); timeout.DefValue != (10 * time.Minute).String() {
		t.Errorf("--timeout default = %q, want %q", timeout.DefValue, (10 * time.Minute).String())
	}

	root.SetArgs([]string{"adopt", "s1", "s2"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil {
		t.Fatal("adopt with two positional args = nil, want a MaximumNArgs(1) error before any daemon call")
	}
}

// TestAdoptLatestExclusivity proves --latest combined with a positional session-id
// errors before any daemon call — mirroring TestRespawnExclusivity's check for `agent
// respawn`'s <id>/--dead exclusivity.
func TestAdoptLatestExclusivity(t *testing.T) {
	root := Root()
	root.SetArgs([]string{"adopt", "sess1", "--latest"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("adopt sess1 --latest err = %v, want an exactly-one error", err)
	}
}

// TestResolveAdoptPrefix covers no match, a unique match, and an ambiguous prefix
// spanning several candidates.
func TestResolveAdoptPrefix(t *testing.T) {
	candidates := []adoptCandidateView{
		{SessionID: "abc111"},
		{SessionID: "abc222"},
		{SessionID: "def333"},
	}
	for _, tc := range []struct {
		name    string
		prefix  string
		want    string
		wantErr string
	}{
		{"no match", "zzz", "", "no adoptable session matches"},
		{"unique prefix", "def", "def333", ""},
		{"unique full id", "abc111", "abc111", ""},
		{"ambiguous prefix", "abc", "", "ambiguous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAdoptPrefix(candidates, tc.prefix)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolveAdoptPrefix(%q) err = %v, want it to contain %q", tc.prefix, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAdoptPrefix(%q): %v", tc.prefix, err)
			}
			if got != tc.want {
				t.Errorf("resolveAdoptPrefix(%q) = %q, want %q", tc.prefix, got, tc.want)
			}
		})
	}
}

// TestHumanizeAge covers the four scaled units the AGE column renders.
func TestHumanizeAge(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    time.Duration
		want string
	}{
		{"seconds", 45 * time.Second, "45s"},
		{"minutes", 3 * time.Minute, "3m"},
		{"hours", 2 * time.Hour, "2h"},
		{"days", 50 * time.Hour, "2d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := humanizeAge(tc.d); got != tc.want {
				t.Errorf("humanizeAge(%s) = %q, want %q", tc.d, got, tc.want)
			}
		})
	}
}

// TestAdoptAge proves adoptAge parses an RFC3339 mtime relative to now and errors on a
// malformed timestamp rather than silently rendering a blank age.
func TestAdoptAge(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	t.Run("parses and humanizes", func(t *testing.T) {
		got, err := adoptAge(now.Add(-5*time.Minute).Format(time.RFC3339), now)
		if err != nil {
			t.Fatalf("adoptAge: %v", err)
		}
		if got != "5m" {
			t.Errorf("adoptAge = %q, want %q", got, "5m")
		}
	})
	t.Run("malformed mtime errors", func(t *testing.T) {
		if _, err := adoptAge("not-a-time", now); err == nil {
			t.Fatal("adoptAge(malformed) = nil error, want a parse error")
		}
	})
}

// TestAdoptReadyCell proves the READY column shows "yes" when ready and the daemon's
// reason text otherwise.
func TestAdoptReadyCell(t *testing.T) {
	if got := adoptReadyCell(true, ""); got != "yes" {
		t.Errorf("adoptReadyCell(true, \"\") = %q, want yes", got)
	}
	if got := adoptReadyCell(false, "session is mid-turn"); got != "session is mid-turn" {
		t.Errorf("adoptReadyCell(false, reason) = %q, want the reason verbatim", got)
	}
}

// TestIsAdoptNotReady proves the client-side retry discriminator matches only runOp's
// reconstructed "NotReady: <message>" text — the sole refusal the wait loop retries — and
// rejects Conflict and every other code as fatal.
func TestIsAdoptNotReady(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want bool
	}{
		{"NotReady: session is mid-turn", true},
		{"Conflict: agent is already managed — use cco agent respawn", false},
		{"InvalidRequest: adopt requires a session_id", false},
		{"boom", false},
	} {
		if got := isAdoptNotReady(errString(tc.msg)); got != tc.want {
			t.Errorf("isAdoptNotReady(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// TestRenderAdoptListEmpty proves the no-candidates view is a clear message naming cwd,
// not a bare empty table.
func TestRenderAdoptListEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := renderAdoptList(&buf, "/repo", nil); err != nil {
		t.Fatalf("renderAdoptList: %v", err)
	}
	if want := "no adoptable sessions under /repo\n"; buf.String() != want {
		t.Fatalf("renderAdoptList(empty) = %q, want %q", buf.String(), want)
	}
}

// TestRenderAdoptListTable proves the candidate table carries every column the wire
// contract specifies, including a not-ready reason surfaced in the READY column and the
// short 8-char SESSION id.
func TestRenderAdoptListTable(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	candidates := []adoptCandidateView{
		{
			SessionID: "abcdef1234567890", Cwd: "/repo", GitBranch: "main",
			FirstPrompt: "fix the bug", MTime: now.Add(-3 * time.Minute).Format(time.RFC3339),
			State: "idle", Live: true, PID: 42, Ready: true,
		},
		{
			SessionID: "0011223344556677", Cwd: "/repo", GitBranch: "feature",
			FirstPrompt: "add a feature", MTime: now.Add(-90 * time.Second).Format(time.RFC3339),
			State: "working", Live: true, PID: 43, Ready: false, Reason: "session is mid-turn",
		},
	}
	var buf bytes.Buffer
	if err := renderAdoptList(&buf, "/repo", candidates); err != nil {
		t.Fatalf("renderAdoptList: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"SESSION", "AGE", "LIVE", "STATE", "READY", "BRANCH", "PROMPT",
		"abcdef12", "main", "fix the bug", "idle", "yes",
		"00112233", "feature", "add a feature", "working", "session is mid-turn",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("renderAdoptList table missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "abcdef1234567890") {
		t.Errorf("renderAdoptList table leaked the full session id, want the 8-char short form:\n%s", out)
	}
}
