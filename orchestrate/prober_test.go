package orchestrate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/store"
)

// Real claude 2.1.x startup dialogs, captured live / from the binary.
const (
	trustPane = " Quick safety check: Is this a project you created or one you trust? (Like your own code, a\n" +
		" well-known open source project, or work from your team).\n\n" +
		" Claude Code'll be able to read, edit, and execute files here.\n\n" +
		" ❯ 1. Yes, I trust this folder\n   2. No, exit\n\n Enter to confirm · Esc to cancel"
	externalImportPane = " Allow external CLAUDE.md file imports?\n\n" +
		" ❯ 1. Yes, allow external imports\n   2. No, disable external imports\n\n Enter to confirm · Esc to cancel"
	bypassPane = "WARNING: Claude Code running in Bypass Permissions mode\n  1. No, exit\n  2. Yes, I accept"
)

func TestMatchPrompt(t *testing.T) {
	for _, tc := range []struct {
		name, pane, want string
	}{
		{"trust dialog", trustPane, "trust"},
		{"external claude.md import dialog", externalImportPane, "external-claude-md"},
		{"bypass dialog", bypassPane, "bypass-permissions"},
		{"unrelated screen", "$ ls -la\ntotal 0", ""},
		{"blank", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := matchPrompt(tc.pane)
			switch {
			case tc.want == "" && got != nil:
				t.Fatalf("matchPrompt = %q, want no match", got.name)
			case tc.want != "" && (got == nil || got.name != tc.want):
				t.Fatalf("matchPrompt = %v, want %q", got, tc.want)
			}
		})
	}
}

func TestNativeText(t *testing.T) {
	for _, tc := range []struct {
		name     string
		keys     []string
		wantText string
		wantOK   bool
	}{
		{"bare enter accepts default", []string{"Enter"}, "", true},
		{"digit then enter", []string{"1", "Enter"}, "1", true},
		{"bare named key unsupported", []string{"Down", "Enter"}, "", false},
		{"no trailing enter unsupported", []string{"1"}, "", false},
		{"empty unsupported", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, ok := nativeText(tc.keys)
			if ok != tc.wantOK || text != tc.wantText {
				t.Fatalf("nativeText(%v) = (%q,%v), want (%q,%v)", tc.keys, text, ok, tc.wantText, tc.wantOK)
			}
		})
	}
}

func TestResolveProbePolicy(t *testing.T) {
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), databaseStoreSchema())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	db := st.DB()
	ctx := context.Background()

	t.Run("default answers all", func(t *testing.T) {
		ans, err := resolveProbePolicy(ctx, db)
		if err != nil {
			t.Fatalf("resolveProbePolicy: %v", err)
		}
		if !ans(policyTrust) || !ans(policyGrant) {
			t.Fatal("unset policy should auto-answer every prompt")
		}
	})
	t.Run("trust-only", func(t *testing.T) {
		if err := setConfig(ctx, db, probePolicyKey, "auto-answer-trust-only"); err != nil {
			t.Fatal(err)
		}
		ans, _ := resolveProbePolicy(ctx, db)
		if !ans(policyTrust) || ans(policyGrant) {
			t.Fatal("trust-only should answer trust but not grant")
		}
	})
	t.Run("detect-and-surface-only", func(t *testing.T) {
		if err := setConfig(ctx, db, probePolicyKey, "detect-and-surface-only"); err != nil {
			t.Fatal(err)
		}
		ans, _ := resolveProbePolicy(ctx, db)
		if ans(policyTrust) || ans(policyGrant) {
			t.Fatal("surface-only should answer no prompt")
		}
	})
	t.Run("config-read error fails safe to surface-only", func(t *testing.T) {
		closed, err := store.Open(ctx, filepath.Join(t.TempDir(), "closed.db"), databaseStoreSchema())
		if err != nil {
			t.Fatal(err)
		}
		_ = closed.Close() // force getConfig to error on a closed DB
		ans, err := resolveProbePolicy(ctx, closed.DB())
		if err == nil {
			t.Fatal("expected a config-read error on a closed db")
		}
		if ans(policyTrust) || ans(policyGrant) {
			t.Fatal("on error the policy must answer nothing (fail safe)")
		}
	})
}

// fakeScreen scripts capture frames (repeating the last) and records the key
// sequences answer() is handed. capErr/ansErr force the respective failure.
type fakeScreen struct {
	frames []string
	calls  int
	keys   [][]string
	capErr error
	ansErr error
}

func (f *fakeScreen) capture(context.Context) (string, error) {
	i := f.calls
	f.calls++
	if f.capErr != nil {
		return "", f.capErr
	}
	if len(f.frames) == 0 {
		return "", nil
	}
	if i >= len(f.frames) {
		i = len(f.frames) - 1
	}
	return f.frames[i], nil
}

func (f *fakeScreen) answer(_ context.Context, keys ...string) error {
	if f.ansErr != nil {
		return f.ansErr
	}
	f.keys = append(f.keys, append([]string{}, keys...))
	return nil
}

func collectStates() (func(Status) error, *[]Status) {
	var states []Status
	return func(st Status) error { states = append(states, st); return nil }, &states
}

func hasState(states []Status, s State) bool {
	for _, st := range states {
		if st.State == s {
			return true
		}
	}
	return false
}

func lastState(states []Status) State {
	if len(states) == 0 {
		return ""
	}
	return states[len(states)-1].State
}

func TestDriveProbe(t *testing.T) {
	ctx := context.Background()
	tick := time.Millisecond
	all := func(promptPolicy) bool { return true }
	none := func(promptPolicy) bool { return false }
	never := func() bool { return false }
	now := func() bool { return true }
	// after returns a transcript signal that fires once n prompts have been answered.
	after := func(s *fakeScreen, n int) func() bool { return func() bool { return len(s.keys) >= n } }

	t.Run("single prompt is answered, transcript ends it", func(t *testing.T) {
		s := &fakeScreen{frames: []string{trustPane}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, after(s, 1), tick)
		if len(s.keys) != 1 || s.keys[0][0] != "Enter" {
			t.Fatalf("answered keys = %v, want one [Enter]", s.keys)
		}
		if !hasState(*states, StateBlocked) || hasState(*states, StateStuck) {
			t.Fatalf("states = %+v, want blocked and no stuck", *states)
		}
		if lastState(*states) != StateUnknown {
			t.Fatalf("cleared state = %v, want a reset to unknown", lastState(*states))
		}
	})

	t.Run("a sequence of prompts is driven in turn", func(t *testing.T) {
		s := &fakeScreen{frames: []string{trustPane, externalImportPane}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, after(s, 2), tick)
		if len(s.keys) != 2 {
			t.Fatalf("answered %d prompts, want 2 (trust then external-import)", len(s.keys))
		}
		if hasState(*states, StateStuck) {
			t.Fatalf("ended stuck on a driveable sequence: %+v", *states)
		}
		if lastState(*states) != StateUnknown {
			t.Fatalf("cleared state = %v, want a reset to unknown", lastState(*states))
		}
	})

	t.Run("existing transcript returns without driving", func(t *testing.T) {
		s := &fakeScreen{frames: []string{trustPane}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, now, tick)
		if len(s.keys) != 0 || len(*states) != 0 {
			t.Fatalf("drove despite an existing transcript: keys=%v states=%v", s.keys, *states)
		}
	})

	t.Run("unrecognized non-blank screen ends stuck", func(t *testing.T) {
		s := &fakeScreen{frames: []string{"$ random shell output"}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, never, tick)
		if len(s.keys) != 0 {
			t.Fatalf("answered an unrecognized screen: %v", s.keys)
		}
		if lastState(*states) != StateStuck {
			t.Fatalf("state = %v, want stuck", lastState(*states))
		}
	})

	t.Run("blank pane ends stuck", func(t *testing.T) {
		s := &fakeScreen{frames: []string{""}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, never, tick)
		if lastState(*states) != StateStuck {
			t.Fatalf("state = %v, want stuck", lastState(*states))
		}
	})

	t.Run("policy gate surfaces blocked without answering", func(t *testing.T) {
		s := &fakeScreen{frames: []string{trustPane}}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, none, emit, never, tick)
		if len(s.keys) != 0 {
			t.Fatalf("answered a policy-gated prompt: %v", s.keys)
		}
		if lastState(*states) != StateBlocked {
			t.Fatalf("state = %v, want blocked", lastState(*states))
		}
	})

	t.Run("answer error surfaces blocked", func(t *testing.T) {
		s := &fakeScreen{frames: []string{trustPane}, ansErr: context.DeadlineExceeded}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, never, tick)
		if lastState(*states) != StateBlocked {
			t.Fatalf("state = %v, want blocked after answer error", lastState(*states))
		}
	})

	t.Run("persistent capture error surfaces stuck without abandoning", func(t *testing.T) {
		s := &fakeScreen{capErr: context.DeadlineExceeded}
		emit, states := collectStates()
		driveProbe(ctx, "a1", s, all, emit, never, tick)
		if len(s.keys) != 0 {
			t.Fatalf("sent keys despite capture errors: %v", s.keys)
		}
		if lastState(*states) != StateStuck {
			t.Fatalf("state = %v, want stuck (surfaced, not invisible)", lastState(*states))
		}
	})
}
