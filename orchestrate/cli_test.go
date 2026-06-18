package orchestrate

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/consume"
	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/store"
	"github.com/yasyf/cc-interact/subject"
)

// TestWorkstreamCommandTree asserts the `workstream` group (alias `ws`) carries
// its four subcommands and their flags, and that `agent spawn` gained --workstream
// alongside --repo. The tree is built from Root() and never executes a RunE, so it
// needs no daemon.
func TestWorkstreamCommandTree(t *testing.T) {
	root := Root()

	ws, _, err := root.Find([]string{"workstream"})
	if err != nil || ws.Name() != "workstream" {
		t.Fatalf("Find(workstream) = %v, %v", ws, err)
	}
	if !ws.HasAlias("ws") {
		t.Errorf("workstream missing alias ws; aliases=%v", ws.Aliases)
	}

	for _, tc := range []struct {
		sub   string
		flags []string
	}{
		{"list", []string{"repo"}},
		{"create", []string{"repo", "branch"}},
		{"activate", []string{"repo"}},
		{"kill", []string{"repo"}},
	} {
		t.Run("workstream "+tc.sub, func(t *testing.T) {
			sub, _, err := root.Find([]string{"workstream", tc.sub})
			if err != nil || sub.Name() != tc.sub {
				t.Fatalf("Find(workstream %s) = %v, %v", tc.sub, sub, err)
			}
			for _, f := range tc.flags {
				if sub.Flags().Lookup(f) == nil {
					t.Errorf("workstream %s missing --%s flag", tc.sub, f)
				}
			}
		})
	}

	spawn, _, err := root.Find([]string{"agent", "spawn"})
	if err != nil || spawn.Name() != "spawn" {
		t.Fatalf("Find(agent spawn) = %v, %v", spawn, err)
	}
	for _, f := range []string{"repo", "workstream", "sprint"} {
		if spawn.Flags().Lookup(f) == nil {
			t.Errorf("agent spawn missing --%s flag", f)
		}
	}
}

// TestSprintCommandTree asserts the `sprint` group carries its three subcommands,
// each gated by a --workstream flag. The tree is built from Root() and never
// executes a RunE, so it needs no daemon.
func TestSprintCommandTree(t *testing.T) {
	root := Root()

	sprint, _, err := root.Find([]string{"sprint"})
	if err != nil || sprint.Name() != "sprint" {
		t.Fatalf("Find(sprint) = %v, %v", sprint, err)
	}

	for _, tc := range []struct {
		sub   string
		flags []string
	}{
		{"list", []string{"workstream"}},
		{"create", []string{"workstream"}},
		{"activate", []string{"workstream"}},
	} {
		t.Run("sprint "+tc.sub, func(t *testing.T) {
			sub, _, err := root.Find([]string{"sprint", tc.sub})
			if err != nil || sub.Name() != tc.sub {
				t.Fatalf("Find(sprint %s) = %v, %v", tc.sub, sub, err)
			}
			for _, f := range tc.flags {
				if sub.Flags().Lookup(f) == nil {
					t.Errorf("sprint %s missing --%s flag", tc.sub, f)
				}
			}
		})
	}
}

// TestAgentWatchObservesReport proves the parent watch streams an agent's
// orchestrate.report event (Origin=agent). streamAgent builds its StreamSource with
// a zero-value ExcludeOrigin, so the observer sees every origin; under cc-interact
// v0.1.1 that zero value disguised a hardcoded exclude_origin=agent and silently
// dropped this exact frame. It seeds the report through the real handler + the
// s.Append chokepoint, then drives the identical consume path against the daemon's
// SSE plane.
func TestAgentWatchObservesReport(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()

	s, err := daemon.New(daemon.Config{
		AppName:        AppName,
		Paths:          appPaths(),
		Version:        Version,
		ActiveStatuses: []string{"active"},
		WindowAlive:    func(int) bool { return true },
		Migrate:        migrate,
	})
	if err != nil {
		t.Fatalf("daemon.New: %v", err)
	}
	db := s.DB()

	subjects := subject.Resolver{Store: store.NewSubjectStore(db, []string{"active"})}
	sub, _, err := subjects.Start(ctx, subject.Window{Session: "child-sid"}, "/work", "agent-child-sid", lifecycle, true)
	if err != nil {
		t.Fatalf("Start subject: %v", err)
	}

	// Seed an OriginAgent EventReport through the real handler and the s.Append
	// chokepoint, so the streamed frame carries the exact orchestrate.report payload
	// a live agent emits.
	reply := handleReport(daemon.HandlerCtx{
		Ctx:      ctx,
		Env:      daemon.Envelope{Session: "child-sid", Body: mustJSON(t, map[string]string{"text": "halfway done", "state": "working"})},
		Window:   subject.Window{Session: "child-sid"},
		Scope:    "/work",
		Subjects: subjects, DB: db, Append: s.Append,
	})
	if !reply.OK {
		t.Fatalf("handleReport: %s", reply.Error)
	}

	ts := httptest.NewServer(s.Mux())
	t.Cleanup(ts.Close)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse %q: %v", ts.URL, err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parse port from %q: %v", ts.URL, err)
	}

	// The identical StreamSource streamAgent builds: a zero-value ExcludeOrigin, so
	// the observer streams agent-origin frames.
	src := consume.StreamSource{
		Port: port, SubjectID: sub.ID, Consumer: watchConsumer, ClaudePID: os.Getpid(),
		Paths: appPaths(), WindowAlive: windowAlive,
	}

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var got string
	if err := consume.ConsumeEvents(cctx, src, func(_ int64, data string) (bool, error) {
		got = data
		return true, nil
	}); err != nil {
		t.Fatalf("ConsumeEvents: %v", err)
	}

	var pl reportPayload
	if err := json.Unmarshal([]byte(got), &pl); err != nil {
		t.Fatalf("frame %q is not a report payload: %v", got, err)
	}
	if pl.Type != EventReport {
		t.Fatalf("frame type = %q, want %q (the agent-origin frame the old exclude_origin=agent dropped)", pl.Type, EventReport)
	}
	if pl.Text != "halfway done" || pl.State != "working" {
		t.Fatalf("report payload = %+v, want text=halfway done state=working", pl)
	}
}
