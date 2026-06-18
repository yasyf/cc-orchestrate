package backend

import (
	"context"
	"reflect"
	"testing"
)

// tmuxRecorder is a fake runner that records every argv it is handed and replays
// canned output captured from real tmux 3.6a.
type tmuxRecorder struct {
	calls [][]string
	out   string
	err   error
}

func (r *tmuxRecorder) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	return []byte(r.out), r.err
}

func TestTmux(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		out       string
		do        func(t *testing.T, b tmux)
		wantCalls [][]string
	}{
		{
			name: "EnsureReady issues no command",
			do: func(t *testing.T, b tmux) {
				if err := b.EnsureReady(ctx); err != nil {
					t.Fatalf("EnsureReady: %v", err)
				}
			},
			wantCalls: nil,
		},
		{
			name: "CreateWorkstream sanitizes session name for id and targeting",
			do: func(t *testing.T, b tmux) {
				got, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: "my.proj:1", Cwd: "/work"})
				if err != nil {
					t.Fatalf("CreateWorkstream: %v", err)
				}
				want := WorkstreamHandle{Backend: "tmux", ID: "my_proj_1", Name: "my.proj:1", Cwd: "/work", Worktree: "/work"}
				if got != want {
					t.Fatalf("handle = %+v, want %+v", got, want)
				}
			},
			wantCalls: [][]string{{"tmux", "new-session", "-d", "-s", "my_proj_1", "-c", "/work"}},
		},
		{
			name: "ListWorkstreams parses one session name per line",
			out:  "proj_one\nproj_two\n",
			do: func(t *testing.T, b tmux) {
				got, err := b.ListWorkstreams(ctx)
				if err != nil {
					t.Fatalf("ListWorkstreams: %v", err)
				}
				want := []WorkstreamHandle{
					{Backend: "tmux", ID: "proj_one", Name: "proj_one"},
					{Backend: "tmux", ID: "proj_two", Name: "proj_two"},
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("projects = %+v, want %+v", got, want)
				}
			},
			wantCalls: [][]string{{"tmux", "list-sessions", "-F", "#{session_name}"}},
		},
		{
			name: "Spawn appends command after -- and parses the printed pane id",
			out:  "%3\n",
			do: func(t *testing.T, b tmux) {
				got, err := b.Spawn(ctx, SpawnSpec{
					Workstream: WorkstreamHandle{Backend: "tmux", ID: "proj_one"},
					Name:       "agent-a",
					Cwd:        "/work",
					Command:    []string{"claude", "--session-id", "sid-123"},
					SessionID:  "sid-123",
				})
				if err != nil {
					t.Fatalf("Spawn: %v", err)
				}
				want := AgentHandle{Backend: "tmux", ID: "%3", WorkstreamID: "proj_one", Name: "agent-a", SessionID: "sid-123"}
				if got != want {
					t.Fatalf("handle = %+v, want %+v", got, want)
				}
			},
			wantCalls: [][]string{{
				"tmux", "new-window", "-d", "-P", "-F", "#{pane_id}",
				"-t", "proj_one", "-n", "agent-a", "-c", "/work", "--",
				"claude", "--session-id", "sid-123",
			}},
		},
		{
			name: "ListAgents parses pane id and window name pairs",
			out:  "%0\tzsh\n%2\tagent-a\n%3\tagent-b\n",
			do: func(t *testing.T, b tmux) {
				got, err := b.ListAgents(ctx, WorkstreamHandle{Backend: "tmux", ID: "proj_one"})
				if err != nil {
					t.Fatalf("ListAgents: %v", err)
				}
				want := []AgentHandle{
					{Backend: "tmux", ID: "%0", WorkstreamID: "proj_one", Name: "zsh"},
					{Backend: "tmux", ID: "%2", WorkstreamID: "proj_one", Name: "agent-a"},
					{Backend: "tmux", ID: "%3", WorkstreamID: "proj_one", Name: "agent-b"},
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("agents = %+v, want %+v", got, want)
				}
			},
			wantCalls: [][]string{{"tmux", "list-panes", "-s", "-t", "proj_one", "-F", "#{pane_id}\t#{window_name}"}},
		},
		{
			name: "Kill targets the pane id",
			do: func(t *testing.T, b tmux) {
				if err := b.Kill(ctx, AgentHandle{Backend: "tmux", ID: "%3"}); err != nil {
					t.Fatalf("Kill: %v", err)
				}
			},
			wantCalls: [][]string{{"tmux", "kill-pane", "-t", "%3"}},
		},
		{
			name: "KillWorkstream targets the session id",
			do: func(t *testing.T, b tmux) {
				if err := b.KillWorkstream(ctx, WorkstreamHandle{Backend: "tmux", ID: "proj_one"}); err != nil {
					t.Fatalf("KillWorkstream: %v", err)
				}
			},
			wantCalls: [][]string{{"tmux", "kill-session", "-t", "proj_one"}},
		},
		{
			name: "SendText types the text literally then submits with Enter",
			do: func(t *testing.T, b tmux) {
				if err := b.SendText(ctx, AgentHandle{Backend: "tmux", ID: "%3"}, "hi -n there"); err != nil {
					t.Fatalf("SendText: %v", err)
				}
			},
			wantCalls: [][]string{
				{"tmux", "send-keys", "-t", "%3", "-l", "--", "hi -n there"},
				{"tmux", "send-keys", "-t", "%3", "Enter"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &tmuxRecorder{out: tc.out}
			b := tmux{run: rec.run}
			tc.do(t, b)
			if !reflect.DeepEqual(rec.calls, tc.wantCalls) {
				t.Fatalf("argv =\n  %v\nwant\n  %v", rec.calls, tc.wantCalls)
			}
		})
	}
}

func TestTmuxCaps(t *testing.T) {
	c := (tmux{}).Caps()
	if !c.Has(CanSendText) || !c.Has(CanEnumerate) || c.Has(CanCapture) {
		t.Fatalf("Caps = %+v, want CanSendText+CanEnumerate (no CanCapture)", c)
	}
}
