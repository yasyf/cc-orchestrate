package backend

import (
	"context"
	"errors"
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

func (r *tmuxRecorder) run(_ context.Context, name string, args ...string) ([]byte, error) {
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
				want := WorkstreamHandle{Backend: "tmux", ID: "my_proj_1-0c9a453f", Name: "my.proj:1", Cwd: "/work", Worktree: "/work"}
				if got != want {
					t.Fatalf("handle = %+v, want %+v", got, want)
				}
			},
			wantCalls: [][]string{{"tmux", "new-session", "-d", "-s", "my_proj_1-0c9a453f", "-c", "/work"}},
		},
		{
			name: "CreateWorkstream disambiguates same-name workstreams by cwd",
			do: func(t *testing.T, b tmux) {
				a, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: "main", Cwd: "/repos/a"})
				if err != nil {
					t.Fatalf("CreateWorkstream(a): %v", err)
				}
				repo2, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: "main", Cwd: "/repos/b"})
				if err != nil {
					t.Fatalf("CreateWorkstream(b): %v", err)
				}
				if a.ID == repo2.ID {
					t.Fatalf("two repos' primary workstreams both named %q collided on session id %q", "main", a.ID)
				}
			},
			wantCalls: [][]string{
				{"tmux", "new-session", "-d", "-s", "main-95b628e2", "-c", "/repos/a"},
				{"tmux", "new-session", "-d", "-s", "main-5199bb15", "-c", "/repos/b"},
			},
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
		{
			name: "Capture reads the pane screen as joined plain text",
			out:  "Do you trust the files in this folder?\n",
			do: func(t *testing.T, b tmux) {
				got, err := b.Capture(ctx, AgentHandle{Backend: "tmux", ID: "%3"})
				if err != nil {
					t.Fatalf("Capture: %v", err)
				}
				if got != "Do you trust the files in this folder?\n" {
					t.Fatalf("screen = %q", got)
				}
			},
			wantCalls: [][]string{{"tmux", "capture-pane", "-p", "-J", "-t", "%3"}},
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
	if !c.Has(CanSendText) || !c.Has(CanCapture) || !c.Has(CanEnumerate) {
		t.Fatalf("Caps = %+v, want CanSendText+CanCapture+CanEnumerate", c)
	}
}

func TestTmuxAgentAlive(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		out       string
		err       error
		wantAlive bool
		wantErr   bool
	}{
		{name: "live pane", out: "0\n", wantAlive: true},
		{name: "dead pane under remain-on-exit", out: "1\n", wantAlive: false},
		{name: "vanished pane surfaces the error", err: errors.New("can't find pane"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &tmuxRecorder{out: tc.out, err: tc.err}
			alive, err := (tmux{run: rec.run}).AgentAlive(ctx, AgentHandle{ID: "%3"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
			}
			if err == nil && alive != tc.wantAlive {
				t.Fatalf("alive = %t, want %t", alive, tc.wantAlive)
			}
			want := [][]string{{"tmux", "display-message", "-p", "-t", "%3", "#{pane_dead}"}}
			if !reflect.DeepEqual(rec.calls, want) {
				t.Fatalf("argv = %v, want %v", rec.calls, want)
			}
		})
	}
}

// TestTmuxAttachArgv asserts the focus pre-steps target the pane id and the returned
// argv attaches this terminal to the agent's session, and that a pre-step failure
// propagates without yielding an argv.
func TestTmuxAttachArgv(t *testing.T) {
	ctx := context.Background()
	agent := AgentHandle{Backend: "tmux", ID: "%3", WorkstreamID: "proj_one"}

	t.Run("focuses the pane then returns the attach-session argv", func(t *testing.T) {
		rec := &tmuxRecorder{}
		argv, err := (tmux{run: rec.run}).AttachArgv(ctx, agent)
		if err != nil {
			t.Fatalf("AttachArgv: %v", err)
		}
		wantCalls := [][]string{
			{"tmux", "select-window", "-t", "%3"},
			{"tmux", "select-pane", "-t", "%3"},
		}
		if !reflect.DeepEqual(rec.calls, wantCalls) {
			t.Fatalf("pre-step calls = %v, want %v", rec.calls, wantCalls)
		}
		want := []string{"tmux", "attach-session", "-t", "proj_one"}
		if !reflect.DeepEqual(argv, want) {
			t.Fatalf("argv = %v, want %v", argv, want)
		}
	})

	t.Run("a focus pre-step error propagates", func(t *testing.T) {
		rec := &tmuxRecorder{err: errors.New("can't find pane")}
		argv, err := (tmux{run: rec.run}).AttachArgv(ctx, agent)
		if err == nil {
			t.Fatal("AttachArgv err = nil, want the pre-step error")
		}
		if argv != nil {
			t.Fatalf("argv = %v, want nil on error", argv)
		}
	})
}
