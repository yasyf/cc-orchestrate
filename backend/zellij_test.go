package backend

import (
	"context"
	"slices"
	"testing"
)

// realPanesJSON is captured verbatim from `zellij --session <s> action list-panes
// --json` (zellij 0.44.1) after spawning a pane named "myagent" running
// `sleep 600`. It is a flat array: a plugin pane, the session's default shell pane
// (no command), and the spawned agent pane (terminal_1).
const realPanesJSON = `[
  {
    "id": 0,
    "is_plugin": true,
    "is_focused": false,
    "title": "(.) - zellij:link",
    "exited": false,
    "exit_status": null,
    "terminal_command": null,
    "plugin_url": "zellij:link",
    "tab_id": 0,
    "tab_name": "Tab #1"
  },
  {
    "id": 0,
    "is_plugin": false,
    "is_focused": true,
    "title": "Pane #1",
    "exited": false,
    "exit_status": null,
    "terminal_command": null,
    "plugin_url": null,
    "tab_id": 0,
    "tab_name": "Tab #1",
    "pane_cwd": "/Users/yasyf/Code/cc-orchestrate"
  },
  {
    "id": 1,
    "is_plugin": false,
    "is_focused": false,
    "title": "myagent",
    "exited": false,
    "exit_status": null,
    "terminal_command": "sleep 600",
    "plugin_url": null,
    "tab_id": 0,
    "tab_name": "Tab #1",
    "pane_cwd": "/private/tmp"
  }
]`

// realSessionsList is captured from `zellij list-sessions --no-formatting --short`:
// one session name per line.
const realSessionsList = "bioqa-build-10366\nbioqa-build-51694\nccorch-verify-zj\n"

type recordedCall struct {
	name string
	args []string
}

func recorder(out string) (*[]recordedCall, runner) {
	calls := &[]recordedCall{}
	r := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		*calls = append(*calls, recordedCall{name: name, args: args})
		return []byte(out), nil
	}
	return calls, r
}

func TestZellijArgv(t *testing.T) {
	ctx := context.Background()
	project := WorkstreamHandle{Backend: "zellij", ID: "proj-1", Name: "Proj 1", Cwd: "/work"}
	tests := []struct {
		name string
		out  string
		call func(b zellij) error
		want []string
	}{
		{
			name: "CreateWorkstream sanitizes the session name",
			call: func(b zellij) error {
				_, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: "My Proj!", Cwd: "/work"})
				return err
			},
			want: []string{"zellij", "attach", "--create-background", "My-Proj-"},
		},
		{
			name: "ListWorkstreams",
			out:  realSessionsList,
			call: func(b zellij) error { _, err := b.ListWorkstreams(ctx); return err },
			want: []string{"zellij", "list-sessions", "--no-formatting", "--short"},
		},
		{
			name: "Spawn places the full argv after --",
			out:  "terminal_1\n",
			call: func(b zellij) error {
				_, err := b.Spawn(ctx, SpawnSpec{
					Workstream: project, Name: "agent", Cwd: "/work",
					Command: []string{"claude", "--dangerously-skip-permissions"}, SessionID: "sess-9",
				})
				return err
			},
			want: []string{
				"zellij", "--session", "proj-1", "action", "new-pane",
				"--cwd", "/work", "--name", "agent", "--",
				"claude", "--dangerously-skip-permissions",
			},
		},
		{
			name: "ListAgents",
			out:  realPanesJSON,
			call: func(b zellij) error { _, err := b.ListAgents(ctx, project); return err },
			want: []string{"zellij", "--session", "proj-1", "action", "list-panes", "--json"},
		},
		{
			name: "Kill targets the pane by id",
			call: func(b zellij) error {
				return b.Kill(ctx, AgentHandle{Backend: "zellij", ID: "terminal_1", WorkstreamID: "proj-1"})
			},
			want: []string{"zellij", "--session", "proj-1", "action", "close-pane", "--pane-id", "terminal_1"},
		},
		{
			name: "Capture dumps the pane screen to stdout by pane id",
			out:  "Do you trust the files in this folder?\n",
			call: func(b zellij) error {
				_, err := b.Capture(ctx, AgentHandle{Backend: "zellij", ID: "terminal_1", WorkstreamID: "proj-1"})
				return err
			},
			want: []string{"zellij", "--session", "proj-1", "action", "dump-screen", "--pane-id", "terminal_1"},
		},
		{
			name: "KillWorkstream",
			call: func(b zellij) error { return b.KillWorkstream(ctx, project) },
			want: []string{"zellij", "kill-session", "proj-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls, r := recorder(tt.out)
			if err := tt.call(zellij{run: r}); err != nil {
				t.Fatalf("call failed: %v", err)
			}
			if len(*calls) != 1 {
				t.Fatalf("want exactly 1 run call, got %d: %v", len(*calls), *calls)
			}
			got := append([]string{(*calls)[0].name}, (*calls)[0].args...)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("argv mismatch:\n got: %v\nwant: %v", got, tt.want)
			}
		})
	}
}

// TestZellijSendText asserts the two-call native send argv: write the characters
// into the pane within its session, then submit with a carriage-return byte.
func TestZellijSendText(t *testing.T) {
	ctx := context.Background()
	calls, r := recorder("")
	agent := AgentHandle{Backend: "zellij", ID: "terminal_1", WorkstreamID: "proj-1"}
	if err := (zellij{run: r}).SendText(ctx, agent, "hi -n there"); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	want := [][]string{
		{"zellij", "--session", "proj-1", "action", "write-chars", "-p", "terminal_1", "--", "hi -n there"},
		{"zellij", "--session", "proj-1", "action", "write", "-p", "terminal_1", "13"},
	}
	if len(*calls) != len(want) {
		t.Fatalf("calls = %v, want %v", *calls, want)
	}
	for i := range want {
		got := append([]string{(*calls)[i].name}, (*calls)[i].args...)
		if !slices.Equal(got, want[i]) {
			t.Fatalf("call %d = %v, want %v", i, got, want[i])
		}
	}
}

func TestZellijSpawnExtractsPaneID(t *testing.T) {
	_, r := recorder("terminal_1\n")
	agent, err := zellij{run: r}.Spawn(context.Background(), SpawnSpec{
		Workstream: WorkstreamHandle{ID: "proj-1"}, Name: "agent", Cwd: "/work",
		Command: []string{"sleep", "600"}, SessionID: "sess-9",
	})
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}
	want := AgentHandle{Backend: "zellij", ID: "terminal_1", WorkstreamID: "proj-1", Name: "agent", SessionID: "sess-9"}
	if agent != want {
		t.Fatalf("agent mismatch:\n got: %+v\nwant: %+v", agent, want)
	}
}

func TestZellijListAgentsParsesRealJSON(t *testing.T) {
	_, r := recorder(realPanesJSON)
	agents, err := zellij{run: r}.ListAgents(context.Background(), WorkstreamHandle{ID: "proj-1"})
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}
	want := []AgentHandle{
		{Backend: "zellij", ID: "terminal_1", WorkstreamID: "proj-1", Name: "myagent"},
	}
	if !slices.Equal(agents, want) {
		t.Fatalf("agents mismatch:\n got: %+v\nwant: %+v", agents, want)
	}
}

func TestZellijListWorkstreamsParsesRealList(t *testing.T) {
	_, r := recorder(realSessionsList)
	projects, err := zellij{run: r}.ListWorkstreams(context.Background())
	if err != nil {
		t.Fatalf("ListWorkstreams failed: %v", err)
	}
	want := []WorkstreamHandle{
		{Backend: "zellij", ID: "bioqa-build-10366", Name: "bioqa-build-10366"},
		{Backend: "zellij", ID: "bioqa-build-51694", Name: "bioqa-build-51694"},
		{Backend: "zellij", ID: "ccorch-verify-zj", Name: "ccorch-verify-zj"},
	}
	if !slices.Equal(projects, want) {
		t.Fatalf("projects mismatch:\n got: %+v\nwant: %+v", projects, want)
	}
}
