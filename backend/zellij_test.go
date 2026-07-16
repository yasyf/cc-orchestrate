package backend

import (
	"context"
	"errors"
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
	r := func(_ context.Context, name string, args ...string) ([]byte, error) {
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

// realExitedPaneJSON is captured from `list-panes --json` after a command pane's child
// exited: zellij holds the pane (exited:true, exit_status:7, is_held:true) with
// terminal_command still populated, so ListAgents still lists it but AgentAlive sees dead.
const realExitedPaneJSON = `[
  {"id":0,"is_plugin":true,"title":"(.) - zellij:link","exited":false,"exit_status":null,"terminal_command":null,"plugin_url":"zellij:link","tab_id":0},
  {"id":1,"is_plugin":false,"title":"myagent","exited":true,"exit_status":7,"is_held":true,"terminal_command":"sh -c exit 7","plugin_url":null,"tab_id":0}
]`

// TestZellijAgentAlive asserts the list-panes argv plus the exited-bit parse: a held
// exited pane reads dead, a running pane reads alive, and a pane absent from the list
// surfaces the not-found error the supervisor reads as "not confirmed dead".
func TestZellijAgentAlive(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		out       string
		paneID    string
		wantAlive bool
		wantErr   bool
	}{
		{name: "running command pane is alive", out: realPanesJSON, paneID: "terminal_1", wantAlive: true},
		{name: "held exited pane is dead", out: realExitedPaneJSON, paneID: "terminal_1", wantAlive: false},
		{name: "vanished pane surfaces the not-found error", out: realPanesJSON, paneID: "terminal_9", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, r := recorder(tc.out)
			alive, err := zellij{run: r}.AgentAlive(ctx, AgentHandle{Backend: "zellij", ID: tc.paneID, WorkstreamID: "proj-1"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
			}
			if err == nil && alive != tc.wantAlive {
				t.Fatalf("alive = %t, want %t", alive, tc.wantAlive)
			}
			want := []string{"zellij", "--session", "proj-1", "action", "list-panes", "--json"}
			got := append([]string{(*calls)[0].name}, (*calls)[0].args...)
			if !slices.Equal(got, want) {
				t.Fatalf("argv = %v, want %v", got, want)
			}
		})
	}
}

// seqRecorder replays per-call output and error in order, so a method issuing several
// zellij calls can be asserted call by call.
func seqRecorder(outs []string, errs []error) (*[]recordedCall, runner) {
	calls := &[]recordedCall{}
	r := func(_ context.Context, name string, args ...string) ([]byte, error) {
		i := len(*calls)
		*calls = append(*calls, recordedCall{name: name, args: args})
		out := ""
		if i < len(outs) {
			out = outs[i]
		}
		var err error
		if i < len(errs) {
			err = errs[i]
		}
		return []byte(out), err
	}
	return calls, r
}

// TestZellijKillWorkstream asserts the conditional cleanup: kill-session always runs,
// then delete-session --force runs only when the session still appears in an
// exact-name list-sessions match — a fresh session that kill-session dropped is left
// alone, while a lingering EXITED stub is force-deleted.
func TestZellijKillWorkstream(t *testing.T) {
	ctx := context.Background()
	killCall := []string{"zellij", "kill-session", "proj-1"}
	listCall := []string{"zellij", "list-sessions", "--no-formatting", "--short"}
	for _, tc := range []struct {
		name    string
		outs    []string
		errs    []error
		wantErr bool
		want    [][]string
	}{
		{
			name: "fresh session vanishes so no delete-session",
			// list after kill has near-miss names but not the exact session.
			outs: []string{"", "proj-10\nproj-1-sub\nother\n"},
			want: [][]string{killCall, listCall},
		},
		{
			name: "lingering stub still listed gets delete-session --force",
			outs: []string{"", "other\nproj-1\n"},
			want: [][]string{killCall, listCall, {"zellij", "delete-session", "--force", "proj-1"}},
		},
		{
			name:    "kill-session failure aborts before listing",
			errs:    []error{errors.New("no such session")},
			wantErr: true,
			want:    [][]string{killCall},
		},
		{
			name:    "list failure after kill surfaces as an error",
			outs:    []string{"", ""},
			errs:    []error{nil, errors.New("daemon unreachable")},
			wantErr: true,
			want:    [][]string{killCall, listCall},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, r := seqRecorder(tc.outs, tc.errs)
			err := zellij{run: r}.KillWorkstream(ctx, WorkstreamHandle{Backend: "zellij", ID: "proj-1", Name: "Proj 1"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
			}
			if len(*calls) != len(tc.want) {
				t.Fatalf("calls = %v, want %v", *calls, tc.want)
			}
			for i := range tc.want {
				got := append([]string{(*calls)[i].name}, (*calls)[i].args...)
				if !slices.Equal(got, tc.want[i]) {
					t.Fatalf("call %d = %v, want %v", i, got, tc.want[i])
				}
			}
		})
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
