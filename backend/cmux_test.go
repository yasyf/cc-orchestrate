package backend

import (
	"context"
	"reflect"
	"testing"
)

// cmuxCall records one runner invocation so a test can assert the exact argv a
// driver method builds.
type cmuxCall struct {
	name string
	args []string
}

// cmuxFakeRunner is a runner stub: it records every call and returns the next
// queued (real, captured) output in order.
type cmuxFakeRunner struct {
	calls   []cmuxCall
	outputs [][]byte
}

func (f *cmuxFakeRunner) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	out := f.outputs[len(f.calls)]
	f.calls = append(f.calls, cmuxCall{name: name, args: args})
	return out, nil
}

// Real captured `cmux list-workspaces --json` output (default ref id-format).
const cmuxWorkspacesJSON = `{
  "window_ref" : "window:1",
  "workspaces" : [
    {
      "current_directory" : "/tmp/ccorch-fix",
      "index" : 0,
      "pinned" : false,
      "ref" : "workspace:7",
      "selected" : false,
      "title" : "ccorch-fixture"
    },
    {
      "current_directory" : "/Users/yasyf/Code/bioqa",
      "index" : 1,
      "pinned" : false,
      "ref" : "workspace:1",
      "selected" : true,
      "title" : "~/C/bioqa"
    }
  ]
}`

// Real captured `cmux list-panes --workspace workspace:7 --json` output.
const cmuxPanesJSON = `{
  "panes" : [
    {
      "focused" : true,
      "index" : 0,
      "ref" : "pane:9",
      "selected_surface_ref" : "surface:10",
      "surface_count" : 1,
      "surface_refs" : [ "surface:10" ]
    },
    {
      "focused" : false,
      "index" : 1,
      "ref" : "pane:10",
      "selected_surface_ref" : "surface:11",
      "surface_count" : 1,
      "surface_refs" : [ "surface:11" ]
    }
  ],
  "window_ref" : "window:1",
  "workspace_ref" : "workspace:7"
}`

func TestCmux(t *testing.T) {
	cases := []struct {
		name    string
		outputs [][]byte
		invoke  func(b cmux) (any, error)
		want    any
		calls   []cmuxCall
	}{
		{
			name:    "CreateProject parses the workspace ref from the OK line",
			outputs: [][]byte{[]byte("OK workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return b.CreateProject(context.Background(), ProjectSpec{Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix"})
			},
			want:  ProjectHandle{Backend: "cmux", ID: "workspace:7", Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix"},
			calls: []cmuxCall{{name: "cmux", args: []string{"new-workspace", "--cwd", "/tmp/ccorch-fix", "--name", "ccorch-fixture"}}},
		},
		{
			name:    "ListProjects maps ref/title/current_directory from JSON",
			outputs: [][]byte{[]byte(cmuxWorkspacesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListProjects(context.Background())
			},
			want: []ProjectHandle{
				{Backend: "cmux", ID: "workspace:7", Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix"},
				{Backend: "cmux", ID: "workspace:1", Name: "~/C/bioqa", Cwd: "/Users/yasyf/Code/bioqa"},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"list-workspaces", "--json"}}},
		},
		{
			name: "Spawn opens a pane then sends the command with a trailing newline",
			outputs: [][]byte{
				[]byte("OK surface:10 pane:9 workspace:7\n"),
				[]byte("OK surface:10 workspace:7\n"),
			},
			invoke: func(b cmux) (any, error) {
				return b.Spawn(context.Background(), SpawnSpec{
					Project:   ProjectHandle{Backend: "cmux", ID: "workspace:7"},
					Name:      "agent-1",
					Command:   []string{"echo", "hi"},
					SessionID: "sess-abc",
				})
			},
			want: AgentHandle{Backend: "cmux", ID: "surface:10", ProjectID: "workspace:7", Name: "agent-1", SessionID: "sess-abc"},
			calls: []cmuxCall{
				{name: "cmux", args: []string{"new-pane", "--workspace", "workspace:7"}},
				{name: "cmux", args: []string{"send", "--workspace", "workspace:7", "--surface", "surface:10", "--", `echo hi\n`}},
			},
		},
		{
			name:    "ListAgents maps selected_surface_ref to agent ids scoped by workspace_ref",
			outputs: [][]byte{[]byte(cmuxPanesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListAgents(context.Background(), ProjectHandle{Backend: "cmux", ID: "workspace:7"})
			},
			want: []AgentHandle{
				{Backend: "cmux", ID: "surface:10", ProjectID: "workspace:7"},
				{Backend: "cmux", ID: "surface:11", ProjectID: "workspace:7"},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"list-panes", "--workspace", "workspace:7", "--json"}}},
		},
		{
			name:    "Kill closes the agent surface",
			outputs: [][]byte{[]byte("OK surface:10 workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return nil, b.Kill(context.Background(), AgentHandle{Backend: "cmux", ID: "surface:10", ProjectID: "workspace:7"})
			},
			want:  nil,
			calls: []cmuxCall{{name: "cmux", args: []string{"close-surface", "--workspace", "workspace:7", "--surface", "surface:10"}}},
		},
		{
			name:    "KillProject closes the workspace",
			outputs: [][]byte{[]byte("OK workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return nil, b.KillProject(context.Background(), ProjectHandle{Backend: "cmux", ID: "workspace:7"})
			},
			want:  nil,
			calls: []cmuxCall{{name: "cmux", args: []string{"close-workspace", "--workspace", "workspace:7"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &cmuxFakeRunner{outputs: tc.outputs}
			got, err := tc.invoke(cmux{run: f.run})
			if err != nil {
				t.Fatalf("invoke: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("result = %#v, want %#v", got, tc.want)
			}
			if !reflect.DeepEqual(f.calls, tc.calls) {
				t.Errorf("calls = %#v, want %#v", f.calls, tc.calls)
			}
		})
	}
}

func TestCmuxRefMissing(t *testing.T) {
	if _, err := cmuxRef([]byte("OK pane:9 workspace:7\n"), "surface:"); err == nil {
		t.Fatal("expected an error when no surface ref is present")
	}
}

func TestCmuxStatics(t *testing.T) {
	b := cmux{run: execRunner}
	if b.Name() != "cmux" {
		t.Errorf("Name() = %q, want cmux", b.Name())
	}
	if b.Caps() != (Caps{SendText: true, Capture: true}) {
		t.Errorf("Caps() = %#v", b.Caps())
	}
	if err := b.EnsureReady(context.Background()); err != nil {
		t.Errorf("EnsureReady() = %v, want nil", err)
	}
}
