package backend

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
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

func (f *cmuxFakeRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
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
			name:    "CreateWorkstream parses the workspace ref from the OK line",
			outputs: [][]byte{[]byte("OK workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return b.CreateWorkstream(context.Background(), WorkstreamSpec{Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix"})
			},
			want:  WorkstreamHandle{Backend: "cmux", ID: "workspace:7", Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix", Worktree: "/tmp/ccorch-fix"},
			calls: []cmuxCall{{name: "cmux", args: []string{"new-workspace", "--cwd", "/tmp/ccorch-fix", "--name", "ccorch-fixture"}}},
		},
		{
			name:    "ListWorkstreams maps ref/title/current_directory from JSON",
			outputs: [][]byte{[]byte(cmuxWorkspacesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListWorkstreams(context.Background())
			},
			want: []WorkstreamHandle{
				{Backend: "cmux", ID: "workspace:7", Name: "ccorch-fixture", Cwd: "/tmp/ccorch-fix"},
				{Backend: "cmux", ID: "workspace:1", Name: "~/C/bioqa", Cwd: "/Users/yasyf/Code/bioqa"},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"list-workspaces", "--json"}}},
		},
		{
			name:    "ListAgents maps selected_surface_ref to agent ids scoped by workspace_ref",
			outputs: [][]byte{[]byte(cmuxPanesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListAgents(context.Background(), WorkstreamHandle{Backend: "cmux", ID: "workspace:7"})
			},
			want: []AgentHandle{
				{Backend: "cmux", ID: "surface:10", WorkstreamID: "workspace:7"},
				{Backend: "cmux", ID: "surface:11", WorkstreamID: "workspace:7"},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"list-panes", "--workspace", "workspace:7", "--json"}}},
		},
		{
			name:    "Capture reads the surface screen as plain text",
			outputs: [][]byte{[]byte("Do you trust the files in this folder?\n")},
			invoke: func(b cmux) (any, error) {
				return b.Capture(context.Background(), AgentHandle{Backend: "cmux", ID: "surface:10", WorkstreamID: "workspace:7"})
			},
			want:  "Do you trust the files in this folder?\n",
			calls: []cmuxCall{{name: "cmux", args: []string{"read-screen", "--workspace", "workspace:7", "--surface", "surface:10"}}},
		},
		{
			name:    "Kill closes the agent surface",
			outputs: [][]byte{[]byte("OK surface:10 workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return nil, b.Kill(context.Background(), AgentHandle{Backend: "cmux", ID: "surface:10", WorkstreamID: "workspace:7"})
			},
			want:  nil,
			calls: []cmuxCall{{name: "cmux", args: []string{"close-surface", "--workspace", "workspace:7", "--surface", "surface:10"}}},
		},
		{
			name:    "KillWorkstream closes the workspace",
			outputs: [][]byte{[]byte("OK workspace:7\n")},
			invoke: func(b cmux) (any, error) {
				return nil, b.KillWorkstream(context.Background(), WorkstreamHandle{Backend: "cmux", ID: "workspace:7"})
			},
			want:  nil,
			calls: []cmuxCall{{name: "cmux", args: []string{"close-workspace", "--workspace", "workspace:7"}}},
		},
		{
			name:    "SendText types the text then submits a separate enter key",
			outputs: [][]byte{[]byte(""), []byte("")},
			invoke: func(b cmux) (any, error) {
				return nil, b.SendText(context.Background(), AgentHandle{Backend: "cmux", ID: "surface:10", WorkstreamID: "workspace:7"}, "hi -n there")
			},
			want: nil,
			calls: []cmuxCall{
				{name: "cmux", args: []string{"send", "--workspace", "workspace:7", "--surface", "surface:10", "--", "hi -n there"}},
				{name: "cmux", args: []string{"send-key", "--workspace", "workspace:7", "--surface", "surface:10", "enter"}},
			},
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

// TestCmuxSpawn proves the launch path: new-pane then a send whose typed text is
// only a metacharacter-free `bash <temp-path>\n`, while the real argv (compact
// JSON, a multi-line brief, and a prompt loaded with shell metacharacters) rides
// a self-removing temp script that round-trips through bash with no injection.
func TestCmuxSpawn(t *testing.T) {
	work := t.TempDir()
	// Stand-in for claude: record each argv element NUL-separated beside itself.
	recorder := filepath.Join(work, "recorder.sh")
	if err := os.WriteFile(recorder, []byte("#!/bin/bash\nprintf '%s\\0' \"$@\" > \"$(dirname \"$0\")/got.bin\"\n"), 0o755); err != nil { //nolint:gosec // G306: test recorder script must be executable
		t.Fatal(err)
	}
	// If any of these markers were interpreted by a shell, the named file appears.
	subst := filepath.Join(work, "PWNED_SUBST")
	backtick := filepath.Join(work, "PWNED_BT")
	semi := filepath.Join(work, "PWNED_SEMI")
	brief := "line1\nline2 with 'quote' and \"dq\" and $(touch " + subst + ") and `touch " + backtick + "`"
	command := []string{
		recorder,
		"--mcp-config", `{"a":"b c"}`,
		"--append-system-prompt", brief,
		"go ahead; touch " + semi,
	}

	f := &cmuxFakeRunner{outputs: [][]byte{
		[]byte("OK surface:10 pane:9 workspace:7\n"),
		[]byte("OK surface:10 workspace:7\n"),
	}}
	got, err := cmux{run: f.run}.Spawn(context.Background(), SpawnSpec{
		Workstream: WorkstreamHandle{Backend: "cmux", ID: "workspace:7"},
		Name:       "agent-1",
		Command:    command,
		SessionID:  "sess-abc",
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	want := AgentHandle{Backend: "cmux", ID: "surface:10", WorkstreamID: "workspace:7", Name: "agent-1", SessionID: "sess-abc"}
	if got != want {
		t.Errorf("handle = %#v, want %#v", got, want)
	}
	if len(f.calls) != 2 {
		t.Fatalf("calls = %#v, want a new-pane then a send", f.calls)
	}
	if wantPane := []string{"new-pane", "--workspace", "workspace:7"}; !reflect.DeepEqual(f.calls[0].args, wantPane) {
		t.Errorf("pane args = %#v, want %#v", f.calls[0].args, wantPane)
	}

	send := f.calls[1].args
	if pre := []string{"send", "--workspace", "workspace:7", "--surface", "surface:10", "--"}; !reflect.DeepEqual(send[:len(pre)], pre) {
		t.Errorf("send prefix = %#v, want %#v", send[:len(pre)], pre)
	}
	sent := send[len(send)-1]

	// The typed text injects nothing: a bash invocation of the temp path plus the
	// documented "\n" Enter, with no real newline or shell metacharacter from argv.
	if !strings.HasPrefix(sent, "bash ") || !strings.HasSuffix(sent, `\n`) {
		t.Fatalf("sent = %q, want `bash <path>\\n`", sent)
	}
	if strings.Contains(sent, "\n") {
		t.Errorf("sent contains a real newline that cmux would type as Enter: %q", sent)
	}
	for _, meta := range []string{"$(", "`", ";", `"`, "PWNED"} {
		if strings.Contains(sent, meta) {
			t.Errorf("sent leaks %q from the argv: %q", meta, sent)
		}
	}

	path := strings.TrimSuffix(strings.TrimPrefix(sent, "bash "), `\n`)
	t.Cleanup(func() { _ = os.Remove(path) })
	script, err := os.ReadFile(path) //nolint:gosec // G304: test reads the temp launch script it just generated
	if err != nil {
		t.Fatalf("launch script: %v", err)
	}
	if !strings.HasPrefix(string(script), "rm -f -- \"$0\"\n") {
		t.Errorf("script does not self-remove first: %q", script)
	}
	// The metacharacters are carried in the file (quoted), never typed.
	if !strings.Contains(string(script), "$(touch "+subst+")") {
		t.Errorf("script dropped the brief's command substitution: %q", script)
	}

	// Drive bash exactly as the pane shell would: type only the path. The argv
	// must reach the recorder intact and nothing must be interpreted as a shell.
	if out, err := exec.CommandContext(context.Background(), "bash", path).CombinedOutput(); err != nil { //nolint:gosec // G204: test runs bash on the temp launch script it just generated
		t.Fatalf("bash launch script: %v: %s", err, out)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("launch script did not remove itself: %v", err)
	}
	for _, marker := range []string{subst, backtick, semi} {
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Errorf("injection executed: %s exists", marker)
		}
	}
	raw, err := os.ReadFile(filepath.Join(work, "got.bin")) //nolint:gosec // G304: test reads a file under its own temp dir
	if err != nil {
		t.Fatalf("recorder output: %v", err)
	}
	gotArgs := bytes.Split(raw, []byte{0})
	gotArgs = gotArgs[:len(gotArgs)-1] // trailing NUL yields an empty tail element
	wantArgs := command[1:]
	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("recorder saw %d args %q, want %d %q", len(gotArgs), gotArgs, len(wantArgs), wantArgs)
	}
	for i, a := range wantArgs {
		if string(gotArgs[i]) != a {
			t.Errorf("arg[%d] = %q, want %q", i, gotArgs[i], a)
		}
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
	if c := b.Caps(); !c.Has(CanSendText) || !c.Has(CanCapture) || !c.Has(CanEnumerate) {
		t.Errorf("Caps() = %#v, want CanSendText+CanCapture+CanEnumerate", c)
	}
	if err := b.EnsureReady(context.Background()); err != nil {
		t.Errorf("EnsureReady() = %v, want nil", err)
	}
}
