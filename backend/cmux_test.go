package backend

import (
	"bytes"
	"context"
	"errors"
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

// cmuxFakeRunner is a runner stub: it records every call and replays per-call
// (real, captured) output and error in order. A short outputs/errs slice yields nil
// for the trailing calls, so a success run needs no error entries.
type cmuxFakeRunner struct {
	calls   []cmuxCall
	outputs [][]byte
	errs    []error
}

func (f *cmuxFakeRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	i := len(f.calls)
	f.calls = append(f.calls, cmuxCall{name: name, args: args})
	var out []byte
	if i < len(f.outputs) {
		out = f.outputs[i]
	}
	var err error
	if i < len(f.errs) {
		err = f.errs[i]
	}
	return out, err
}

// The workspace and surface UUIDs below are captured live from cmux (workspace
// create + new-pane under --id-format uuids); the throwaway probe workspace was
// closed afterward, and bioqa is a pre-existing workspace left untouched.
const (
	cmuxWSUUID    = "6C9AFD15-F200-4AF9-A655-F2FA55848728"
	cmuxBioqaUUID = "54A8AD1A-E06C-48B9-813D-93456A339D32"
	cmuxSurfShell = "016506FB-AEBE-418A-A77B-FD5CAA3AAFC3"
	cmuxSurfAgent = "8710BE32-E5AB-41F2-B376-8B3A7124CF5B"
)

// Real captured `cmux --id-format both list-workspaces --json`: each workspace
// carries both its short ref and its stable UUID id — the shape CreateWorkstream's
// ref-to-UUID lookup reads.
const cmuxWorkspacesBothJSON = `{
  "window_id" : "C2697DBC-DEDF-4975-B04D-5C067A8FD3E1",
  "window_ref" : "window:1",
  "workspaces" : [
    {
      "current_directory" : "/tmp/ccorch-cmux-probe",
      "id" : "6C9AFD15-F200-4AF9-A655-F2FA55848728",
      "index" : 0,
      "pinned" : false,
      "ref" : "workspace:2",
      "selected" : false,
      "title" : "ccorch-probe"
    },
    {
      "current_directory" : "/Users/yasyf/Code/bioqa",
      "id" : "54A8AD1A-E06C-48B9-813D-93456A339D32",
      "index" : 1,
      "pinned" : false,
      "ref" : "workspace:1",
      "selected" : true,
      "title" : "~/C/bioqa"
    }
  ]
}`

// Real captured `cmux --id-format uuids list-workspaces --json`: id is the UUID and
// there is no ref field — the shape ListWorkstreams reads.
const cmuxWorkspacesJSON = `{
  "window_id" : "C2697DBC-DEDF-4975-B04D-5C067A8FD3E1",
  "workspaces" : [
    {
      "current_directory" : "/tmp/ccorch-cmux-probe",
      "id" : "6C9AFD15-F200-4AF9-A655-F2FA55848728",
      "index" : 0,
      "pinned" : false,
      "selected" : false,
      "title" : "ccorch-probe"
    },
    {
      "current_directory" : "/Users/yasyf/Code/bioqa",
      "id" : "54A8AD1A-E06C-48B9-813D-93456A339D32",
      "index" : 1,
      "pinned" : false,
      "selected" : true,
      "title" : "~/C/bioqa"
    }
  ]
}`

// cmuxWorkspacesAfterCloseJSON is the uuids list once the probe workspace is closed:
// only the untouched bioqa workspace remains — a successful KillWorkstream verification.
const cmuxWorkspacesAfterCloseJSON = `{
  "window_id" : "C2697DBC-DEDF-4975-B04D-5C067A8FD3E1",
  "workspaces" : [
    {
      "current_directory" : "/Users/yasyf/Code/bioqa",
      "id" : "54A8AD1A-E06C-48B9-813D-93456A339D32",
      "index" : 0,
      "pinned" : false,
      "selected" : true,
      "title" : "~/C/bioqa"
    }
  ]
}`

// cmuxWorkspacesEmptyJSON is the uuids list when closing the last workspace emptied
// the window; the killed workspace is absent, so KillWorkstream still succeeds.
const cmuxWorkspacesEmptyJSON = `{
  "window_id" : "C2697DBC-DEDF-4975-B04D-5C067A8FD3E1",
  "workspaces" : []
}`

// Real captured `cmux --id-format uuids list-panes --workspace <uuid> --json`:
// panes carry a selected_surface_id UUID scoped by the top-level workspace_id.
const cmuxPanesJSON = `{
  "panes" : [
    {
      "focused" : true,
      "id" : "5AA87959-362C-488B-B0E9-29337A256183",
      "index" : 0,
      "selected_surface_id" : "016506FB-AEBE-418A-A77B-FD5CAA3AAFC3",
      "surface_count" : 1,
      "surface_ids" : [ "016506FB-AEBE-418A-A77B-FD5CAA3AAFC3" ]
    },
    {
      "focused" : false,
      "id" : "0598EE26-478D-4C0E-88ED-D7BF5C4316E6",
      "index" : 1,
      "selected_surface_id" : "8710BE32-E5AB-41F2-B376-8B3A7124CF5B",
      "surface_count" : 1,
      "surface_ids" : [ "8710BE32-E5AB-41F2-B376-8B3A7124CF5B" ]
    }
  ],
  "window_id" : "C2697DBC-DEDF-4975-B04D-5C067A8FD3E1",
  "workspace_id" : "6C9AFD15-F200-4AF9-A655-F2FA55848728"
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
			name:    "CreateWorkstream resolves the short ref to the workspace UUID",
			outputs: [][]byte{[]byte("OK workspace:2\n"), []byte(cmuxWorkspacesBothJSON)},
			invoke: func(b cmux) (any, error) {
				return b.CreateWorkstream(context.Background(), WorkstreamSpec{Name: "ccorch-probe", Cwd: "/tmp/ccorch-cmux-probe"})
			},
			want: WorkstreamHandle{Backend: "cmux", ID: cmuxWSUUID, Name: "ccorch-probe", Cwd: "/tmp/ccorch-cmux-probe", Worktree: "/tmp/ccorch-cmux-probe"},
			calls: []cmuxCall{
				{name: "cmux", args: []string{"new-workspace", "--cwd", "/tmp/ccorch-cmux-probe", "--name", "ccorch-probe"}},
				{name: "cmux", args: []string{"--id-format", "both", "list-workspaces", "--json"}},
			},
		},
		{
			name:    "ListWorkstreams maps id/title/current_directory from uuids JSON",
			outputs: [][]byte{[]byte(cmuxWorkspacesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListWorkstreams(context.Background())
			},
			want: []WorkstreamHandle{
				{Backend: "cmux", ID: cmuxWSUUID, Name: "ccorch-probe", Cwd: "/tmp/ccorch-cmux-probe"},
				{Backend: "cmux", ID: cmuxBioqaUUID, Name: "~/C/bioqa", Cwd: "/Users/yasyf/Code/bioqa"},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"--id-format", "uuids", "list-workspaces", "--json"}}},
		},
		{
			name:    "ListAgents maps selected_surface_id to agent ids scoped by workspace_id",
			outputs: [][]byte{[]byte(cmuxPanesJSON)},
			invoke: func(b cmux) (any, error) {
				return b.ListAgents(context.Background(), WorkstreamHandle{Backend: "cmux", ID: cmuxWSUUID})
			},
			want: []AgentHandle{
				{Backend: "cmux", ID: cmuxSurfShell, WorkstreamID: cmuxWSUUID},
				{Backend: "cmux", ID: cmuxSurfAgent, WorkstreamID: cmuxWSUUID},
			},
			calls: []cmuxCall{{name: "cmux", args: []string{"--id-format", "uuids", "list-panes", "--workspace", cmuxWSUUID, "--json"}}},
		},
		{
			name:    "Capture reads the surface screen as plain text",
			outputs: [][]byte{[]byte("hello probe\n")},
			invoke: func(b cmux) (any, error) {
				return b.Capture(context.Background(), AgentHandle{Backend: "cmux", ID: cmuxSurfAgent, WorkstreamID: cmuxWSUUID})
			},
			want:  "hello probe\n",
			calls: []cmuxCall{{name: "cmux", args: []string{"--id-format", "uuids", "read-screen", "--workspace", cmuxWSUUID, "--surface", cmuxSurfAgent}}},
		},
		{
			name:    "Kill closes the agent surface",
			outputs: [][]byte{[]byte("OK " + cmuxSurfAgent + " " + cmuxWSUUID + "\n")},
			invoke: func(b cmux) (any, error) {
				return nil, b.Kill(context.Background(), AgentHandle{Backend: "cmux", ID: cmuxSurfAgent, WorkstreamID: cmuxWSUUID})
			},
			want:  nil,
			calls: []cmuxCall{{name: "cmux", args: []string{"--id-format", "uuids", "close-surface", "--workspace", cmuxWSUUID, "--surface", cmuxSurfAgent}}},
		},
		{
			name:    "SendText types the text then submits a separate enter key",
			outputs: [][]byte{[]byte(""), []byte("")},
			invoke: func(b cmux) (any, error) {
				return nil, b.SendText(context.Background(), AgentHandle{Backend: "cmux", ID: cmuxSurfAgent, WorkstreamID: cmuxWSUUID}, "hi -n there")
			},
			want: nil,
			calls: []cmuxCall{
				{name: "cmux", args: []string{"--id-format", "uuids", "send", "--workspace", cmuxWSUUID, "--surface", cmuxSurfAgent, "--", "hi -n there"}},
				{name: "cmux", args: []string{"--id-format", "uuids", "send-key", "--workspace", cmuxWSUUID, "--surface", cmuxSurfAgent, "enter"}},
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

// TestCmuxSpawn proves the launch path: new-pane then a respawn-pane whose --command is
// only a metacharacter-free `bash <temp-path>`, while the real argv (compact JSON, a
// multi-line brief, and a prompt loaded with shell metacharacters) rides a self-removing
// temp script that execs through bash with no injection, making the agent the surface's
// top-level process.
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
		[]byte("OK " + cmuxSurfAgent + " 0598EE26-478D-4C0E-88ED-D7BF5C4316E6 " + cmuxWSUUID + "\n"),
		[]byte("OK " + cmuxSurfAgent + " " + cmuxWSUUID + "\n"),
	}}
	got, err := cmux{run: f.run}.Spawn(context.Background(), SpawnSpec{
		Workstream: WorkstreamHandle{Backend: "cmux", ID: cmuxWSUUID},
		Name:       "agent-1",
		Command:    command,
		SessionID:  "sess-abc",
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	want := AgentHandle{Backend: "cmux", ID: cmuxSurfAgent, WorkstreamID: cmuxWSUUID, Name: "agent-1", SessionID: "sess-abc"}
	if got != want {
		t.Errorf("handle = %#v, want %#v", got, want)
	}
	if len(f.calls) != 2 {
		t.Fatalf("calls = %#v, want a new-pane then a respawn-pane", f.calls)
	}
	if wantPane := []string{"--id-format", "uuids", "new-pane", "--workspace", cmuxWSUUID}; !reflect.DeepEqual(f.calls[0].args, wantPane) {
		t.Errorf("pane args = %#v, want %#v", f.calls[0].args, wantPane)
	}

	respawn := f.calls[1].args
	if pre := []string{"--id-format", "uuids", "respawn-pane", "--workspace", cmuxWSUUID, "--surface", cmuxSurfAgent, "--command"}; !reflect.DeepEqual(respawn[:len(pre)], pre) {
		t.Errorf("respawn-pane prefix = %#v, want %#v", respawn[:len(pre)], pre)
	}
	sent := respawn[len(respawn)-1]

	// The command injects nothing: a bash invocation of the temp path, with no real
	// newline or shell metacharacter from the argv reaching the command line.
	if !strings.HasPrefix(sent, "bash ") {
		t.Fatalf("command = %q, want `bash <path>`", sent)
	}
	if strings.ContainsAny(sent, "\n\r\t") {
		t.Errorf("command leaks a control character from the argv: %q", sent)
	}
	for _, meta := range []string{"$(", "`", ";", `"`, "PWNED"} {
		if strings.Contains(sent, meta) {
			t.Errorf("command leaks %q from the argv: %q", meta, sent)
		}
	}

	path := strings.TrimPrefix(sent, "bash ")
	t.Cleanup(func() { _ = os.Remove(path) })
	script, err := os.ReadFile(path) //nolint:gosec // G304: test reads the temp launch script it just generated
	if err != nil {
		t.Fatalf("launch script: %v", err)
	}
	// Restore PATH, self-remove, then exec the agent so it replaces bash as the
	// surface's top-level process.
	wantPrefix := "export PATH=" + ShellQuote(os.Getenv("PATH")) + "\nrm -f -- \"$0\"\nexec "
	if !strings.HasPrefix(string(script), wantPrefix) {
		t.Errorf("script does not restore PATH then self-remove then exec: %q", script)
	}
	// The metacharacters are carried in the file (quoted), never on the command line.
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

// TestCmuxKillWorkstream asserts the two-step close-then-verify path: close-workspace
// followed by a uuids list that confirms the workspace is gone. Because cmux declines
// to close the last workspace in a window, the driver treats a workspace still present
// after close as an error rather than a silent success.
func TestCmuxKillWorkstream(t *testing.T) {
	const okClose = "OK workspace:2\n"
	closeCall := cmuxCall{name: "cmux", args: []string{"--id-format", "uuids", "close-workspace", "--workspace", cmuxWSUUID}}
	listCall := cmuxCall{name: "cmux", args: []string{"--id-format", "uuids", "list-workspaces", "--json"}}
	cases := []struct {
		name    string
		outputs [][]byte
		errs    []error
		wantErr string
		calls   []cmuxCall
	}{
		{
			name:    "close then verify gone with other workspaces remaining",
			outputs: [][]byte{[]byte(okClose), []byte(cmuxWorkspacesAfterCloseJSON)},
			calls:   []cmuxCall{closeCall, listCall},
		},
		{
			name:    "closing the last workspace empties the window and still succeeds",
			outputs: [][]byte{[]byte(okClose), []byte(cmuxWorkspacesEmptyJSON)},
			calls:   []cmuxCall{closeCall, listCall},
		},
		{
			name:    "a refused close-workspace propagates its error",
			errs:    []error{errors.New("cmux: cannot close the last workspace in a window")},
			wantErr: "cannot close the last workspace",
			calls:   []cmuxCall{closeCall},
		},
		{
			name:    "a failed verification list surfaces as an error",
			outputs: [][]byte{[]byte(okClose), nil},
			errs:    []error{nil, errors.New("socket not found")},
			wantErr: "verify workspace",
			calls:   []cmuxCall{closeCall, listCall},
		},
		{
			name:    "a workspace still present after close is reported",
			outputs: [][]byte{[]byte(okClose), []byte(cmuxWorkspacesJSON)},
			wantErr: "still present after close-workspace",
			calls:   []cmuxCall{closeCall, listCall},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &cmuxFakeRunner{outputs: tc.outputs, errs: tc.errs}
			err := cmux{run: f.run}.KillWorkstream(context.Background(), WorkstreamHandle{Backend: "cmux", ID: cmuxWSUUID})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("KillWorkstream: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("KillWorkstream error = %v, want containing %q", err, tc.wantErr)
			}
			if !reflect.DeepEqual(f.calls, tc.calls) {
				t.Errorf("calls = %#v, want %#v", f.calls, tc.calls)
			}
		})
	}
}

// TestCmuxLaunchScriptRestoresHostilePath proves the export-PATH line round-trips a
// PATH holding spaces, single quotes, or nothing: the script restores exactly the
// daemon's PATH before exec, whatever characters it carries.
func TestCmuxLaunchScriptRestoresHostilePath(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash not found: %v", err)
	}
	for _, tc := range []struct{ name, path string }{
		{"spaces", "/opt/my bin:/usr/bin"},
		{"single quotes", "/opt/o'brien/bin:/usr/bin"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PATH", tc.path)
			out := filepath.Join(t.TempDir(), "path.out")
			// Absolute argv so the exec resolves under a hostile or empty PATH; it
			// writes back the PATH the script restored before the exec.
			launch, err := cmuxLaunchScript([]string{"/bin/sh", "-c", `printf '%s' "$PATH" > ` + ShellQuote(out)})
			if err != nil {
				t.Fatalf("cmuxLaunchScript: %v", err)
			}
			script := strings.TrimPrefix(launch, "bash ")
			t.Cleanup(func() { _ = os.Remove(script) })
			if runOut, err := exec.CommandContext(context.Background(), bashPath, script).CombinedOutput(); err != nil { //nolint:gosec // G204: test runs the temp launch script it just generated
				t.Fatalf("run launch script: %v: %s", err, runOut)
			}
			got, err := os.ReadFile(out) //nolint:gosec // G304: test reads a file under its own temp dir
			if err != nil {
				t.Fatalf("read restored PATH: %v", err)
			}
			if string(got) != tc.path {
				t.Errorf("restored PATH = %q, want %q", got, tc.path)
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
	if c := b.Caps(); !c.Has(CanSendText) || !c.Has(CanCapture) || !c.Has(CanEnumerate) {
		t.Errorf("Caps() = %#v, want CanSendText+CanCapture+CanEnumerate", c)
	}
	if err := b.EnsureReady(context.Background()); err != nil {
		t.Errorf("EnsureReady() = %v, want nil", err)
	}
}
