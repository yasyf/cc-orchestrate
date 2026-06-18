package ccnotes

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"testing"
)

type recordedCall struct {
	dir  string
	name string
	args []string
}

// recordingRunner returns a runner that records the single call it receives and
// replies with out, so a create helper's argv can be asserted without cc-notes.
func recordingRunner(out string, rec *recordedCall) runner {
	return func(_ context.Context, dir, name string, args ...string) ([]byte, error) {
		rec.dir, rec.name, rec.args = dir, name, args
		return []byte(out), nil
	}
}

// swapSeams installs stub run/lookPath and restores the originals when the test
// ends, so the package globals never leak across tests.
func swapSeams(t *testing.T, r runner, lp func(string) (string, error)) {
	t.Helper()
	origRun, origLook := run, lookPath
	run, lookPath = r, lp
	t.Cleanup(func() { run, lookPath = origRun, origLook })
}

func TestCreateArgv(t *testing.T) {
	// One compact JSON line, the shape every cc-notes add command emits under --json.
	const out = `{"id":"c0b3932915b8486d9f5d90a88444392a4ba4fb1b","status":"active"}`

	for _, tc := range []struct {
		name     string
		invoke   func(ctx context.Context) (string, error)
		wantArgs []string
	}{
		{
			name: "CreateProject",
			invoke: func(ctx context.Context) (string, error) {
				return CreateProject(ctx, "/repo", "Build the thing")
			},
			wantArgs: []string{"project", "add", "Build the thing", "--json"},
		},
		{
			name: "CreateSprint",
			invoke: func(ctx context.Context) (string, error) {
				return CreateSprint(ctx, "/repo", "proj-id", "Sprint 1")
			},
			wantArgs: []string{"sprint", "add", "Sprint 1", "--project", "proj-id", "--json"},
		},
		{
			name: "CreateTask",
			invoke: func(ctx context.Context) (string, error) {
				return CreateTask(ctx, "/repo", "Do the work", "feature/x", "sprint-id", "proj-id")
			},
			wantArgs: []string{
				"task", "add", "Do the work",
				"--branch", "feature/x",
				"--sprint", "sprint-id",
				"--project", "proj-id",
				"--no-validation-criteria",
				"--json",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec recordedCall
			swapSeams(t, recordingRunner(out, &rec), exec.LookPath)
			id, err := tc.invoke(context.Background())
			if err != nil {
				t.Fatalf("invoke: %v", err)
			}
			if id != "c0b3932915b8486d9f5d90a88444392a4ba4fb1b" {
				t.Errorf("id = %q, want the full hex id from --json", id)
			}
			if rec.dir != "/repo" {
				t.Errorf("dir = %q, want /repo (cc-notes is scoped by cwd)", rec.dir)
			}
			if rec.name != bin {
				t.Errorf("binary = %q, want %q", rec.name, bin)
			}
			if !slices.Equal(rec.args, tc.wantArgs) {
				t.Errorf("argv = %v, want %v", rec.args, tc.wantArgs)
			}
		})
	}
}

// TestParseIDRejectsMissingID proves a create surfaces a clear error when cc-notes
// emits a DTO with no id, rather than silently returning an empty binding.
func TestParseIDRejectsMissingID(t *testing.T) {
	swapSeams(t, recordingRunner(`{"status":"active"}`, &recordedCall{}), exec.LookPath)
	if _, err := CreateProject(context.Background(), "/repo", "x"); err == nil {
		t.Fatal("CreateProject err = nil, want a no-id error")
	}
}

func TestEnabled(t *testing.T) {
	present := func(string) (string, error) { return "/usr/local/bin/cc-notes", nil }
	absent := func(string) (string, error) { return "", errors.New("not found") }

	for _, tc := range []struct {
		name    string
		look    func(string) (string, error)
		gitOut  string
		gitErr  error
		want    bool
		wantRun bool // whether the git ref probe must have run
	}{
		{name: "binary absent short-circuits", look: absent, want: false, wantRun: false},
		{name: "installed with refs", look: present, gitOut: "refs/cc-notes/projects/abc\n", want: true, wantRun: true},
		{name: "installed without refs", look: present, gitOut: "", want: false, wantRun: true},
		{name: "installed but git fails", look: present, gitErr: errors.New("not a git repo"), want: false, wantRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec recordedCall
			ran := false
			swapSeams(t, func(_ context.Context, dir, name string, args ...string) ([]byte, error) {
				ran = true
				rec.dir, rec.name, rec.args = dir, name, args
				return []byte(tc.gitOut), tc.gitErr
			}, tc.look)

			if got := Enabled(context.Background(), "/repo"); got != tc.want {
				t.Errorf("Enabled = %v, want %v", got, tc.want)
			}
			if ran != tc.wantRun {
				t.Fatalf("git ref probe ran = %v, want %v", ran, tc.wantRun)
			}
			if tc.wantRun {
				wantArgs := []string{"for-each-ref", "--count=1", "refs/cc-notes/"}
				if rec.name != "git" || !slices.Equal(rec.args, wantArgs) || rec.dir != "/repo" {
					t.Errorf("probe = %s %v in %q, want git %v in /repo", rec.name, rec.args, rec.dir, wantArgs)
				}
			}
		})
	}
}
