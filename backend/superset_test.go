package backend

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Fixtures captured verbatim from superset v0.2.22 against a live, authenticated
// host service (status, auth whoami, projects list --local, workspaces list
// --local); values are trimmed to the fields the driver reads. The workspace and
// terminal create fixtures are modeled on those verified element shapes
// (`workspaces create` returns a workspace, `terminals create --json` returns
// {terminalId}) rather than captured live, since creating them has side effects.
const (
	supersetStatusJSON = `{
  "running": true,
  "healthy": true,
  "pid": 1524,
  "port": 48505,
  "endpoint": "http://127.0.0.1:48505",
  "organizationId": "02b83abb-9da1-44e6-9170-ff67488df839",
  "hostId": "1c81b26bf35dee5ff6bd704bd8578d66",
  "hostName": "yasyf",
  "uptimeSec": 81495
}`

	supersetWhoamiJSON = `{
  "userId": "1c9a2ce4-5ab0-46b5-957e-801c44b44fc2",
  "email": "yasyfm@gmail.com",
  "name": "Yasyf Mohamedali",
  "organizationId": "02b83abb-9da1-44e6-9170-ff67488df839",
  "organizationName": "Yasyf Mohamedali's Team",
  "authSource": "oauth"
}`

	supersetProjectsJSON = `[
  {"id":"98228586-8a1e-494e-b73b-2c5352422812","name":"bioqa","slug":"bioqa","repoCloneUrl":"https://github.com/anetaco/backend","githubRepositoryId":"9bb2d389-fc48-4b80-b427-81a392938c5b","setUp":"yes","path":"/Users/yasyf/Code/bioqa"},
  {"id":"48f92b66-fbd7-473f-a7ad-6b8e583e933a","name":"cc-orchestrate","slug":"cc-orchestrate","repoCloneUrl":"https://github.com/yasyf/cc-orchestrate","githubRepositoryId":null,"setUp":"yes","path":"/Users/yasyf/Code/cc-orchestrate"},
  {"id":"a036a3fb-f75d-4f9a-ab8b-9e1a6c5e72e6","name":"claude-pool","slug":"claude-pool","repoCloneUrl":null,"githubRepositoryId":null,"setUp":"yes","path":"/Users/yasyf/Code/claude-pool"}
]`

	supersetWorkspacesJSON = `[
  {"id":"99b1c139-7250-4cd9-9b40-fda16963d665","name":"main","branch":"main","projectId":"a036a3fb-f75d-4f9a-ab8b-9e1a6c5e72e6","projectName":"claude-pool","hostId":"1c81b26bf35dee5ff6bd704bd8578d66","type":"main","createdAt":"2026-06-06T01:15:08.199Z","hostName":"yasyf"},
  {"id":"c4f1ce2a-16f8-4006-866e-53b83bc1006a","name":"yasyf/expensive-tilapia","branch":"yasyf/expensive-tilapia","projectId":"98228586-8a1e-494e-b73b-2c5352422812","projectName":"bioqa","hostId":"1c81b26bf35dee5ff6bd704bd8578d66","type":"worktree","createdAt":"2026-06-06T06:03:00.086Z","hostName":"yasyf"}
]`

	supersetWorkspaceCreateJSON = `{"id":"d1e2f3a4-0000-4aaa-bbbb-ccccddddeeee","name":"cc-orch-test","branch":"main","projectId":"48f92b66-fbd7-473f-a7ad-6b8e583e933a","projectName":"cc-orchestrate","hostId":"1c81b26bf35dee5ff6bd704bd8578d66","type":"worktree","createdAt":"2026-06-16T00:00:00.000Z","hostName":"yasyf"}`

	supersetTerminalCreateJSON = `{"terminalId":"term_9f8e7d6c5b4a"}`
)

// supersetRunner is a fake runner that records every argv it receives and replays
// per-call canned output, so a method making several CLI calls can be asserted in
// order.
type supersetRunner struct {
	calls [][]string
	outs  []string
	errs  []error
}

func (r *supersetRunner) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	i := len(r.calls)
	r.calls = append(r.calls, append([]string{name}, args...))
	out := ""
	if i < len(r.outs) {
		out = r.outs[i]
	}
	var err error
	if i < len(r.errs) {
		err = r.errs[i]
	}
	return []byte(out), err
}

func assertCalls(t *testing.T, got, want [][]string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calls =\n  %v\nwant\n  %v", got, want)
	}
}

func TestSupersetMetadata(t *testing.T) {
	b := superset{}
	if b.Name() != "superset" {
		t.Errorf("Name() = %q, want superset", b.Name())
	}
	if got := b.Caps(); got != (Caps{}) {
		t.Errorf("Caps() = %+v, want spawn-only (no SendText/Capture)", got)
	}
}

func TestSupersetEnsureReady(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		outs      []string
		errs      []error
		wantErr   bool
		wantCalls [][]string
	}{
		{
			name: "ready when running, healthy, and authenticated",
			outs: []string{supersetStatusJSON, supersetWhoamiJSON},
			wantCalls: [][]string{
				{"superset", "status", "--json"},
				{"superset", "auth", "whoami", "--json"},
			},
		},
		{
			name:      "errors when the host service is not healthy",
			outs:      []string{`{"running":true,"healthy":false}`},
			wantErr:   true,
			wantCalls: [][]string{{"superset", "status", "--json"}},
		},
		{
			name:      "errors when the status command fails",
			errs:      []error{errors.New("connection refused")},
			wantErr:   true,
			wantCalls: [][]string{{"superset", "status", "--json"}},
		},
		{
			name:    "errors when whoami returns no identity",
			outs:    []string{supersetStatusJSON, `{}`},
			wantErr: true,
			wantCalls: [][]string{
				{"superset", "status", "--json"},
				{"superset", "auth", "whoami", "--json"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &supersetRunner{outs: tc.outs, errs: tc.errs}
			err := superset{run: r.run}.EnsureReady(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
			}
			assertCalls(t, r.calls, tc.wantCalls)
		})
	}
}

func TestSupersetCreateProjectExistingProject(t *testing.T) {
	cwd := "/Users/yasyf/Code/cc-orchestrate"
	r := &supersetRunner{outs: []string{
		supersetProjectsJSON,        // projects list --local --json
		"feature/login\n",           // git rev-parse
		supersetWorkspaceCreateJSON, // workspaces create
	}}
	got, err := superset{run: r.run}.CreateProject(context.Background(), ProjectSpec{Name: "cc-orch-test", Cwd: cwd})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	assertCalls(t, r.calls, [][]string{
		{"superset", "projects", "list", "--local", "--json"},
		{"git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"},
		{"superset", "workspaces", "create", "--local",
			"--project", "48f92b66-fbd7-473f-a7ad-6b8e583e933a",
			"--branch", "feature/login", "--name", "cc-orch-test", "--json"},
	})
	want := ProjectHandle{Backend: "superset", ID: "d1e2f3a4-0000-4aaa-bbbb-ccccddddeeee", Name: "cc-orch-test", Cwd: cwd}
	if got != want {
		t.Fatalf("handle = %+v, want %+v", got, want)
	}
}

func TestSupersetCreateProjectImportsWhenMissing(t *testing.T) {
	cwd := "/Users/yasyf/Code/brand-new"
	listWith := `[{"id":"new-proj-id","name":"brand-new","slug":"brand-new","setUp":"yes","path":"/Users/yasyf/Code/brand-new"}]`
	r := &supersetRunner{outs: []string{
		supersetProjectsJSON,        // list: cwd absent
		`{"id":"new-proj-id"}`,      // setup --import
		listWith,                    // list: cwd now present
		"main\n",                    // git rev-parse
		supersetWorkspaceCreateJSON, // workspaces create
	}}
	got, err := superset{run: r.run}.CreateProject(context.Background(), ProjectSpec{Name: "brand-new", Cwd: cwd})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	assertCalls(t, r.calls, [][]string{
		{"superset", "projects", "list", "--local", "--json"},
		{"superset", "projects", "setup", "--import", cwd, "--local", "--json"},
		{"superset", "projects", "list", "--local", "--json"},
		{"git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"},
		{"superset", "workspaces", "create", "--local",
			"--project", "new-proj-id", "--branch", "main", "--name", "brand-new", "--json"},
	})
	if got.ID != "d1e2f3a4-0000-4aaa-bbbb-ccccddddeeee" {
		t.Fatalf("workspace id = %q, want d1e2f3a4-...", got.ID)
	}
}

func TestSupersetListProjectsParsesRealJSON(t *testing.T) {
	r := &supersetRunner{outs: []string{supersetWorkspacesJSON}}
	got, err := superset{run: r.run}.ListProjects(context.Background())
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	assertCalls(t, r.calls, [][]string{{"superset", "workspaces", "list", "--local", "--json"}})
	want := []ProjectHandle{
		{Backend: "superset", ID: "99b1c139-7250-4cd9-9b40-fda16963d665", Name: "main"},
		{Backend: "superset", ID: "c4f1ce2a-16f8-4006-866e-53b83bc1006a", Name: "yasyf/expensive-tilapia"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("projects = %+v, want %+v", got, want)
	}
}

func TestSupersetSpawn(t *testing.T) {
	ctx := context.Background()
	project := ProjectHandle{Backend: "superset", ID: "ws-1"}

	t.Run("absolute claude path is wrapped and quoted", func(t *testing.T) {
		r := &supersetRunner{outs: []string{supersetTerminalCreateJSON}}
		got, err := superset{run: r.run}.Spawn(ctx, SpawnSpec{
			Project:   project,
			Name:      "agent-a",
			Cwd:       "/work",
			Command:   []string{"/Users/yasyf/.local/bin/claude", "--session-id", "sess-1", "-p", "hello world"},
			SessionID: "sess-1",
		})
		if err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		wantCmd := `bash -lc '/Users/yasyf/.local/bin/claude --session-id sess-1 -p \'hello world\''`
		assertCalls(t, r.calls, [][]string{{
			"superset", "terminals", "create",
			"--workspace", "ws-1", "--cwd", "/work",
			"--command", wantCmd, "--json",
		}})
		want := AgentHandle{Backend: "superset", ID: "term_9f8e7d6c5b4a", ProjectID: "ws-1", Name: "agent-a", SessionID: "sess-1"}
		if got != want {
			t.Fatalf("agent = %+v, want %+v", got, want)
		}
	})

	t.Run("bare claude resolves to the real binary", func(t *testing.T) {
		orig := resolveClaude
		resolveClaude = func() (string, error) { return "/opt/claude", nil }
		defer func() { resolveClaude = orig }()
		r := &supersetRunner{outs: []string{supersetTerminalCreateJSON}}
		if _, err := (superset{run: r.run}).Spawn(ctx, SpawnSpec{
			Project: project, Name: "agent-b", Cwd: "/work",
			Command: []string{"claude", "--session-id", "s2", "-p", "hi"}, SessionID: "s2",
		}); err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		assertCalls(t, r.calls, [][]string{{
			"superset", "terminals", "create",
			"--workspace", "ws-1", "--cwd", "/work",
			"--command", `bash -lc '/opt/claude --session-id s2 -p hi'`, "--json",
		}})
	})

	t.Run("resolveClaude failure aborts before any CLI call", func(t *testing.T) {
		orig := resolveClaude
		resolveClaude = func() (string, error) { return "", errors.New("no claude") }
		defer func() { resolveClaude = orig }()
		r := &supersetRunner{}
		if _, err := (superset{run: r.run}).Spawn(ctx, SpawnSpec{
			Project: project, Name: "agent-c", Cwd: "/work", Command: []string{"claude", "-p", "hi"},
		}); err == nil {
			t.Fatal("Spawn: want error, got nil")
		}
		if len(r.calls) != 0 {
			t.Fatalf("want no CLI calls, got %v", r.calls)
		}
	})
}

func TestSupersetKill(t *testing.T) {
	ctx := context.Background()
	t.Run("kills by session id after a -- guard", func(t *testing.T) {
		r := &supersetRunner{}
		if err := (superset{run: r.run}).Kill(ctx, AgentHandle{SessionID: "sess-1"}); err != nil {
			t.Fatalf("Kill: %v", err)
		}
		assertCalls(t, r.calls, [][]string{{"pkill", "-f", "--", "--session-id sess-1"}})
	})
	t.Run("errors without a session id", func(t *testing.T) {
		r := &supersetRunner{}
		if err := (superset{run: r.run}).Kill(ctx, AgentHandle{}); err == nil {
			t.Fatal("Kill: want error, got nil")
		}
		if len(r.calls) != 0 {
			t.Fatalf("want no CLI calls, got %v", r.calls)
		}
	})
}

func TestSupersetKillProject(t *testing.T) {
	r := &supersetRunner{outs: []string{`{"deleted":["ws-1"]}`}}
	if err := (superset{run: r.run}).KillProject(context.Background(), ProjectHandle{ID: "ws-1"}); err != nil {
		t.Fatalf("KillProject: %v", err)
	}
	assertCalls(t, r.calls, [][]string{{"superset", "workspaces", "delete", "ws-1", "--local", "--json"}})
}

func TestSupersetListAgentsIsEmptyAndQuiet(t *testing.T) {
	r := &supersetRunner{}
	got, err := superset{run: r.run}.ListAgents(context.Background(), ProjectHandle{ID: "ws-1"})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("agents = %+v, want none", got)
	}
	if len(r.calls) != 0 {
		t.Fatalf("want no CLI calls, got %v", r.calls)
	}
}

func TestMatchProjectID(t *testing.T) {
	projects := []supersetProject{
		{ID: "root", Path: "/Users/yasyf/Code"},
		{ID: "orch", Path: "/Users/yasyf/Code/cc-orchestrate"},
		{ID: "other", Path: "/Users/yasyf/Code/bioqa"},
		{ID: "empty", Path: ""},
	}
	cases := []struct{ name, cwd, want string }{
		{"exact match", "/Users/yasyf/Code/cc-orchestrate", "orch"},
		{"nearest ancestor wins", "/Users/yasyf/Code/cc-orchestrate/backend", "orch"},
		{"falls back to shallow ancestor", "/Users/yasyf/Code/scratch", "root"},
		{"no match", "/tmp/elsewhere", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchProjectID(projects, tc.cwd); got != tc.want {
				t.Fatalf("matchProjectID(%q) = %q, want %q", tc.cwd, got, tc.want)
			}
		})
	}
}

func TestSupersetGitBranch(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		out     string
		err     error
		want    string
		wantErr bool
	}{
		{name: "uses the checked-out branch", out: "feature/login\n", want: "feature/login"},
		{name: "defaults to main on detached HEAD", out: "HEAD\n", want: "main"},
		{name: "defaults to main on empty output", out: "\n", want: "main"},
		{name: "propagates a git execution failure", err: errors.New("not a repo"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &supersetRunner{outs: []string{tc.out}, errs: []error{tc.err}}
			got, err := (superset{run: r.run}).gitBranch(ctx, "/work")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("gitBranch err = nil, want a wrapped git failure")
				}
			} else if err != nil {
				t.Fatalf("gitBranch err = %v, want nil", err)
			} else if got != tc.want {
				t.Fatalf("gitBranch = %q, want %q", got, tc.want)
			}
			assertCalls(t, r.calls, [][]string{{"git", "-C", "/work", "rev-parse", "--abbrev-ref", "HEAD"}})
		})
	}
}

func TestShellQuote(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"empty becomes quoted empty", "", "''"},
		{"bare word", "claude", "claude"},
		{"absolute path is safe", "/Users/yasyf/.local/bin/claude", "/Users/yasyf/.local/bin/claude"},
		{"short flag", "-p", "-p"},
		{"long flag", "--session-id", "--session-id"},
		{"spaces are quoted", "hello world", "'hello world'"},
		{"single quote is escaped", "it's", `'it'\''s'`},
		{"leading-quote token", "a'b", `'a'\''b'`},
		{"dollar is quoted literally", "$HOME", "'$HOME'"},
		{"shell metachars are quoted", "a&b|c;d", "'a&b|c;d'"},
		{"equals is safe", "key=val", "key=val"},
		{"comma and colon are safe", "a,b:c", "a,b:c"},
		{"percent is safe", "100%", "100%"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShellQuote(tc.in); got != tc.want {
				t.Fatalf("ShellQuote(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFishQuote(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"empty", "", "''"},
		{"bare word is still wrapped", "claude", "'claude'"},
		{"spaces", "a b", "'a b'"},
		{"single quote backslash-escaped", "it's", `'it\'s'`},
		{"backslash doubled", `back\slash`, `'back\\slash'`},
		{"dollar stays literal", "x$Y", "'x$Y'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fishQuote(tc.in); got != tc.want {
				t.Fatalf("fishQuote(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestWrapBashLogin(t *testing.T) {
	cases := []struct {
		name string
		cmd  []string
		want string
	}{
		{"simple argv", []string{"claude", "-p", "hi"}, `bash -lc 'claude -p hi'`},
		{"inner spaces get nested quoting", []string{"/abs/claude", "--session-id", "s1", "-p", "hello world"}, `bash -lc '/abs/claude --session-id s1 -p \'hello world\''`},
		{"embedded single quote", []string{"echo", "a'b"}, `bash -lc 'echo \'a\'\\\'\'b\''`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wrapBashLogin(tc.cmd); got != tc.want {
				t.Fatalf("wrapBashLogin(%v) =\n  %q\nwant\n  %q", tc.cmd, got, tc.want)
			}
		})
	}
}

// TestShellQuoteRoundTripThroughBash proves the inner (bash -lc) quoting survives
// a real bash parse: printf echoes each argument back unchanged.
func TestShellQuoteRoundTripThroughBash(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH")
	}
	args := []string{"a b", "it's", "x$Y", "semi;colon", "amp&", `quote"d`, `back\slash`, "100%", "tab\tchar"}
	cmd := append([]string{"printf", `%s\n`}, args...)
	quoted := make([]string, len(cmd))
	for i, tok := range cmd {
		quoted[i] = ShellQuote(tok)
	}
	out, err := exec.Command(bash, "-c", strings.Join(quoted, " ")).Output()
	if err != nil {
		t.Fatalf("bash: %v", err)
	}
	if want := strings.Join(args, "\n") + "\n"; string(out) != want {
		t.Fatalf("round trip =\n  %q\nwant\n  %q", string(out), want)
	}
}

// TestWrapBashLoginRoundTripThroughFish proves the full two-level wrapping survives
// the actual outer login shell (fish) plus the inner bash -lc reparse.
func TestWrapBashLoginRoundTripThroughFish(t *testing.T) {
	fish, err := exec.LookPath("fish")
	if err != nil {
		t.Skip("fish not on PATH")
	}
	args := []string{"a b", "it's", "x$Y", "semi;colon", "amp&", `quote"d`, `back\slash`}
	cmd := append([]string{"printf", `%s\n`}, args...)
	out, err := exec.Command(fish, "--no-config", "-c", wrapBashLogin(cmd)).Output()
	if err != nil {
		t.Fatalf("fish: %v", err)
	}
	if want := strings.Join(args, "\n") + "\n"; string(out) != want {
		t.Fatalf("round trip =\n  %q\nwant\n  %q", string(out), want)
	}
}
