package backend

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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

func (r *supersetRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
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
	got := b.Caps()
	if got.Has(CanSendText) || got.Has(CanCapture) {
		t.Errorf("Caps() = %+v, want no CanSendText/CanCapture", got)
	}
	if !got.Has(CanEnumerate) {
		t.Errorf("Caps() = %+v, want CanEnumerate", got)
	}
	if !got.Has(ManagesWorktree) {
		t.Errorf("Caps() = %+v, want ManagesWorktree", got)
	}
}

// serveFakePtyDaemon listens on socket and answers one hello/list exchange with the
// given sessions, exercising the real frame codec end to end (a real ephemeral
// socket, not a mock). The returned func stops the listener and joins the goroutine.
func serveFakePtyDaemon(t *testing.T, socket string, sessions []supersetSession) func() {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen %s: %v", socket, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := readSupersetFrame(conn); err != nil { // hello
			return
		}
		if err := writeSupersetFrame(conn, map[string]any{"type": "hello-ack", "protocol": supersetDaemonProtocol, "daemonVersion": "0.2.4"}); err != nil {
			return
		}
		if _, err := readSupersetFrame(conn); err != nil { // list
			return
		}
		_ = writeSupersetFrame(conn, supersetFrame{Type: "list-reply", Sessions: sessions})
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}

func TestSupersetListAgents(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ptyd.sock")
	stop := serveFakePtyDaemon(t, socket, []supersetSession{
		{ID: "term-alive-1", PID: 100, Alive: true},
		{ID: "term-dead", PID: 0, Alive: false},
		{ID: "term-alive-2", PID: 200, Alive: true},
	})
	defer stop()

	orig := supersetDaemonSocketPath
	supersetDaemonSocketPath = func() (string, error) { return socket, nil }
	t.Cleanup(func() { supersetDaemonSocketPath = orig })

	got, err := (superset{}).ListAgents(context.Background(), WorkstreamHandle{})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	ids := make([]string, len(got))
	for i, h := range got {
		ids[i] = h.ID
	}
	if want := []string{"term-alive-1", "term-alive-2"}; !slices.Equal(ids, want) {
		t.Fatalf("ListAgents ids = %v, want %v (only alive sessions)", ids, want)
	}
}

// stubWorktreeBase pins supersetWorktreeBase to base for the duration of the test,
// so the worktree path CreateWorkstream derives is deterministic without depending
// on the invoking user's home.
func stubWorktreeBase(t *testing.T, base string) {
	t.Helper()
	orig := supersetWorktreeBase
	supersetWorktreeBase = func() (string, error) { return base, nil }
	t.Cleanup(func() { supersetWorktreeBase = orig })
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

func TestSupersetCreateWorkstreamExistingProject(t *testing.T) {
	stubWorktreeBase(t, "/wt")
	cwd := "/Users/yasyf/Code/cc-orchestrate"
	r := &supersetRunner{outs: []string{
		supersetProjectsJSON,        // projects list --local --json
		supersetWorkspaceCreateJSON, // workspaces create
	}}
	got, err := superset{run: r.run}.CreateWorkstream(context.Background(), WorkstreamSpec{Name: "cc-orch-test", Cwd: cwd, RepoCwd: cwd, Branch: "feature/login"})
	if err != nil {
		t.Fatalf("CreateWorkstream: %v", err)
	}
	assertCalls(t, r.calls, [][]string{
		{"superset", "projects", "list", "--local", "--json"},
		{
			"superset", "workspaces", "create", "--local",
			"--project", "48f92b66-fbd7-473f-a7ad-6b8e583e933a",
			"--branch", "feature/login", "--name", "cc-orch-test", "--json",
		},
	})
	want := WorkstreamHandle{
		Backend: "superset", ID: "d1e2f3a4-0000-4aaa-bbbb-ccccddddeeee", Name: "cc-orch-test", Cwd: cwd,
		Worktree: "/wt/48f92b66-fbd7-473f-a7ad-6b8e583e933a/feature/login",
	}
	if got != want {
		t.Fatalf("handle = %+v, want %+v", got, want)
	}
}

func TestSupersetCreateWorkstreamImportsWhenMissing(t *testing.T) {
	stubWorktreeBase(t, "/wt")
	cwd := "/Users/yasyf/Code/brand-new"
	listWith := `[{"id":"new-proj-id","name":"brand-new","slug":"brand-new","setUp":"yes","path":"/Users/yasyf/Code/brand-new"}]`
	r := &supersetRunner{outs: []string{
		supersetProjectsJSON,        // list: cwd absent
		`{"id":"new-proj-id"}`,      // setup --import
		listWith,                    // list: cwd now present
		supersetWorkspaceCreateJSON, // workspaces create
	}}
	got, err := superset{run: r.run}.CreateWorkstream(context.Background(), WorkstreamSpec{Name: "brand-new", Cwd: cwd, RepoCwd: cwd, Branch: "main"})
	if err != nil {
		t.Fatalf("CreateWorkstream: %v", err)
	}
	assertCalls(t, r.calls, [][]string{
		{"superset", "projects", "list", "--local", "--json"},
		{"superset", "projects", "setup", "--import", cwd, "--local", "--json"},
		{"superset", "projects", "list", "--local", "--json"},
		{
			"superset", "workspaces", "create", "--local",
			"--project", "new-proj-id", "--branch", "main", "--name", "brand-new", "--json",
		},
	})
	if got.ID != "d1e2f3a4-0000-4aaa-bbbb-ccccddddeeee" {
		t.Fatalf("workspace id = %q, want d1e2f3a4-...", got.ID)
	}
	if want := "/wt/new-proj-id/main"; got.Worktree != want {
		t.Fatalf("worktree = %q, want %q", got.Worktree, want)
	}
}

// TestSupersetCreateWorkstreamRequiresBranch proves CreateWorkstream fails loud on
// an empty branch (the superset CLI rejects a workspace create without one) before
// any CLI call, rather than silently defaulting.
func TestSupersetCreateWorkstreamRequiresBranch(t *testing.T) {
	r := &supersetRunner{}
	if _, err := (superset{run: r.run}).CreateWorkstream(context.Background(), WorkstreamSpec{Name: "x", Cwd: "/work"}); err == nil {
		t.Fatal("CreateWorkstream: want error for empty branch, got nil")
	}
	if len(r.calls) != 0 {
		t.Fatalf("want no CLI calls before the branch check, got %v", r.calls)
	}
}

func TestSupersetListWorkstreamsParsesRealJSON(t *testing.T) {
	r := &supersetRunner{outs: []string{supersetWorkspacesJSON}}
	got, err := superset{run: r.run}.ListWorkstreams(context.Background())
	if err != nil {
		t.Fatalf("ListWorkstreams: %v", err)
	}
	assertCalls(t, r.calls, [][]string{{"superset", "workspaces", "list", "--local", "--json"}})
	want := []WorkstreamHandle{
		{Backend: "superset", ID: "99b1c139-7250-4cd9-9b40-fda16963d665", Name: "main"},
		{Backend: "superset", ID: "c4f1ce2a-16f8-4006-866e-53b83bc1006a", Name: "yasyf/expensive-tilapia"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("workstreams = %+v, want %+v", got, want)
	}
}

func TestSupersetSpawn(t *testing.T) {
	ctx := context.Background()
	workstream := WorkstreamHandle{Backend: "superset", ID: "ws-1"}

	t.Run("absolute claude path is wrapped and quoted", func(t *testing.T) {
		r := &supersetRunner{outs: []string{supersetTerminalCreateJSON}}
		got, err := superset{run: r.run}.Spawn(ctx, SpawnSpec{
			Workstream: workstream,
			Name:       "agent-a",
			Cwd:        "/work",
			Command:    []string{"/Users/yasyf/.local/bin/claude", "--session-id", "sess-1", "-p", "hello world"},
			SessionID:  "sess-1",
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
		want := AgentHandle{Backend: "superset", ID: "term_9f8e7d6c5b4a", WorkstreamID: "ws-1", Name: "agent-a", SessionID: "sess-1"}
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
			Workstream: workstream, Name: "agent-b", Cwd: "/work",
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
			Workstream: workstream, Name: "agent-c", Cwd: "/work", Command: []string{"claude", "-p", "hi"},
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
	t.Run("treats pkill exit 1 (no match) as success", func(t *testing.T) {
		r := &supersetRunner{errs: []error{exitErr(t, 1)}}
		if err := (superset{run: r.run}).Kill(ctx, AgentHandle{SessionID: "sess-1"}); err != nil {
			t.Fatalf("Kill: want nil on pkill exit 1, got %v", err)
		}
	})
	t.Run("propagates a real pkill failure", func(t *testing.T) {
		r := &supersetRunner{errs: []error{exitErr(t, 2)}}
		if err := (superset{run: r.run}).Kill(ctx, AgentHandle{SessionID: "sess-1"}); err == nil {
			t.Fatal("Kill: want error on pkill exit 2, got nil")
		}
	})
}

// exitErr returns a real *exec.ExitError carrying the given exit code, so a test can
// drive the pkill exit-code branch of superset.Kill without a real pkill.
func exitErr(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("sh", "-c", "exit "+strconv.Itoa(code)).Run() //nolint:gosec // G204: code is an int, not tainted input
	if err == nil {
		t.Fatalf("exit %d produced no error", code)
	}
	return err
}

func TestSupersetKillWorkstream(t *testing.T) {
	r := &supersetRunner{outs: []string{`{"deleted":["ws-1"]}`}}
	if err := (superset{run: r.run}).KillWorkstream(context.Background(), WorkstreamHandle{ID: "ws-1"}); err != nil {
		t.Fatalf("KillWorkstream: %v", err)
	}
	assertCalls(t, r.calls, [][]string{{"superset", "workspaces", "delete", "ws-1", "--local", "--json"}})
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
	out, err := exec.CommandContext(context.Background(), bash, "-c", strings.Join(quoted, " ")).Output() //nolint:gosec // G204: test drives the shell with a fixed printf round-trip command
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
	out, err := exec.CommandContext(context.Background(), fish, "--no-config", "-c", wrapBashLogin(cmd)).Output() //nolint:gosec // G204: test drives fish with a fixed printf round-trip command
	if err != nil {
		t.Fatalf("fish: %v", err)
	}
	if want := strings.Join(args, "\n") + "\n"; string(out) != want {
		t.Fatalf("round trip =\n  %q\nwant\n  %q", string(out), want)
	}
}
