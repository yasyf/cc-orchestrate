package backend

import (
	"context"
	"slices"
	"testing"
)

// Fixtures below are verbatim stdout captured from herdr 0.7.0 against a real
// running server (workspace/agent/pane create, list, and close), then the test
// resources were torn down.
const (
	herdCreateOut = `{"id":"cli:workspace:create","result":{"root_pane":{"agent_status":"unknown","cwd":"/private/tmp","focused":false,"foreground_cwd":"/private/tmp","pane_id":"w65466e4ca40bb5-1","revision":0,"tab_id":"w65466e4ca40bb5:1","terminal_id":"term_65466e4ca40b86","workspace_id":"w65466e4ca40bb5"},"tab":{"agent_status":"unknown","focused":false,"label":"1","number":1,"pane_count":1,"tab_id":"w65466e4ca40bb5:1","workspace_id":"w65466e4ca40bb5"},"type":"workspace_created","workspace":{"active_tab_id":"w65466e4ca40bb5:1","agent_status":"unknown","focused":false,"label":"cc-orch-test-ws","number":2,"pane_count":1,"tab_count":1,"workspace_id":"w65466e4ca40bb5"}}}`

	herdWorkspaceListOut = `{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"active_tab_id":"w6545b068248ab4:1","agent_status":"unknown","focused":true,"label":"~","number":1,"pane_count":1,"tab_count":1,"workspace_id":"w6545b068248ab4"}]}}`

	herdStartOut = `{"id":"cli:agent:start","result":{"agent":{"agent_status":"unknown","cwd":"/private/tmp","focused":false,"foreground_cwd":"/private/tmp","name":"cc-orch-test-agent","pane_id":"w65466e4ca40bb5-2","revision":0,"tab_id":"w65466e4ca40bb5:1","terminal_id":"term_65466e54d57ac7","workspace_id":"w65466e4ca40bb5"},"argv":["sh","-c","sleep 120"],"type":"agent_started"}}`

	herdAgentListOut = `{"id":"cli:agent:list","result":{"agents":[{"agent_status":"unknown","cwd":"/private/tmp","focused":false,"foreground_cwd":"/private/tmp","name":"cc-orch-test-agent","pane_id":"w65466e4ca40bb5-2","revision":0,"tab_id":"w65466e4ca40bb5:1","terminal_id":"term_65466e54d57ac7","workspace_id":"w65466e4ca40bb5"}],"type":"agent_list"}}`

	herdPaneCloseOut      = `{"id":"cli:pane:close","result":{"type":"ok"}}`
	herdWorkspaceCloseOut = `{"id":"cli:workspace:close","result":{"type":"ok"}}`

	herdAgentReadOut = `{"id":"cli:agent:read","result":{"type":"pane_read","read":{"pane_id":"w65466e4ca40bb5-2","workspace_id":"w65466e4ca40bb5","tab_id":"w65466e4ca40bb5:1","source":"visible","format":"text","text":"Do you trust the files in this folder?","revision":0,"truncated":false}}}`
)

type herdRecordedCall struct {
	name string
	args []string
}

func recordingRunner(out string, rec *herdRecordedCall) runner {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		rec.name = name
		rec.args = args
		return []byte(out), nil
	}
}

func TestHerdMethods(t *testing.T) {
	tests := []struct {
		name     string
		output   string
		invoke   func(ctx context.Context, b herd) (any, error)
		wantArgv []string
		check    func(t *testing.T, got any)
	}{
		{
			name:   "CreateWorkstream parses result.workspace.workspace_id",
			output: herdCreateOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.CreateWorkstream(ctx, WorkstreamSpec{Name: "cc-orch-test-ws", Cwd: "/tmp"})
			},
			wantArgv: []string{"workspace", "create", "--cwd", "/tmp", "--label", "cc-orch-test-ws"},
			check: func(t *testing.T, got any) {
				p := got.(WorkstreamHandle)
				want := WorkstreamHandle{Backend: "herd", ID: "w65466e4ca40bb5", Name: "cc-orch-test-ws", Cwd: "/tmp", Worktree: "/tmp"}
				if p != want {
					t.Errorf("project = %+v, want %+v", p, want)
				}
			},
		},
		{
			name:   "ListWorkstreams maps workspaces",
			output: herdWorkspaceListOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.ListWorkstreams(ctx)
			},
			wantArgv: []string{"workspace", "list"},
			check: func(t *testing.T, got any) {
				ps := got.([]WorkstreamHandle)
				want := []WorkstreamHandle{{Backend: "herd", ID: "w6545b068248ab4", Name: "~"}}
				if !slices.Equal(ps, want) {
					t.Errorf("projects = %+v, want %+v", ps, want)
				}
			},
		},
		{
			name:   "Spawn parses result.agent.pane_id and carries session/project",
			output: herdStartOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.Spawn(ctx, SpawnSpec{
					Workstream: WorkstreamHandle{Backend: "herd", ID: "w65466e4ca40bb5"},
					Name:       "cc-orch-test-agent",
					Cwd:        "/tmp",
					Command:    []string{"sh", "-c", "sleep 120"},
					SessionID:  "sess-abc",
				})
			},
			wantArgv: []string{"agent", "start", "cc-orch-test-agent", "--workspace", "w65466e4ca40bb5", "--cwd", "/tmp", "--", "sh", "-c", "sleep 120"},
			check: func(t *testing.T, got any) {
				a := got.(AgentHandle)
				want := AgentHandle{Backend: "herd", ID: "w65466e4ca40bb5-2", WorkstreamID: "w65466e4ca40bb5", Name: "cc-orch-test-agent", SessionID: "sess-abc"}
				if a != want {
					t.Errorf("agent = %+v, want %+v", a, want)
				}
			},
		},
		{
			name:   "ListAgents returns agents in the project workspace",
			output: herdAgentListOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.ListAgents(ctx, WorkstreamHandle{Backend: "herd", ID: "w65466e4ca40bb5"})
			},
			wantArgv: []string{"agent", "list"},
			check: func(t *testing.T, got any) {
				as := got.([]AgentHandle)
				want := []AgentHandle{{Backend: "herd", ID: "w65466e4ca40bb5-2", WorkstreamID: "w65466e4ca40bb5", Name: "cc-orch-test-agent"}}
				if !slices.Equal(as, want) {
					t.Errorf("agents = %+v, want %+v", as, want)
				}
			},
		},
		{
			name:   "ListAgents filters out foreign workspaces",
			output: herdAgentListOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.ListAgents(ctx, WorkstreamHandle{Backend: "herd", ID: "wOTHER0000000000"})
			},
			wantArgv: []string{"agent", "list"},
			check: func(t *testing.T, got any) {
				if as := got.([]AgentHandle); len(as) != 0 {
					t.Errorf("agents = %+v, want none", as)
				}
			},
		},
		{
			name:   "Capture reads result.read.text from agent read",
			output: herdAgentReadOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return b.Capture(ctx, AgentHandle{Backend: "herd", ID: "w65466e4ca40bb5-2"})
			},
			wantArgv: []string{"agent", "read", "w65466e4ca40bb5-2", "--source", "visible", "--format", "text"},
			check: func(t *testing.T, got any) {
				if s := got.(string); s != "Do you trust the files in this folder?" {
					t.Errorf("screen = %q, want the trust prompt text", s)
				}
			},
		},
		{
			name:   "Kill closes the agent pane",
			output: herdPaneCloseOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return nil, b.Kill(ctx, AgentHandle{Backend: "herd", ID: "w65466e4ca40bb5-2"})
			},
			wantArgv: []string{"pane", "close", "w65466e4ca40bb5-2"},
		},
		{
			name:   "KillWorkstream closes the workspace",
			output: herdWorkspaceCloseOut,
			invoke: func(ctx context.Context, b herd) (any, error) {
				return nil, b.KillWorkstream(ctx, WorkstreamHandle{Backend: "herd", ID: "w65466e4ca40bb5"})
			},
			wantArgv: []string{"workspace", "close", "w65466e4ca40bb5"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var rec herdRecordedCall
			b := herd{run: recordingRunner(tc.output, &rec)}
			got, err := tc.invoke(context.Background(), b)
			if err != nil {
				t.Fatalf("invoke: %v", err)
			}
			if rec.name != herdBin {
				t.Errorf("binary = %q, want %q", rec.name, herdBin)
			}
			if !slices.Equal(rec.args, tc.wantArgv) {
				t.Errorf("argv = %v, want %v", rec.args, tc.wantArgv)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

// TestHerdSendText asserts the two-call native send argv: type the text, then
// submit with a separate Enter key.
func TestHerdSendText(t *testing.T) {
	ctx := context.Background()
	var calls [][]string
	run := func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, append([]string{name}, args...))
		return []byte(`{"id":"x","result":{"type":"ok"}}`), nil
	}
	if err := (herd{run: run}).SendText(ctx, AgentHandle{Backend: "herd", ID: "pane-9"}, "hi -n there"); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	want := [][]string{
		{"herdr", "pane", "send-text", "pane-9", "hi -n there"},
		{"herdr", "pane", "send-keys", "pane-9", "Enter"},
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if !slices.Equal(calls[i], want[i]) {
			t.Fatalf("call %d = %v, want %v", i, calls[i], want[i])
		}
	}
}

func TestHerdMetadata(t *testing.T) {
	b := herd{}
	if b.Name() != "herd" {
		t.Errorf("Name() = %q, want herd", b.Name())
	}
	if c := b.Caps(); !c.Has(CanSendText) || !c.Has(CanCapture) || !c.Has(CanEnumerate) {
		t.Errorf("Caps() = %+v, want CanSendText+CanCapture+CanEnumerate", c)
	}
}

func TestHerdEnsureReadyDoesNotInvokeCLI(t *testing.T) {
	b := herd{run: func(_ context.Context, name string, args ...string) ([]byte, error) {
		t.Fatalf("EnsureReady must not invoke the CLI, got %s %v", name, args)
		return nil, nil
	}}
	if err := b.EnsureReady(context.Background()); err != nil {
		t.Fatalf("EnsureReady: %v", err)
	}
}
