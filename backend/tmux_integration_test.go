package backend

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// paneIDRe matches a tmux pane id, the "%N" form tmux prints for #{pane_id}.
var paneIDRe = regexp.MustCompile(`^%\d+$`)

// integrationSessionID is the fixed --session-id the spawned agent carries; the
// integration test asserts it round-trips back out of the AgentHandle unchanged.
const integrationSessionID = "11111111-2222-3333-4444-555555555555"

// TestTmuxIntegration drives the real tmux binary (not the fake recorder) to prove
// the backend round-trips against a live mux: CreateWorkstream → Spawn → list → Kill →
// KillWorkstream, asserting real ids (pane "%N", sanitized session name) flow through.
// It is hermetic: TMUX_TMPDIR points tmux at a throwaway socket dir so the server is
// private to this test and the user's default tmux server is never touched.
func TestTmuxIntegration(t *testing.T) {
	if _, err := exec.LookPath(tmuxBin); err != nil {
		t.Skipf("tmux not installed (%v); skipping real-tmux integration test", err)
	}
	// Point tmux at a private socket dir so its server is isolated from the user's
	// default server. It lives under /tmp, not t.TempDir(), because the long
	// $TMPDIR path on macOS overruns the Unix-socket sun_path limit.
	sockDir, err := os.MkdirTemp("/tmp", "cco-tmux-")
	if err != nil {
		t.Fatalf("tmux socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	t.Setenv("TMUX_TMPDIR", sockDir)

	ctx := context.Background()
	b := tmux{run: execRunner}
	cwd := t.TempDir()

	// Raw name carries the '.' and ':' tmux reserves so the sanitized round trip is
	// observable; unique per run via t.Name with no rand/date needed.
	rawName := t.Name() + ".integ:0"
	wantSession := tmuxNameReplacer.Replace(rawName)

	// Force-kill the private server no matter how the test exits so a failed
	// assertion never leaks a tmux session or socket.
	t.Cleanup(func() {
		b.run(context.Background(), tmuxBin, "kill-session", "-t", wantSession)
		b.run(context.Background(), tmuxBin, "kill-server")
	})

	proj, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: rawName, Cwd: cwd})
	if err != nil {
		t.Fatalf("CreateWorkstream: %v", err)
	}
	want := WorkstreamHandle{Backend: "tmux", ID: wantSession, Name: rawName, Cwd: cwd, Worktree: cwd}
	if proj != want {
		t.Fatalf("project handle = %+v, want %+v", proj, want)
	}
	if proj.ID == rawName || strings.ContainsAny(proj.ID, ".:") {
		t.Fatalf("session id %q not sanitized (still holds tmux separators)", proj.ID)
	}

	projects, err := b.ListWorkstreams(ctx)
	if err != nil {
		t.Fatalf("ListWorkstreams: %v", err)
	}
	if !containsWorkstream(projects, proj.ID) {
		t.Fatalf("ListWorkstreams %+v missing created session %q", projects, proj.ID)
	}

	agent, err := b.Spawn(ctx, SpawnSpec{
		Workstream: proj,
		Name:       "agent-a",
		Cwd:        cwd,
		Command:    []string{"sh", "-c", "sleep 30"},
		SessionID:  integrationSessionID,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if !paneIDRe.MatchString(agent.ID) {
		t.Fatalf("agent id %q is not a tmux pane id (%%N)", agent.ID)
	}
	wantAgent := AgentHandle{Backend: "tmux", ID: agent.ID, WorkstreamID: proj.ID, Name: "agent-a", SessionID: integrationSessionID}
	if agent != wantAgent {
		t.Fatalf("agent handle = %+v, want %+v", agent, wantAgent)
	}

	agents, err := b.ListAgents(ctx, proj)
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	listed, ok := findAgent(agents, agent.ID)
	if !ok {
		t.Fatalf("ListAgents %+v missing spawned pane %q", agents, agent.ID)
	}
	if want := (AgentHandle{Backend: "tmux", ID: agent.ID, WorkstreamID: proj.ID, Name: "agent-a"}); listed != want {
		t.Fatalf("listed agent = %+v, want %+v", listed, want)
	}

	if err := b.Kill(ctx, agent); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	afterKill, err := b.ListAgents(ctx, proj)
	if err != nil {
		t.Fatalf("ListAgents after Kill: %v", err)
	}
	if _, ok := findAgent(afterKill, agent.ID); ok {
		t.Fatalf("pane %q still present after Kill: %+v", agent.ID, afterKill)
	}

	if err := b.KillWorkstream(ctx, proj); err != nil {
		t.Fatalf("KillWorkstream: %v", err)
	}
	// Killing the last session shuts the private server down, so ListWorkstreams either
	// succeeds without the session or fails with "no server running"; both prove gone.
	switch afterKillProj, err := b.ListWorkstreams(ctx); {
	case err != nil:
		if !strings.Contains(err.Error(), "no server running") {
			t.Fatalf("ListWorkstreams after KillWorkstream: unexpected error: %v", err)
		}
	case containsWorkstream(afterKillProj, proj.ID):
		t.Fatalf("session %q still present after KillWorkstream: %+v", proj.ID, afterKillProj)
	}
}

func containsWorkstream(projects []WorkstreamHandle, id string) bool {
	for _, p := range projects {
		if p.ID == id {
			return true
		}
	}
	return false
}

func findAgent(agents []AgentHandle, id string) (AgentHandle, bool) {
	for _, a := range agents {
		if a.ID == id {
			return a, true
		}
	}
	return AgentHandle{}, false
}
