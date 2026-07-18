package backend

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
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
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	t.Setenv("TMUX_TMPDIR", sockDir)

	ctx := context.Background()
	b := tmux{run: execRunner}
	cwd := t.TempDir()

	// Raw name carries the '.' and ':' tmux reserves so the sanitized round trip is
	// observable; unique per run via t.Name with no rand/date needed.
	rawName := t.Name() + ".integ:0"
	wantSession := tmuxSessionName(WorkstreamSpec{Name: rawName, Cwd: cwd})

	// Force-kill the private server no matter how the test exits so a failed
	// assertion never leaks a tmux session or socket.
	t.Cleanup(func() {
		_, _ = b.run(context.Background(), tmuxBin, "kill-session", "-t", wantSession)
		_, _ = b.run(context.Background(), tmuxBin, "kill-server")
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
	// Killing the last session shuts the private server down; success without the
	// session or either server-gone error string proves teardown.
	switch afterKillProj, err := b.ListWorkstreams(ctx); {
	case err != nil:
		if !strings.Contains(err.Error(), "no server running") && !strings.Contains(err.Error(), "server exited unexpectedly") {
			t.Fatalf("ListWorkstreams after KillWorkstream: unexpected error: %v", err)
		}
	case containsWorkstream(afterKillProj, proj.ID):
		t.Fatalf("session %q still present after KillWorkstream: %+v", proj.ID, afterKillProj)
	}
}

// TestTmuxAgentAliveIntegration drives real tmux to prove the remain-on-exit blind spot
// AgentAlive closes: a pane whose command exits lingers as a dead pane that ListAgents
// still enumerates, yet AgentAlive reports it not-alive while a live pane reports alive.
func TestTmuxAgentAliveIntegration(t *testing.T) {
	if _, err := exec.LookPath(tmuxBin); err != nil {
		t.Skipf("tmux not installed (%v); skipping real-tmux integration test", err)
	}
	sockDir, err := os.MkdirTemp("/tmp", "cco-tmux-alive-")
	if err != nil {
		t.Fatalf("tmux socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	t.Setenv("TMUX_TMPDIR", sockDir)

	ctx := context.Background()
	b := tmux{run: execRunner}
	cwd := t.TempDir()
	t.Cleanup(func() { _, _ = b.run(context.Background(), tmuxBin, "kill-server") })

	proj, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: t.Name(), Cwd: cwd})
	if err != nil {
		t.Fatalf("CreateWorkstream: %v", err)
	}
	// remain-on-exit keeps a pane whose command has exited as a dead pane that ListAgents
	// still lists — the exact case the vanished-handle diff cannot see.
	if _, err := b.run(ctx, tmuxBin, "set-option", "-g", "remain-on-exit", "on"); err != nil {
		t.Fatalf("set remain-on-exit: %v", err)
	}

	live, err := b.Spawn(ctx, SpawnSpec{Workstream: proj, Name: "live", Cwd: cwd, Command: []string{"sh", "-c", "sleep 30"}, SessionID: "s-live"})
	if err != nil {
		t.Fatalf("Spawn live: %v", err)
	}
	dead, err := b.Spawn(ctx, SpawnSpec{Workstream: proj, Name: "dead", Cwd: cwd, Command: []string{"sh", "-c", "exit 7"}, SessionID: "s-dead"})
	if err != nil {
		t.Fatalf("Spawn dead: %v", err)
	}

	// Wait for the exiter to finish and tmux to flag its pane dead.
	deadline := time.Now().Add(5 * time.Second)
	for {
		alive, err := b.AgentAlive(ctx, dead)
		if err == nil && !alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead pane never reported not-alive (last alive=%v err=%v)", alive, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if alive, err := b.AgentAlive(ctx, live); err != nil || !alive {
		t.Fatalf("AgentAlive(live) = %v, %v; want true, nil", alive, err)
	}

	// The dead pane is STILL enumerated — proving the diff alone misses it and the
	// AgentAlive corroboration is what lets the supervisor resume it.
	agents, err := b.ListAgents(ctx, proj)
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if _, ok := findAgent(agents, dead.ID); !ok {
		t.Fatalf("dead pane %q not listed; remain-on-exit should keep it: %+v", dead.ID, agents)
	}
	if _, ok := findAgent(agents, live.ID); !ok {
		t.Fatalf("live pane %q not listed: %+v", live.ID, agents)
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
