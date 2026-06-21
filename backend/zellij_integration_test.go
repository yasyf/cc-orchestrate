package backend

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestZellijAgentAliveIntegration drives real zellij to prove the remain-on-exit blind
// spot AgentAlive closes: a command pane whose child exits is held in the session
// (exited:true) and still enumerated by ListAgents, yet AgentAlive reports it not-alive
// while a live pane reports alive. The session is a throwaway, uniquely named per process
// and force-removed on exit so it never touches the user's other zellij sessions.
func TestZellijAgentAliveIntegration(t *testing.T) {
	if _, err := exec.LookPath(zellijBin); err != nil {
		t.Skipf("zellij not installed (%v); skipping real-zellij integration test", err)
	}
	ctx := context.Background()
	b := zellij{run: execRunner}
	cwd := t.TempDir()

	session := fmt.Sprintf("ccorch-zj-alive-%d", os.Getpid())
	// Force-remove the session no matter how the test exits so a failed assertion never
	// leaks a background zellij session; delete-session purges its on-disk resurrection cache.
	t.Cleanup(func() {
		_, _ = b.run(context.Background(), zellijBin, "kill-session", session)
		_, _ = b.run(context.Background(), zellijBin, "delete-session", session)
	})

	proj, err := b.CreateWorkstream(ctx, WorkstreamSpec{Name: session, Cwd: cwd})
	if err != nil {
		t.Fatalf("CreateWorkstream: %v", err)
	}

	live, err := b.Spawn(ctx, SpawnSpec{Workstream: proj, Name: "live", Cwd: cwd, Command: []string{"sh", "-c", "sleep 30"}, SessionID: "s-live"})
	if err != nil {
		t.Fatalf("Spawn live: %v", err)
	}
	dead, err := b.Spawn(ctx, SpawnSpec{Workstream: proj, Name: "dead", Cwd: cwd, Command: []string{"sh", "-c", "exit 7"}, SessionID: "s-dead"})
	if err != nil {
		t.Fatalf("Spawn dead: %v", err)
	}

	// Wait for the exiter to finish and zellij to hold its pane with exited:true.
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

	// The held dead pane is STILL enumerated — proving the diff alone misses it and the
	// AgentAlive corroboration is what lets the supervisor resume it.
	agents, err := b.ListAgents(ctx, proj)
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if _, ok := findAgent(agents, dead.ID); !ok {
		t.Fatalf("dead pane %q not listed; zellij should hold it: %+v", dead.ID, agents)
	}
	if _, ok := findAgent(agents, live.ID); !ok {
		t.Fatalf("live pane %q not listed: %+v", live.ID, agents)
	}
}
