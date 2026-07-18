package orchestrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// TestMigrateBackfillsSpawnNonce proves migrate amends a pre-existing agents table
// that predates spawn_nonce: CREATE TABLE IF NOT EXISTS alone would leave the old
// shape in place and break every agentColumns read on the first boot after upgrade.
// A second migrate over the amended DB must stay a no-op.
func TestMigrateBackfillsSpawnNonce(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The pre-spawn_nonce agents shape, as an upgraded daemon would find it.
	if _, err := db.ExecContext(ctx, `CREATE TABLE agents (
		id                      TEXT PRIMARY KEY,
		sprint_id               TEXT NOT NULL,
		backend                 TEXT NOT NULL,
		backend_terminal_handle TEXT,
		session_id              TEXT,
		scope                   TEXT NOT NULL,
		name                    TEXT,
		prompt                  TEXT,
		subject_id              TEXT,
		ccnotes_task            TEXT,
		status                  TEXT NOT NULL,
		state                   TEXT NOT NULL DEFAULT 'unknown',
		activity                TEXT,
		tokens                  INTEGER NOT NULL DEFAULT 0,
		updated_at              TEXT,
		created_at              TEXT NOT NULL,
		restart_count           INTEGER NOT NULL DEFAULT 0,
		last_restart_at         TEXT
	)`); err != nil {
		t.Fatalf("create legacy agents table: %v", err)
	}

	for _, pass := range []string{"backfill", "idempotent re-run"} {
		if err := migrate(ctx, db); err != nil {
			t.Fatalf("migrate (%s): %v", pass, err)
		}
	}

	ag := agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", TerminalHandle: "term-1",
		SessionID: "sess-1", Scope: "/s", Status: StatusActive, State: StateUnknown,
		CreatedAt: "t0", SpawnNonce: "n1",
	}
	if err := insertAgent(ctx, db, ag); err != nil {
		t.Fatalf("insert agent on migrated DB: %v", err)
	}
	got, err := getAgent(ctx, db, "a1")
	if err != nil {
		t.Fatalf("getAgent: %v", err)
	}
	if got.SpawnNonce != "n1" {
		t.Fatalf("spawn_nonce = %q, want n1 round-tripped through the backfilled column", got.SpawnNonce)
	}
}

// TestMigrateEnforcesSessionUniqueness proves the agents_session_id_unique partial
// index: a second row sharing a non-empty session id is rejected, so
// getAgentBySession stays deterministic, while session-less rows never collide.
func TestMigrateEnforcesSessionUniqueness(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(ctx, t)

	first := agentRow{
		ID: "a1", SprintID: "s1", Backend: "tmux", Scope: "/s",
		SessionID: "sess-dup", Status: StatusActive, State: StateUnknown, CreatedAt: "t0",
	}
	if err := insertAgent(ctx, db, first); err != nil {
		t.Fatalf("insertAgent a1: %v", err)
	}
	dup := first
	dup.ID = "a2"
	if err := insertAgent(ctx, db, dup); err == nil {
		t.Fatal("insertAgent a2 with a1's session id succeeded, want a unique-index violation")
	}
	got, err := getAgentBySession(ctx, db, "sess-dup")
	if err != nil {
		t.Fatalf("getAgentBySession: %v", err)
	}
	if got.ID != "a1" {
		t.Fatalf("getAgentBySession(sess-dup).ID = %q, want a1 (the sole surviving row)", got.ID)
	}

	for _, id := range []string{"a3", "a4"} {
		sessionless := first
		sessionless.ID = id
		sessionless.SessionID = ""
		if err := insertAgent(ctx, db, sessionless); err != nil {
			t.Fatalf("insertAgent %s without a session id: %v (empty session ids must be exempt)", id, err)
		}
	}
}
