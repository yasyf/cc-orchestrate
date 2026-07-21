package orchestrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-interact/store"
	_ "modernc.org/sqlite"
)

func TestDatabaseSchemaCreatesAndReopensExactV1(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	for _, pass := range []string{"create", "reopen"} {
		st, err := store.Open(ctx, path, databaseStoreSchema())
		if err != nil {
			t.Fatalf("open exact store (%s): %v", pass, err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close exact store (%s): %v", pass, err)
		}
	}
	st, err := store.Open(ctx, path, databaseStoreSchema())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, table := range []string{"agents", "orchestrate_agents"} {
		var name string
		if err := st.DB().QueryRowContext(ctx, `SELECT name FROM sqlite_schema WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("lookup %s: %v", table, err)
		}
	}
}

func TestDatabaseSchemaRejectsForeignEpochAndFingerprint(t *testing.T) {
	ctx := context.Background()
	t.Run("foreign epoch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `PRAGMA user_version = 2`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = store.Open(ctx, path, databaseStoreSchema())
		if err == nil || !strings.Contains(err.Error(), "want exactly 1") {
			t.Fatalf("store.Open() error = %v, want exact epoch rejection", err)
		}
	})

	t.Run("fingerprint drift", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.db")
		st, err := store.Open(ctx, path, databaseStoreSchema())
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		drifted := store.Schema{DDL: databaseDDL + "\nCREATE TABLE drift(id TEXT PRIMARY KEY);"}
		_, err = store.Open(ctx, path, drifted)
		if err == nil || !strings.Contains(err.Error(), "fingerprint") {
			t.Fatalf("store.Open() error = %v, want fingerprint rejection", err)
		}
	})
}

func TestDatabaseSchemaEnforcesSessionUniqueness(t *testing.T) {
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
		t.Fatalf("getAgentBySession(sess-dup).ID = %q, want a1", got.ID)
	}

	for _, id := range []string{"a3", "a4"} {
		sessionless := first
		sessionless.ID = id
		sessionless.SessionID = ""
		if err := insertAgent(ctx, db, sessionless); err != nil {
			t.Fatalf("insertAgent %s without a session id: %v", id, err)
		}
	}
}
