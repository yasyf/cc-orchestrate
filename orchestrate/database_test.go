package orchestrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func openRawDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "state.db")+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDatabaseSchemaCreatesAndReopensExactV1(t *testing.T) {
	ctx := context.Background()
	db := openRawDatabase(t)
	for _, pass := range []string{"create", "reopen"} {
		if err := initializeDatabaseSchema(ctx, db); err != nil {
			t.Fatalf("initialize schema (%s): %v", pass, err)
		}
	}
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != databaseSchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, databaseSchemaVersion)
	}
}

func TestDatabaseSchemaRejectsLegacyOtherEpochAndDrift(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *sql.DB)
		want  string
	}{
		{
			name: "unversioned legacy table",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE TABLE agents (id TEXT PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
			},
			want: "manually transfer",
		},
		{
			name: "other epoch",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
					t.Fatal(err)
				}
			},
			want: "expected exactly 1",
		},
		{
			name: "v1 schema drift",
			setup: func(t *testing.T, db *sql.DB) {
				if err := initializeDatabaseSchema(ctx, db); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`ALTER TABLE agents ADD COLUMN legacy TEXT`); err != nil {
					t.Fatal(err)
				}
			},
			want: "does not match v1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openRawDatabase(t)
			tc.setup(t, db)
			err := initializeDatabaseSchema(ctx, db)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("initializeDatabaseSchema() error = %v, want containing %q", err, tc.want)
			}
		})
	}
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
