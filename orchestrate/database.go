package orchestrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const databaseSchemaVersion = 1

type databaseSchemaObject struct {
	name string
	kind string
	sql  string
}

var databaseSchema = []databaseSchemaObject{
	{
		name: "repos",
		kind: "table",
		sql: `CREATE TABLE repos (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			backend    TEXT NOT NULL,
			cwd        TEXT NOT NULL,
			status     TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
	},
	{
		name: "workstreams",
		kind: "table",
		sql: `CREATE TABLE workstreams (
			id                       TEXT PRIMARY KEY,
			repo_id                  TEXT NOT NULL,
			name                     TEXT NOT NULL,
			backend                  TEXT NOT NULL,
			backend_workspace_handle TEXT,
			branch                   TEXT NOT NULL,
			worktree                 TEXT NOT NULL,
			is_primary               INTEGER NOT NULL DEFAULT 0,
			ccnotes_project          TEXT,
			status                   TEXT NOT NULL,
			created_at               TEXT NOT NULL
		)`,
	},
	{
		name: "sprints",
		kind: "table",
		sql: `CREATE TABLE sprints (
			id             TEXT PRIMARY KEY,
			workstream_id  TEXT NOT NULL,
			name           TEXT NOT NULL,
			ccnotes_sprint TEXT,
			status         TEXT NOT NULL,
			created_at     TEXT NOT NULL
		)`,
	},
	{
		name: "agents",
		kind: "table",
		sql: `CREATE TABLE agents (
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
			last_restart_at         TEXT,
			spawn_nonce             TEXT
		)`,
	},
	{
		name: "config",
		kind: "table",
		sql: `CREATE TABLE config (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	},
	{
		name: "agents_session_id_unique",
		kind: "index",
		sql: `CREATE UNIQUE INDEX agents_session_id_unique
			ON agents(session_id) WHERE session_id <> ''`,
	},
}

func initializeDatabaseSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read database schema version: %w", err)
	}
	switch version {
	case 0:
		if err := createDatabaseSchema(ctx, db); err != nil {
			return err
		}
	case databaseSchemaVersion:
	default:
		return fmt.Errorf("database schema version %d is unsupported; expected exactly %d", version, databaseSchemaVersion)
	}
	return validateDatabaseSchema(ctx, db)
}

func createDatabaseSchema(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_schema
		WHERE (tbl_name IN ('repos', 'workstreams', 'sprints', 'agents', 'config')
		       OR name IN ('repos', 'workstreams', 'sprints', 'agents', 'config', 'agents_session_id_unique'))
		  AND name NOT LIKE 'sqlite_autoindex_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("inspect fresh database namespace: %w", err)
	}
	var existing []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan database namespace: %w", err)
		}
		existing = append(existing, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate database namespace: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close database namespace scan: %w", err)
	}
	if len(existing) != 0 {
		return fmt.Errorf("unversioned cc-orchestrate schema exists (%s); manually transfer repos, workstreams, sprints, and config into the v1 namespace", strings.Join(existing, ", "))
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin database schema: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, object := range databaseSchema {
		if _, err := tx.ExecContext(ctx, object.sql); err != nil {
			return fmt.Errorf("create database schema object %q: %w", object.name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
		return fmt.Errorf("stamp database schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database schema: %w", err)
	}
	return nil
}

func validateDatabaseSchema(ctx context.Context, db *sql.DB) error {
	expected := make(map[string]databaseSchemaObject, len(databaseSchema))
	for _, object := range databaseSchema {
		expected[object.name] = object
	}
	rows, err := db.QueryContext(ctx, `
		SELECT type, name, sql FROM sqlite_schema
		WHERE tbl_name IN ('repos', 'workstreams', 'sprints', 'agents', 'config')
		  AND name NOT LIKE 'sqlite_autoindex_%'
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("inspect database schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	seen := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var kind, name, statement string
		if err := rows.Scan(&kind, &name, &statement); err != nil {
			return fmt.Errorf("scan database schema: %w", err)
		}
		object, ok := expected[name]
		if !ok {
			return fmt.Errorf("database schema contains unexpected %s %q", kind, name)
		}
		if kind != object.kind || normalizeSchemaSQL(statement) != normalizeSchemaSQL(object.sql) {
			return fmt.Errorf("database schema object %q does not match v1", name)
		}
		seen[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate database schema: %w", err)
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("database schema is missing v1 object %q", name)
		}
	}
	return nil
}

func normalizeSchemaSQL(statement string) string { return strings.Join(strings.Fields(statement), " ") }
