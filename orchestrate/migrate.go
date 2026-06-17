package orchestrate

import (
	"context"
	"database/sql"
	"fmt"
)

// migrate adds cc-orchestrate's own tables after cc-interact's core schema. It is
// idempotent (CREATE TABLE IF NOT EXISTS) so every daemon boot can run it safely.
func migrate(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS projects (
			id                       TEXT PRIMARY KEY,
			name                     TEXT NOT NULL,
			backend                  TEXT NOT NULL,
			backend_workspace_handle TEXT,
			cwd                      TEXT NOT NULL,
			status                   TEXT NOT NULL,
			created_at               TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agents (
			id                      TEXT PRIMARY KEY,
			project_id              TEXT NOT NULL,
			backend                 TEXT NOT NULL,
			backend_terminal_handle TEXT,
			session_id              TEXT,
			scope                   TEXT NOT NULL,
			name                    TEXT,
			prompt                  TEXT,
			subject_id              TEXT,
			status                  TEXT NOT NULL,
			state                   TEXT NOT NULL DEFAULT 'unknown',
			activity                TEXT,
			tokens                  INTEGER NOT NULL DEFAULT 0,
			updated_at              TEXT,
			created_at              TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS config (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate orchestrate schema: %w", err)
		}
	}
	return nil
}
