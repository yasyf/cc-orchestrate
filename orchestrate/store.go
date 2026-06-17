package orchestrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/yasyf/cc-orchestrate/backend"
)

// projectColumns is the canonical projects read list; nullable columns are
// coalesced to "" so every scan target is a plain string.
const projectColumns = `id, name, backend, COALESCE(backend_workspace_handle, ''), cwd, status, created_at`

// agentColumns is the canonical agents read list; nullable columns are coalesced
// to "" so every scan target is a plain string.
const agentColumns = `id, project_id, backend, COALESCE(backend_terminal_handle, ''), ` +
	`COALESCE(session_id, ''), scope, COALESCE(name, ''), COALESCE(prompt, ''), ` +
	`COALESCE(subject_id, ''), status, state, COALESCE(activity, ''), tokens, ` +
	`COALESCE(updated_at, ''), created_at`

// projectRow is one row of the projects table: an orchestration project bound to
// a backend workspace.
type projectRow struct {
	ID              string
	Name            string
	Backend         backend.BackendName
	WorkspaceHandle string
	Cwd             string
	Status          LifecycleStatus
	CreatedAt       string
}

// agentRow is one row of the agents table: a spawned child agent and its derived
// transcript status.
type agentRow struct {
	ID             string
	ProjectID      string
	Backend        backend.BackendName
	TerminalHandle string
	SessionID      string
	Scope          string
	Name           string
	Prompt         string
	SubjectID      string
	Status         LifecycleStatus
	State          string
	Activity       string
	Tokens         int
	UpdatedAt      string
	CreatedAt      string
}

// rowScanner is the Scan surface shared by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanProject(s rowScanner) (projectRow, error) {
	var p projectRow
	err := s.Scan(&p.ID, &p.Name, &p.Backend, &p.WorkspaceHandle, &p.Cwd, &p.Status, &p.CreatedAt)
	return p, err
}

func scanAgent(s rowScanner) (agentRow, error) {
	var a agentRow
	err := s.Scan(
		&a.ID, &a.ProjectID, &a.Backend, &a.TerminalHandle, &a.SessionID, &a.Scope,
		&a.Name, &a.Prompt, &a.SubjectID, &a.Status, &a.State, &a.Activity, &a.Tokens,
		&a.UpdatedAt, &a.CreatedAt,
	)
	return a, err
}

func insertProject(ctx context.Context, db *sql.DB, p projectRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO projects (id, name, backend, backend_workspace_handle, cwd, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.Name, p.Backend, p.WorkspaceHandle, p.Cwd, p.Status, p.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert project %q: %w", p.ID, err)
	}
	return nil
}

func listProjects(ctx context.Context, db *sql.DB) ([]projectRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+projectColumns+` FROM projects ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	out := []projectRow{}
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projects: %w", err)
	}
	return out, nil
}

// getProject resolves a project by canonical id first, then by name.
func getProject(ctx context.Context, db *sql.DB, idOrName string) (projectRow, error) {
	p, err := scanProject(db.QueryRowContext(ctx,
		`SELECT `+projectColumns+` FROM projects WHERE id = ? OR name = ?
		 ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END LIMIT 1`,
		idOrName, idOrName, idOrName))
	if errors.Is(err, sql.ErrNoRows) {
		return projectRow{}, fmt.Errorf("project not found: %s", idOrName)
	}
	if err != nil {
		return projectRow{}, fmt.Errorf("get project %q: %w", idOrName, err)
	}
	return p, nil
}

func setProjectStatus(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE projects SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set project %q status: %w", id, err)
	}
	return nil
}

func insertAgent(ctx context.Context, db *sql.DB, a agentRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO agents (id, project_id, backend, backend_terminal_handle, session_id, scope,
			name, prompt, subject_id, status, state, activity, tokens, updated_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ProjectID, a.Backend, a.TerminalHandle, a.SessionID, a.Scope,
		a.Name, a.Prompt, a.SubjectID, a.Status, a.State, a.Activity, a.Tokens, a.UpdatedAt, a.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert agent %q: %w", a.ID, err)
	}
	return nil
}

// listAgents returns every agent, or only those in projectFilter when it is set.
func listAgents(ctx context.Context, db *sql.DB, projectFilter string) ([]agentRow, error) {
	query := `SELECT ` + agentColumns + ` FROM agents`
	args := []any{}
	if projectFilter != "" {
		query += ` WHERE project_id = ?`
		args = append(args, projectFilter)
	}
	return queryAgents(ctx, db, query+` ORDER BY created_at`, args...)
}

func listActiveAgents(ctx context.Context, db *sql.DB) ([]agentRow, error) {
	return queryAgents(ctx, db, `SELECT `+agentColumns+` FROM agents WHERE status = ? ORDER BY created_at`, StatusActive)
}

func queryAgents(ctx context.Context, db *sql.DB, query string, args ...any) ([]agentRow, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer rows.Close()
	out := []agentRow{}
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agents: %w", err)
	}
	return out, nil
}

func getAgent(ctx context.Context, db *sql.DB, id string) (agentRow, error) {
	a, err := scanAgent(db.QueryRowContext(ctx, `SELECT `+agentColumns+` FROM agents WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return agentRow{}, fmt.Errorf("agent not found: %s", id)
	}
	if err != nil {
		return agentRow{}, fmt.Errorf("get agent %q: %w", id, err)
	}
	return a, nil
}

func setAgentLifecycle(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE agents SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set agent %q lifecycle: %w", id, err)
	}
	return nil
}

// applyStatus folds a transcript-derived Status into the agent row, stamping a
// fresh updated_at in RFC3339 UTC.
func applyStatus(ctx context.Context, db *sql.DB, id string, st Status) error {
	_, err := db.ExecContext(ctx,
		`UPDATE agents SET state = ?, activity = ?, tokens = ?, updated_at = ? WHERE id = ?`,
		st.State, statusActivity(st), st.Tokens, time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("apply status to agent %q: %w", id, err)
	}
	return nil
}

// statusActivity renders a Status's tool and target as a compact "Tool: Target"
// line, collapsing to the tool alone when there is no target and to "" when there
// is no tool.
func statusActivity(st Status) string {
	switch {
	case st.Tool == "":
		return ""
	case st.Target == "":
		return st.Tool
	default:
		return st.Tool + ": " + st.Target
	}
}

func getConfig(ctx context.Context, db *sql.DB, key string) (string, bool, error) {
	var value string
	err := db.QueryRowContext(ctx, `SELECT value FROM config WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get config %q: %w", key, err)
	}
	return value, true, nil
}

func setConfig(ctx context.Context, db *sql.DB, key, value string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO config (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value)
	if err != nil {
		return fmt.Errorf("set config %q: %w", key, err)
	}
	return nil
}
