package orchestrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // register the sqlite database/sql driver

	"github.com/yasyf/cc-orchestrate/backend"
)

// repoColumns is the canonical repos read list; nullable columns are
// coalesced to "" so every scan target is a plain string.
const repoColumns = `id, name, backend, cwd, status, created_at`

// workstreamColumns is the canonical workstreams read list; nullable columns are
// coalesced to "" so every scan target is a plain string.
const workstreamColumns = `id, repo_id, name, backend, COALESCE(backend_workspace_handle, ''), ` +
	`branch, worktree, is_primary, COALESCE(ccnotes_project, ''), status, created_at`

// sprintColumns is the canonical sprints read list; nullable columns are coalesced
// to "" so every scan target is a plain string.
const sprintColumns = `id, workstream_id, name, COALESCE(ccnotes_sprint, ''), status, created_at`

// agentColumns is the canonical agents read list, every column qualified with the
// agents table so the list stays unambiguous when joined against sprints and
// workstreams; nullable columns are coalesced to "" so every scan target is a plain
// string.
const agentColumns = `agents.id, agents.sprint_id, agents.backend, COALESCE(agents.backend_terminal_handle, ''), ` +
	`COALESCE(agents.session_id, ''), agents.scope, COALESCE(agents.name, ''), COALESCE(agents.prompt, ''), ` +
	`COALESCE(agents.subject_id, ''), COALESCE(agents.ccnotes_task, ''), agents.status, agents.state, ` +
	`COALESCE(agents.activity, ''), agents.tokens, COALESCE(agents.updated_at, ''), agents.created_at, ` +
	`agents.restart_count, COALESCE(agents.last_restart_at, '')`

// repoRow is one row of the repos table: an orchestration repo, the container its
// workstreams branch from. The backend workspace now lives on each workstream.
type repoRow struct {
	ID        string
	Name      string
	Backend   backend.Name
	Cwd       string
	Status    LifecycleStatus
	CreatedAt string
}

// workstreamRow is one row of the workstreams table: a git worktree on its own
// branch (1:1), bound to a backend workspace. The primary workstream (IsPrimary
// true) is the repo's own checkout: for backends cc-orchestrate creates worktrees
// for, Worktree is the repo root and is never a git worktree add; a ManagesWorktree
// backend forks its own, so Worktree there is that fork. Worktree teardown is gated
// on (!ManagesWorktree && !IsPrimary), so a primary checkout is never removed.
type workstreamRow struct {
	ID              string
	RepoID          string
	Name            string
	Backend         backend.Name
	WorkspaceHandle string
	Branch          string
	Worktree        string
	IsPrimary       bool
	CCNotesProject  string
	Status          LifecycleStatus
	CreatedAt       string
}

// sprintRow is one row of the sprints table: a planning sub-group of a workstream,
// bound to a cc-notes sprint. A sprint shares its workstream's worktree — it has no
// worktree of its own — so its teardown never touches git.
type sprintRow struct {
	ID            string
	WorkstreamID  string
	Name          string
	CCNotesSprint string
	Status        LifecycleStatus
	CreatedAt     string
}

// agentRow is one row of the agents table: a spawned child agent and its derived
// transcript status. It attaches to a sprint; its workstream and repo are derived
// through the sprint join.
type agentRow struct {
	ID             string
	SprintID       string
	Backend        backend.Name
	TerminalHandle string
	SessionID      string
	Scope          string
	Name           string
	Prompt         string
	SubjectID      string
	CCNotesTask    string
	Status         LifecycleStatus
	State          State
	Activity       string
	Tokens         int
	UpdatedAt      string
	CreatedAt      string
	RestartCount   int
	LastRestartAt  string
}

// rowScanner is the Scan surface shared by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// nowStamp is the canonical RFC3339 UTC timestamp every row write stamps into a
// created_at/updated_at column.
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }

// errNotFound marks a lookup that resolved no matching row. The socket adapter
// (replyError) maps it to a NotFound status; a real query failure stays
// InternalError, so a missing id and a broken DB never wear the same code.
var errNotFound = errors.New("not found")

// lookupError is a not-found lookup error that keeps its human message while matching
// errNotFound through errors.Is, so a caller maps it to NotFound without string
// matching.
type lookupError struct{ msg string }

func (e *lookupError) Error() string        { return e.msg }
func (e *lookupError) Is(target error) bool { return target == errNotFound }

func notFoundf(format string, args ...any) error {
	return &lookupError{msg: fmt.Sprintf(format, args...)}
}

func scanRepo(s rowScanner) (repoRow, error) {
	var p repoRow
	err := s.Scan(&p.ID, &p.Name, &p.Backend, &p.Cwd, &p.Status, &p.CreatedAt)
	return p, err
}

func scanWorkstream(s rowScanner) (workstreamRow, error) {
	var w workstreamRow
	err := s.Scan(
		&w.ID, &w.RepoID, &w.Name, &w.Backend, &w.WorkspaceHandle,
		&w.Branch, &w.Worktree, &w.IsPrimary, &w.CCNotesProject, &w.Status, &w.CreatedAt,
	)
	return w, err
}

func scanSprint(s rowScanner) (sprintRow, error) {
	var sp sprintRow
	err := s.Scan(&sp.ID, &sp.WorkstreamID, &sp.Name, &sp.CCNotesSprint, &sp.Status, &sp.CreatedAt)
	return sp, err
}

func scanAgent(s rowScanner) (agentRow, error) {
	var a agentRow
	err := s.Scan(
		&a.ID, &a.SprintID, &a.Backend, &a.TerminalHandle, &a.SessionID, &a.Scope,
		&a.Name, &a.Prompt, &a.SubjectID, &a.CCNotesTask, &a.Status, &a.State, &a.Activity, &a.Tokens,
		&a.UpdatedAt, &a.CreatedAt, &a.RestartCount, &a.LastRestartAt,
	)
	return a, err
}

func insertRepo(ctx context.Context, db *sql.DB, p repoRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO repos (id, name, backend, cwd, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.Name, p.Backend, p.Cwd, p.Status, p.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert repo %q: %w", p.ID, err)
	}
	return nil
}

// listRepos returns every repo, or only those with status when it is set, ordered by
// creation.
func listRepos(ctx context.Context, db *sql.DB, status LifecycleStatus) ([]repoRow, error) {
	query := `SELECT ` + repoColumns + ` FROM repos`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	rows, err := db.QueryContext(ctx, query+` ORDER BY created_at`, args...)
	if err != nil {
		return nil, fmt.Errorf("list repos: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []repoRow{}
	for rows.Next() {
		p, err := scanRepo(rows)
		if err != nil {
			return nil, fmt.Errorf("scan repo: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repos: %w", err)
	}
	return out, nil
}

// getRepo resolves a repo by canonical id first, then by name.
func getRepo(ctx context.Context, db *sql.DB, idOrName string) (repoRow, error) {
	p, err := scanRepo(db.QueryRowContext(ctx,
		`SELECT `+repoColumns+` FROM repos WHERE id = ? OR name = ?
		 ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END LIMIT 1`,
		idOrName, idOrName, idOrName))
	if errors.Is(err, sql.ErrNoRows) {
		return repoRow{}, notFoundf("repo not found: %s", idOrName)
	}
	if err != nil {
		return repoRow{}, fmt.Errorf("get repo %q: %w", idOrName, err)
	}
	return p, nil
}

func setRepoStatus(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE repos SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set repo %q status: %w", id, err)
	}
	return nil
}

func insertWorkstream(ctx context.Context, db *sql.DB, w workstreamRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO workstreams (id, repo_id, name, backend, backend_workspace_handle,
			branch, worktree, is_primary, ccnotes_project, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.RepoID, w.Name, w.Backend, w.WorkspaceHandle,
		w.Branch, w.Worktree, w.IsPrimary, w.CCNotesProject, w.Status, w.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert workstream %q: %w", w.ID, err)
	}
	return nil
}

// listWorkstreams returns every workstream, narrowed to repoFilter and/or status when
// either is set, ordered by creation.
func listWorkstreams(ctx context.Context, db *sql.DB, repoFilter string, status LifecycleStatus) ([]workstreamRow, error) {
	query := `SELECT ` + workstreamColumns + ` FROM workstreams`
	conds := []string{}
	args := []any{}
	if repoFilter != "" {
		conds = append(conds, `repo_id = ?`)
		args = append(args, repoFilter)
	}
	if status != "" {
		conds = append(conds, `status = ?`)
		args = append(args, status)
	}
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	rows, err := db.QueryContext(ctx, query+` ORDER BY created_at`, args...)
	if err != nil {
		return nil, fmt.Errorf("list workstreams: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []workstreamRow{}
	for rows.Next() {
		w, err := scanWorkstream(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workstream: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workstreams: %w", err)
	}
	return out, nil
}

// getWorkstream resolves a workstream by canonical id first, then by name. A name
// match is restricted to repoID when it is set, so two repos may hold a workstream
// of the same name without colliding; an empty repoID matches a name globally.
func getWorkstream(ctx context.Context, db *sql.DB, ref, repoID string) (workstreamRow, error) {
	w, err := scanWorkstream(db.QueryRowContext(ctx,
		`SELECT `+workstreamColumns+` FROM workstreams
		 WHERE id = ? OR (name = ? AND (? = '' OR repo_id = ?))
		 ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END LIMIT 1`,
		ref, ref, repoID, repoID, ref))
	if errors.Is(err, sql.ErrNoRows) {
		return workstreamRow{}, notFoundf("workstream not found: %s", ref)
	}
	if err != nil {
		return workstreamRow{}, fmt.Errorf("get workstream %q: %w", ref, err)
	}
	return w, nil
}

// getPrimaryWorkstream returns a repo's primary workstream: its own checkout, the
// single-stream default an agent spawns into when no workstream is named.
func getPrimaryWorkstream(ctx context.Context, db *sql.DB, repoID string) (workstreamRow, error) {
	w, err := scanWorkstream(db.QueryRowContext(ctx,
		`SELECT `+workstreamColumns+` FROM workstreams WHERE repo_id = ? AND is_primary = 1 LIMIT 1`, repoID))
	if errors.Is(err, sql.ErrNoRows) {
		return workstreamRow{}, notFoundf("no primary workstream for repo: %s", repoID)
	}
	if err != nil {
		return workstreamRow{}, fmt.Errorf("get primary workstream for repo %q: %w", repoID, err)
	}
	return w, nil
}

func setWorkstreamStatus(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE workstreams SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set workstream %q status: %w", id, err)
	}
	return nil
}

func insertSprint(ctx context.Context, db *sql.DB, sp sprintRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO sprints (id, workstream_id, name, ccnotes_sprint, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		sp.ID, sp.WorkstreamID, sp.Name, sp.CCNotesSprint, sp.Status, sp.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert sprint %q: %w", sp.ID, err)
	}
	return nil
}

// listSprints returns every sprint, narrowed to workstreamFilter and/or status when
// either is set, ordered by creation.
func listSprints(ctx context.Context, db *sql.DB, workstreamFilter string, status LifecycleStatus) ([]sprintRow, error) {
	query := `SELECT ` + sprintColumns + ` FROM sprints`
	conds := []string{}
	args := []any{}
	if workstreamFilter != "" {
		conds = append(conds, `workstream_id = ?`)
		args = append(args, workstreamFilter)
	}
	if status != "" {
		conds = append(conds, `status = ?`)
		args = append(args, status)
	}
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	rows, err := db.QueryContext(ctx, query+` ORDER BY created_at`, args...)
	if err != nil {
		return nil, fmt.Errorf("list sprints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []sprintRow{}
	for rows.Next() {
		sp, err := scanSprint(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sprint: %w", err)
		}
		out = append(out, sp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sprints: %w", err)
	}
	return out, nil
}

// getSprint resolves a sprint by canonical id first, then by name. A name match is
// restricted to workstreamID when it is set, so two workstreams may hold a sprint of
// the same name without colliding; an empty workstreamID matches a name globally.
func getSprint(ctx context.Context, db *sql.DB, ref, workstreamID string) (sprintRow, error) {
	sp, err := scanSprint(db.QueryRowContext(ctx,
		`SELECT `+sprintColumns+` FROM sprints
		 WHERE id = ? OR (name = ? AND (? = '' OR workstream_id = ?))
		 ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END LIMIT 1`,
		ref, ref, workstreamID, workstreamID, ref))
	if errors.Is(err, sql.ErrNoRows) {
		return sprintRow{}, notFoundf("sprint not found: %s", ref)
	}
	if err != nil {
		return sprintRow{}, fmt.Errorf("get sprint %q: %w", ref, err)
	}
	return sp, nil
}

// getDefaultSprint returns a workstream's default sprint: its earliest-created active
// sprint, ordered by creation then rowid to break a same-second tie in favour of the
// earlier insert. A killed sprint is never a default — an agent spawn that names no
// sprint falls back through this, and a killed sprint must never be the fallback.
func getDefaultSprint(ctx context.Context, db *sql.DB, workstreamID string) (sprintRow, error) {
	sp, err := scanSprint(db.QueryRowContext(ctx,
		`SELECT `+sprintColumns+` FROM sprints WHERE workstream_id = ? AND status = ? ORDER BY created_at, rowid LIMIT 1`,
		workstreamID, StatusActive))
	if errors.Is(err, sql.ErrNoRows) {
		return sprintRow{}, notFoundf("no active default sprint for workstream: %s", workstreamID)
	}
	if err != nil {
		return sprintRow{}, fmt.Errorf("get default sprint for workstream %q: %w", workstreamID, err)
	}
	return sp, nil
}

func setSprintStatus(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE sprints SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set sprint %q status: %w", id, err)
	}
	return nil
}

func insertAgent(ctx context.Context, db *sql.DB, a agentRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO agents (id, sprint_id, backend, backend_terminal_handle, session_id, scope,
			name, prompt, subject_id, ccnotes_task, status, state, activity, tokens, updated_at, created_at,
			restart_count, last_restart_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.SprintID, a.Backend, a.TerminalHandle, a.SessionID, a.Scope,
		a.Name, a.Prompt, a.SubjectID, a.CCNotesTask, a.Status, a.State, a.Activity, a.Tokens, a.UpdatedAt, a.CreatedAt,
		a.RestartCount, a.LastRestartAt)
	if err != nil {
		return fmt.Errorf("insert agent %q: %w", a.ID, err)
	}
	return nil
}

// listAgents returns every agent, narrowed to sprintFilter and/or status when either
// is set.
func listAgents(ctx context.Context, db *sql.DB, sprintFilter string, status LifecycleStatus) ([]agentRow, error) {
	query := `SELECT ` + agentColumns + ` FROM agents`
	conds := []string{}
	args := []any{}
	if sprintFilter != "" {
		conds = append(conds, `agents.sprint_id = ?`)
		args = append(args, sprintFilter)
	}
	if status != "" {
		conds = append(conds, `agents.status = ?`)
		args = append(args, status)
	}
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	return queryAgents(ctx, db, query+` ORDER BY agents.created_at`, args...)
}

// listWorkstreamAgents returns every agent of a workstream, joining through its
// sprints (an agent attaches to a sprint, the sprint to a workstream).
func listWorkstreamAgents(ctx context.Context, db *sql.DB, workstreamID string) ([]agentRow, error) {
	return queryAgents(ctx, db,
		`SELECT `+agentColumns+` FROM agents
		 JOIN sprints ON agents.sprint_id = sprints.id
		 WHERE sprints.workstream_id = ? ORDER BY agents.created_at`, workstreamID)
}

// listRepoAgents returns every agent of a repo, joining through its sprints and
// workstreams, narrowed to status when it is set.
func listRepoAgents(ctx context.Context, db *sql.DB, repoID string, status LifecycleStatus) ([]agentRow, error) {
	query := `SELECT ` + agentColumns + ` FROM agents
		 JOIN sprints ON agents.sprint_id = sprints.id
		 JOIN workstreams ON sprints.workstream_id = workstreams.id
		 WHERE workstreams.repo_id = ?`
	args := []any{repoID}
	if status != "" {
		query += ` AND agents.status = ?`
		args = append(args, status)
	}
	return queryAgents(ctx, db, query+` ORDER BY agents.created_at`, args...)
}

func listActiveAgents(ctx context.Context, db *sql.DB) ([]agentRow, error) {
	return queryAgents(ctx, db, `SELECT `+agentColumns+` FROM agents WHERE agents.status = ? ORDER BY agents.created_at`, StatusActive)
}

func queryAgents(ctx context.Context, db *sql.DB, query string, args ...any) ([]agentRow, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	defer func() { _ = rows.Close() }()
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
	a, err := scanAgent(db.QueryRowContext(ctx, `SELECT `+agentColumns+` FROM agents WHERE agents.id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return agentRow{}, notFoundf("agent not found: %s", id)
	}
	if err != nil {
		return agentRow{}, fmt.Errorf("get agent %q: %w", id, err)
	}
	return a, nil
}

// agentExists reports whether an agent row is present, the absent-vs-present branch
// restore needs: an absent agent is re-inserted from its bundle, a present one only
// has its terminal recreated.
func agentExists(ctx context.Context, db *sql.DB, id string) (bool, error) {
	return rowExists(ctx, db, "agents", id)
}

// rowExists reports whether a row with the given primary-key id is present in table.
// table is always a trusted internal constant (never user input), so interpolating it
// is safe. It is the absent-vs-present branch restore takes to recreate a wiped
// hierarchy without rewriting rows a live DB still holds.
func rowExists(ctx context.Context, db *sql.DB, table, id string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM `+table+` WHERE id = ? LIMIT 1`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check %s %q exists: %w", table, id, err)
	}
	return true, nil
}

func setAgentLifecycle(ctx context.Context, db *sql.DB, id string, status LifecycleStatus) error {
	_, err := db.ExecContext(ctx, `UPDATE agents SET status = ?, updated_at = ? WHERE id = ?`, status, nowStamp(), id)
	if err != nil {
		return fmt.Errorf("set agent %q lifecycle: %w", id, err)
	}
	return nil
}

func markRestartAttempt(ctx context.Context, db *sql.DB, id string, count int, at string) error {
	_, err := db.ExecContext(ctx, `UPDATE agents SET restart_count = ?, last_restart_at = ? WHERE id = ?`, count, at, id)
	if err != nil {
		return fmt.Errorf("mark agent %q restart attempt: %w", id, err)
	}
	return nil
}

func setAgentTerminalHandle(ctx context.Context, db *sql.DB, id, handle string) error {
	_, err := db.ExecContext(ctx, `UPDATE agents SET backend_terminal_handle = ? WHERE id = ?`, handle, id)
	if err != nil {
		return fmt.Errorf("set agent %q terminal handle: %w", id, err)
	}
	return nil
}

func resetRestart(ctx context.Context, db *sql.DB, id string) error {
	_, err := db.ExecContext(ctx, `UPDATE agents SET restart_count = 0, last_restart_at = '' WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("reset agent %q restart state: %w", id, err)
	}
	return nil
}

// applyStatus folds a transcript-derived Status into the agent row, stamping a
// fresh updated_at in RFC3339 UTC.
func applyStatus(ctx context.Context, db *sql.DB, id string, st Status) error {
	_, err := db.ExecContext(ctx,
		`UPDATE agents SET state = ?, activity = ?, tokens = ?, updated_at = ? WHERE id = ?`,
		st.State, statusActivity(st), st.Tokens, nowStamp(), id)
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

func clearConfig(ctx context.Context, db *sql.DB, key string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM config WHERE key = ?`, key)
	if err != nil {
		return fmt.Errorf("clear config %q: %w", key, err)
	}
	return nil
}

// configEntry is one persisted config key-value pair, the shape cco.config.list
// returns.
type configEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// listConfig returns every persisted config key-value pair, ordered by key.
func listConfig(ctx context.Context, db *sql.DB) ([]configEntry, error) {
	rows, err := db.QueryContext(ctx, `SELECT key, value FROM config ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("list config: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []configEntry{}
	for rows.Next() {
		var e configEntry
		if err := rows.Scan(&e.Key, &e.Value); err != nil {
			return nil, fmt.Errorf("scan config: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate config: %w", err)
	}
	return out, nil
}
