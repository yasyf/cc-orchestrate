package orchestrate

import "github.com/yasyf/cc-interact/store"

const databaseDDL = `
CREATE TABLE repos (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  backend    TEXT NOT NULL,
  cwd        TEXT NOT NULL,
  status     TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE workstreams (
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
);
CREATE TABLE sprints (
  id             TEXT PRIMARY KEY,
  workstream_id  TEXT NOT NULL,
  name           TEXT NOT NULL,
  ccnotes_sprint TEXT,
  status         TEXT NOT NULL,
  created_at     TEXT NOT NULL
);
CREATE TABLE orchestrate_agents (
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
);
CREATE TABLE config (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE UNIQUE INDEX orchestrate_agents_session_id_unique
  ON orchestrate_agents(session_id) WHERE session_id <> '';
`

func databaseStoreSchema() store.Schema { return store.Schema{DDL: databaseDDL} }
