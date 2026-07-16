# `cco` command reference

Every command, flag, and MCP tool. The binary is `cc-orchestrate`; `cco` is the
Homebrew alias for it. Run `cco <command> --help` for the canonical text.

## Domain commands

### backends — inspect and pin the runtime

| Command | Args | Description |
|---|---|---|
| `cco backends list` | none | List backends and availability (`BACKEND`, `INSTALLED`, `DEFAULT`). `*` marks the effective default. |
| `cco backends select <backend>` | exactly 1 | Persist the default backend. Must be installed. |

Precedence when none is pinned: **herd, superset, cmux, zellij, tmux** (first installed wins).

### config — read persisted config

| Command | Args | Description |
|---|---|---|
| `cco config get <key>` | exactly 1 | Print one config value. Keys: `backend`, `active-repo`, `active-workstream`, `active-sprint`. |

### repo — backend workspaces over a git repo

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco repo list` | none | — | List repos (`ID`, `NAME`, `BACKEND`, `STATUS`, `CWD`). |
| `cco repo create <name>` | exactly 1 | `--backend <b>` (default: selected/first available), `--cwd <dir>` (default: cwd) | Create a repo + backend workspace; provisions its primary workstream and default sprint. |
| `cco repo activate <id>` | exactly 1 | — | Mark a repo active (sets `active-repo`). |
| `cco repo kill <id>` | exactly 1 | — | Soft-terminate a repo; cascades to its workstreams, sprints, and agents. |

### workstream (alias `ws`) — one git worktree per branch

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco workstream list` | none | `--repo <r>` | List workstreams (`ID`, `NAME`, `REPO`, `BRANCH`, `WORKTREE`, `PRIMARY`, `STATUS`). |
| `cco workstream create <name>` | exactly 1 | `--repo <r>`, `--branch <b>` (default: the name) | Create a workstream + worktree + backend workspace. |
| `cco workstream activate <id\|name>` | exactly 1 | `--repo <r>` (disambiguate name) | Mark active and the spawn default. |
| `cco workstream kill <id\|name>` | exactly 1 | `--repo <r>` (disambiguate name) | Tear down the backend workspace and remove the worktree. |

### sprint — group a workstream's agents

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco sprint list` | none | `--workstream <w>` | List sprints (`ID`, `NAME`, `WORKSTREAM`, `STATUS`). |
| `cco sprint create <name>` | exactly 1 | `--workstream <w>` | Create a sprint in a workstream. |
| `cco sprint activate <id\|name>` | exactly 1 | `--workstream <w>` (disambiguate name) | Mark active and the spawn default. |

### agent — spawn and control child sessions

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco agent spawn` | none | `--repo <r>` \| `--workstream <w>` \| `--sprint <s>`, `--name <n>`, `--cwd <dir>` (default: the workstream worktree), `--prompt <p>` | Spawn a child agent into a sprint. With only `--repo`, lands in its primary workstream's default sprint. Prints `agent`, `backend`, `terminal`. |
| `cco agent list` | none | `--repo <r>` | List agents (`ID`, `NAME`, `SPRINT`, `BACKEND`, `STATE`, `STATUS`, `TOKENS`, `RESTARTS`, `ACTIVITY`). |
| `cco agent status <id>` | exactly 1 | — | One agent's derived status (`status`, `state`, `activity`, `tokens`, `restart`, `updated`). |
| `cco agent send-message <id> <text>` | exactly 2 | — | Send an instruction to a running agent. Prints `seq`, `transport`. |
| `cco agent watch` | none | `--id <id>` \| `--all` (exactly one required) | Stream agent status/report events as line-delimited JSON. `--all` tags each line with `agent_id`. Runs under a Monitor. |
| `cco agent kill <id>` | exactly 1 | — | Kill a running agent. |

### serialize / restore — snapshot and rehydrate the fleet

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco serialize` | none | `--out <path>` (default: the serialize dir) | Snapshot every active agent into a restorable bundle. Prints `path`, `count`. |
| `cco restore <bundle>` | exactly 1 | — | Restore agents from a bundle (re-inserts missing rows, resumes sessions). Prints `count`. |

### mcp — parent-facing control server

`cco mcp` runs the MCP control server over stdio (no args). Register it in a parent
session's `.mcp.json` to drive the fleet from another agent. See *MCP tools* below.

## Substrate commands (cc-interact plane)

The low-level event plane most workflows never touch directly. `--session` defaults to
`cc-orchestrate` where it applies.

| Command | Description |
|---|---|
| `cco daemon` | Start the daemon and keep it alive (auto-starts on first use). |
| `cco status` | Query the daemon's status/version. |
| `cco stop` | Stop the daemon. |
| `cco watch` | Stream events from a session (`--session`, `--subject-id`, `--consumer`, `--exclude-origin`). |
| `cco session record` | Record a transcript/events for a session (`--session`, `--output`). |
| `cco guard-edit` | Edit a guard-style prompt (`--session`, `--name`, `--edit`). |
| `cco channel` | Send a message over the channel (`--session`, `--subject-id`, `--text`, `--file`). |
| `cco channel-ack` | Acknowledge a channel message (`--session`, `--subject-id`, `--seq`). |

## MCP tools

`cco mcp` exposes one tool per op, mirroring the CLI:

- **backends**: `backends_list`, `backend_select`
- **config**: `config_get`, `config_set`
- **repo**: `repo_create`, `repo_list`, `repo_activate`, `repo_kill`
- **workstream**: `workstream_create`, `workstream_list`, `workstream_activate`, `workstream_kill`
- **sprint**: `sprint_create`, `sprint_list`, `sprint_activate`
- **agent**: `agent_spawn`, `agent_list`, `agent_show`, `agent_send_message`, `agent_kill`
- **fleet**: `fleet_serialize`, `fleet_restore`

The MCP surface is request/response only — `agent_list` and `agent_show` return a
point-in-time snapshot. For live status, run `cco agent watch` under a Monitor
alongside the MCP session. (`agent watch` is CLI-only.)
