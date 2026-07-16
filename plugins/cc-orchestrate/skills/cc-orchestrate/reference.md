# `cco` command reference

Every command, flag, and MCP tool. The binary is `cc-orchestrate`; `cco` is the
Homebrew alias for it. Run `cco <command> --help` for the canonical text.

Every data-emitting domain command takes the persistent `--json` flag and prints
the daemon's JSON reply verbatim (cc-interact's substrate commands keep their own
output). The `list` commands take `--status <active|exited|killed>` to filter.

## Domain commands

### backends — inspect and pin the runtime

| Command | Args | Description |
|---|---|---|
| `cco backends list` | none | List backends and availability (`BACKEND`, `INSTALLED`, `DEFAULT`). `*` marks the effective default. |
| `cco backends select <backend>` | exactly 1 | Persist the default backend. Must be installed. |

Precedence when none is pinned: **herd, superset, cmux, zellij, tmux** (first installed wins).

### config — read and write persisted config

| Command | Args | Description |
|---|---|---|
| `cco config get <key>` | exactly 1 | Print one config value. Keys: `backend`, `active-repo`, `active-workstream`, `active-sprint`. |
| `cco config set <key> <value>` | exactly 2 | Upsert one config value. |
| `cco config unset <key>` | exactly 1 | Delete one config key. |
| `cco config list` | none | List every persisted key (`KEY`, `VALUE`). |

### repo — backend workspaces over a git repo

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco repo list` | none | `--status <s>` | List repos (`ID`, `NAME`, `BACKEND`, `STATUS`, `CWD`). |
| `cco repo show <id\|name>` | exactly 1 | — | One repo's fields as key/value lines. |
| `cco repo create <name>` | exactly 1 | `--backend <b>` (default: selected/first available), `--cwd <dir>` (default: cwd) | Create a repo + backend workspace; provisions its primary workstream and default sprint. |
| `cco repo activate <id>` | exactly 1 | — | Mark a repo active (sets `active-repo`). |
| `cco repo kill <id>` | exactly 1 | — | Soft-terminate a repo; cascades to its workstreams, sprints, and agents. |

### workstream (alias `ws`) — one git worktree per branch

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco workstream list` | none | `--repo <r>`, `--status <s>` | List workstreams (`ID`, `NAME`, `REPO`, `BRANCH`, `WORKTREE`, `PRIMARY`, `STATUS`). |
| `cco workstream show <id\|name>` | exactly 1 | `--repo <r>` (disambiguate name) | One workstream's fields as key/value lines. |
| `cco workstream create <name>` | exactly 1 | `--repo <r>`, `--branch <b>` (default: the name) | Create a workstream + worktree + backend workspace. |
| `cco workstream activate <id\|name>` | exactly 1 | `--repo <r>` (disambiguate name) | Mark active and the spawn default. |
| `cco workstream kill <id\|name>` | exactly 1 | `--repo <r>` (disambiguate name) | Tear down the backend workspace and remove the worktree. |

### sprint — group a workstream's agents

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco sprint list` | none | `--workstream <w>`, `--status <s>` | List sprints (`ID`, `NAME`, `WORKSTREAM`, `STATUS`). |
| `cco sprint show <id\|name>` | exactly 1 | `--workstream <w>` (disambiguate name) | One sprint's fields as key/value lines. |
| `cco sprint create <name>` | exactly 1 | `--workstream <w>` | Create a sprint in a workstream. |
| `cco sprint activate <id\|name>` | exactly 1 | `--workstream <w>` (disambiguate name) | Mark active and the spawn default. |
| `cco sprint kill <id\|name>` | exactly 1 | `--workstream <w>` (disambiguate name) | Kill a sprint: exits its agents and tears down their terminals. |

### agent — spawn and control child sessions

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco agent spawn` | none | `--repo <r>` \| `--workstream <w>` \| `--sprint <s>`, `--name <n>`, `--cwd <dir>` (default: the workstream worktree), `--prompt <p>` | Spawn a child agent into a sprint. With only `--repo`, lands in its primary workstream's default sprint. Prints `agent`, `backend`, `terminal`. |
| `cco agent list` | none | `--repo <r>`, `--status <s>` | List agents (`ID`, `NAME`, `SPRINT`, `BACKEND`, `STATE`, `STATUS`, `TOKENS`, `RESTARTS`, `ACTIVITY`). |
| `cco agent show <id>` | exactly 1 | — | One agent's derived status (alias: `status`). |
| `cco agent send-message <id> <text>` | exactly 2 | — | Send an instruction to a running agent over the event plane. Prints `seq`. |
| `cco agent watch` | none | `--id <id>` \| `--all` (exactly one required) | Stream agent events, one formatted line each; `--json` restores the raw NDJSON (`--all` tags lines with `agent_id`). Runs under a Monitor. |
| `cco agent kill <id>` | exactly 1 | — | Kill a running agent. |
| `cco agent respawn [<id>]` | 0 or 1 | `--dead` (exactly one of arg/flag) | Revive one exited agent into its old session, or sweep every eligible exited agent. |
| `cco agent capture <id>` | exactly 1 | — | Print the agent's current terminal screen as text. |

### fleet — the fleet-wide snapshot and stream

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco fleet status` | none | `--watch` (live repaint on fleet events) | Summary head + a joined agent table across every repo/workstream/sprint. |
| `cco fleet watch` | none | — | Stream fleet lifecycle frames, one formatted line each; `--json` for the raw NDJSON frames. |

### serialize / restore — snapshot and rehydrate the fleet

| Command | Args | Flags | Description |
|---|---|---|---|
| `cco serialize` | none | `--out <path>` (default: the serialize dir) | Snapshot every active agent into a restorable bundle. Prints `path`, `count`. |
| `cco restore <bundle>` | exactly 1 | — | Restore agents from a bundle (re-inserts missing rows, resumes sessions). Prints `count`. |

### mcp — parent-facing control server

`cco mcp` runs the MCP control server over stdio (no args). Register it in a parent
session's `.mcp.json` to drive the fleet from another agent. See *MCP tools* below.

### setup-channels — approve channel delivery (hidden)

| Command | Flags | Description |
|---|---|---|
| `cco setup-channels` | `--check` (default), `--apply`, `--decline` | `--check` prints `{"offer": bool, "reason": "…"}`. `--apply` adds the cc-orchestrate plugin to Claude's managed channel allowlist (one-time macOS admin prompt) so spawned agents receive `<channel source="cc-orchestrate">` tags. `--decline` records the refusal so the offer is not repeated. |

## Substrate commands (cc-interact plane)

The low-level event plane most workflows never touch directly. `--session` defaults to
`cc-orchestrate` where it applies; `cco channel` instead defaults to
`$CLAUDE_CODE_SESSION_ID`, so the plugin's flagless invocation binds the hosting
Claude session.

| Command | Description |
|---|---|
| `cco daemon` | Start the daemon and keep it alive (auto-starts on first use). |
| `cco status` | Query the daemon's status/version. |
| `cco stop` | Stop the daemon. |
| `cco watch` | Stream events from a session (`--session`, `--subject-id`, `--consumer`, `--exclude-origin`). |
| `cco session record` | Record a transcript/events for a session (`--session`, `--output`). |
| `cco guard-edit` | Edit a guard-style prompt (`--session`, `--name`, `--edit`). |
| `cco channel` | Run the channel MCP server over stdio (`--session`, default `$CLAUDE_CODE_SESSION_ID`; `--cwd`, default the current directory). Loaded into every session by the cc-orchestrate plugin; pushes the session's events as channel tags and serves the `report` tool. |
| `cco channel-ack` | Prove the channel round trip when the first tag arrives (`--session`, `--cwd`). |

## MCP tools

`cco mcp` exposes one tool per op, mirroring the CLI:

- **backends**: `backends_list`, `backend_select`
- **config**: `config_get`, `config_set`
- **repo**: `repo_create`, `repo_list`, `repo_activate`, `repo_kill`
- **workstream**: `workstream_create`, `workstream_list`, `workstream_activate`, `workstream_kill`
- **sprint**: `sprint_create`, `sprint_list`, `sprint_activate`, `sprint_kill`
- **agent**: `agent_spawn`, `agent_list`, `agent_show`, `agent_send_message`, `agent_kill`, `agent_respawn`, `agent_capture`
- **fleet**: `fleet_status`, `fleet_serialize`, `fleet_restore`

The MCP surface is request/response only — `agent_list`, `agent_show`, and
`fleet_status` return point-in-time snapshots. For live status, run `cco agent
watch` or `cco fleet watch` under a Monitor alongside the MCP session, or
consume the HTTP event stream (`docs/xrpc.md` in the repo). The `show` verbs on
repo/workstream/sprint and `config list`/`unset` are CLI- and HTTP-only.
