---
name: cc-orchestrate
description: Drive cc-orchestrate (the cco CLI) to spawn, watch, message, and kill fleets of Claude Code agents across backends (herd, superset, cmux, zellij, tmux). Use when orchestrating agents with cco or cc-orchestrate — creating repos, workstreams, and sprints; spawning agents into git worktrees; streaming agent status; or driving a fleet from a parent session over MCP.
---

# Driving cc-orchestrate (`cco`)

`cc-orchestrate` runs a fleet of Claude Code agents through one CLI instead of a
sprawl of terminal tabs. The binary is `cc-orchestrate`; the Homebrew cask also
installs a short **`cco`** alias — they are the same binary, fully interchangeable.
From source, use `go run .` or `./cc-orchestrate`. This skill uses `cco`.

For the exhaustive command/flag table and the full MCP tool list, read
`reference.md` beside this file.

## The model

A four-level tree; each level nests in the one above:

```
repo            a git repository
└─ workstream   a git worktree on its own branch — the unit of isolation
   └─ sprint    a grouping of agents that shares the workstream's worktree
      └─ agent  one spawned Claude Code session
```

- **A workstream is exactly one git worktree.** superset forks its own worktree
  (cc-orchestrate adopts the path); every other backend gets a `git worktree add`
  under `~/.cc-orchestrate/worktrees`. The repo's *primary* workstream is the repo's
  own checkout and is never removed.
- Creating a repo auto-provisions its primary workstream and a default sprint, so the
  single-stream flow needs no extra steps. Reach for explicit workstreams to isolate
  parallel features, and sprints to batch a workstream's agents.
- A lazy **daemon** owns all state (SQLite under `~/.cc-orchestrate`) and tails each
  agent's transcript. It auto-starts on first use — never launch it by hand.

## Backends

The runtime that places and spawns an agent. cc-orchestrate uses the first installed
one in a fixed precedence: **herd, superset, cmux, zellij, tmux**. Inspect and pin:

```bash
cco backends list                # show installed backends and the effective default (*)
cco backends select tmux         # pin one (must be installed)
```

## Core flow

```bash
# 1. Track a repo (also provisions its primary workstream + default sprint)
cco repo create demo --cwd .

# 2. Open an isolated workstream — cuts a git worktree on a new branch
cco workstream create feat-x --repo demo
# → prints: workstream id, branch, worktree path

# 3. Spawn an agent into the workstream (runs in the worktree, not the repo root)
cco agent spawn --workstream feat-x --name a1 --prompt "summarize the repo and wait"
# → prints: agent id, backend, terminal

# 4. Observe the fleet
cco fleet status                 # one joined table across every repo/workstream/sprint
cco agent list                   # point-in-time snapshot of every agent
cco agent show a1f3c2            # one agent's derived status (state, tokens, activity)
cco agent capture a1f3c2         # the agent's current terminal screen as text
cco agent watch --id a1f3c2      # stream that agent's events, one line each
cco fleet watch                  # stream every fleet lifecycle frame

# 5. Steer and stop
cco agent send-message a1f3c2 "now open a PR with your summary"
cco agent kill a1f3c2
cco agent respawn a1f3c2                 # revive it later, same session
cco workstream kill feat-x --repo demo   # tears down the worktree
```

Every data command takes `--json` for the daemon's reply verbatim — the shape
scripts and jq pipelines should consume.

The active repo / workstream / sprint set the target for a bare `cco agent spawn`.
`cco <entity> activate <id|name>` sets them; the most recent activation wins, and a
deeper `activate` resets the more-specific selections. With only `--repo`, a spawn
lands in that repo's primary workstream and default sprint; `--workstream` and
`--sprint` target deeper.

## How status and messaging flow

Everything rides an event plane, not the terminal:

- **Status** is derived by tailing each agent's transcript
  (`~/.claude/projects/**/<session-id>.jsonl`); `agent status` / `agent list` read it.
- **Orchestrator to agent** rides the agent's watch Monitor: `agent send-message`
  reaches the child as a new instruction.
- **Agent to orchestrator** rides the `report` MCP tool wired into every spawned agent;
  the child calls it to report progress, a result, or a question.

`agent list` / `agent status` are snapshots — for live status, run `cco agent watch`
under a Monitor.

## Drive a fleet from a parent agent over MCP

A parent Claude Code session can run the whole fleet through MCP tools. Register the
control server in the parent's `.mcp.json`:

```json
{
  "mcpServers": {
    "cc-orchestrate": { "command": "cc-orchestrate", "args": ["mcp"] }
  }
}
```

It exposes one tool per op, grouped by entity: `backends_list` / `backend_select`;
`config_get` / `config_set`; `repo_*`; `workstream_*`; `sprint_*` (including
`sprint_kill`); `agent_spawn` / `agent_list` / `agent_show` / `agent_send_message` /
`agent_kill` / `agent_respawn` / `agent_capture`; and `fleet_status` /
`fleet_serialize` / `fleet_restore`. The MCP surface is request/response only; for
live status, run `cco agent watch` or `cco fleet watch` alongside it. See
`reference.md` for the full tool list and every flag. The same ops are also an HTTP
API with a fleet-wide event stream — `docs/xrpc.md` in the repo covers it.
