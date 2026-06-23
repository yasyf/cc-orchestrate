# cc-orchestrate

![cc-orchestrate banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-orchestrate/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

Orchestrate fleets of Claude Code agents over pluggable backends, from one seat.

cc-orchestrate drives a fleet of Claude Code agents through one CLI instead of a
sprawl of terminal tabs. It models the work as a four-level tree and gives every
workstream its own git worktree, so two agents in the same repo never trip over each
other's checkout. Every interaction rides an event plane, not the terminal, so the
orchestration never cares which backend an agent runs on.

```
repo            a git repository
└─ workstream   a git worktree on its own branch — the unit of isolation
   └─ sprint    a grouping of agents that shares the workstream's worktree
      └─ agent  one spawned Claude Code session
```

It builds on [cc-interact](https://github.com/yasyf/cc-interact), which supplies the
lazy daemon, the append-only SQLite event log, the HTTP/SSE plane, and the MCP
channel. cc-orchestrate adds repos, workstreams, sprints, agents, and the five backend
drivers on top.

## Install

```bash
brew install --cask yasyf/tap/cc-orchestrate
```

Homebrew installs the `cc-orchestrate` binary plus a short **`cco`** alias — the same
binary, so use whichever you prefer. This README uses `cco`.

## Concepts

- **Backend** — the runtime that places and spawns an agent: a terminal multiplexer
  or workspace manager. cc-orchestrate uses the first installed one in a fixed
  precedence: herd, superset, cmux, zellij, then tmux. Pin one with `backends select`.
- **Repo** — a git repository cc-orchestrate tracks. Creating a repo records it and
  provisions a *primary* workstream over the repo's existing checkout, with a default
  sprint, so the single-stream flow needs no extra steps.
- **Workstream** — one git worktree on its own branch, and the unit of isolation:
  strictly one worktree per workstream, never per agent. It owns the backend workspace
  agents spawn into.
- **Sprint** — a grouping of agents inside a workstream that shares the workstream's
  worktree. Every workstream gets a default sprint, so you only reach for sprints when
  you want to slice a workstream's agents into named batches.
- **Agent** — one spawned Claude Code session, a cc-interact subject keyed by its
  `--session-id`. It belongs to a sprint, runs in that sprint's workstream worktree,
  and its status, messages, and reports flow through the subject's event log.
- **Daemon** — a lazy, auto-started process that owns all state under
  `~/.cc-orchestrate` (repos, workstreams, sprints, agents, and config in SQLite) and
  tails each agent's transcript. The CLI starts it on first use; you never launch it
  by hand.

## Quickstart

Pin a backend, create a repo, open an isolated workstream, and spawn an agent into it:

```bash
cco backends select tmux                 # pin a runner (default: first installed)
cco repo create demo --cwd .             # records the repo + its primary workstream
cco workstream create feat-x --repo demo # cuts a git worktree on a new branch
cco agent spawn --workstream feat-x --name a1 --prompt "summarize the repo and wait"
```

`agent spawn` prints the new agent's id, backend, and terminal:

```
agent:    a1f3c2
backend:  tmux
terminal: feat-x:0.0
```

Watch the fleet, send a follow-up instruction, then tear it down:

```bash
cco agent list
cco agent status a1f3c2
cco agent send-message a1f3c2 "now open a PR with your summary"
cco agent kill a1f3c2
cco workstream kill feat-x --repo demo   # removes the worktree
```

The active repo, workstream, and sprint set the target for a bare `agent spawn`. Each
`activate` resets the more-specific selections, so the most recent activation wins;
killing an active entity clears its selection.

## Commands

cco groups its surface by what you're orchestrating. Run any command with `--help` for
its flags.

| Command | What it does |
| --- | --- |
| `backends list` / `backends select <backend>` | Show installed runners and pin the default. |
| `config get <key>` | Read a persisted config value (`backend`, `active-repo`, `active-workstream`, `active-sprint`). |
| `repo list` / `create` / `activate` / `kill` | Manage repos. A kill cascades to the repo's workstreams, sprints, and agents. |
| `workstream list` / `create` / `activate` / `kill` | Manage worktrees (alias `ws`). A kill tears down the backend workspace and removes the worktree. |
| `sprint list` / `create` / `activate` | Group a workstream's agents. |
| `agent spawn` | Spawn a Claude agent into the targeted repo (primary workstream), workstream, or sprint. |
| `agent list` / `status <id>` | Read a point-in-time snapshot of the fleet or one agent. |
| `agent send-message <id> "text"` | Push an instruction to a running agent. |
| `agent watch --all` / `--id <id>` | Stream agent events as line-delimited JSON. |
| `agent kill <id>` | Stop a running agent. |
| `serialize` / `restore <bundle>` | Snapshot every active agent into a restorable bundle, then rehydrate the fleet from one. |
| `mcp` | Run the parent-facing MCP control server over stdio (see below). |

Beneath the domain commands, cco re-exposes the cc-interact substrate (`daemon`,
`status`, `stop`, `watch`, `session record`, `guard-edit`, `channel`, `channel-ack`).
You rarely touch these — the daemon auto-starts, and status and messaging normally flow
through `agent …`.

## How it works

A workstream is exactly one git worktree, kept 1:1 no matter how the backend behaves:
cc-orchestrate either adopts the path superset reports or runs `git worktree add`
itself under `~/.cc-orchestrate/worktrees/`, and colocates an independent jj repo on
Jujutsu checkouts. Status comes from tailing each agent's transcript; the orchestrator
reaches agents over their watch monitor, and agents report back through a `report` MCP
tool. The mechanics live in [AGENTS.md](AGENTS.md).

## Drive a fleet from a parent agent over MCP

A parent Claude Code session can run the whole fleet through MCP tools. Register the
control server in the parent's `.mcp.json`:

```json
{
  "mcpServers": {
    "cc-orchestrate": {
      "command": "cc-orchestrate",
      "args": ["mcp"]
    }
  }
}
```

The server exposes one request/response tool per orchestration op, grouped by entity
(`backends_list`, `repo_create`, `workstream_create`, `sprint_create`, `agent_spawn`,
`agent_status`, …). Because `agent_list` and `agent_status` return a point-in-time
snapshot, run `cco agent watch` under a monitor alongside the MCP session for live
status.

## Claude Code plugin

cc-orchestrate ships a Claude Code plugin so a parent Claude session knows how to drive
the fleet. Add the marketplace and install the plugin:

```
/plugin marketplace add yasyf/cc-orchestrate
/plugin install cc-orchestrate@cc-orchestrate
```

The skill loads the command surface into context and registers a `/cc-orchestrate`
command, so Claude can orchestrate with the `cco` CLI directly.

## cc-notes integration

When a repo already uses [cc-notes](https://github.com/yasyf/cc-notes), cc-orchestrate
mirrors its tree into cc-notes entities: a workstream becomes a cc-notes project, a
sprint becomes a cc-notes sprint, and each spawned agent becomes a cc-notes task tagged
with the workstream's branch. The cc-notes library is linked and driven in-process, so
there's no `cc-notes` binary to install for the integration to run.

The binding is gated on the repo already holding cc-notes entities (`refs/cc-notes/*`):
repos that don't use cc-notes spawn exactly as before. Opt a repo in by creating
cc-notes entities in it with the cc-notes CLI; from then on cc-orchestrate keeps the
two trees in sync.

## Documentation

The conventions and architecture live in [AGENTS.md](AGENTS.md) and
[STYLEGUIDE.md](STYLEGUIDE.md). For the framework underneath, read
[cc-interact](https://github.com/yasyf/cc-interact).

## License

PolyForm-Noncommercial-1.0.0. See [LICENSE](LICENSE).
