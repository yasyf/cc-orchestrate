# ![cc-orchestrate](docs/assets/readme-banner.webp)

**Stop letting your Claude agents fight over one working copy.** cc-orchestrate cuts a fresh worktree per workstream and spawns Claude agents across five backends, all driven from one CLI or 19 MCP tools.

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-orchestrate/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/cc-orchestrate)](https://github.com/yasyf/cc-orchestrate/releases)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

## Get started

```bash
brew install --cask yasyf/tap/cc-orchestrate
cco --help
```

<img src="docs/assets/demo.png" alt="Terminal running 'cco --help' — the command groups for agents, repos, workstreams, sprints, backends, serialize/restore, and the MCP server" width="700">

Homebrew installs the `cc-orchestrate` binary plus a short `cco` alias — the same binary. This README uses `cco`.

Driving with an agent? Paste this:

```text
/plugin marketplace add yasyf/cc-orchestrate
/plugin install cc-orchestrate@cc-orchestrate
```

The plugin loads the command surface into context and registers a `/cc-orchestrate` command, so Claude drives the fleet with the `cco` CLI directly.

---

## Use cases

### Work three branches at once without agents clobbering each other's checkout

Two agents editing one checkout stomp each other's files and race on the git index. Give each stream of work its own worktree, then spawn into it:

```bash
cco repo create demo --cwd .
cco workstream create feat-x --repo demo
cco agent spawn --workstream feat-x --name a1 --prompt "summarize the repo and wait"
```

`agent spawn` places the agent in a fresh terminal on the selected backend and prints where it landed:

```
agent:    a1f3c2
backend:  tmux
terminal: feat-x:0.0
```

Repeat `workstream create` per branch. Agents inside a workstream share its worktree; no two workstreams ever share one.

### Drive the whole fleet from a parent Claude session over MCP

Orchestrating by hand makes you the router between terminal tabs. Register the control server in the parent session's `.mcp.json` and hand the fleet to Claude:

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

The server exposes 19 request/response tools, one per orchestration op, grouped by entity (`backends_list`, `repo_create`, `workstream_create`, `sprint_create`, `agent_spawn`, `agent_status`, …). `agent_list` and `agent_status` return point-in-time snapshots, so run `cco agent watch` under a monitor alongside the MCP session for live status.

### Snapshot tonight's running fleet and restore it tomorrow

Closing your laptop at 6pm shouldn't cost you the fleet. Serialize every active agent into a bundle, then rehydrate from it:

```bash
cco serialize
# the next morning
cco restore ~/.cc-orchestrate/serialize/20260702T180000Z.json
```

`serialize` prints `serialized 3 agent(s) to <bundle>`. `restore` re-inserts any missing rows and resumes each agent's session into a fresh backend terminal.

## The model

cc-orchestrate models the work as a four-level tree, so isolation is structural, not conventional:

```
repo            a git repository
└─ workstream   a git worktree on its own branch — the unit of isolation
   └─ sprint    a grouping of agents that shares the workstream's worktree
      └─ agent  one spawned Claude Code session
```

- **Backend** — the runtime that places and spawns an agent: a terminal multiplexer
  or workspace manager. cc-orchestrate uses the first installed one in a fixed
  precedence — herd, superset, cmux, zellij, then tmux — and `backends select` pins one.
- **Repo** — a git repository cc-orchestrate tracks. Creating a repo records it and
  provisions a *primary* workstream over the repo's existing checkout, with a default
  sprint, so the single-stream flow needs no extra steps.
- **Workstream** — one git worktree on its own branch: strictly one worktree per
  workstream, never per agent. It owns the backend workspace agents spawn into.
- **Sprint** — a grouping of agents inside a workstream. Every workstream gets a
  default sprint, so you only reach for sprints to slice a workstream's agents into
  named batches.
- **Agent** — one spawned Claude Code session, a cc-interact subject keyed by its
  `--session-id`. Its status, messages, and reports flow through the subject's event log.
- **Daemon** — a lazy, auto-started process that owns all state under
  `~/.cc-orchestrate` (repos, workstreams, sprints, agents, and config in SQLite) and
  tails each agent's transcript. The CLI starts it on first use; you never launch it
  by hand.

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
| `mcp` | Run the parent-facing MCP control server over stdio. |

The active repo, workstream, and sprint set the target for a bare `agent spawn`. Each
`activate` resets the more-specific selections, so the most recent activation wins;
killing an active entity clears its selection.

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
tool. It builds on [cc-interact](https://github.com/yasyf/cc-interact), which supplies
the lazy daemon, the append-only SQLite event log, the HTTP/SSE plane, and the MCP
channel — cc-orchestrate adds repos, workstreams, sprints, agents, and the five backend
drivers on top. The mechanics live in [AGENTS.md](AGENTS.md).

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

Licensed under [PolyForm Noncommercial 1.0.0](LICENSE).
