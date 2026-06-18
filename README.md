# cc-orchestrate

![cc-orchestrate banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-orchestrate/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

Orchestrate fleets of Claude Code agents over pluggable backends, from one seat.

cc-orchestrate drives a fleet of Claude Code agents through one CLI instead of a
sprawl of terminal tabs. It models the work as a four-level tree and gives every
workstream its own git worktree, so two agents working the same repo never trip over
each other's checkout. Backends are pluggable: the same commands spawn, message, and
watch agents whether they live in herd workspaces, superset worktrees, cmux sessions,
or plain zellij and tmux panes. Every interaction rides an event plane, not the
terminal, so the orchestration never cares where an agent actually runs.

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

Install with Homebrew:

```bash
brew install --cask yasyf/tap/cc-orchestrate
```

Or build from source with the Go toolchain:

```bash
go install github.com/yasyf/cc-orchestrate@latest
```

## Concepts

- **Backend** — the runtime that places and spawns an agent: a terminal multiplexer
  or workspace manager. cc-orchestrate uses the first installed one in a fixed
  precedence: herd, superset, cmux, zellij, then tmux. Pin one with `backends select`.
- **Repo** — a git repository cc-orchestrate tracks. Creating a repo records it and
  provisions a *primary* workstream over the repo's existing checkout, with a default
  sprint, so the single-stream flow needs no extra steps.
- **Workstream** — one git worktree on its own branch, and the unit of isolation:
  strictly one worktree per workstream, never per agent. It owns the backend workspace
  agents spawn into. A backend that forks its own worktree (superset) is adopted; for
  every other backend cc-orchestrate runs `git worktree add` under
  `~/.cc-orchestrate/worktrees`. On a Jujutsu repo it colocates a fresh jj repo inside
  that worktree, so jj workspaces never collide.
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

See which backends are installed and which one is the effective default:

```bash
cc-orchestrate backends list
```

```
BACKEND   INSTALLED  DEFAULT
herd      yes        *
superset  yes
cmux      no
zellij    yes
tmux      yes
```

`INSTALLED` is `no` when a backend's CLI isn't on your `PATH`. The `*` marks the
default: the backend you pinned, or the first installed one when you've pinned none.
Pin tmux for this walkthrough:

```bash
cc-orchestrate backends select tmux
```

Create a repo in the current directory. This also provisions its primary workstream
(the current checkout) and a default sprint:

```bash
cc-orchestrate repo create demo --cwd .
```

Open an isolated workstream for a feature. cc-orchestrate cuts a git worktree on a new
branch and prints where it landed:

```bash
cc-orchestrate workstream create feat-x --repo demo
```

```
workstream: feat-x-bd73b6c8
branch:     feat-x
worktree:   ~/.cc-orchestrate/worktrees/demo-0bea8a2c/feat-x
```

Spawn an agent into that workstream with a prompt; it runs in the worktree, not the
repo root:

```bash
cc-orchestrate agent spawn --workstream feat-x --name a1 --prompt "summarize the repo and wait"
```

`agent spawn` prints the new agent's id, backend, and terminal:

```
agent:    a1f3c2
backend:  tmux
terminal: feat-x:0.0
```

List the fleet, then read one agent's status derived from its transcript:

```bash
cc-orchestrate agent list
cc-orchestrate agent status a1f3c2
```

Send a new instruction to a running agent; it arrives on the agent's watch Monitor:

```bash
cc-orchestrate agent send-message a1f3c2 "now open a PR with your summary"
```

When the agent is done, kill it; then tear down the workstream, which removes its
worktree:

```bash
cc-orchestrate agent kill a1f3c2
cc-orchestrate workstream kill feat-x --repo demo
```

## Commands

cc-orchestrate groups its surface by what you're orchestrating:

- `backends list` / `backends select <backend>` — show installed runners and pin the default.
- `config get <key>` — read a persisted config value (`backend`, `active-repo`, `active-workstream`, `active-sprint`).
- `repo list` / `repo create <name> [--backend B] [--cwd DIR]` / `repo activate <id>` / `repo kill <id>` — manage repos. A kill soft-terminates the repo and cascades to its workstreams, sprints, and agents.
- `workstream list [--repo R]` / `workstream create <name> [--repo R] [--branch B]` / `workstream activate <id|name>` / `workstream kill <id|name>` — manage worktrees (alias `ws`). A kill tears down the backend workspace and removes the worktree.
- `sprint list [--workstream W]` / `sprint create <name> [--workstream W]` / `sprint activate <id|name>` — group a workstream's agents.
- `agent spawn [--repo R | --workstream W | --sprint S] [--name N] [--cwd DIR] --prompt "..."` — spawn a Claude agent. With only `--repo`, it lands in that repo's primary workstream and default sprint; `--workstream` and `--sprint` target deeper.
- `agent list [--repo R]` / `agent status <id>` — read a point-in-time snapshot of the fleet or one agent.
- `agent send-message <id> "text"` — push an instruction to a running agent.
- `agent watch --all` / `agent watch --id <id>` — stream agent events as line-delimited JSON.
- `agent kill <id>` — stop a running agent.
- `mcp` — run the parent-facing MCP control server over stdio (see below).

The active repo, workstream, and sprint set the target for a bare `agent spawn`. Each
`activate` resets the more-specific selections, so the most recent activation wins;
killing an active entity clears its selection.

## Worktree isolation

A workstream is exactly one git worktree, and cc-orchestrate keeps that 1:1 invariant
no matter how the backend behaves. superset forks a worktree per workspace, so
cc-orchestrate adopts the path superset reports. herd, cmux, zellij, and tmux don't
manage worktrees, so cc-orchestrate runs `git worktree add` itself under
`~/.cc-orchestrate/worktrees/<repo>/<workstream>` and points the backend at it. The
primary workstream is special: it's the repo's own checkout, so it never gets a
`git worktree add` and its teardown never removes the directory.

On a Jujutsu repo, cc-orchestrate still creates a real git worktree, then runs
`jj git init --git-repo .` inside it to colocate an independent jj repo. That sidesteps
the cross-conflicts you hit when several `jj workspace`s share one backing repo.

## cc-notes integration

When a repo already uses [cc-notes](https://github.com/yasyf/cc-notes), cc-orchestrate
mirrors its tree into cc-notes entities: a workstream becomes a cc-notes project, a
sprint becomes a cc-notes sprint, and each spawned agent becomes a cc-notes task tagged
with the workstream's branch. The binding is gated — it fires only when the `cc-notes`
binary is on your `PATH` and the repo already has `refs/cc-notes/*` — so repos that
don't use cc-notes spawn exactly as before.

cc-notes is an optional prerequisite, installed separately:

```bash
brew tap yasyf/cc-notes https://github.com/yasyf/cc-notes
brew install cc-notes
```

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

The server exposes one tool per orchestration op, grouped by entity:

- backends: `backends_list`, `backend_select`
- config: `config_get`
- repo: `repo_create`, `repo_list`, `repo_activate`, `repo_kill`
- workstream: `workstream_create`, `workstream_list`, `workstream_activate`, `workstream_kill`
- sprint: `sprint_create`, `sprint_list`, `sprint_activate`
- agent: `agent_spawn`, `agent_list`, `agent_status`, `agent_send_message`, `agent_kill`

The MCP surface is request/response only — `agent_list` and `agent_status` return a
point-in-time snapshot. For live status, run `cc-orchestrate agent watch` under a
Monitor alongside the MCP session.

## How status and messaging work

Three flows keep the orchestrator and its agents in sync, all over the event plane:

- **Status** comes from tailing each agent's transcript under
  `~/.claude/projects/**/<session-id>.jsonl` (or `$CLAUDE_CONFIG_DIR`). The daemon
  derives state, token count, and last activity; `agent status` and `agent list` read
  the result.
- **Orchestrator to agent** rides the agent's watch Monitor. The spawn brief arms each
  child with a persistent Monitor, so an `agent send-message` reaches it as a new
  instruction.
- **Agent to orchestrator** rides the `report` MCP tool, wired into every spawned
  agent. The agent calls it to report progress, a result, or a question, which appends
  an `orchestrate.report` event to its subject's log.

## Documentation

The conventions and architecture live in [AGENTS.md](AGENTS.md) and
[STYLEGUIDE.md](STYLEGUIDE.md). For the framework underneath, read
[cc-interact](https://github.com/yasyf/cc-interact).

## License

PolyForm-Noncommercial-1.0.0. See [LICENSE](LICENSE).
