# cc-orchestrate

![cc-orchestrate banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-orchestrate/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](LICENSE)

Orchestrate fleets of Claude Code agents over pluggable backends, from one seat.

cc-orchestrate drives a fleet of Claude Code agents through one CLI instead of a
sprawl of terminal tabs. Backends are pluggable: the same commands spawn, message,
and watch agents whether they live in herd workspaces, superset worktrees, cmux
sessions, or plain zellij and tmux panes. Every interaction rides an event plane,
not the terminal, so the orchestration never cares where an agent actually runs.

It builds on [cc-interact](https://github.com/yasyf/cc-interact), which supplies the
lazy daemon, the append-only SQLite event log, the HTTP/SSE plane, and the MCP
channel. cc-orchestrate adds projects, agents, and the five backend drivers on top.

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
- **Project** — a backend workspace that a fleet shares. You create a project on a
  backend, then spawn agents into it.
- **Agent** — one spawned Claude Code session, which is a cc-interact subject keyed by
  its `--session-id` and working directory. Status, messages, and reports all flow
  through that subject's event log.
- **Daemon** — a lazy, auto-started process that owns all state under
  `~/.cc-orchestrate` (projects, agents, and config in SQLite) and tails each agent's
  transcript. The CLI starts it on first use; you never launch it by hand.

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

Create a project in the current directory, then spawn an agent into it with a prompt:

```bash
cc-orchestrate projects create demo --cwd .
cc-orchestrate agent spawn --project demo --name a1 --prompt "summarize the repo and wait"
```

`agent spawn` prints the new agent's id, backend, and terminal:

```
agent:    a1f3c2
backend:  tmux
terminal: demo:0.0
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

Stream live status for every agent. Run `agent watch` under a Claude Code Monitor so
events push as they arrive — on its own it tails until you interrupt it:

```bash
cc-orchestrate agent watch --all
```

When an agent is done, kill it:

```bash
cc-orchestrate agent kill a1f3c2
```

## Commands

cc-orchestrate groups its surface by what you're orchestrating:

- `backends list` / `backends select <backend>` — show installed runners and pin the default.
- `projects list` / `projects create <name> [--backend B] [--cwd DIR]` / `projects activate <id>` — manage backend workspaces.
- `agent spawn --project P [--name N] [--backend B] [--cwd DIR] --prompt "..."` — spawn a Claude agent into a project.
- `agent list [--project P]` / `agent status <id>` — read a point-in-time snapshot of the fleet or one agent.
- `agent send-message <id> "text"` — push an instruction to a running agent.
- `agent watch --all` / `agent watch --id <id>` — stream agent events as line-delimited JSON.
- `agent kill <id>` — stop a running agent.
- `mcp` — run the parent-facing MCP control server over stdio (see below).

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

The server exposes ten tools, one per orchestration op: `backends_list`,
`backend_select`, `project_create`, `project_list`, `project_activate`,
`agent_spawn`, `agent_list`, `agent_send_message`, `agent_status`, and `agent_kill`.

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

One current limitation: `agent watch` streams status and orchestrator messages, but
agent reports are recorded in the event log and not yet surfaced in the watch stream.
That gap is [tracked](https://github.com/yasyf/cc-orchestrate/issues).

## Documentation

The conventions and architecture live in [AGENTS.md](AGENTS.md) and
[STYLEGUIDE.md](STYLEGUIDE.md). For the framework underneath, read
[cc-interact](https://github.com/yasyf/cc-interact).

## License

PolyForm-Noncommercial-1.0.0. See [LICENSE](LICENSE).
