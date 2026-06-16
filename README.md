# cc-orchestrate

![cc-orchestrate banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/ci.yml?branch=main&label=CI)](https://github.com/yasyf/cc-orchestrate/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm-Noncommercial-1.0.0-blue.svg)](LICENSE)

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

## Quickstart

See which backends are installed on your machine:

```bash
cc-orchestrate backends list
```

```
herd      available
superset  available
cmux      not found
zellij    available
tmux      available
```

A backend reports `not found` when its CLI isn't on your `PATH`. cc-orchestrate uses
the first installed backend in a fixed order: herd, superset, cmux, zellij, then
tmux. To pin one instead:

```bash
cc-orchestrate backends select tmux
```

## Commands

cc-orchestrate groups its surface by what you're orchestrating:

- `backends {list,select}` — show installed runners and pin the default.
- `projects {list,create,activate}` — a project is the backend workspace a fleet shares.
- `agent {spawn,list,send-message,status,watch,kill}` — spawn a Claude agent into a project, message it, read live status derived from its transcript, stream updates, or kill it.
- `mcp` — a stdio MCP server that a parent Claude session registers to drive the whole fleet through tools.

Spawn an agent into a project and stream what it does:

```bash
cc-orchestrate projects create demo --cwd /tmp/demo
cc-orchestrate agent spawn --project demo --prompt "summarize the repo and wait"
cc-orchestrate agent watch --all
```

## Documentation

The conventions and architecture live in [AGENTS.md](AGENTS.md) and
[STYLEGUIDE.md](STYLEGUIDE.md). For the framework underneath, read
[cc-interact](https://github.com/yasyf/cc-interact).

## License

PolyForm-Noncommercial-1.0.0. See [LICENSE](LICENSE).
