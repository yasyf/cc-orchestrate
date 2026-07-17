# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Channel delivery: spawned agents receive orchestrator messages as
  `<channel source="cc-orchestrate">` tags pushed by the plugin-loaded `cco channel`
  MCP server, with the watch Monitor as the fallback until the agent's first
  channel ack.
- Hidden `cco setup-channels` (`--check`/`--apply`/`--decline`): one-time approval
  that adds the plugin to Claude's managed channel allowlist via a macOS admin prompt.
- The Claude Code plugin manifest now ships the channel MCP server (`mcpServers` +
  `channels` entries), so children load the channel from the installed plugin.

### Changed
- Channel setup now comes from cc-interact v0.10.0: the local `channelsetup`
  package and hand-rolled `setup-channels` command are deleted in favor of
  `channelsetup.Plugin` + `cmd.SetupChannelsCmd`, and the channel instructions
  and spawn brief's receive protocol render through
  `channel.Instructions`/`channel.ReceiveProtocol`. Behavior is unchanged except
  the channel-instructions closer now carries the standard "never speaks
  unsolicited" sentence.
- `agent send-message` always appends to the event log; the reply is `{seq}` — the
  `transport` field is gone from the CLI output, the MCP tool, and the XRPC
  `cco.agent.sendMessage` result.
- Spawned children run with the full user environment: `--mcp-config` and
  `--strict-mcp-config` are dropped, and children opt into the channel via the
  `--channels` flag (the settings `channels` key does not feed the session
  channel gate).

### Removed
- The native terminal-typing send path; backend `SendText` now serves only the
  startup prober.
- The `orchestrate.inbound` transcript-audit event.

### Fixed
- A rebuilt dev binary now evicts a stale running daemon: unstamped builds report
  `9999.<binary-mtime>.0-dev`, which wins cc-interact's newest-wins eviction
  (previously `0.2.0-dev` lost to any installed release, so the old daemon kept
  serving old code until a manual `cco stop`).

## [0.3.0] - 2026-07-17

### Added
- XRPC-style HTTP API: every op is callable as `GET`/`POST /xrpc/cco.<noun>.<verb>`,
  with a self-describing catalog at `/xrpc/cco.server.describe` (JSON schemas per
  method, ready for client type generation) and typed error envelopes mapped to
  HTTP statuses. Documented in `docs/xrpc.md`.
- Fleet event stream: `GET /events?session=fleet` mirrors every lifecycle change
  as compact typed frames (`fleet.agent.spawned`, `fleet.agent.status`, …) with
  gap-free `Last-Event-ID` resume; `cco.fleet.status` returns the whole tree plus
  the resume cursor in one call.
- `--json` on every data command, printing the daemon's reply verbatim.
- New verbs: `config set` / `unset` / `list`; `repo`/`workstream`/`sprint show`;
  `sprint kill`; `agent respawn [--dead]` to revive exited agents into their old
  sessions; `agent capture` for on-demand terminal screenshots as text;
  `fleet status [--watch]` and `fleet watch`; `--status` filters on the list verbs.
- MCP tools generated from the method registry — `agent_capture`, `agent_respawn`,
  `sprint_kill`, `fleet_status`, `fleet_serialize`, `fleet_restore`, and
  `config_set` join the set, and the tool list can no longer drift from the ops.
- `projects` command group (`list`, `create`, `activate`) for managing backend
  workspaces, and `backends select` to pin the default placement backend.
- `agent` command group (`spawn`, `list`, `send-message`, `status`, `watch`,
  `kill`) for spawning and controlling child Claude Code agents, with status
  derived from each agent's transcript.
- Backend drivers for herd, superset, cmux, zellij, and tmux, resolved in that
  precedence order.
- `mcp` parent-facing control server exposing the orchestration ops as MCP tools.

### Changed
- One method registry now drives the socket ops, the HTTP routes, the MCP tool
  list, and the CLI; daemon op strings are the `cco.<noun>.<verb>` method names.
- `agent status` is now `agent show` (MCP: `agent_show`); `status` remains a CLI
  alias.
- Rewritten from Python to a single pure-Go CLI built on the
  [cc-interact](https://github.com/yasyf/cc-interact) framework. Distribution
  moves from PyPI wheels to prebuilt binaries and a Homebrew tap.
- `backends` now lists every backend in precedence order with its install status
  and the effective default.

## [0.1.0] - 2026-06-12

### Added
- Initial scaffolding.
- `cc-orchestrate backends` command reporting which backends (cmux, superset)
  are installed.

[Unreleased]: https://github.com/yasyf/cc-orchestrate/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/yasyf/cc-orchestrate/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/yasyf/cc-orchestrate/releases/tag/v0.1.0
