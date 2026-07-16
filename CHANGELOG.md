# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/yasyf/cc-orchestrate/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yasyf/cc-orchestrate/releases/tag/v0.1.0
