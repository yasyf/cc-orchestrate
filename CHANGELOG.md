# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `projects` command group (`list`, `create`, `activate`) for managing backend
  workspaces, and `backends select` to pin the default placement backend.
- `agent` command group (`spawn`, `list`, `send-message`, `status`, `watch`,
  `kill`) for spawning and controlling child Claude Code agents, with status
  derived from each agent's transcript.
- Backend drivers for herd, superset, cmux, zellij, and tmux, resolved in that
  precedence order.
- `mcp` parent-facing control server exposing the orchestration ops as MCP tools.

### Changed
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
