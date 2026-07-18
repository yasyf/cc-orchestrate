# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Immediate child-exit liveness: a pty-hosted child's exit is now reported to the
  daemon by the wrapper as its last act, after its own socket teardown (the
  socket-only `cco.agent.childExited` op, off the parent XRPC/MCP surfaces), so
  the supervisor resumes the agent's session at once instead of waiting out the
  ~2-minute transcript-staleness (or membership) latency. The report carries the
  session id plus a per-(re)spawn nonce identifying the reporting incarnation,
  resumes the SAME session, and tears the surviving wrapper terminal down first;
  it is a no-op for an already-terminal agent, for a stale report whose nonce
  no longer matches the row (a delayed duplicate arriving after a concurrent
  kill+respawn), and for any report carrying an empty nonce (a migrated pre-nonce
  row reads an empty nonce until its next respawn, so the upgrade window is
  explicitly a no-op rather than a match), so a healthy fresh incarnation is never
  killed by its predecessor's report. A respawn persists its new terminal handle
  and nonce in one atomic statement, so no torn write can pair a fresh terminal
  with a stale nonce. The pty-host control socket is now derived per incarnation
  from the session id plus 64 bits of the spawn nonce's SHA-256 — a nonce-less
  legacy wrapper keeps the old session-derived path — so a kill-driven respawn's
  replacement binds its own socket, out of reach of the signaled old wrapper's
  deferred cleanup. A spawn's post-insert steps (hierarchy re-check, spawned
  event, tailer start) run under the agent's lock, so a fast child exit whose
  report lands right after the insert serializes behind the spawn instead of
  respawning first and having its replacement's tailer cancelled by the spawn's
  stale continuation; screen capture re-reads the agent row under the same lock,
  so a capture racing a respawn dials the current incarnation's socket, never the
  old one's. The report only fires on a natural child exit — a signal-driven
  teardown skips it — and is best-effort: a daemon that is down at exit time is
  tolerated, with the existing membership/prober/staleness fallbacks covering that
  window unchanged.
- `child.launcher` config key: a JSON string-array argv prefix (e.g.
  `cco config set child.launcher '["my-launcher","wrap","--"]'`) that wraps every
  child agent's launch — spawn and resume alike — in front of the whole claude
  invocation, nested inside the pty-host and under the env scrub. Under the
  pty-host the launcher head and the claude token both resolve at spawn time, so
  a bare `claude` still skips the superset wrapper shim behind a prefix. Unset or
  empty runs children bare, byte-for-byte as before; a malformed value (anything
  but a JSON array of non-empty strings) fails the spawn loudly. The launcher
  MUST exec into the claude invocation it is handed (replace itself, the way
  `cc-runtime wrap` does), never fork claude and exit: under a pty-host the
  child-exit liveness report fires when the host's direct child — the launcher —
  exits, so a fork-and-exit launcher makes a live session report as dead and get
  killed and respawned mid-flight. The violation is not detectable at runtime, so
  the exec contract is a hard requirement on any custom launcher.
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
- One agent row per session id, enforced structurally: a partial unique index on
  `agents.session_id` (session-less rows exempt) plus an up-front duplicate-session
  check on restore bundles. Previously a hand-edited or corrupt bundle could
  restore two rows sharing one session id, making the child-exit report's
  session-id lookup nondeterministic — a valid report could resolve the wrong row,
  fail its nonce check, and silently degrade immediate liveness to the
  membership/staleness fallbacks. A duplicate-session bundle is now rejected
  whole, before anything is restored.
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
