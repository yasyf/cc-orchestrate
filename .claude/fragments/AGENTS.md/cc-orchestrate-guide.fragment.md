# cc-orchestrate Development Guide

Orchestrate fleets of Claude Code agents across pluggable backends. A single Go CLI
(module `github.com/yasyf/cc-orchestrate`, `go 1.26.5`) built as a consumer of the
[cc-interact](https://github.com/yasyf/cc-interact) framework: cc-interact brings the
lazy daemon, append-only SQLite event log, HTTP/SSE plane, stdio MCP channel, and
hook handlers; cc-orchestrate adds the repo → workstream → sprint → agent domain
tree (a workstream is one git worktree, the unit of isolation) and backend drivers on top.
Distributed as a static binary via GoReleaser and a Homebrew tap
(`brew install --cask yasyf/tap/cc-orchestrate`), not PyPI.

## Repository Structure

```
cc-orchestrate/
├── main.go           # package main — thin entrypoint: builds orchestrate.Root() and executes it
├── orchestrate/      # composition root: cmd.Deps, daemon.New + domain ops, transcript tailer, cobra tree
├── backend/          # Backend interface, specs/handles, registry + precedence, the 5 drivers
├── ptyhost/          # pty-host + terminal-grid capture: cgo libghostty-vt grid, pure-Go x/vt fallback
├── worktree/         # git/jj worktree helper: Add/Remove/UsesJJ/InitJJ/CurrentBranch
├── ccnotes/          # optional, gated cc-notes binding (workstream→project, sprint→sprint, agent→task)
├── plugins/          # the cc-orchestrate Claude Code plugin (published via .claude-plugin/marketplace.json)
├── scripts/          # build-libghostty.sh — native libghostty-vt build for the cgo path
├── docs/             # brand assets + the GitHub Pages landing site (docs/site)
├── .github/          # Actions: CI (build, integration, cgo, lint, vuln) and the GoReleaser release
├── AGENTS.md         # This file — shared conventions
└── README.md         # Project overview
```

cc-orchestrate follows cc-interact's flat-package convention — no `internal/`
directory; hide helpers with lowercase identifiers and doc comments, not the
compiler. cc-interact's `examples/echo/main.go` is the canonical consumer template:
it wires `daemon.New`, registers domain ops, and assembles the cobra tree from the
`cmd.*` constructors — read it before adding a command. Five backends ship in a fixed
precedence order: herd, superset, cmux, zellij, then tmux. `Available()` filters the
list to what's installed, and `backends select` overrides the pick.
