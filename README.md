# cc-orchestrate

![cc-orchestrate banner](https://github.com/yasyf/cc-orchestrate/raw/main/docs/assets/readme-banner.webp)

[![PyPI](https://img.shields.io/pypi/v/cc-orchestrate.svg)](https://pypi.org/project/cc-orchestrate/)
[![Python](https://img.shields.io/pypi/pyversions/cc-orchestrate.svg)](https://pypi.org/project/cc-orchestrate/)
[![Docs](https://img.shields.io/github/actions/workflow/status/yasyf/cc-orchestrate/docs.yml?branch=main&label=docs)](https://yasyf.github.io/cc-orchestrate/)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm-Noncommercial-1.0.0-blue.svg)](https://github.com/yasyf/cc-orchestrate/blob/main/LICENSE)

Orchestrate fleets of Claude Code agents across pluggable backends like cmux and superset.

cc-orchestrate is a CLI for running fleets of Claude Code agents from one seat
instead of a sprawl of terminal tabs. Backends are pluggable: the same
orchestration drives agents living in cmux sessions, superset worktrees, or
whatever runner you wire in next, so fleet logic never cares where an agent
actually executes.

## Install

No install needed — run everything through [uvx](https://docs.astral.sh/uv/):

```bash
uvx cc-orchestrate --help
```

`uvx` fetches cc-orchestrate into a throwaway environment and runs it. To add it
to a project instead:

```bash
uv add cc-orchestrate
```

## Quickstart

Check which backends are installed on your machine:

```bash
uvx cc-orchestrate backends
```

```
cmux	available
superset	available
```

A backend reports `not found` when its CLI is missing from your `PATH`.
Install the runner and re-run the check.

## What problems does this solve?

- Running more than a couple of Claude Code agents means babysitting a sprawl
  of terminal tabs and tmux panes by hand. cc-orchestrate gives the whole
  fleet one front door.
- Every runner has its own invocation quirks — cmux sessions, superset
  worktrees, plain terminals. Backends absorb those differences, so what you
  dispatch stays portable across them.
- Knowing what a machine can even run is guesswork. `cc-orchestrate backends`
  reports which runners are installed before you dispatch anything.

## Docs

[Read the docs](https://yasyf.github.io/cc-orchestrate/) for the full guide and API reference.
