# cc-orchestrate Style Guide

The concrete style rules for the Go in this repository. cc-orchestrate is a
single Go CLI that consumes the [cc-interact](https://github.com/yasyf/cc-interact)
framework, so these rules mirror cc-interact's own conventions — match that module
when in doubt. Target the `go 1.26.5` toolchain (cc-interact's floor).

## Core Principles

1. **Fail fast, fail loud.** No defensive coding: no fallbacks, shims, or
   backwards-compat layers, and no guards against impossible states. No sentinel
   values, no silent defaults. If unused, delete it. Crash on the unexpected.
2. **Make invalid states unrepresentable.** Named types over bare strings, structs
   for grouped data, required fields over zero-value flags. A `BackendName` is a
   defined type, not a loose `string`.
3. **Minimal changes.** Stay within scope. Make the test pass, then stop. Improve
   only the code you touch.
4. **Match surrounding code.** Follow this guide first, then the file you're in,
   then the module. If surrounding code violates this guide, fix it.
5. **Flat over nested.** Early returns and flat control flow. Handle the error and
   return; nesting deeper than three levels is a smell.

## Package Layout

cc-orchestrate ships flat packages, no `internal/` directory — the same shape
cc-interact uses. Hide helpers with lowercase identifiers and doc comments, not the
compiler. A package is one concern: `backend` holds the `Backend` interface, the
spec/handle types, the registry, and the five drivers; domain ops, the transcript
tailer, and the cobra tree live in their own packages beside `main.go`.

`main.go` is the composition root. It builds `cmd.Deps`, wires `daemon.New` with the
consumer DDL via `daemon.Config.Migrate`, registers domain ops, and assembles the
cobra tree from cc-interact's `cmd.*` constructors plus the local domain commands —
exactly as `examples/echo/main.go` does in cc-interact. Read that file before adding a
command.

## Error Handling

Wrap errors with context and let them propagate; never swallow one.

```go
// Good — wrapped, contextual, propagated
if _, err := s.Append(ctx, ev); err != nil {
    return fmt.Errorf("append %s event: %w", ev.Type, err)
}

// Bad — context lost, error swallowed
s.Append(ctx, ev)
```

Use `%w` so callers can `errors.Is`/`errors.As` the cause. Keep the error-handling
block minimal: only the operation that can fail belongs inside. No catch-all that
hides the failure. Read required configuration so a missing key fails at startup, and
return a typed result instead of a sentinel value. Reserve `panic` for programmer
error the API forbids (e.g. a duplicate `Server.Register` op), never for runtime
failure — a backend that isn't installed returns an error, it does not panic.

## Context

Pass `context.Context` as the first argument through every command, IPC call, daemon
op, and `os/exec` invocation. Cancellation is how a killed `agent watch` frees its
parked goroutine and how a slow backend command gets torn down. The `Backend`
interface threads `ctx` through `EnsureReady`, `CreateProject`, `Spawn`, and the rest
for exactly this reason.

## Cobra Commands

Each command is a constructor returning `*cobra.Command`, taking the dependencies it
needs (`cmd.Deps` for substrate commands, a backend registry for domain commands).
Bind flags with `c.Flags().StringVar(&v, …)`; put the logic in `RunE` and return the
error rather than printing and exiting inside the handler. Set `SilenceUsage` and
`SilenceErrors` on the root and print the error once in `main`. Reuse cc-interact's
`cmd.DaemonCmd`/`WatchCmd`/`StatusCmd`/`StopCmd`/`SessionRecordCmd`/`GuardEditCmd`/
`ChannelCmd`/`ChannelAckCmd` for the substrate; do not re-implement them.

```go
func spawnCmd(d cmd.Deps, reg *backend.Registry) *cobra.Command {
    var project, prompt string
    c := &cobra.Command{
        Use:   "spawn",
        Short: "Spawn an agent into a project",
        Args:  cobra.NoArgs,
        RunE: func(c *cobra.Command, _ []string) error {
            if err := d.EnsureCurrent(c.Context()); err != nil {
                return err
            }
            return runSpawn(c.Context(), reg, project, prompt)
        },
    }
    c.Flags().StringVar(&project, "project", "", "project id or name")
    c.Flags().StringVar(&prompt, "prompt", "", "the agent's task")
    return c
}
```

## Backend Drivers (`os/exec`)

A backend is *placement + spawn* only; everything interactive rides cc-interact's
event plane. Drivers shell out via `os/exec`, parsing `--json` output where the CLI
offers it and falling back to documented text formats where it doesn't. Rules:

- `Available()` is `exec.LookPath` on the backend's binary (note `herd` invokes
  `herdr`). It returns a bool, never an error.
- Build argv as an explicit `[]string`; never interpolate user input into a shell
  string. The one exception is superset, which needs `bash -lc "<one quoted line>"`
  because its terminal spawns a fresh login shell — quote that line deterministically
  and use the absolute `claude` path, since the login shell is fish.
- Run commands with the call's `context.Context` (`exec.CommandContext`) so a
  cancelled op kills the child process.
- Wrap a failed command with its captured stderr: `fmt.Errorf("herdr workspace
  create: %w: %s", err, stderr)`.
- The child references the orchestrator binary by its absolute path from
  `os.Executable()` — the child's `PATH`/env differs (especially under superset), so a
  bare `cc-orchestrate` won't resolve.

## SQLite & State

State lives under `paths.Paths{App: ".cc-orchestrate"}` → `~/.cc-orchestrate/`. The
consumer's tables (`repos`, `workstreams`, `sprints`, `agents`, `config`) are created through
`daemon.Config.Migrate` and queried through `HandlerCtx.DB`; cc-interact owns the
`subjects` and `events` tables, which you touch only via `subject.Resolver` and the
`Append` chokepoint. cc-interact has no migration framework beyond `Config.Migrate`,
so keep all DDL idempotent (`CREATE TABLE IF NOT EXISTS`). Each record gets exactly
one write codepath — two call sites writing the same row diverge. The single-backend
selection persists in the `config` table, not a TOML file.

## Functions & Code Organization

Keep functions small and single-purpose; a long handler is a sign it should delegate
to named helpers. Options and flags ride a struct (`SpawnSpec`, `ProjectSpec`) rather
than a long positional argument list — accept what the caller naturally holds.

Order each file: imports, constants, type definitions, then functions and methods.
Constants sit immediately after imports. Group related constants in a single `const`
block, as `examples/echo/main.go` does for its op names and event types. Use Go's
exported/unexported capitalization to control visibility, not naming prefixes.

## Comments & Doc Comments

Code documents itself through names, types, and organization. No comments except
TODOs, non-obvious workarounds, or disabled code. Document the exported API only,
with a leading doc comment in the `// Identifier …` form; a comment that restates the
signature is clutter to delete. A non-obvious invariant earns a comment — e.g. why
the transcript tailer globs by the unique session id rather than reversing the lossy
slug.

## Testing

Tests live beside the code as `*_test.go`; run them with `go test -race ./...` from
the repo root. Use table-driven tests with `t.Run` subtests, each case named and
carrying its own expected values; a test that can't fail uncovers nothing.

Mock the boundaries the code talks to — the network, the clock, and the backend CLIs
(stub `os/exec` so a driver test asserts the argv it builds without needing the real
binary installed). A database is **not** a mock boundary: store and tailer tests run
against a real ephemeral on-disk SQLite (a `t.TempDir()` path through `store.Open`),
never a mock or in-memory fake. The full spawn → status → message → kill loop gets a
real-`tmux` integration test.

```go
func TestPrecedence(t *testing.T) {
    for _, tc := range []struct {
        name      string
        available []BackendName
        want      BackendName
    }{
        {"all present picks herd", allBackends, "herd"},
        {"herd absent falls to superset", allBackends[1:], "superset"},
    } {
        t.Run(tc.name, func(t *testing.T) {
            if got := resolve(tc.available); got != tc.want {
                t.Fatalf("resolve(%v) = %q, want %q", tc.available, got, tc.want)
            }
        })
    }
}
```
