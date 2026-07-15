## Go Style

Target the `go 1.26.5` toolchain (cc-interact's floor). Quick local check:
`CGO_ENABLED=0 go build ./...`, `CGO_ENABLED=0 go vet ./...`, and
`CGO_ENABLED=0 go test ./...` — the pure-Go x/vt grid path. `-race` needs cgo,
and a cgo build links libghostty-vt: run `scripts/build-libghostty.sh` once,
then `PKG_CONFIG_PATH=$PWD/.libghostty/ghostty-native/share/pkgconfig
go test -race ./...`. CI's `cgo` job covers that path on every push.

**Flat packages, no `internal/`.** Mirror cc-interact: export what consumers need,
hide helpers with lowercase identifiers and doc comments, not the compiler.
cc-interact's `examples/echo/main.go` is the consumer template — read it before adding
a command.

**Comments are terse and used sparingly — the code documents itself** through names,
types, and organization. The one exception is documentation-generation comments:
godoc on exported types, funcs, and the package, each starting with the identifier's
name (`// NewRootCmd builds …`); unexported helpers get none. Beyond godoc, comment
only for TODOs, non-obvious workarounds, or disabled code — never to restate the
signature.

**Wrap errors, never swallow.** `fmt.Errorf("doing X: %w", err)` and let it propagate;
reserve `panic` for programmer error the API forbids. Thread `context.Context` first
through every command, op, and `os/exec` call so cancellation tears down the child.
See STYLEGUIDE.md § Error Handling.

@STYLEGUIDE.md

## General Rules

**Minimal changes.** Stay within scope; fix the issue, then stop.

**Match surrounding code.** Follow the conventions of the file you're in, then the module.

**No defensive coding.** No fallbacks, shims, or backwards-compat layers; no guards against impossible states. If unused, delete it. Crash on the unexpected.

**Search before writing.** Before creating a helper, query the codebase via `ccx code search` (intent) or `ccx code symbol` (a named symbol). Sibling modules and the cc-interact surface win over re-implementation.

**Code stewardship.** When you touch a file, fix nearby bugs, style violations, and broken tests; don't wave them off as pre-existing or out of scope.

**Observe, don't infer.** Inspect actual data — read fixtures, dump objects, run the code — before reasoning from assumption.

**Don't use external failures as an excuse to stop.** API quota, rate-limit, and outage errors rarely block the whole task; trace the catch sites and confirm a failure actually stops you before claiming it does.

**Verify before asserting.** Don't report something as working, fixed, blocked, or impossible until you've checked — run it, read the output, reproduce the failure. "It should work" is not "it works."

**Reproduce before fixing.** When something breaks, isolate the smallest failing case before editing or re-running. Re-running the whole command while changing code between runs hides the root cause; narrow to the one failing call, payload, or test first.

**Research after repeated failure.** After ~2 failed approaches, stop guessing and gather evidence — search the web, read the docs and source — before a third attempt.

**Get a second opinion on a plateau.** On a debugging plateau (2 failed attempts before a 3rd), a non-trivial architectural decision, or algorithmic/security-sensitive code, get an outside check (e.g. `/codex`) before committing to the approach.

**Don't contort code to satisfy a checker.** `go vet` and the linter serve the code, not the other way around. Don't reshape a data model, widen a type to `any`, or bolt on a blanket `//nolint` just to silence a diagnostic. If a clean fix isn't obvious, leave the diagnostic — a visible diagnostic is preferable to scar tissue. (Most checker noise isn't worth acting on at all; act only when it flags a real bug.)

**Mechanical linting.** CI and hooks run `gofmt`/`goimports` and `go vet`; fix only what needs human judgment. When reviewing code, don't flag mechanical lint violations (formatting, import order, line length, trailing whitespace).

**Testing.** Go tests live next to the code as `*_test.go`; run `go test -race ./...` from the repo root. Use table-driven tests with `t.Run` subtests and strict assertions. Stub `os/exec` for backend-driver tests so a driver asserts the argv it builds without the real binary; store and tailer tests run against a real ephemeral on-disk SQLite, not a mock. The full spawn → status → message → kill loop gets a real-`tmux` integration test.

**Writing docs.** When writing or revising docs, a README, a tutorial, a how-to, or reference, use the `writing-docs` skill (Diataxis modes, voice rules, and runnable code-sample rules) and run `slop-cop check <file> --lang=markdown` before you finish.

**Git.** Commits should be atomic and scoped. One logical change per commit.

**Releases.** Tagging `v*` triggers the GoReleaser release workflow in `.github/`, which builds the cross-platform binaries (darwin/linux × arm64/amd64), publishes a GitHub release, and updates the Homebrew tap. The version comes from the tag, and the release runs only against a merged commit on `main` — tag `origin/main`, not a feature branch.
