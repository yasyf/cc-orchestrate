// verify-backend-drivers — re-verify each of the 5 backend drivers' command map
// against the real underlying CLI. One agent per backend, run in parallel; each
// returns a per-driver pass/fail. Mirrors Phase 2's backend-layer verification.
//
// Run with: /verify-backend-drivers

export const meta = {
  name: 'verify-backend-drivers',
  description: "Re-verify each backend driver's emitted command map against the real CLI (one agent per backend, parallel)",
  phases: [{ title: 'Verify', detail: 'one agent per driver checks every emitted argv against the live CLI' }],
}

const REPO = '/Users/yasyf/Code/cc-orchestrate'

// One entry per backend. `bin` is the CLI the driver shells out to (note herd's
// binary is `herdr`, not `herd`); `file` is the driver source under backend/.
const DRIVERS = [
  { name: 'herd', bin: 'herdr', file: 'backend/herd.go' },
  { name: 'superset', bin: 'superset', file: 'backend/superset.go' },
  { name: 'cmux', bin: 'cmux', file: 'backend/cmux.go' },
  { name: 'zellij', bin: 'zellij', file: 'backend/zellij.go' },
  { name: 'tmux', bin: 'tmux', file: 'backend/tmux.go' },
]

// Each command the driver emits is judged ok / mismatch / unverifiable; the
// driver passes only when nothing is a mismatch.
const SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['backend', 'bin', 'available', 'pass', 'commands', 'summary'],
  properties: {
    backend: { type: 'string' },
    bin: { type: 'string' },
    available: { type: 'boolean', description: 'whether the CLI is installed on this host' },
    pass: { type: 'boolean', description: 'true when no command is a mismatch' },
    summary: { type: 'string' },
    commands: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['method', 'argv', 'verdict', 'note'],
        properties: {
          method: { type: 'string', description: 'Backend interface method, e.g. CreateProject' },
          argv: { type: 'string', description: 'the emitted command line, e.g. "tmux new-session -d -s ..."' },
          verdict: { enum: ['ok', 'mismatch', 'unverifiable'] },
          note: { type: 'string', description: 'evidence: which help/usage line confirms or refutes the flags' },
        },
      },
    },
  },
}

function driverPrompt(d) {
  return `You verify the "${d.name}" backend driver of cc-orchestrate (repo at ${REPO}).

This driver shells out to the "${d.bin}" CLI. Steps:

1. Read ${REPO}/${d.file} IN FULL, plus ${REPO}/backend/backend.go for the Backend
   interface. Enumerate every distinct command the driver emits via b.run(...) —
   one per interface method (CreateProject, ListProjects, Spawn, ListAgents, Kill,
   KillProject, and any readiness/auth probe). Record the exact argv, including
   subcommands and flags.

2. Check whether the CLI is installed: run \`command -v ${d.bin}\` via Bash. Set
   "available" accordingly.

3. For each emitted command, verify its subcommand + flags exist in the REAL CLI:
   - If installed, run \`${d.bin} --help\` and the relevant \`${d.bin} <subcommand> --help\`
     (or \`man ${d.bin}\` for tmux) and confirm every flag the driver passes is a
     real flag with the meaning the driver assumes (e.g. -F format strings, --json
     output, --local scoping). Quote the usage line as evidence in "note".
   - If NOT installed, mark every command "unverifiable" and say so in the note —
     do not guess. A driver with no installed CLI still returns pass=true only if
     no command is a definite mismatch from the source alone (e.g. an obviously
     wrong subcommand name you can refute from docs you fetched).

4. Flag a "mismatch" for any flag/subcommand the current CLI no longer accepts,
   renamed flags, or output-format assumptions (--json / -F) the CLI no longer
   honors. These are the regressions this workflow exists to catch.

Return the schema. Set backend="${d.name}", bin="${d.bin}". pass=false if ANY
command is a mismatch.`
}

phase('Verify')
const results = await parallel(
  DRIVERS.map((d) => () => agent(driverPrompt(d), { label: `verify:${d.name}`, phase: 'Verify', schema: SCHEMA, effort: 'high' }))
)

const clean = results.filter(Boolean)
const failed = clean.filter((r) => !r.pass).map((r) => r.backend)
log(`Drivers verified: ${clean.length}/${DRIVERS.length}; failing: ${failed.length ? failed.join(', ') : 'none'}`)

return {
  pass: failed.length === 0,
  failing: failed,
  perDriver: clean,
  agentsCrashed: results.map((r, i) => (r ? null : DRIVERS[i].name)).filter(Boolean),
}
