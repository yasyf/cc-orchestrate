// spawn-smoke — end-to-end spawn smoke against the tmux backend. One agent drives
// the whole live loop: build the binary, select tmux, create a temp repo, create a
// workstream (asserting its worktree dir), spawn an agent, assert status populates
// (the prober drives the first-run "trust this folder?" dialog so the transcript
// appears) + send-message delivers + kill cleans up, then tears everything down.
// Returns the observed agent status.
//
// Run with: /spawn-smoke

export const meta = {
  name: 'spawn-smoke',
  description: 'End-to-end spawn smoke on the tmux backend: build, repo + workstream create, spawn, assert status + send-message + kill, then clean up',
  phases: [{ title: 'Smoke', detail: 'live build → repo → workstream → spawn → status → send-message → kill → teardown' }],
}

const REPO = '/Users/yasyf/Code/cc-orchestrate'

// The observed end state of the smoke run. `status` mirrors `agent status`
// (state/status/activity/tokens); the booleans gate each leg of the loop.
const SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['pass', 'built', 'tmuxAvailable', 'workstreamCreated', 'spawned', 'statusPopulated', 'promptDriven', 'stuck', 'messageDelivered', 'killedClean', 'status', 'notes'],
  properties: {
    pass: { type: 'boolean', description: 'true only when every asserted leg succeeded' },
    built: { type: 'boolean' },
    tmuxAvailable: { type: 'boolean' },
    workstreamCreated: { type: 'boolean', description: 'workstream create succeeded and its printed worktree dir exists on disk' },
    spawned: { type: 'boolean', description: 'spawn returned an agent id + tmux terminal' },
    statusPopulated: { type: 'boolean', description: 'agent status reached a real transcript-derived state (working/idle/awaiting-input) within the timeout — NOT unknown/blocked/stuck' },
    promptDriven: { type: 'boolean', description: 'a poll observed state=blocked or activity containing "prompt:" that later resolved — i.e. the prober drove the trust dialog' },
    stuck: { type: 'boolean', description: 'a poll observed state=stuck — the prober could not drive the screen (a failure)' },
    messageDelivered: { type: 'boolean', description: 'send-message exited 0 and printed `sent to <id> (seq N)` — an event-plane enqueue check, not proof the child acted on it (that is the manual channel E2E)' },
    killedClean: { type: 'boolean', description: 'after kill, the tmux window/pane is closed and the agent shows STATUS=exited in `agent list` (an intentional append-only tombstone), not active' },
    status: { type: 'string', description: 'the verbatim `agent status <id>` output observed (state/status/activity/tokens/updated)' },
    notes: { type: 'string', description: 'anything skipped or unverifiable, e.g. claude not installed so no transcript' },
  },
}

const PROMPT = `You run a live end-to-end spawn smoke test of cc-orchestrate (repo at ${REPO}) against the tmux backend. Use Bash for every step and OBSERVE real output — never assume. The CLI is run as the freshly built binary, not uvx.

0. PRECONDITION: run \`command -v tmux\`. If tmux is absent, set tmuxAvailable=false, pass=false, explain in notes, and stop — this smoke requires tmux.

1. BUILD: \`cd ${REPO} && CGO_ENABLED=0 go build -o /tmp/cc-orchestrate-smoke .\` (pure-Go; the prober uses tmux's native capture so no libghostty/cgo toolchain is needed for this smoke). Set built from the exit code. Use BIN=/tmp/cc-orchestrate-smoke for everything below.

1b. FRESH DAEMON VIA EVICTION: dev builds embed an mtime-derived version (\`9999.<mtime>.0-dev\`), so the first real \`$BIN\` command evicts a stale running daemon on its own (newest-wins). The daemon is lazy — from a clean state \`$BIN status\` reports "not running" without starting one, so do the verification AFTER step 2 has run: \`$BIN status\` must then report a daemon whose version has the \`9999.<mtime>.0-dev\` shape, proving the just-built binary's daemon is serving. A daemon reporting an older version at that point is a FAILURE: set pass=false and explain in notes.

2. SELECT tmux: \`$BIN backends select tmux\`. Then \`$BIN backends list\` and confirm tmux shows available + selected.

3. TEMP REPO: make an isolated workdir \`PRJ=$(mktemp -d)\` and \`git init\` in it (backends may assume a repo). Create the repo:
   \`$BIN repo create smoke-$$ --backend tmux --cwd "$PRJ"\`. Capture the printed repo id. Confirm a detached tmux session now exists (\`tmux list-sessions\`).

4. WORKSTREAM: create a workstream in the repo: \`$BIN workstream create feat-smoke --repo <repo-id>\`. Capture the printed workstream id and \`worktree:\` path. Assert the worktree dir exists with \`test -d "<worktree>"\`. Set workstreamCreated=true only when the command exits 0 AND that dir exists.

5. SPAWN: \`$BIN agent spawn --workstream <workstream-id> --name smoke --prompt "say hello then wait"\`. Capture the agent id, backend, and terminal (a tmux pane id). Set spawned from whether all three are present. (cwd defaults to the workstream worktree.)

6. STATUS: poll \`$BIN agent status <agent-id>\` every ~2s for up to ~45s. Record the LAST output verbatim into "status". A first run of \`claude\` in a freshly-created dir hits the "Is this a project you created or one you trust?" dialog (options "1. Yes, I trust this folder" / "2. No, exit"); the prober now drives it (Enter confirms the default Yes), so the state should transition unknown → blocked → working/idle on its own. Interpret the polled \`state\`:
   - \`blocked\` is TRANSIENT (the prober is answering the dialog) — keep polling. Set promptDriven=true the first time you see \`state: blocked\` OR an \`activity\` containing "prompt:".
   - \`stuck\` is a FAILURE (the prober could not drive the screen) — set stuck=true, record the status, stop polling early.
   - \`working\`, \`idle\`, or \`awaiting-input\` is a real transcript-derived state — set statusPopulated=true.
   - \`unknown\` is not yet populated — keep polling.
   With \`claude\` installed, a fresh-dir spawn should reach statusPopulated=true and typically promptDriven=true (the trust dialog fires on first run). The derived status is tailed from ~/.claude (or \$CLAUDE_CONFIG_DIR)/projects/**/<session>.jsonl. If \`claude\` is NOT installed (\`command -v claude\` fails), no transcript ever appears and no pane prompt is detected: set statusPopulated=false, promptDriven=false, explain in notes, and do NOT hang past the timeout.

7. SEND-MESSAGE: \`$BIN agent send-message <agent-id> "ping"\`. Set messageDelivered=true if it exits 0 and prints \`sent to <agent-id> (seq N)\` — messages ride the event plane; there is no native-send variant.

8. KILL + CLEANUP: \`$BIN agent kill <agent-id>\`, then \`$BIN agent list --repo <repo-id>\` and confirm the agent now shows STATUS=exited (the event log keeps killed agents as tombstones — exited, not active, is correct; do NOT expect it to vanish), and \`tmux list-panes -s -t <session>\` (or list-windows) confirms the agent's window/pane is closed. Set killedClean=true when the pane is closed AND the agent is exited. Finally tear down: \`$BIN repo kill <repo-id>\` (cascades the workstream's worktree + backend workspace and any remaining agents), then \`tmux kill-session -t <session>\`, then \`$BIN stop\` — the smoke daemon's mtime-derived version outranks every release, so leaving it running would block release binaries from ever evicting it — and only then \`rm -rf "$PRJ" /tmp/cc-orchestrate-smoke\`. Confirm the worktree dir from step 4 is gone. Leave no smoke-* tmux sessions, temp dirs, or smoke daemon behind.

Set pass=true only if built, workstreamCreated, spawned, messageDelivered, and killedClean are all true AND stuck is false. Additionally, when \`claude\` IS installed (\`command -v claude\` succeeds), pass also requires statusPopulated=true — that is the regression check that the prober drove the first-run trust dialog so the transcript appeared. When \`claude\` is absent, statusPopulated/promptDriven may be false; record that in notes. Return the schema.`

phase('Smoke')
const result = await agent(PROMPT, { label: 'spawn-smoke', phase: 'Smoke', schema: SCHEMA, effort: 'high' })

log(result ? `smoke pass=${result.pass} (built=${result.built} spawned=${result.spawned} status=${result.statusPopulated} promptDriven=${result.promptDriven} stuck=${result.stuck} msg=${result.messageDelivered} kill=${result.killedClean})` : 'smoke agent crashed')

return result
