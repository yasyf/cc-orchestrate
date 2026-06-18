// spawn-smoke — end-to-end spawn smoke against the tmux backend. One agent drives
// the whole live loop: build the binary, select tmux, create a temp repo, create a
// workstream (asserting its worktree dir), spawn an agent, assert status populates +
// send-message delivers + kill cleans up, then tears everything down. Returns the
// observed agent status.
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
  required: ['pass', 'built', 'tmuxAvailable', 'workstreamCreated', 'spawned', 'statusPopulated', 'messageDelivered', 'killedClean', 'status', 'notes'],
  properties: {
    pass: { type: 'boolean', description: 'true only when every asserted leg succeeded' },
    built: { type: 'boolean' },
    tmuxAvailable: { type: 'boolean' },
    workstreamCreated: { type: 'boolean', description: 'workstream create succeeded and its printed worktree dir exists on disk' },
    spawned: { type: 'boolean', description: 'spawn returned an agent id + tmux terminal' },
    statusPopulated: { type: 'boolean', description: 'agent status returned a non-empty state within the timeout' },
    messageDelivered: { type: 'boolean', description: 'send-message returned a seq with no error' },
    killedClean: { type: 'boolean', description: 'after kill, the agent is gone from `agent list` and the tmux window is closed' },
    status: { type: 'string', description: 'the verbatim `agent status <id>` output observed (state/status/activity/tokens/updated)' },
    notes: { type: 'string', description: 'anything skipped or unverifiable, e.g. claude not installed so no transcript' },
  },
}

const PROMPT = `You run a live end-to-end spawn smoke test of cc-orchestrate (repo at ${REPO}) against the tmux backend. Use Bash for every step and OBSERVE real output — never assume. The CLI is run as the freshly built binary, not uvx.

0. PRECONDITION: run \`command -v tmux\`. If tmux is absent, set tmuxAvailable=false, pass=false, explain in notes, and stop — this smoke requires tmux.

1. BUILD: \`cd ${REPO} && go build -o /tmp/cc-orchestrate-smoke .\`. Set built from the exit code. Use BIN=/tmp/cc-orchestrate-smoke for everything below.

2. SELECT tmux: \`$BIN backends select tmux\`. Then \`$BIN backends list\` and confirm tmux shows available + selected.

3. TEMP REPO: make an isolated workdir \`PRJ=$(mktemp -d)\` and \`git init\` in it (backends may assume a repo). Create the repo:
   \`$BIN repo create smoke-$$ --backend tmux --cwd "$PRJ"\`. Capture the printed repo id. Confirm a detached tmux session now exists (\`tmux list-sessions\`).

4. WORKSTREAM: create a workstream in the repo: \`$BIN workstream create feat-smoke --repo <repo-id>\`. Capture the printed workstream id and \`worktree:\` path. Assert the worktree dir exists with \`test -d "<worktree>"\`. Set workstreamCreated=true only when the command exits 0 AND that dir exists.

5. SPAWN: \`$BIN agent spawn --workstream <workstream-id> --name smoke --prompt "say hello then wait"\`. Capture the agent id, backend, and terminal (a tmux pane id). Set spawned from whether all three are present. (cwd defaults to the workstream worktree.)

6. STATUS: poll \`$BIN agent status <agent-id>\` every ~2s for up to ~30s. Record the LAST output verbatim into "status". Set statusPopulated=true once "state" is non-empty (e.g. running/active). NOTE: the derived status is tailed from ~/.claude (or \$CLAUDE_CONFIG_DIR)/projects/**/<session>.jsonl — it only fully populates if \`claude\` actually runs in the pane. If \`claude\` is not installed so no transcript ever appears, set statusPopulated=false and say so in notes; do NOT hang past the timeout.

7. SEND-MESSAGE: \`$BIN agent send-message <agent-id> "ping"\`. Set messageDelivered=true if it prints a seq and exits 0.

8. KILL + CLEANUP: \`$BIN agent kill <agent-id>\`, then \`$BIN agent list --repo <repo-id>\` and confirm the agent is gone, and \`tmux list-panes -s -t <session>\` (or list-windows) confirms the agent's window/pane is closed. Set killedClean accordingly. Finally tear down: \`$BIN repo kill <repo-id>\` (cascades the workstream's worktree + backend workspace and any remaining agents), then \`tmux kill-session -t <session>\` and \`rm -rf "$PRJ" /tmp/cc-orchestrate-smoke\`. Confirm the worktree dir from step 4 is gone. Leave no smoke-* tmux sessions or temp dirs behind.

Set pass=true only if built, workstreamCreated, spawned, messageDelivered, and killedClean are all true (statusPopulated may be false when claude is unavailable — record that in notes). Return the schema.`

phase('Smoke')
const result = await agent(PROMPT, { label: 'spawn-smoke', phase: 'Smoke', schema: SCHEMA, effort: 'high' })

log(result ? `smoke pass=${result.pass} (built=${result.built} spawned=${result.spawned} status=${result.statusPopulated} msg=${result.messageDelivered} kill=${result.killedClean})` : 'smoke agent crashed')

return result
