package orchestrate

import (
	"context"
	"encoding/json"
	"os"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/channelsetup"
)

// channelNotifyMethod is the JSON-RPC method each subject event is pushed under
// to a child agent's MCP channel.
const channelNotifyMethod = "notifications/claude/channel"

const channelInstructions = `This MCP server is the cc-orchestrate channel. Orchestrator directives arrive as <channel source="` + channelsetup.ChannelSource + `" type="..."> tags whose inner JSON has a "type" field identifying the event.

An orchestrate.message event is a directive: its "text" field is an instruction from the orchestrator to act on. Other event types, such as status frames, are informational and need no reply.

Outside an orchestrated child session the channel is silent, and silence needs nothing from you.`

// channelTools advertises the child agent's one domain channel tool — report —
// the orchestrated agent uses to send progress, results, or questions back to its
// orchestrator. The handler round-trips to the daemon via cco.agent.report because the
// channel server is a separate stdio process and cannot Append directly.
func channelTools(_ context.Context, session, scope string) ([]channel.Tool, string, string, error) {
	client := newClient()
	pid := os.Getpid()
	report := channel.Tool{
		Name:        "report",
		Description: "Report progress, a result, or a question to your orchestrator; appends an orchestrate.report event (OriginAgent) to your subject's log.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"text":  map[string]any{"type": "string", "description": "the progress, result, or question to send to your orchestrator"},
				"state": map[string]any{"type": "string", "description": "optional run state", "enum": []string{"working", "blocked", "done"}},
			},
			"required": []string{"text"},
		},
		Handler: func(ctx context.Context, args json.RawMessage) (string, bool) {
			reply, err := client.Do(ctx, daemon.Envelope{
				Op: mAgentReport.op(), Session: session, ClaudePID: pid, Scope: scope, Body: args,
			})
			if err != nil {
				return "report failed: " + err.Error(), true
			}
			if !reply.OK {
				return reply.Error, true
			}
			return string(reply.Body), false
		},
	}
	return []channel.Tool{report}, channelNotifyMethod, channelInstructions, nil
}
