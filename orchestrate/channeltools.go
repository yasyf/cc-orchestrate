package orchestrate

import (
	"context"

	"github.com/yasyf/cc-interact/channel"
)

// channelNotifyMethod is the JSON-RPC method each subject event is pushed under
// to a child agent's MCP channel.
const channelNotifyMethod = "notifications/claude/channel"

// channelTools advertises the child agent's domain channel tools. The real
// report/reply tools land in a later phase; for now the channel carries only the
// event push, with no tools.
func channelTools(ctx context.Context, session, scope string) ([]channel.Tool, string, error) {
	return []channel.Tool{}, channelNotifyMethod, nil
}
