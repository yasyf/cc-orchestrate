package orchestrate

import (
	"encoding/json"
	"sort"
	"strings"
)

// Derived agent states, surfaced on Status.State and persisted in the agents
// table's state column.
const (
	StateWorking  = "working"        // an assistant tool_use has no matching tool_result yet
	StateIdle     = "idle"           // the last assistant turn ended with stop_reason end_turn
	StateAwaiting = "awaiting-input" // a pending AskUserQuestion is blocking on the user
	StateUnknown  = "unknown"        // no assistant activity observed yet
)

// Status is the transcript-derived snapshot of an agent: its coarse state, the
// last tool it invoked with that tool's primary target, the last assistant text
// block, and the cumulative token count across the session.
type Status struct {
	State    string
	Tool     string
	Target   string
	LastText string
	Tokens   int
}

// usage is the per-message token accounting on an assistant line. Claude Code
// repeats the same usage object on every line of a single logical message (one
// content block per line), so it is summed once per message id, not per line.
type usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// contentBlock is one entry in a message's content array. A single block carries
// the union of the fields across block types; only the ones matching .Type are
// populated.
type contentBlock struct {
	Type      string                     `json:"type"`
	Text      string                     `json:"text"`
	Name      string                     `json:"name"`
	ID        string                     `json:"id"`
	Input     map[string]json.RawMessage `json:"input"`
	ToolUseID string                     `json:"tool_use_id"`
}

// message is the assistant/user payload. Content is left raw because a user
// message's content is sometimes a plain string rather than a block array.
type message struct {
	ID         string          `json:"id"`
	StopReason string          `json:"stop_reason"`
	Usage      *usage          `json:"usage"`
	Content    json.RawMessage `json:"content"`
}

// transcriptLine is one JSONL record. Only assistant and user records with a
// message body drive status; every other type, and any line on a sidechain, is
// ignored.
type transcriptLine struct {
	Type        string   `json:"type"`
	IsSidechain bool     `json:"isSidechain"`
	Message     *message `json:"message"`
}

// statusAcc folds a JSONL transcript line-by-line into a Status. It tolerates
// malformed lines, skips sidechains, and counts each assistant message's usage
// exactly once even when the message spans several lines.
type statusAcc struct {
	pending    map[string]string // tool_use_id -> tool name, awaiting a tool_result
	seenMsg    map[string]bool   // message ids whose usage is already counted
	lastTool   string
	lastTarget string
	lastText   string
	stopReason string
	tokens     int
}

func newStatusAcc() *statusAcc {
	return &statusAcc{pending: map[string]string{}, seenMsg: map[string]bool{}}
}

// feed parses one JSONL line and updates the accumulator. A line that fails to
// parse, lives on a sidechain, or carries no message body is silently skipped.
func (a *statusAcc) feed(line []byte) {
	var l transcriptLine
	if json.Unmarshal(line, &l) != nil || l.IsSidechain || l.Message == nil {
		return
	}
	switch l.Type {
	case "assistant":
		a.feedAssistant(l.Message)
	case "user":
		for _, b := range decodeBlocks(l.Message.Content) {
			if b.Type == "tool_result" {
				delete(a.pending, b.ToolUseID)
			}
		}
	}
}

func (a *statusAcc) feedAssistant(m *message) {
	if m.Usage != nil && !a.seenMsg[m.ID] {
		a.seenMsg[m.ID] = true
		a.tokens += m.Usage.InputTokens + m.Usage.OutputTokens
	}
	if m.StopReason != "" {
		a.stopReason = m.StopReason
	}
	for _, b := range decodeBlocks(m.Content) {
		switch b.Type {
		case "text":
			if b.Text != "" {
				a.lastText = b.Text
			}
		case "tool_use":
			a.lastTool = b.Name
			a.lastTarget = toolTarget(b.Name, b.Input)
			a.pending[b.ID] = b.Name
		}
	}
}

// status derives the current snapshot. Pending tool_use wins (awaiting-input when
// the pending tool is AskUserQuestion, working otherwise); with nothing pending,
// an end_turn stop is idle, no assistant activity is unknown, and a mid-turn stop
// is working.
func (a *statusAcc) status() Status {
	return Status{
		State:    a.state(),
		Tool:     a.lastTool,
		Target:   a.lastTarget,
		LastText: a.lastText,
		Tokens:   a.tokens,
	}
}

func (a *statusAcc) state() string {
	if len(a.pending) > 0 {
		for _, name := range a.pending {
			if name == "AskUserQuestion" {
				return StateAwaiting
			}
		}
		return StateWorking
	}
	switch a.stopReason {
	case "":
		return StateUnknown
	case "end_turn":
		return StateIdle
	default:
		return StateWorking
	}
}

// decodeBlocks reads a message's content as a block array, returning nil when the
// content is absent or a plain string (a human prompt rather than tool traffic).
func decodeBlocks(raw json.RawMessage) []contentBlock {
	var blocks []contentBlock
	if json.Unmarshal(raw, &blocks) != nil {
		return nil
	}
	return blocks
}

// toolTarget extracts a tool_use's primary subject: the command for Bash, the
// file for Read/Edit/Write, the pattern for Grep/Glob, the description for Task,
// and otherwise the value of the alphabetically first input key.
func toolTarget(name string, input map[string]json.RawMessage) string {
	switch name {
	case "Bash":
		return rawString(input["command"])
	case "Read", "Edit", "Write":
		return rawString(input["file_path"])
	case "Grep", "Glob":
		return rawString(input["pattern"])
	case "Task":
		return rawString(input["description"])
	default:
		return firstValue(input)
	}
}

func firstValue(input map[string]json.RawMessage) string {
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return rawString(input[keys[0]])
}

// rawString unwraps a JSON string value, falling back to the trimmed raw JSON for
// non-string values (e.g. an AskUserQuestion questions array).
func rawString(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}
