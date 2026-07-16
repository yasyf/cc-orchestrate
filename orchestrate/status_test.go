package orchestrate

import "testing"

// Representative transcript lines mirroring the real ~/.claude/projects schema:
// one content block per line, per-message usage repeated across a message's lines,
// stop_reason on every assistant line, and user tool_result blocks keyed by
// tool_use_id.
const (
	lineText = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_t","role":"assistant","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":20},"content":[{"type":"text","text":"All done."}]}}`

	lineBash = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_b","role":"assistant","stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":7},"content":[{"type":"tool_use","id":"toolu_bash","name":"Bash","input":{"command":"go test ./...","description":"run tests"}}]}}`

	lineEdit = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_e","role":"assistant","stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":4},"content":[{"type":"tool_use","id":"toolu_edit","name":"Edit","input":{"file_path":"/tmp/x.go","old_string":"a","new_string":"b"}}]}}`

	lineResultBash = `{"type":"user","isSidechain":false,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_bash","content":"ok"}]}}`

	lineResultEdit = `{"type":"user","isSidechain":false,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_edit","content":"ok"}]}}`

	lineAsk = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_a","role":"assistant","stop_reason":"tool_use","usage":{"input_tokens":2,"output_tokens":3},"content":[{"type":"tool_use","id":"toolu_ask","name":"AskUserQuestion","input":{"questions":[{"question":"Which?"}]}}]}}`

	lineUserPrompt = `{"type":"user","isSidechain":false,"message":{"role":"user","content":"a plain human prompt"}}`

	// Two lines of one logical message (thinking then tool_use), sharing id and the
	// repeated usage object — the per-message dedup must count it once.
	lineSplitThink = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_s","role":"assistant","stop_reason":"tool_use","usage":{"input_tokens":2,"output_tokens":4046},"content":[{"type":"thinking","thinking":"hmm","signature":"sig"}]}}`
	lineSplitRead  = `{"type":"assistant","isSidechain":false,"message":{"id":"msg_s","role":"assistant","stop_reason":"tool_use","usage":{"input_tokens":2,"output_tokens":4046},"content":[{"type":"tool_use","id":"toolu_read","name":"Read","input":{"file_path":"/etc/hosts"}}]}}`

	lineSidechain = `{"type":"assistant","isSidechain":true,"message":{"id":"msg_side","role":"assistant","stop_reason":"end_turn","usage":{"input_tokens":1000,"output_tokens":1000},"content":[{"type":"text","text":"sidechain text"}]}}`
)

func TestStatusAccumulator(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  Status
	}{
		{
			name:  "empty accumulator is unknown",
			lines: nil,
			want:  Status{State: StateUnknown},
		},
		{
			name:  "assistant end_turn text is idle",
			lines: []string{lineText},
			want:  Status{State: StateIdle, LastText: "All done.", Tokens: 30},
		},
		{
			name:  "pending bash tool_use is working with command target",
			lines: []string{lineBash},
			want:  Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12},
		},
		{
			name:  "edit tool_use targets file_path",
			lines: []string{lineEdit},
			want:  Status{State: StateWorking, Tool: "Edit", Target: "/tmp/x.go", Tokens: 7},
		},
		{
			name:  "tool_result clears pending but mid-turn stays working",
			lines: []string{lineBash, lineResultBash},
			want:  Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12},
		},
		{
			name:  "cleared pending then end_turn goes idle keeping last tool",
			lines: []string{lineBash, lineResultBash, lineText},
			want:  Status{State: StateIdle, Tool: "Bash", Target: "go test ./...", LastText: "All done.", Tokens: 42},
		},
		{
			name:  "pending AskUserQuestion is awaiting-input",
			lines: []string{lineAsk},
			want:  Status{State: StateAwaiting, Tool: "AskUserQuestion", Target: `[{"question":"Which?"}]`, Tokens: 5},
		},
		{
			name:  "usage sums across distinct messages",
			lines: []string{lineBash, lineEdit},
			want:  Status{State: StateWorking, Tool: "Edit", Target: "/tmp/x.go", Tokens: 19},
		},
		{
			name:  "usage counted once per message id across split lines",
			lines: []string{lineSplitThink, lineSplitRead},
			want:  Status{State: StateWorking, Tool: "Read", Target: "/etc/hosts", Tokens: 4048},
		},
		{
			name:  "sidechain lines are skipped",
			lines: []string{lineSidechain, lineBash},
			want:  Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12},
		},
		{
			name:  "unparseable lines are tolerated",
			lines: []string{`{not json`, lineBash},
			want:  Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12},
		},
		{
			name:  "string user content does not disturb state",
			lines: []string{lineBash, lineUserPrompt},
			want:  Status{State: StateWorking, Tool: "Bash", Target: "go test ./...", Tokens: 12},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc := newStatusAcc()
			for _, l := range tc.lines {
				acc.feed([]byte(l))
			}
			if got := acc.status(); got != tc.want {
				t.Errorf("status() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
