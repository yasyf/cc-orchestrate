package orchestrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
)

// TestRenderReplyVerbatim proves --json mode writes the daemon reply body byte-for-byte
// (plus a trailing newline) across the list, show, config-list, and fleet-status shapes,
// and never runs the human renderer. Each body carries a field the local response struct
// omits — a re-marshal through the typed struct would silently drop it.
func TestRenderReplyVerbatim(t *testing.T) {
	assertVerbatim[[]repoView](t, `[{"id":"p1","name":"alpha","daemon_added":"keep me"}]`)
	assertVerbatim[agentView](t, `{"id":"a1","name":"worker","daemon_added":true}`)
	assertVerbatim[[]configEntry](t, `[{"key":"backend","value":"tmux","daemon_added":9}]`)
	assertVerbatim[fleetStatusResult](t, `{"fleet_subject":"s","seq":7,"http_port":8080,"daemon_added":[1,2]}`)
}

func assertVerbatim[T any](t *testing.T, body string) {
	t.Helper()
	var buf bytes.Buffer
	err := renderReply(&buf, true, json.RawMessage(body), func(io.Writer, T) error {
		t.Fatal("human renderer ran in JSON mode")
		return nil
	})
	if err != nil {
		t.Fatalf("renderReply: %v", err)
	}
	if got, want := buf.String(), body+"\n"; got != want {
		t.Fatalf("json output = %q, want %q (verbatim body + newline)", got, want)
	}
}

// TestRenderReplyHuman proves human mode decodes into the local struct — dropping a
// daemon-added field the struct does not carry — and renders through the human func.
func TestRenderReplyHuman(t *testing.T) {
	body := json.RawMessage(`[{"id":"p1","name":"alpha","future_field":"dropped"}]`)
	var buf bytes.Buffer
	err := renderReply(&buf, false, body, func(w io.Writer, views []repoView) error {
		_, e := fmt.Fprintf(w, "%d:%s", len(views), views[0].Name)
		return e
	})
	if err != nil {
		t.Fatalf("renderReply: %v", err)
	}
	if got, want := buf.String(), "1:alpha"; got != want {
		t.Fatalf("human output = %q, want %q", got, want)
	}
}

// TestBackendsSelectRenderModes pins the specialized config-set renderer: human mode
// keeps its legacy line, while JSON mode preserves the daemon body byte-for-byte.
func TestBackendsSelectRenderModes(t *testing.T) {
	const name = "tmux"
	body := json.RawMessage(`{"key":"backend","value":"tmux","daemon_added":1}`)
	human := func(w io.Writer, _ configSetResult) error {
		_, err := fmt.Fprintf(w, "selected backend: %s\n", name)
		return err
	}

	for _, tc := range []struct {
		name   string
		asJSON bool
		want   string
	}{
		{"human", false, "selected backend: tmux\n"},
		{"json", true, string(body) + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := renderReply(&buf, tc.asJSON, body, human); err != nil {
				t.Fatalf("renderReply: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Fatalf("output = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAgentShowHumanMatchesLegacy pins the `agent show` (alias `status`) human view to
// the exact bytes the hand-padded status command emitted before it was routed through
// renderKV, so the alias renders identically and scripts keep working.
func TestAgentShowHumanMatchesLegacy(t *testing.T) {
	a := agentView{
		ID: "a1", Name: "worker", Status: "active", State: "working",
		Activity: "Bash: ls", Tokens: 10, RestartCount: 2, UpdatedAt: "2026-06-16T00:00:00Z",
	}
	var legacy bytes.Buffer
	fmt.Fprintf(&legacy, "agent:    %s\n", a.ID)
	fmt.Fprintf(&legacy, "name:     %s\n", a.Name)
	fmt.Fprintf(&legacy, "status:   %s\n", a.Status)
	fmt.Fprintf(&legacy, "state:    %s\n", a.State)
	fmt.Fprintf(&legacy, "activity: %s\n", a.Activity)
	fmt.Fprintf(&legacy, "tokens:   %d\n", a.Tokens)
	fmt.Fprintf(&legacy, "restart:  %d\n", a.RestartCount)
	fmt.Fprintf(&legacy, "updated:  %s\n", a.UpdatedAt)

	var got bytes.Buffer
	err := renderKV(&got, [][2]string{
		{"agent", a.ID}, {"name", a.Name}, {"status", a.Status}, {"state", a.State},
		{"activity", a.Activity}, {"tokens", strconv.Itoa(a.Tokens)},
		{"restart", strconv.Itoa(a.RestartCount)}, {"updated", a.UpdatedAt},
	})
	if err != nil {
		t.Fatalf("renderKV: %v", err)
	}
	if got.String() != legacy.String() {
		t.Fatalf("renderKV =\n%q\nlegacy =\n%q", got.String(), legacy.String())
	}
}

// TestViewCompletionFields asserts every field Phase 5 added to the views is populated
// from its row and reaches the marshaled JSON, so a late-connecting TUI can read them.
func TestViewCompletionFields(t *testing.T) {
	agent := marshalToMap(t, newAgentView(agentRow{
		ID: "a1", TerminalHandle: "term-1", Prompt: "fix it", SubjectID: "subj-1",
		CCNotesTask: "task-1", CreatedAt: "t0", LastRestartAt: "t1",
	}))
	for _, k := range []string{"terminal_handle", "prompt", "subject_id", "ccnotes_task", "created_at", "last_restart_at"} {
		if _, ok := agent[k]; !ok {
			t.Errorf("agentView JSON missing %q", k)
		}
	}
	if agent["terminal_handle"] != "term-1" || agent["ccnotes_task"] != "task-1" || agent["last_restart_at"] != "t1" {
		t.Errorf("agentView fields not populated from the row: %v", agent)
	}

	ws := marshalToMap(t, newWorkstreamView(workstreamRow{
		ID: "w1", WorkspaceHandle: "ws-handle", CCNotesProject: "proj-1",
	}))
	for _, k := range []string{"workspace_handle", "ccnotes_project"} {
		if _, ok := ws[k]; !ok {
			t.Errorf("workstreamView JSON missing %q", k)
		}
	}
	if ws["workspace_handle"] != "ws-handle" || ws["ccnotes_project"] != "proj-1" {
		t.Errorf("workstreamView fields not populated from the row: %v", ws)
	}

	sp := marshalToMap(t, newSprintView(sprintRow{ID: "s1", CCNotesSprint: "sprint-cc"}))
	if sp["ccnotes_sprint"] != "sprint-cc" {
		t.Errorf("sprintView ccnotes_sprint = %v, want sprint-cc", sp["ccnotes_sprint"])
	}
}

// headHasLine reports whether the KV head carries a "key: value" line, tolerant of the
// alignment padding between the colon and the value.
func headHasLine(out, key, value string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, key+":") && strings.TrimSpace(strings.TrimPrefix(line, key+":")) == value {
			return true
		}
	}
	return false
}

func marshalToMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// TestRenderFleetStatus proves the human fleet status joins each agent's repo,
// workstream, and sprint names from the snapshot's own views and reports the summary
// head counts.
func TestRenderFleetStatus(t *testing.T) {
	res := fleetStatusResult{
		FleetSubject: "subj-fleet", Seq: 42, HTTPPort: 8080,
		Repos:       []repoView{{ID: "r1", Name: "myrepo", Backend: "tmux"}},
		Workstreams: []workstreamView{{ID: "w1", RepoID: "r1", Name: "feature-ws"}},
		Sprints:     []sprintView{{ID: "s1", WorkstreamID: "w1", Name: "sprint-one"}},
		Agents:      []agentView{{ID: "a1", Name: "worker", SprintID: "s1", State: "working", Status: "active", Tokens: 99}},
	}
	var buf bytes.Buffer
	if err := renderFleetStatus(&buf, res); err != nil {
		t.Fatalf("renderFleetStatus: %v", err)
	}
	out := buf.String()
	for _, kv := range [][2]string{
		{"agents", "1"}, {"repos", "1"}, {"workstreams", "1"}, {"sprints", "1"},
		{"backend", "tmux"}, {"subject", "subj-fleet"}, {"seq", "42"}, {"port", "8080"},
	} {
		if !headHasLine(out, kv[0], kv[1]) {
			t.Errorf("fleet status head missing %q: %q\n%s", kv[0], kv[1], out)
		}
	}
	// The joined agent row must carry the names walked up the hierarchy, not the raw ids.
	var row string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "worker") {
			row = line
		}
	}
	if row == "" {
		t.Fatalf("no joined agent row for worker in\n%s", out)
	}
	for _, want := range []string{"worker", "myrepo", "feature-ws", "sprint-one", "working", "active", "99"} {
		if !strings.Contains(row, want) {
			t.Errorf("joined agent row missing %q: %q", want, row)
		}
	}
}

// TestRenderFleetStatusDanglingLink proves a mid-teardown snapshot — an agent whose
// sprint view is gone — still renders, degrading to the raw sprint id rather than
// panicking on the missing map entry.
func TestRenderFleetStatusDanglingLink(t *testing.T) {
	res := fleetStatusResult{
		Agents: []agentView{{ID: "a1", Name: "orphan", SprintID: "s-gone", State: "idle", Status: "exited"}},
	}
	var buf bytes.Buffer
	if err := renderFleetStatus(&buf, res); err != nil {
		t.Fatalf("renderFleetStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "s-gone") {
		t.Errorf("dangling sprint id not surfaced:\n%s", buf.String())
	}
}

// TestFormatFleetFrame covers every fleet frame type: the ts prefix, the "fleet."-less
// type token, and the per-type detail suffix must all reach the rendered line.
func TestFormatFleetFrame(t *testing.T) {
	const ts = "2026-06-16T00:00:00Z"
	for _, tc := range []struct {
		name      string
		payload   string
		short     string
		suffix    string
		sanitized bool
	}{
		{"spawned", `{"type":"fleet.agent.spawned","ts":"TS","agent_id":"a1","name":"worker","backend":"tmux"}`, "agent.spawned", "a1 worker backend=tmux", false},
		{"status", `{"type":"fleet.agent.status","ts":"TS","agent_id":"a1","state":"working","tool":"Bash","target":"ls","tokens":10}`, "agent.status", "a1 working Bash ls tokens=10", false},
		{"status multiline target", `{"type":"fleet.agent.status","ts":"TS","agent_id":"a1","state":"working","tool":"Bash","target":"line one\r\nline two","tokens":10}`, "agent.status", `a1 working Bash line one\nline two tokens=10`, true},
		{"status no tool", `{"type":"fleet.agent.status","ts":"TS","agent_id":"a1","state":"idle","tokens":0}`, "agent.status", "a1 idle tokens=0", false},
		{"message", `{"type":"fleet.agent.message","ts":"TS","agent_id":"a1"}`, "agent.message", "a1", false},
		{"report", `{"type":"fleet.agent.report","ts":"TS","agent_id":"a1","state":"working"}`, "agent.report", "a1 state=working", false},
		{"exited", `{"type":"fleet.agent.exited","ts":"TS","agent_id":"a1","reason":"killed"}`, "agent.exited", "a1 reason=killed", false},
		{"restarted", `{"type":"fleet.agent.restarted","ts":"TS","agent_id":"a1","attempt":2}`, "agent.restarted", "a1 attempt=2", false},
		{"abandoned", `{"type":"fleet.agent.abandoned","ts":"TS","agent_id":"a1","attempts":3}`, "agent.abandoned", "a1 attempts=3", false},
		{"repo created", `{"type":"fleet.repo.created","ts":"TS","id":"r1","name":"myrepo"}`, "repo.created", "r1 myrepo", false},
		{"workstream activated", `{"type":"fleet.workstream.activated","ts":"TS","id":"w1","name":"feat"}`, "workstream.activated", "w1 feat", false},
		{"sprint killed", `{"type":"fleet.sprint.killed","ts":"TS","id":"s1","name":"main"}`, "sprint.killed", "s1 main", false},
		{"serialized", `{"type":"fleet.serialized","ts":"TS","path":"/b.json","count":4}`, "serialized", "/b.json count=4", false},
		{"restored", `{"type":"fleet.restored","ts":"TS","path":"/b.json","count":4}`, "restored", "/b.json count=4", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := formatFleetFrame(strings.ReplaceAll(tc.payload, "TS", ts))
			if !strings.HasPrefix(line, ts+"  ") {
				t.Errorf("line %q missing ts prefix", line)
			}
			if !strings.Contains(line, tc.short) {
				t.Errorf("line %q missing type token %q", line, tc.short)
			}
			if !strings.HasSuffix(line, tc.suffix) {
				t.Errorf("line %q missing detail suffix %q", line, tc.suffix)
			}
			if tc.sanitized {
				if strings.ContainsAny(line, "\r\n") {
					t.Errorf("line contains a physical line break: %q", line)
				}
				if !strings.Contains(line, "\\n") {
					t.Errorf("line %q missing literal \\n escape", line)
				}
			}
		})
	}
}

// TestFormatFleetFramePassThrough proves an unparseable payload passes through verbatim
// rather than erroring or dropping the line.
func TestFormatFleetFramePassThrough(t *testing.T) {
	if got := formatFleetFrame("not json"); got != "not json" {
		t.Errorf("formatFleetFrame(bad) = %q, want verbatim", got)
	}
}

// TestFormatEventLine covers the per-agent event human renderer used by `agent watch`.
func TestFormatEventLine(t *testing.T) {
	for _, tc := range []struct {
		name      string
		payload   string
		want      string
		sanitized bool
	}{
		{"status", `{"type":"orchestrate.status","state":"working","tool":"Bash","target":"ls","tokens":10}`, "status    working Bash ls tokens=10", false},
		{"message", `{"type":"orchestrate.message","text":"go on"}`, "message   go on", false},
		{"message multiline", `{"type":"orchestrate.message","text":"first\nsecond"}`, `message   first\nsecond`, true},
		{"report", `{"type":"orchestrate.report","text":"halfway","state":"working"}`, "report    halfway state=working", false},
		{"inbound", `{"type":"orchestrate.inbound","text":"hi"}`, "inbound   hi", false},
		{"exited", `{"type":"orchestrate.exited"}`, "exited", false},
		{"spawned", `{"type":"orchestrate.spawned","backend":"tmux","terminal":"t1"}`, "spawned   backend=tmux terminal=t1", false},
		{"restarted", `{"type":"orchestrate.restarted","terminal":"t2","attempt":1}`, "restarted terminal=t2 attempt=1", false},
		{"abandoned", `{"type":"orchestrate.abandoned","attempts":3}`, "abandoned attempts=3", false},
		{"restored", `{"type":"orchestrate.restored","terminal":"t3"}`, "restored  terminal=t3", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := formatEventLine(tc.payload)
			if got != tc.want {
				t.Errorf("formatEventLine = %q, want %q", got, tc.want)
			}
			if tc.sanitized {
				if strings.ContainsAny(got, "\r\n") {
					t.Errorf("line contains a physical line break: %q", got)
				}
				if !strings.Contains(got, "\\n") {
					t.Errorf("line %q missing literal \\n escape", got)
				}
			}
		})
	}
}

// TestFormatEventLinePassThrough proves an unrecognized frame type passes through
// verbatim.
func TestFormatEventLinePassThrough(t *testing.T) {
	if got := formatEventLine(`{"type":"orchestrate.unknown"}`); got != `{"type":"orchestrate.unknown"}` {
		t.Errorf("formatEventLine(unknown) = %q, want verbatim", got)
	}
}

// TestRenderBackendsSplit proves the local backends view marshals to the expected JSON
// shape, the daemon-free surface `backends list --json` emits.
func TestRenderBackendsSplit(t *testing.T) {
	view := backendView{Name: "tmux", Installed: true, Default: true}
	b, err := json.Marshal([]backendView{view})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(b), `[{"name":"tmux","installed":true,"default":true}]`; got != want {
		t.Fatalf("backendView JSON = %q, want %q", got, want)
	}
}
