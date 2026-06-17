package orchestrate

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestMCPServer drives the parent control server over an in-memory stdio pipe:
// initialize, tools/list, and a daemon-free tools/call. A fixed request reader
// feeds three newline-delimited JSON-RPC messages; the server answers each and
// exits at EOF, so every reply is in the buffer once runMCP returns.
func TestMCPServer(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // backends_list reads state off disk; keep it hermetic

	requests := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"backends_list","arguments":{}}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := runMCP(context.Background(), strings.NewReader(requests), &out); err != nil {
		t.Fatalf("runMCP: %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d reply lines, want 3:\n%s", len(lines), out.String())
	}

	var initReply struct {
		Result struct {
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &initReply); err != nil {
		t.Fatalf("initialize reply: %v", err)
	}
	if initReply.Result.ServerInfo.Name != AppName {
		t.Errorf("serverInfo.name = %q, want %q", initReply.Result.ServerInfo.Name, AppName)
	}

	var listReply struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &listReply); err != nil {
		t.Fatalf("tools/list reply: %v", err)
	}
	got := make(map[string]bool, len(listReply.Result.Tools))
	for _, tool := range listReply.Result.Tools {
		got[tool.Name] = true
	}
	want := []string{
		"backends_list", "backend_select", "project_create", "project_list", "project_activate", "project_kill",
		"agent_spawn", "agent_list", "agent_send_message", "agent_status", "agent_kill",
	}
	if len(listReply.Result.Tools) != len(want) {
		t.Fatalf("advertised %d tools, want %d: %v", len(listReply.Result.Tools), len(want), got)
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("tools/list missing %q", name)
		}
	}

	var callReply struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[2]), &callReply); err != nil {
		t.Fatalf("tools/call reply: %v", err)
	}
	if callReply.Result.IsError {
		t.Errorf("backends_list returned an error: %+v", callReply.Result.Content)
	}
	if len(callReply.Result.Content) == 0 || !strings.Contains(callReply.Result.Content[0].Text, "BACKEND") {
		t.Errorf("backends_list content = %+v, want a backends table", callReply.Result.Content)
	}
}
