package orchestrate

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
)

var methodNameRE = regexp.MustCompile(`^cco\.[a-z]+\.[a-zA-Z]+$`)

// TestMethodRegistryInvariants asserts the table's structural contract: unique
// cco.<noun>.<verb> names, a valid kind, socket exposure, a live adapter, and a
// req/res schema that generates.
func TestMethodRegistryInvariants(t *testing.T) {
	seen := map[string]bool{}
	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			if !methodNameRE.MatchString(m.name) {
				t.Errorf("name %q does not match %s", m.name, methodNameRE)
			}
			if seen[m.name] {
				t.Errorf("duplicate method name %q", m.name)
			}
			seen[m.name] = true
			if m.kind != kindQuery && m.kind != kindProcedure {
				t.Errorf("kind %q is neither query nor procedure", m.kind)
			}
			if !m.exposes.has(exposeSocket) {
				t.Errorf("method %q is not socket-exposed", m.name)
			}
			if m.adapt == nil {
				t.Errorf("method %q has a nil adapter", m.name)
			}
			if schemaFor(m.reqType)["type"] == nil {
				t.Errorf("req schema for %q has no type", m.name)
			}
			if schemaFor(m.resType)["type"] == nil {
				t.Errorf("res schema for %q has no type", m.name)
			}
		})
	}
	if len(seen) != len(methods) {
		t.Errorf("table has %d entries but %d unique names", len(methods), len(seen))
	}
}

// TestExposeBits asserts the transport exposure contract: report is socket-only, the
// new getters are off the parent MCP list, and everything else is xrpc-exposed.
func TestExposeBits(t *testing.T) {
	if mAgentReport.exposes != exposeSocket {
		t.Errorf("cco.agent.report exposes = %b, want socket-only", mAgentReport.exposes)
	}
	for _, m := range methods {
		if m.name == mAgentReport.name {
			if m.exposes.has(exposeXRPC) || m.exposes.has(exposeMCP) {
				t.Errorf("%q must not be on xrpc or mcp", m.name)
			}
			continue
		}
		if !m.exposes.has(exposeXRPC) {
			t.Errorf("%q must be xrpc-exposed", m.name)
		}
	}
}

func TestDecodeBodyStrict(t *testing.T) {
	t.Run("empty body yields the zero request", func(t *testing.T) {
		req, err := decodeBody[agentShowRequest](nil)
		if err != nil || req.AgentID != "" {
			t.Fatalf("decodeBody(nil) = %+v, %v; want zero, nil", req, err)
		}
	})
	t.Run("known field decodes", func(t *testing.T) {
		req, err := decodeBody[agentShowRequest]([]byte(`{"agent_id":"a1"}`))
		if err != nil || req.AgentID != "a1" {
			t.Fatalf("decodeBody = %+v, %v; want {a1}, nil", req, err)
		}
	})
	t.Run("unknown field is rejected", func(t *testing.T) {
		if _, err := decodeBody[agentShowRequest]([]byte(`{"bogus":1}`)); err == nil {
			t.Fatal("decodeBody accepted an unknown field")
		}
	})
}

// TestRunTypedErrorCodes covers the adapter's failure taxonomy: a decode failure is
// InvalidRequest, a store not-found is NotFound, an opErr tag is honored, an untagged
// error is InternalError, and a success marshals the response.
func TestRunTypedErrorCodes(t *testing.T) {
	ok := func(daemon.HandlerCtx, agentShowRequest) (agentView, error) {
		return agentView{ID: "a1"}, nil
	}
	for _, tc := range []struct {
		name       string
		fn         func(daemon.HandlerCtx, agentShowRequest) (agentView, error)
		body       string
		wantOK     bool
		wantPrefix string
	}{
		{"decode failure", ok, `{"bogus":1}`, false, "InvalidRequest: "},
		{"tagged conflict", func(daemon.HandlerCtx, agentShowRequest) (agentView, error) {
			return agentView{}, opErr(codeConflict, errors.New("boom"))
		}, `{}`, false, "Conflict: "},
		{"store not-found", func(daemon.HandlerCtx, agentShowRequest) (agentView, error) {
			return agentView{}, notFoundf("agent not found: x")
		}, `{}`, false, "NotFound: "},
		{"untagged internal", func(daemon.HandlerCtx, agentShowRequest) (agentView, error) {
			return agentView{}, errors.New("splat")
		}, `{}`, false, "InternalError: "},
		{"success", ok, `{"agent_id":"a1"}`, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := runTyped(tc.fn, daemon.HandlerCtx{Env: daemon.Envelope{Body: []byte(tc.body)}})
			if reply.OK != tc.wantOK {
				t.Fatalf("OK = %v, want %v (error %q)", reply.OK, tc.wantOK, reply.Error)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(reply.Error, tc.wantPrefix) {
				t.Errorf("error = %q, want prefix %q", reply.Error, tc.wantPrefix)
			}
			if tc.wantOK && len(reply.Body) == 0 {
				t.Error("success reply has no body")
			}
		})
	}
}

// TestRegisterOpsNoPanic exercises the atomic registration path: registerOps must
// register every socket method under its cco.* op name without colliding with a
// cc-interact reserved op or double-registering (either panics in Server.Register).
func TestRegisterOpsNoPanic(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, err := daemon.New(daemon.Config{
		AppName:        AppName,
		Paths:          appPaths(),
		Version:        Version,
		ActiveStatuses: []string{string(StatusActive)},
		Migrate:        migrate,
	})
	if err != nil {
		t.Fatalf("daemon.New: %v", err)
	}
	registerOps(s)
}

func TestMCPName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"cco.agent.sendMessage", "agent_send_message"},
		{"cco.agent.show", "agent_show"},
		{"cco.fleet.serialize", "fleet_serialize"},
		{"cco.config.set", "config_set"},
		{"cco.repo.create", "repo_create"},
		{"cco.workstream.activate", "workstream_activate"},
	} {
		if got := mcpName(tc.in); got != tc.want {
			t.Errorf("mcpName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestMCPToolParity asserts the generated tool set equals the expected snake_case set
// (the two local backend tools plus every mcp-exposed method), and that the
// socket-only report tool never leaks onto the parent surface.
func TestMCPToolParity(t *testing.T) {
	want := map[string]bool{
		"backends_list": true, "backend_select": true,
		"config_get": true, "config_set": true,
		"repo_create": true, "repo_list": true, "repo_activate": true, "repo_kill": true,
		"workstream_create": true, "workstream_list": true, "workstream_activate": true, "workstream_kill": true,
		"sprint_create": true, "sprint_list": true, "sprint_activate": true,
		"agent_spawn": true, "agent_list": true, "agent_show": true, "agent_send_message": true, "agent_kill": true,
		"fleet_serialize": true, "fleet_restore": true,
	}
	got := map[string]bool{}
	for _, tool := range mcpTools() {
		if got[tool.Name] {
			t.Errorf("duplicate MCP tool %q", tool.Name)
		}
		got[tool.Name] = true
	}
	if len(got) != len(want) {
		t.Errorf("mcpTools() has %d tools, want %d", len(got), len(want))
	}
	for name := range want {
		if !got[name] {
			t.Errorf("mcpTools() missing %q", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("mcpTools() has unexpected tool %q", name)
		}
	}
	if got["agent_report"] {
		t.Error("agent_report is socket-only and must not be on the parent MCP list")
	}
}
