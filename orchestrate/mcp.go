package orchestrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// runMCP serves the parent-facing MCP control server over in/out until in reaches
// EOF. It is request/response control only — there is no event-stream pump, so the
// parent subscribes to live agent updates out of band via `agent watch` under a
// Claude Code Monitor.
func runMCP(ctx context.Context, in io.Reader, out io.Writer) error {
	srv := channel.NewServer(channel.ServerInfo{Name: AppName, Version: buildVersion()}, mcpTools())
	return srv.Serve(ctx, in, out)
}

// mcpDispatch is the shared MCP tool round trip, mirroring the CLI's runOp: ensure
// the daemon is current, send one domain envelope keyed to the orchestrator's
// session and cwd, and return the reply body as the tool result text (or the error
// text with isErr=true).
func mcpDispatch(ctx context.Context, op daemon.Op, args json.RawMessage) (string, bool) {
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return err.Error(), true
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err.Error(), true
	}
	client, err := newClient(ctx)
	if err != nil {
		return err.Error(), true
	}
	reply, err := client.Do(ctx, daemon.Envelope{
		Op: op, Session: AppName, ClaudePID: d.ClaudePID(), Scope: cwd, Body: args,
	})
	if err != nil {
		return err.Error(), true
	}
	if !reply.OK {
		return reply.Error, true
	}
	return string(reply.Body), false
}

// objectSchema builds a JSON-Schema object node with the given properties, marking
// the named keys required.
func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func stringProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

// mcpName derives a method's MCP tool name: strip the cco. prefix, replace dots with
// underscores, and break camelCase verbs (cco.agent.sendMessage → agent_send_message).
func mcpName(method string) string {
	return camelToSnake(strings.ReplaceAll(strings.TrimPrefix(method, "cco."), ".", "_"))
}

// camelToSnake lowercases s and inserts an underscore before each interior capital.
func camelToSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// mcpTool builds the MCP tool that forwards to a method's daemon op, its input schema
// generated from the method's request type.
func (m method) mcpTool() channel.Tool {
	return channel.Tool{
		Name:        mcpName(m.name),
		Description: m.desc,
		InputSchema: schemaFor(m.reqType),
		Handler: func(ctx context.Context, args json.RawMessage, _ func(string)) (string, bool) {
			return mcpDispatch(ctx, m.op(), args)
		},
	}
}

// mcpTools is the parent-facing control surface: the two daemon-free backend tools
// plus one generated tool per mcp-exposed registry method, so the surface can never
// drift from the op registry.
func mcpTools() []channel.Tool {
	tools := []channel.Tool{
		{
			Name:        "backends_list",
			Description: "List the agent placement backends, their install status, and the effective default. Needs no running daemon.",
			InputSchema: objectSchema(map[string]any{}),
			Handler: func(context.Context, json.RawMessage, func(string)) (string, bool) {
				table, err := backendsTable()
				if err != nil {
					return err.Error(), true
				}
				return table, false
			},
		},
		{
			Name:        "backend_select",
			Description: "Persist the default placement backend for new repos. The backend must be installed.",
			InputSchema: objectSchema(map[string]any{"backend": stringProp("backend name, e.g. herd, superset, cmux, zellij, tmux")}, "backend"),
			Handler:     mcpBackendSelect,
		},
	}
	for _, m := range methods {
		if m.exposes.has(exposeMCP) {
			tools = append(tools, m.mcpTool())
		}
	}
	return tools
}

// mcpBackendSelect validates the named backend is installed (mirroring the CLI's
// backends select) before persisting it through config-set.
func mcpBackendSelect(ctx context.Context, args json.RawMessage, _ func(string)) (string, bool) {
	var b struct {
		Backend string `json:"backend"`
	}
	if err := json.Unmarshal(args, &b); err != nil {
		return "bad backend_select arguments: " + err.Error(), true
	}
	if err := backend.ValidateBackend(backend.Name(b.Backend)); err != nil {
		return fmt.Sprintf("%s; run the backends_list tool", err.Error()), true
	}
	body, _ := json.Marshal(map[string]string{"key": "backend", "value": b.Backend})
	return mcpDispatch(ctx, mConfigSet.op(), body)
}
