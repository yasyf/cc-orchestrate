package orchestrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/yasyf/cc-interact/channel"
	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// runMCP serves the parent-facing MCP control server over in/out until in reaches
// EOF. It is request/response control only — there is no event-stream pump, so the
// parent subscribes to live agent updates out of band via `agent watch` under a
// Claude Code Monitor.
func runMCP(ctx context.Context, in io.Reader, out io.Writer) error {
	srv := channel.NewServer(channel.ServerInfo{Name: AppName, Version: Version}, mcpTools())
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
	reply, err := newClient().Do(ctx, daemon.Envelope{
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

// opTool builds a pass-through MCP tool whose handler forwards its arguments
// verbatim to the daemon op.
func opTool(name, description string, schema map[string]any, op daemon.Op) channel.Tool {
	return channel.Tool{
		Name: name, Description: description, InputSchema: schema,
		Handler: func(ctx context.Context, args json.RawMessage) (string, bool) {
			return mcpDispatch(ctx, op, args)
		},
	}
}

// mcpTools is the parent-facing control surface mapping the orchestration ops
// onto MCP, in the order tools/list advertises them.
func mcpTools() []channel.Tool {
	return []channel.Tool{
		{
			Name:        "backends_list",
			Description: "List the agent placement backends, their install status, and the effective default. Needs no running daemon.",
			InputSchema: objectSchema(map[string]any{}),
			Handler: func(context.Context, json.RawMessage) (string, bool) {
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
		opTool("config_get",
			"Read one persisted config key's value.",
			objectSchema(map[string]any{"key": stringProp("config key, e.g. backend")}, "key"),
			opConfigGet),
		opTool("repo_create",
			"Create an orchestration repo and its backend workspace.",
			objectSchema(map[string]any{
				"name":    stringProp("human-readable repo name"),
				"backend": stringProp("backend override (defaults to the selected/first available)"),
				"cwd":     stringProp("working directory for the repo (defaults to the current directory)"),
			}, "name"),
			opRepoCreate),
		opTool("repo_list",
			"List the orchestration repos.",
			objectSchema(map[string]any{}),
			opRepoList),
		opTool("repo_activate",
			"Mark a repo active.",
			objectSchema(map[string]any{"id": stringProp("repo id or name")}, "id"),
			opRepoActivate),
		opTool("repo_kill",
			"Kill a repo, its backend workspace, and all of its agents.",
			objectSchema(map[string]any{"id": stringProp("repo id or name")}, "id"),
			opRepoKill),
		opTool("workstream_create",
			"Create a workstream — a branch and its worktree — in a repo, backed by a fresh backend workspace.",
			objectSchema(map[string]any{
				"repo":   stringProp("repo id or name to create the workstream in"),
				"name":   stringProp("human-readable workstream name"),
				"branch": stringProp("git branch for the worktree (defaults to the name)"),
			}, "name"),
			opWorkstreamCreate),
		opTool("workstream_list",
			"List the workstreams, optionally filtered by repo.",
			objectSchema(map[string]any{"repo": stringProp("filter by repo id or name")}),
			opWorkstreamList),
		opTool("workstream_activate",
			"Mark a workstream active and record it as the default an agent spawn lands in.",
			objectSchema(map[string]any{
				"id":   stringProp("workstream id or name"),
				"repo": stringProp("repo id or name to disambiguate the workstream name"),
			}, "id"),
			opWorkstreamActivate),
		opTool("workstream_kill",
			"Kill a workstream: tear down its backend workspace and worktree and exit its agents.",
			objectSchema(map[string]any{
				"id":   stringProp("workstream id or name"),
				"repo": stringProp("repo id or name to disambiguate the workstream name"),
			}, "id"),
			opWorkstreamKill),
		opTool("sprint_create",
			"Create a sprint — a planning group an agent spawns into — in a workstream. A sprint shares its workstream's worktree.",
			objectSchema(map[string]any{
				"workstream": stringProp("workstream id or name to create the sprint in"),
				"name":       stringProp("human-readable sprint name"),
			}, "name"),
			opSprintCreate),
		opTool("sprint_list",
			"List the sprints, optionally filtered by workstream.",
			objectSchema(map[string]any{"workstream": stringProp("filter by workstream id or name")}),
			opSprintList),
		opTool("sprint_activate",
			"Mark a sprint active and record it as the default an agent spawn lands in.",
			objectSchema(map[string]any{
				"id":         stringProp("sprint id or name"),
				"workstream": stringProp("workstream id or name to disambiguate the sprint name"),
			}, "id"),
			opSprintActivate),
		opTool("agent_spawn",
			"Spawn a child Claude Code agent into a sprint: named explicitly, or a workstream's or repo's default sprint, or the active sprint. The agent reports back via its report tool; watch its progress with agent_list / agent_status, or stream it live with `agent watch` under a Monitor.",
			objectSchema(map[string]any{
				"repo":       stringProp("repo id or name to spawn into (uses its primary workstream's default sprint)"),
				"workstream": stringProp("workstream id or name to spawn into (uses its default sprint)"),
				"sprint":     stringProp("sprint id or name to spawn into"),
				"name":       stringProp("human-readable agent name"),
				"cwd":        stringProp("working directory / scope (defaults to the workstream worktree)"),
				"prompt":     stringProp("initial prompt for the child agent"),
			}, "prompt"),
			opSpawn),
		opTool("agent_list",
			"List agents and their derived status, optionally filtered by repo. This is a point-in-time snapshot; for live updates run `cc-orchestrate agent watch` under a Monitor.",
			objectSchema(map[string]any{"repo": stringProp("filter by repo id or name")}),
			opList),
		opTool("agent_send_message",
			"Send a message (a new instruction) to a running agent; the agent receives it on its watch Monitor.",
			objectSchema(map[string]any{
				"agent_id": stringProp("agent id"),
				"text":     stringProp("message text"),
			}, "agent_id", "text"),
			opSendMessage),
		opTool("agent_status",
			"Show one agent's derived status. This is a point-in-time snapshot; for live updates run `cc-orchestrate agent watch` under a Monitor.",
			objectSchema(map[string]any{"agent_id": stringProp("agent id")}, "agent_id"),
			opStatus),
		opTool("agent_kill",
			"Kill a running agent.",
			objectSchema(map[string]any{"agent_id": stringProp("agent id")}, "agent_id"),
			opAgentKill),
	}
}

// mcpBackendSelect validates the named backend is installed (mirroring the CLI's
// backends select) before persisting it through config-set.
func mcpBackendSelect(ctx context.Context, args json.RawMessage) (string, bool) {
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
	return mcpDispatch(ctx, opConfigSet, body)
}
