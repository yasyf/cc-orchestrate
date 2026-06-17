package orchestrate

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/cmd"
	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// defaultSession re-defaults the substrate commands' --session flag so the
// orchestrator's own control commands resolve without passing it explicitly.
const defaultSession = AppName

// Root assembles the cc-orchestrate command tree: the cc-interact substrate
// commands plus the agent-fleet domain command groups.
func Root() *cobra.Command {
	d := deps()
	r := &cobra.Command{
		Use:           AppName,
		Short:         "Orchestrate fleets of Claude Code agents across pluggable backends",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	r.AddCommand(
		cmd.DaemonCmd(d),
		withSessionDefault(cmd.WatchCmd(d)),
		withSessionDefault(cmd.StatusCmd(d)),
		cmd.StopCmd(d),
		cmd.SessionRecordCmd(d),
		cmd.GuardEditCmd(d),
		withSessionDefault(cmd.ChannelAckCmd(d)),
		withSessionDefault(cmd.ChannelCmd(d)),
		backendsCmd(),
		projectsCmd(),
		agentCmd(),
		mcpCmd(),
	)
	return r
}

// withSessionDefault re-defaults a substrate command's --session flag to the
// orchestrate default so control commands resolve without passing --session.
func withSessionDefault(c *cobra.Command) *cobra.Command {
	if f := c.Flags().Lookup("session"); f != nil {
		_ = f.Value.Set(defaultSession)
		f.DefValue = defaultSession
	}
	return c
}

// notImplemented is the placeholder RunE for every domain command until its phase
// fills it in.
func notImplemented(c *cobra.Command, _ []string) error {
	fmt.Fprintln(c.OutOrStdout(), "not implemented yet")
	return nil
}

// backendsCmd is the `backends` group: inspect and select the placement backend.
func backendsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "backends",
		Short: "Inspect and select the agent placement backend",
	}
	c.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List backends and their availability",
			Args:  cobra.NoArgs,
			RunE: func(c *cobra.Command, _ []string) error {
				selected := selectedBackend()
				rows := []backendRow{}
				defaulted := false
				for _, name := range backend.Precedence {
					b, _ := backend.Get(name)
					available := b.Available()
					isDefault := false
					switch {
					case selected != "":
						isDefault = name == selected
					case available && !defaulted:
						isDefault = true
					}
					defaulted = defaulted || isDefault
					rows = append(rows, backendRow{name: name, available: available, isDefault: isDefault})
				}
				fmt.Fprint(c.OutOrStdout(), formatBackends(rows))
				return nil
			},
		},
		&cobra.Command{
			Use:   "select <backend>",
			Short: "Persist the selected default backend",
			Args:  cobra.ExactArgs(1),
			RunE:  runBackendsSelect,
		},
	)
	return c
}

// runBackendsSelect validates that the named backend is a known, installed
// backend, then persists it as the selected default through the config-set op.
func runBackendsSelect(c *cobra.Command, args []string) error {
	name := args[0]
	b, ok := backend.Get(name)
	if !slices.Contains(backend.Precedence, name) || !ok || !b.Available() {
		return fmt.Errorf("backend %q is not an available backend; run `%s backends list`", name, AppName)
	}
	ctx := c.Context()
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{"key": "backend", "value": name})
	reply, err := newClient().Do(ctx, daemon.Envelope{
		Op: opConfigSet, Session: AppName, ClaudePID: d.ClaudePID(), Scope: cwd, Body: body,
	})
	if err != nil {
		return err
	}
	if !reply.OK {
		return errors.New(reply.Error)
	}
	fmt.Fprintf(c.OutOrStdout(), "selected backend: %s\n", name)
	return nil
}

// selectedBackend reads the persisted default backend straight from the state db
// without spawning the daemon, returning "" when no db exists or none is selected.
func selectedBackend() string {
	dbPath := appPaths().DBPath()
	if _, err := os.Stat(dbPath); err != nil {
		return ""
	}
	db, err := sql.Open("sqlite", dbPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return ""
	}
	defer db.Close()
	var value string
	if db.QueryRow(`SELECT value FROM config WHERE key = 'backend'`).Scan(&value) != nil {
		return ""
	}
	return value
}

// backendRow is one line of `backends list`: a backend name, whether its runtime
// is installed, and whether it is the effective default (the persisted selection,
// or the first available one when none is selected).
type backendRow struct {
	name      string
	available bool
	isDefault bool
}

// formatBackends renders backend rows as an aligned text table with a trailing
// newline and no trailing whitespace on any line.
func formatBackends(rows []backendRow) string {
	var buf strings.Builder
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BACKEND\tINSTALLED\tDEFAULT")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\n", r.name, installedLabel(r.available), defaultMarker(r.isDefault))
	}
	w.Flush()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	return strings.Join(lines, "\n") + "\n"
}

func installedLabel(available bool) string {
	if available {
		return "yes"
	}
	return "no"
}

func defaultMarker(isDefault bool) string {
	if isDefault {
		return "*"
	}
	return ""
}

// projectsCmd is the `projects` group: manage backend workspaces.
func projectsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "projects",
		Short: "Manage orchestration projects (backend workspaces)",
	}

	list := &cobra.Command{
		Use:   "list",
		Short: "List projects",
		Args:  cobra.NoArgs,
		RunE:  notImplemented,
	}

	var createBackend, createCwd string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a project and its backend workspace",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented,
	}
	create.Flags().StringVar(&createBackend, "backend", "", "backend to place the project on (defaults to the selected/first available)")
	create.Flags().StringVar(&createCwd, "cwd", "", "working directory for the project (defaults to the current directory)")

	activate := &cobra.Command{
		Use:   "activate <id>",
		Short: "Mark a project active",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented,
	}

	c.AddCommand(list, create, activate)
	return c
}

// agentCmd is the `agent` group: spawn and control child Claude Code agents.
func agentCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "agent",
		Short: "Spawn and control child Claude Code agents",
	}

	var spawnProject, spawnBackend, spawnName, spawnCwd, spawnPrompt string
	spawn := &cobra.Command{
		Use:   "spawn",
		Short: "Spawn a child agent into a project",
		Args:  cobra.NoArgs,
		RunE:  notImplemented,
	}
	spawn.Flags().StringVar(&spawnProject, "project", "", "project id or name to spawn into")
	spawn.Flags().StringVar(&spawnBackend, "backend", "", "backend override (defaults to the project's backend)")
	spawn.Flags().StringVar(&spawnName, "name", "", "human-readable agent name")
	spawn.Flags().StringVar(&spawnCwd, "cwd", "", "working directory / scope (defaults to the project cwd)")
	spawn.Flags().StringVar(&spawnPrompt, "prompt", "", "initial prompt for the child agent")

	var listProject string
	list := &cobra.Command{
		Use:   "list",
		Short: "List agents",
		Args:  cobra.NoArgs,
		RunE:  notImplemented,
	}
	list.Flags().StringVar(&listProject, "project", "", "filter by project id or name")

	sendMessage := &cobra.Command{
		Use:   "send-message <id> <text>",
		Short: "Send a message to a running agent",
		Args:  cobra.ExactArgs(2),
		RunE:  notImplemented,
	}

	status := &cobra.Command{
		Use:   "status <id>",
		Short: "Show a single agent's derived status",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented,
	}

	var watchAll bool
	var watchID string
	watch := &cobra.Command{
		Use:   "watch",
		Short: "Stream agent status/report events (run under a Monitor)",
		Args:  cobra.NoArgs,
		RunE:  notImplemented,
	}
	watch.Flags().BoolVar(&watchAll, "all", false, "watch every agent")
	watch.Flags().StringVar(&watchID, "id", "", "watch a single agent by id")

	kill := &cobra.Command{
		Use:   "kill <id>",
		Short: "Kill a running agent",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented,
	}

	c.AddCommand(spawn, list, sendMessage, status, watch, kill)
	return c
}

// mcpCmd is the parent-facing MCP control server entry point.
func mcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Run the parent-facing MCP control server (stdio)",
		Args:  cobra.NoArgs,
		RunE:  notImplemented,
	}
}
