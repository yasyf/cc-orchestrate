package orchestrate

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-interact/cmd"
	"github.com/yasyf/cc-interact/consume"
	"github.com/yasyf/cc-interact/daemon"

	"github.com/yasyf/cc-orchestrate/backend"
)

// watchConsumer is the stream-consumer name `agent watch` registers under, keeping
// its resume cursor distinct from the agent's own receive Monitor.
const watchConsumer = "agent-watch"

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
		Version:       buildVersion(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	r.PersistentFlags().Bool(jsonFlag, false, "emit machine-readable JSON (cco's domain commands)")
	r.AddCommand(
		cmd.DaemonCmd(d),
		withSessionDefault(cmd.WatchCmd(d)),
		withSessionDefault(cmd.StatusCmd(d)),
		cmd.StopCmd(d),
		cmd.SessionRecordCmd(d),
		cmd.GuardEditCmd(d),
		withSessionDefault(cmd.ChannelAckCmd(d)),
		cmd.ChannelCmd(d),
		ptyHostCmd(),
		scrubExecCmd(),
		backendsCmd(),
		configCmd(),
		repoCmd(),
		workstreamCmd(),
		sprintCmd(),
		agentCmd(),
		fleetCmd(),
		serializeCmd(),
		restoreCmd(),
		mcpCmd(),
		cmd.SetupChannelsCmd(d, channelPlugin, "Channel delivery is enabled. New agent spawns will now load the cc-orchestrate channel."),
	)
	return r
}

// serializeCmd snapshots every active agent into a restorable bundle: each agent's
// identity plus its captured terminal screen.
func serializeCmd() *cobra.Command {
	var out string
	c := &cobra.Command{
		Use:   "serialize",
		Short: "Snapshot every active agent into a restorable bundle",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mFleetSerialize, map[string]string{"out": out},
				func(w io.Writer, res fleetSerializeResult) error {
					_, err := fmt.Fprintf(w, "serialized %d agent(s) to %s\n", res.Count, res.Path)
					return err
				})
		},
	}
	c.Flags().StringVar(&out, "out", "", "write the bundle to this path instead of the default serialize dir")
	return c
}

// restoreCmd recreates the agents in a bundle: it re-inserts any missing rows and
// resumes each agent's session into a fresh backend terminal.
func restoreCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "restore <bundle>",
		Short: "Restore agents from a serialized bundle",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mFleetRestore, map[string]string{"path": args[0]},
				func(w io.Writer, res fleetRestoreResult) error {
					_, err := fmt.Fprintf(w, "restored %d agent(s) from %s\n", res.Count, args[0])
					return err
				})
		},
	}
	return c
}

// configCmd is the `config` group: read, write, and list the persisted key-value
// config (the generic verbs symmetric with the specialized `backends select` writer).
func configCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "Inspect and edit the persisted orchestrator config",
	}
	get := &cobra.Command{
		Use:   "get <key>",
		Short: "Print one config key's value",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mConfigGet, map[string]string{"key": args[0]},
				func(w io.Writer, res configGetResult) error {
					if !res.Found {
						_, err := fmt.Fprintf(w, "%s: not set\n", args[0])
						return err
					}
					_, err := fmt.Fprintln(w, res.Value)
					return err
				})
		},
	}
	set := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Upsert one config key",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mConfigSet, map[string]string{"key": args[0], "value": args[1]},
				func(w io.Writer, res configSetResult) error {
					_, err := fmt.Fprintf(w, "%s: %s\n", res.Key, res.Value)
					return err
				})
		},
	}
	unset := &cobra.Command{
		Use:   "unset <key>",
		Short: "Delete one config key",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mConfigUnset, map[string]string{"key": args[0]},
				func(w io.Writer, res configUnsetResult) error {
					if !res.Found {
						_, err := fmt.Fprintf(w, "%s: not set\n", res.Key)
						return err
					}
					_, err := fmt.Fprintf(w, "unset %s\n", res.Key)
					return err
				})
		},
	}
	list := &cobra.Command{
		Use:   "list",
		Short: "List the persisted config key-value pairs",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mConfigList, nil,
				func(w io.Writer, entries []configEntry) error {
					rows := make([][]string, len(entries))
					for i, e := range entries {
						rows[i] = []string{e.Key, e.Value}
					}
					_, err := fmt.Fprint(w, renderTable([]string{"KEY", "VALUE"}, rows))
					return err
				})
		},
	}
	c.AddCommand(get, set, unset, list)
	return c
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

// runOp is the shared control-command round trip: ensure the daemon is current,
// send one domain envelope keyed to the orchestrator's session and cwd, and
// return the reply (turning a non-ok reply into an error). A nil body sends no
// domain payload.
func runOp(c *cobra.Command, op daemon.Op, body any) (daemon.Reply, error) {
	ctx := c.Context()
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return daemon.Reply{}, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return daemon.Reply{}, err
	}
	var raw json.RawMessage
	if body != nil {
		if raw, err = json.Marshal(body); err != nil {
			return daemon.Reply{}, err
		}
	}
	reply, err := newClient().Do(ctx, daemon.Envelope{
		Op: op, Session: AppName, ClaudePID: d.ClaudePID(), Scope: cwd, Body: raw,
	})
	if err != nil {
		return daemon.Reply{}, err
	}
	if !reply.OK {
		return daemon.Reply{}, errors.New(reply.Error)
	}
	return reply, nil
}

// renderTable renders a header and rows as an aligned text table with a trailing
// newline and no trailing whitespace on any line.
func renderTable(header []string, rows [][]string) string {
	var buf strings.Builder
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, strings.Join(header, "\t"))
	for _, r := range rows {
		_, _ = fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	_ = w.Flush()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	return strings.Join(lines, "\n") + "\n"
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
				return renderBackends(c)
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
	if err := backend.ValidateBackend(backend.Name(name)); err != nil {
		return fmt.Errorf("%w; run `%s backends list`", err, AppName)
	}
	return runRender(c, mConfigSet, map[string]string{"key": "backend", "value": name},
		func(w io.Writer, _ configSetResult) error {
			_, err := fmt.Fprintf(w, "selected backend: %s\n", name)
			return err
		})
}

// selectedBackend reads the persisted default backend straight from the state db
// without spawning the daemon. It returns "" when no state db exists yet or no
// backend is selected, and a wrapped error when the db cannot be opened or read —
// so a corrupt or locked db is surfaced rather than silently read as unset.
func selectedBackend() (backend.Name, error) {
	dbPath := appPaths().DBPath()
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("stat state db: %w", err)
	}
	db, err := sql.Open("sqlite", dbPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return "", fmt.Errorf("open state db: %w", err)
	}
	defer func() { _ = db.Close() }()
	var value string
	switch err := db.QueryRow(`SELECT value FROM config WHERE key = 'backend'`).Scan(&value); {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("read selected backend: %w", err)
	}
	return backend.Name(value), nil
}

// backendRow is one line of `backends list`: a backend name, whether its runtime
// is installed, and whether it is the effective default (the persisted selection,
// or the first available one when none is selected).
type backendRow struct {
	name      backend.Name
	available bool
	isDefault bool
}

// backendRows builds the `backends list` view: every backend in precedence order with
// its install status and a marker on the effective default (the persisted selection, or
// the first available one when none is selected). It reads state straight off disk, so
// it needs no daemon.
func backendRows() ([]backendRow, error) {
	selected, err := selectedBackend()
	if err != nil {
		return nil, err
	}
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
	return rows, nil
}

// backendsTable renders the `backends list` view as an aligned text table — the
// daemon-free text surface the parent-facing MCP backends_list tool returns.
func backendsTable() (string, error) {
	rows, err := backendRows()
	if err != nil {
		return "", err
	}
	return formatBackends(rows), nil
}

// formatBackends renders backend rows as an aligned text table.
func formatBackends(rows []backendRow) string {
	out := make([][]string, len(rows))
	for i, r := range rows {
		out[i] = []string{string(r.name), installedLabel(r.available), defaultMarker(r.isDefault)}
	}
	return renderTable([]string{"BACKEND", "INSTALLED", "DEFAULT"}, out)
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

// repoCmd is the `repo` group: manage backend workspaces.
func repoCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "repo",
		Short: "Manage orchestration repos (backend workspaces)",
	}

	var listStatus string
	list := &cobra.Command{
		Use:   "list",
		Short: "List repos",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mRepoList, map[string]string{"status": listStatus},
				func(w io.Writer, views []repoView) error {
					rows := make([][]string, len(views))
					for i, p := range views {
						rows[i] = []string{p.ID, p.Name, p.Backend, p.Status, p.Cwd}
					}
					_, err := fmt.Fprint(w, renderTable([]string{"ID", "NAME", "BACKEND", "STATUS", "CWD"}, rows))
					return err
				})
		},
	}
	list.Flags().StringVar(&listStatus, "status", "", "filter by lifecycle status (active, exited, killed)")

	show := &cobra.Command{
		Use:   "show <id|name>",
		Short: "Show one repo",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mRepoShow, map[string]string{"id": args[0]},
				func(w io.Writer, p repoView) error {
					return renderKV(w, [][2]string{
						{"id", p.ID},
						{"name", p.Name},
						{"backend", p.Backend},
						{"cwd", p.Cwd},
						{"status", p.Status},
						{"created", p.CreatedAt},
					})
				})
		},
	}

	var createBackend, createCwd string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a repo and its backend workspace",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mRepoCreate, map[string]string{
				"name": args[0], "backend": createBackend, "cwd": createCwd,
			}, func(w io.Writer, out repoCreateResult) error {
				_, _ = fmt.Fprintf(w, "repo:      %s\n", out.RepoID)
				_, _ = fmt.Fprintf(w, "backend:   %s\n", out.Backend)
				_, err := fmt.Fprintf(w, "workspace: %s\n", out.Workspace)
				return err
			})
		},
	}
	create.Flags().StringVar(&createBackend, "backend", "", "backend to place the repo on (defaults to the selected/first available)")
	create.Flags().StringVar(&createCwd, "cwd", "", "working directory for the repo (defaults to the current directory)")

	activate := &cobra.Command{
		Use:   "activate <id>",
		Short: "Mark a repo active",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mRepoActivate, map[string]string{"id": args[0]},
				func(w io.Writer, out repoLifecycleResult) error {
					_, err := fmt.Fprintf(w, "activated repo: %s\n", out.RepoID)
					return err
				})
		},
	}

	kill := &cobra.Command{
		Use:   "kill <id>",
		Short: "Kill a repo, its backend workspace, and all its agents",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mRepoKill, map[string]string{"id": args[0]},
				func(w io.Writer, _ repoLifecycleResult) error {
					_, err := fmt.Fprintf(w, "killed repo: %s\n", args[0])
					return err
				})
		},
	}

	c.AddCommand(list, show, create, activate, kill)
	return c
}

// workstreamCmd is the `workstream` group (alias `ws`): manage a repo's branches
// and the backend workspaces that back them.
func workstreamCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "workstream",
		Aliases: []string{"ws"},
		Short:   "Manage workstreams (a repo's branches and their backend workspaces)",
	}

	var listRepo, listStatus string
	list := &cobra.Command{
		Use:   "list",
		Short: "List workstreams",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mWorkstreamList, map[string]string{"repo": listRepo, "status": listStatus},
				func(w io.Writer, views []workstreamView) error {
					rows := make([][]string, len(views))
					for i, ws := range views {
						primary := ""
						if ws.IsPrimary {
							primary = "yes"
						}
						rows[i] = []string{ws.ID, ws.Name, ws.RepoID, ws.Branch, ws.Worktree, primary, ws.Status}
					}
					_, err := fmt.Fprint(w, renderTable(
						[]string{"ID", "NAME", "REPO", "BRANCH", "WORKTREE", "PRIMARY", "STATUS"}, rows))
					return err
				})
		},
	}
	list.Flags().StringVar(&listRepo, "repo", "", "filter by repo id or name")
	list.Flags().StringVar(&listStatus, "status", "", "filter by lifecycle status (active, exited, killed)")

	var showRepo string
	show := &cobra.Command{
		Use:   "show <id|name>",
		Short: "Show one workstream",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mWorkstreamShow, map[string]string{"id": args[0], "repo": showRepo},
				func(w io.Writer, ws workstreamView) error {
					return renderKV(w, [][2]string{
						{"id", ws.ID},
						{"repo", ws.RepoID},
						{"name", ws.Name},
						{"backend", ws.Backend},
						{"workspace", ws.WorkspaceHandle},
						{"branch", ws.Branch},
						{"worktree", ws.Worktree},
						{"primary", strconv.FormatBool(ws.IsPrimary)},
						{"ccnotes", ws.CCNotesProject},
						{"status", ws.Status},
						{"created", ws.CreatedAt},
					})
				})
		},
	}
	show.Flags().StringVar(&showRepo, "repo", "", "repo id or name to disambiguate the workstream name")

	var createRepo, createBranch string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a workstream and its backend workspace",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mWorkstreamCreate, map[string]string{
				"repo": createRepo, "name": args[0], "branch": createBranch,
			}, func(w io.Writer, out workstreamCreateResult) error {
				_, _ = fmt.Fprintf(w, "workstream: %s\n", out.WorkstreamID)
				_, _ = fmt.Fprintf(w, "repo:       %s\n", out.RepoID)
				_, _ = fmt.Fprintf(w, "branch:     %s\n", out.Branch)
				_, _ = fmt.Fprintf(w, "worktree:   %s\n", out.Worktree)
				_, err := fmt.Fprintf(w, "workspace:  %s\n", out.Workspace)
				return err
			})
		},
	}
	create.Flags().StringVar(&createRepo, "repo", "", "repo id or name to create the workstream in")
	create.Flags().StringVar(&createBranch, "branch", "", "git branch for the worktree (defaults to the name)")

	var activateRepo string
	activate := &cobra.Command{
		Use:   "activate <id|name>",
		Short: "Mark a workstream active and the spawn default",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mWorkstreamActivate, map[string]string{"id": args[0], "repo": activateRepo},
				func(w io.Writer, out workstreamLifecycleResult) error {
					_, err := fmt.Fprintf(w, "activated workstream: %s\n", out.WorkstreamID)
					return err
				})
		},
	}
	activate.Flags().StringVar(&activateRepo, "repo", "", "repo id or name to disambiguate the workstream name")

	var killRepo string
	kill := &cobra.Command{
		Use:   "kill <id|name>",
		Short: "Kill a workstream, its backend workspace, worktree, and agents",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mWorkstreamKill, map[string]string{"id": args[0], "repo": killRepo},
				func(w io.Writer, _ workstreamLifecycleResult) error {
					_, err := fmt.Fprintf(w, "killed workstream: %s\n", args[0])
					return err
				})
		},
	}
	kill.Flags().StringVar(&killRepo, "repo", "", "repo id or name to disambiguate the workstream name")

	c.AddCommand(list, show, create, activate, kill)
	return c
}

// sprintCmd is the `sprint` group: manage a workstream's planning groups — the unit
// an agent spawns into. A sprint shares its workstream's worktree.
func sprintCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "sprint",
		Short: "Manage sprints (a workstream's planning groups that agents spawn into)",
	}

	var listWorkstream, listStatus string
	list := &cobra.Command{
		Use:   "list",
		Short: "List sprints",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mSprintList, map[string]string{"workstream": listWorkstream, "status": listStatus},
				func(w io.Writer, views []sprintView) error {
					rows := make([][]string, len(views))
					for i, s := range views {
						rows[i] = []string{s.ID, s.Name, s.WorkstreamID, s.Status}
					}
					_, err := fmt.Fprint(w, renderTable(
						[]string{"ID", "NAME", "WORKSTREAM", "STATUS"}, rows))
					return err
				})
		},
	}
	list.Flags().StringVar(&listWorkstream, "workstream", "", "filter by workstream id or name")
	list.Flags().StringVar(&listStatus, "status", "", "filter by lifecycle status (active, exited, killed)")

	var showWorkstream string
	show := &cobra.Command{
		Use:   "show <id|name>",
		Short: "Show one sprint",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mSprintShow, map[string]string{"id": args[0], "workstream": showWorkstream},
				func(w io.Writer, s sprintView) error {
					return renderKV(w, [][2]string{
						{"id", s.ID},
						{"workstream", s.WorkstreamID},
						{"name", s.Name},
						{"ccnotes", s.CCNotesSprint},
						{"status", s.Status},
						{"created", s.CreatedAt},
					})
				})
		},
	}
	show.Flags().StringVar(&showWorkstream, "workstream", "", "workstream id or name to disambiguate the sprint name")

	var createWorkstream string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a sprint in a workstream",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mSprintCreate, map[string]string{
				"workstream": createWorkstream, "name": args[0],
			}, func(w io.Writer, out sprintCreateResult) error {
				_, _ = fmt.Fprintf(w, "sprint:     %s\n", out.SprintID)
				_, _ = fmt.Fprintf(w, "workstream: %s\n", out.WorkstreamID)
				_, err := fmt.Fprintf(w, "name:       %s\n", out.Name)
				return err
			})
		},
	}
	create.Flags().StringVar(&createWorkstream, "workstream", "", "workstream id or name to create the sprint in")

	var activateWorkstream string
	activate := &cobra.Command{
		Use:   "activate <id|name>",
		Short: "Mark a sprint active and the spawn default",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mSprintActivate, map[string]string{"id": args[0], "workstream": activateWorkstream},
				func(w io.Writer, out sprintActivateResult) error {
					_, err := fmt.Fprintf(w, "activated sprint: %s\n", out.SprintID)
					return err
				})
		},
	}
	activate.Flags().StringVar(&activateWorkstream, "workstream", "", "workstream id or name to disambiguate the sprint name")

	var killWorkstream string
	kill := &cobra.Command{
		Use:   "kill <id|name>",
		Short: "Kill a sprint: exit its agents and tear down their terminals",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mSprintKill, map[string]string{"id": args[0], "workstream": killWorkstream},
				func(w io.Writer, _ sprintKillResult) error {
					_, err := fmt.Fprintf(w, "killed sprint: %s\n", args[0])
					return err
				})
		},
	}
	kill.Flags().StringVar(&killWorkstream, "workstream", "", "workstream id or name to disambiguate the sprint name")

	c.AddCommand(list, show, create, activate, kill)
	return c
}

// agentCmd is the `agent` group: spawn and control child Claude Code agents.
func agentCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "agent",
		Short: "Spawn and control child Claude Code agents",
	}

	var spawnRepo, spawnWorkstream, spawnSprint, spawnName, spawnCwd, spawnPrompt string
	spawn := &cobra.Command{
		Use:   "spawn",
		Short: "Spawn a child agent into a sprint",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mAgentSpawn, map[string]string{
				"repo": spawnRepo, "workstream": spawnWorkstream, "sprint": spawnSprint,
				"name": spawnName, "cwd": spawnCwd, "prompt": spawnPrompt,
			}, func(w io.Writer, out agentSpawnResult) error {
				_, _ = fmt.Fprintf(w, "agent:    %s\n", out.ID)
				_, _ = fmt.Fprintf(w, "backend:  %s\n", out.Backend)
				_, err := fmt.Fprintf(w, "terminal: %s\n", out.Terminal)
				return err
			})
		},
	}
	spawn.Flags().StringVar(&spawnRepo, "repo", "", "repo id or name to spawn into (uses its primary workstream's default sprint)")
	spawn.Flags().StringVar(&spawnWorkstream, "workstream", "", "workstream id or name to spawn into (uses its default sprint)")
	spawn.Flags().StringVar(&spawnSprint, "sprint", "", "sprint id or name to spawn into")
	spawn.Flags().StringVar(&spawnName, "name", "", "human-readable agent name")
	spawn.Flags().StringVar(&spawnCwd, "cwd", "", "working directory / scope (defaults to the workstream worktree)")
	spawn.Flags().StringVar(&spawnPrompt, "prompt", "", "initial prompt for the child agent")

	var listRepo, listStatus string
	list := &cobra.Command{
		Use:   "list",
		Short: "List agents and their sprint and status",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runRender(c, mAgentList, map[string]string{"repo": listRepo, "status": listStatus},
				func(w io.Writer, views []agentView) error {
					rows := make([][]string, len(views))
					for i, a := range views {
						rows[i] = []string{a.ID, a.Name, a.SprintID, a.Backend, a.State, a.Status, strconv.Itoa(a.Tokens), strconv.Itoa(a.RestartCount), a.Activity}
					}
					_, err := fmt.Fprint(w, renderTable(
						[]string{"ID", "NAME", "SPRINT", "BACKEND", "STATE", "STATUS", "TOKENS", "RESTARTS", "ACTIVITY"}, rows))
					return err
				})
		},
	}
	list.Flags().StringVar(&listRepo, "repo", "", "filter by repo id or name")
	list.Flags().StringVar(&listStatus, "status", "", "filter by lifecycle status (active, exited, killed)")

	sendMessage := &cobra.Command{
		Use:   "send-message <id> <text>",
		Short: "Send a message to a running agent",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mAgentSendMessage, map[string]string{"agent_id": args[0], "text": args[1]},
				func(w io.Writer, out agentSendMessageResult) error {
					_, err := fmt.Fprintf(w, "sent to %s (seq %d)\n", args[0], out.Seq)
					return err
				})
		},
	}

	show := &cobra.Command{
		Use:     "show <id>",
		Aliases: []string{"status"},
		Short:   "Show a single agent's derived status",
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mAgentShow, map[string]string{"agent_id": args[0]},
				func(w io.Writer, a agentView) error {
					return renderKV(w, [][2]string{
						{"agent", a.ID},
						{"name", a.Name},
						{"status", a.Status},
						{"state", a.State},
						{"activity", a.Activity},
						{"tokens", strconv.Itoa(a.Tokens)},
						{"restart", strconv.Itoa(a.RestartCount)},
						{"updated", a.UpdatedAt},
					})
				})
		},
	}

	capture := &cobra.Command{
		Use:   "capture <id>",
		Short: "Capture an active agent's current terminal screen",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mAgentCapture, map[string]string{"agent_id": args[0]},
				func(w io.Writer, res agentCaptureResult) error {
					_, err := fmt.Fprint(w, res.Content)
					return err
				})
		},
	}

	var respawnDead bool
	respawn := &cobra.Command{
		Use:   "respawn [<id>]",
		Short: "Respawn one exited agent, or every eligible exited agent with --dead",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if (len(args) == 1) == respawnDead {
				return errors.New("pass exactly one of <id> or --dead")
			}
			req := map[string]any{}
			if respawnDead {
				req["dead"] = true
			} else {
				req["agent_id"] = args[0]
			}
			return runRender(c, mAgentRespawn, req, func(w io.Writer, res agentRespawnResult) error {
				rows := make([][]string, len(res.Respawned))
				for i, a := range res.Respawned {
					rows[i] = []string{a.ID, a.Name, a.SprintID, a.Backend, a.State, a.Status}
				}
				if _, err := fmt.Fprint(w, renderTable(
					[]string{"ID", "NAME", "SPRINT", "BACKEND", "STATE", "STATUS"}, rows)); err != nil {
					return err
				}
				for _, f := range res.Failed {
					_, _ = fmt.Fprintf(c.ErrOrStderr(), "failed to respawn %s: %s\n", f.ID, f.Error)
				}
				return nil
			})
		},
	}
	respawn.Flags().BoolVar(&respawnDead, "dead", false, "respawn every eligible exited agent")

	var watchAll bool
	var watchID string
	watch := &cobra.Command{
		Use:   "watch",
		Short: "Stream agent status/report events (run under a Monitor)",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runAgentWatch(c, watchID, watchAll)
		},
	}
	watch.Flags().BoolVar(&watchAll, "all", false, "watch every agent")
	watch.Flags().StringVar(&watchID, "id", "", "watch a single agent by id")

	kill := &cobra.Command{
		Use:   "kill <id>",
		Short: "Kill a running agent",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runRender(c, mAgentKill, map[string]string{"agent_id": args[0]},
				func(w io.Writer, _ agentKillResult) error {
					_, err := fmt.Fprintf(w, "killed agent: %s\n", args[0])
					return err
				})
		},
	}

	attach := &cobra.Command{
		Use:   "attach <id>",
		Short: "Attach this terminal to a running agent's backend session",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runAgentAttach(c, args[0])
		},
	}

	c.AddCommand(spawn, list, sendMessage, show, capture, respawn, watch, kill, attach)
	return c
}

// runAgentWatch streams agent events as line-delimited JSON under the parent's
// Monitor: a single agent (--id) verbatim, or every active agent (--all) with each
// line tagged by its agent id so the parent can demultiplex. Exactly one of --id
// and --all is required.
func runAgentWatch(c *cobra.Command, id string, all bool) error {
	if (id != "") == all {
		return errors.New("pass exactly one of --id or --all")
	}
	ctx := c.Context()
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return err
	}
	if all {
		return watchAllAgents(c, d)
	}
	reply, err := runOp(c, mAgentShow.op(), map[string]string{"agent_id": id})
	if err != nil {
		return err
	}
	var a agentView
	if err := json.Unmarshal(reply.Body, &a); err != nil {
		return err
	}
	out := c.OutOrStdout()
	asJSON := jsonOutput(c)
	return streamAgent(ctx, d, a, func(data string) error {
		line := data
		if !asJSON {
			line = formatEventLine(data)
		}
		_, err := fmt.Fprintln(out, line)
		return err
	})
}

// watchAllAgents streams every active agent's events concurrently, one goroutine
// per subject, tagging each emitted line with its source agent id through a single
// mutex-guarded writer so concurrent streams never interleave a line. The first
// real stream error cancels the rest; a clean terminal/ctx end returns nil.
func watchAllAgents(c *cobra.Command, d cmd.Deps) error {
	reply, err := runOp(c, mAgentList.op(), map[string]string{"repo": ""})
	if err != nil {
		return err
	}
	var views []agentView
	if err := json.Unmarshal(reply.Body, &views); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	out := c.OutOrStdout()
	asJSON := jsonOutput(c)
	var writeMu, errMu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for _, a := range views {
		if a.Status != string(StatusActive) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := streamAgent(ctx, d, a, func(data string) error {
				if asJSON {
					return emitTagged(out, &writeMu, a.ID, data)
				}
				return emitLine(out, &writeMu, a.ID+"  "+formatEventLine(data))
			})
			if err != nil {
				errMu.Lock()
				firstErr = cmp.Or(firstErr, err)
				errMu.Unlock()
				cancel()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// streamAgent resolves an agent's subject and the daemon HTTP port, then streams
// its event log to emit one event at a time, stopping on the agent's terminal exit
// event or when ctx is cancelled.
func streamAgent(ctx context.Context, d cmd.Deps, a agentView, emit func(string) error) error {
	client := newClient()
	pid := d.ClaudePID()
	subjectID, port, err := resolveAgentSubject(ctx, client, a.SessionID, a.Scope, pid)
	if err != nil {
		return err
	}
	// No ExcludeOrigin: the parent watch is an observer and must see every origin, including agent orchestrate.report events.
	src := consume.StreamSource{
		Port: port, SubjectID: subjectID, Consumer: watchConsumer, ClaudePID: pid,
		Paths: d.Paths, WindowAlive: d.WindowAlive,
		Refresh: refreshAgentPort(client, a.SessionID, a.Scope, pid),
	}
	return consume.ConsumeEvents(ctx, src, func(_ int64, data string) (bool, error) {
		if err := emit(data); err != nil {
			return false, err
		}
		return d.TerminalEvent(eventType(data)), nil
	})
}

// taggedEvent wraps one agent's raw event with its agent id so `agent watch --all`
// emits one self-describing JSON object per line.
type taggedEvent struct {
	AgentID string          `json:"agent_id"`
	Event   json.RawMessage `json:"event"`
}

// emitTagged writes one agent's raw event wrapped with its id as a single JSON
// line, serialized through mu so concurrent streams never interleave.
func emitTagged(out io.Writer, mu *sync.Mutex, id, data string) error {
	line, err := json.Marshal(taggedEvent{AgentID: id, Event: json.RawMessage(data)})
	if err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	_, err = fmt.Fprintln(out, string(line))
	return err
}

// emitLine writes one already-formatted line, serialized through mu so concurrent
// agent streams never interleave — the human-mode counterpart to emitTagged.
func emitLine(out io.Writer, mu *sync.Mutex, line string) error {
	mu.Lock()
	defer mu.Unlock()
	_, err := fmt.Fprintln(out, line)
	return err
}

// resolveAgentSubject polls the daemon until a subject exists for the agent's
// session+scope, returning its id and the daemon's HTTP handshake port. It
// replicates cc-interact's unexported cmd.resolveSubject — unreachable across the
// package boundary — differing only in the orchestrate consumer name.
func resolveAgentSubject(ctx context.Context, client *daemon.Client, session, scope string, pid int) (string, int, error) {
	for {
		reply, err := client.Do(ctx, daemon.Envelope{
			Op: daemon.OpResolve, Session: session, ClaudePID: pid, Scope: scope, Consumer: watchConsumer,
		})
		if err != nil {
			return "", 0, err
		}
		if reply.SubjectID != "" {
			return reply.SubjectID, reply.HTTPPort, nil
		}
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// refreshAgentPort re-resolves the daemon's current HTTP port for a streaming
// agent, so a version-skew daemon swap doesn't strand the SSE consumer. It
// replicates cc-interact's unexported cmd.refreshHandshake.
func refreshAgentPort(client *daemon.Client, session, scope string, pid int) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		reply, err := client.Do(ctx, daemon.Envelope{
			Op: daemon.OpResolve, Session: session, ClaudePID: pid, Scope: scope, Consumer: watchConsumer,
		})
		if err != nil {
			return 0, err
		}
		return reply.HTTPPort, nil
	}
}

// eventType extracts an event's "type" field from its raw JSON, feeding the
// terminal-event check cc-interact's watch performs.
func eventType(data string) string {
	var e struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal([]byte(data), &e)
	return e.Type
}

// The fleet-stream consumer names `fleet watch` and `fleet status --watch` register
// under, keeping their resume cursors distinct from each other and from agent watch.
const (
	fleetWatchConsumer  = "fleet-watch"
	fleetStatusConsumer = "fleet-status"
)

// fleetRepaintThrottle bounds fleet-status repaints to one per fixed window: the first
// poke starts the window, and further pokes within it coalesce into the same repaint.
const fleetRepaintThrottle = 250 * time.Millisecond

// fleetCmd is the `fleet` group: the fleet-wide status snapshot and the live event
// stream. The root `watch`/`status` verbs belong to the cc-interact substrate, so the
// fleet views live under their own noun.
func fleetCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "fleet",
		Short: "Inspect and stream the whole agent fleet",
	}

	var watch bool
	status := &cobra.Command{
		Use:   "status",
		Short: "Show the fleet: counts, the resume cursor, and a joined agent table",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			if watch {
				return runFleetStatusWatch(c)
			}
			return runRender(c, mFleetStatus, nil, renderFleetStatus)
		},
	}
	status.Flags().BoolVar(&watch, "watch", false, "repaint the status as fleet events arrive")

	watchCmd := &cobra.Command{
		Use:   "watch",
		Short: "Stream the fleet event subject live (one line per frame)",
		Args:  cobra.NoArgs,
		RunE:  runFleetWatch,
	}

	c.AddCommand(status, watchCmd)
	return c
}

// runFleetWatch streams the fleet subject live: each frame renders as one line — the
// raw NDJSON payload under --json (the canonical wire shape cc-pane also sees), a
// compact formatted line otherwise. It bootstraps the subject id and HTTP port from
// cco.fleet.status, never from OpResolve, so it attaches to the exact fleet subject the
// catalog publishes.
func runFleetWatch(c *cobra.Command, _ []string) error {
	ctx := c.Context()
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return err
	}
	subjectID, port, err := fleetStreamTarget(c)
	if err != nil {
		return err
	}
	out := c.OutOrStdout()
	asJSON := jsonOutput(c)
	return consumeFleet(ctx, d, subjectID, port, fleetWatchConsumer, func(data string) error {
		line := data
		if !asJSON {
			line = formatFleetFrame(data)
		}
		_, err := fmt.Fprintln(out, line)
		return err
	})
}

// runFleetStatusWatch paints the fleet status once, then repaints it on fleet-frame
// bursts, throttled so each fixed window produces at most one redraw. The consume loop only pokes a
// buffered channel; the repaint loop re-fetches the whole snapshot (the source of
// truth), so a dropped or replayed frame never diverges the view.
func runFleetStatusWatch(c *cobra.Command) error {
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	d := deps()
	if err := d.EnsureCurrent(ctx); err != nil {
		return err
	}
	if err := paintFleetStatus(c); err != nil {
		return err
	}
	subjectID, port, err := fleetStreamTarget(c)
	if err != nil {
		return err
	}
	return watchFleetStatus(c.Context(), ctx, cancel, d, subjectID, port, func() error {
		return paintFleetStatus(c)
	})
}

// watchFleetStatus runs the paint-on-poke loop against an already-resolved fleet stream
// target. It returns the stream error when consumption ends on its own, while a genuine
// cancellation of the parent context remains a clean exit.
func watchFleetStatus(parent, ctx context.Context, cancel context.CancelFunc, d cmd.Deps, subjectID string, port int, paint func() error) error {
	poke := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumeFleet(ctx, d, subjectID, port, fleetStatusConsumer, func(string) error {
			select {
			case poke <- struct{}{}:
			default:
			}
			return nil
		})
		cancel()
	}()
	for {
		select {
		case <-ctx.Done():
			if parent.Err() != nil {
				return nil
			}
			return <-errCh
		case <-poke:
			if !throttleRepaint(ctx, poke) {
				return nil
			}
			if err := paint(); err != nil {
				return err
			}
		}
	}
}

// throttleRepaint waits out one fleetRepaintThrottle window from the first poke. Further
// pokes within the fixed window coalesce into its single repaint without resetting the
// timer. It returns false when ctx is cancelled mid-wait.
func throttleRepaint(ctx context.Context, poke <-chan struct{}) bool {
	timer := time.NewTimer(fleetRepaintThrottle)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-poke:
		case <-timer.C:
			return true
		}
	}
}

// paintFleetStatus fetches the current fleet status and renders it: an NDJSON snapshot
// under --json, or a screen-clearing human repaint otherwise.
func paintFleetStatus(c *cobra.Command) error {
	reply, err := runOp(c, mFleetStatus.op(), nil)
	if err != nil {
		return err
	}
	out := c.OutOrStdout()
	if jsonOutput(c) {
		return writeJSONLine(out, reply.Body)
	}
	var res fleetStatusResult
	if err := json.Unmarshal(reply.Body, &res); err != nil {
		return err
	}
	writeFleetStatusHead(out, isTTY(os.Stdout))
	return renderFleetStatus(out, res)
}

// isTTY reports whether f is a real terminal device (a character device), the gate on
// whether repaint-clearing ANSI codes are safe to emit.
func isTTY(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func writeFleetStatusHead(w io.Writer, tty bool) {
	if tty {
		_, _ = io.WriteString(w, "\033[H\033[2J")
		return
	}
	_, _ = io.WriteString(w, "\n")
}

// fleetStreamTarget reads the fleet subject id and the daemon's HTTP port off one
// cco.fleet.status call — the atomic bootstrap a stream attaches with.
func fleetStreamTarget(c *cobra.Command) (subjectID string, port int, err error) {
	reply, err := runOp(c, mFleetStatus.op(), nil)
	if err != nil {
		return "", 0, err
	}
	var res fleetStatusResult
	if err := json.Unmarshal(reply.Body, &res); err != nil {
		return "", 0, err
	}
	if res.FleetSubject == "" {
		return "", 0, errors.New("fleet subject not yet bootstrapped")
	}
	return res.FleetSubject, res.HTTPPort, nil
}

// consumeFleet streams the fleet subject's events to emit, one at a time, until ctx is
// cancelled. It reuses cc-interact's consumer machinery — the same path agent watch
// drives — refreshing the port through cco.fleet.status so a daemon swap never strands
// the stream.
func consumeFleet(ctx context.Context, d cmd.Deps, subjectID string, port int, consumer string, emit func(string) error) error {
	pid := d.ClaudePID()
	src := consume.StreamSource{
		Port: port, SubjectID: subjectID, Consumer: consumer, ClaudePID: pid,
		Paths: d.Paths, WindowAlive: d.WindowAlive,
		Refresh: refreshFleetPort(newClient(), pid),
	}
	return consume.ConsumeEvents(ctx, src, func(_ int64, data string) (bool, error) {
		if err := emit(data); err != nil {
			return false, err
		}
		return false, nil
	})
}

// refreshFleetPort re-reads the daemon's current HTTP port through cco.fleet.status, so
// a version-skew daemon swap doesn't strand the fleet SSE consumer. It avoids OpResolve
// (which would mint a subject) by reusing the status op that returns the live port.
func refreshFleetPort(client *daemon.Client, pid int) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		reply, err := client.Do(ctx, daemon.Envelope{
			Op: mFleetStatus.op(), Session: AppName, ClaudePID: pid, Scope: fleetScope,
		})
		if err != nil {
			return 0, err
		}
		if !reply.OK {
			return 0, errors.New(reply.Error)
		}
		var res fleetStatusResult
		if err := json.Unmarshal(reply.Body, &res); err != nil {
			return 0, err
		}
		return res.HTTPPort, nil
	}
}

// mcpCmd is the parent-facing MCP control server entry point: a stdio JSON-RPC
// server exposing the orchestration ops as MCP tools to the parent claude.
func mcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Run the parent-facing MCP control server (stdio)",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return runMCP(c.Context(), c.InOrStdin(), c.OutOrStdout())
		},
	}
}
