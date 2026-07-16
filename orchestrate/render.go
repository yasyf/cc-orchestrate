package orchestrate

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// jsonFlag is the persistent --json flag Root() installs for cc-orchestrate's domain
// commands. Those commands honor it through runRender; cc-interact's bundled substrate
// commands retain their own output behavior.
const jsonFlag = "json"

// jsonOutput reports whether the persistent --json flag is set. It reads the flag off
// the root command's persistent set, where cobra parses it regardless of which
// subcommand carried it on the line.
func jsonOutput(c *cobra.Command) bool {
	v, err := c.Root().PersistentFlags().GetBool(jsonFlag)
	return err == nil && v
}

// runRender is the single funnel every data-emitting command routes through: it runs
// one op and renders the reply either as the daemon's JSON body verbatim (--json) or
// through the human renderer. The verbatim path never unmarshals — a field the daemon
// adds that T does not carry must survive byte-for-byte — so only the human path
// decodes into T.
func runRender[T any](c *cobra.Command, m method, req any, human func(io.Writer, T) error) error {
	reply, err := runOp(c, m.op(), req)
	if err != nil {
		return err
	}
	return renderReply(c.OutOrStdout(), jsonOutput(c), reply.Body, human)
}

// renderReply is runRender's transport-free core: in JSON mode it writes body
// verbatim (plus a trailing newline), in human mode it decodes body into T and hands
// it to human. Split out so a test drives the verbatim-vs-decoded contract without a
// daemon.
func renderReply[T any](w io.Writer, asJSON bool, body json.RawMessage, human func(io.Writer, T) error) error {
	if asJSON {
		return writeJSONLine(w, body)
	}
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		return err
	}
	return human(w, v)
}

// writeJSONLine writes a JSON body verbatim followed by a newline, never re-marshaling
// it — the daemon-added fields that a local response struct omits survive only if the
// bytes are passed through untouched.
func writeJSONLine(w io.Writer, body json.RawMessage) error {
	if _, err := w.Write(body); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

// renderKV renders a single record as aligned "key: value" lines, the show-view
// complement to renderTable. It pads every key (with its trailing colon) to the widest
// so the values line up, matching the hand-padded layout the agent status view has
// always used.
func renderKV(w io.Writer, pairs [][2]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 1, ' ', 0)
	for _, p := range pairs {
		if _, err := fmt.Fprintf(tw, "%s:\t%s\n", p[0], p[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// backendView is the JSON shape of one `backends list` row: a backend, whether its
// runtime is installed, and whether it is the effective default. It is marshaled
// locally (not off a daemon op) since backend availability is read straight from disk.
type backendView struct {
	Name      string `json:"name"`
	Installed bool   `json:"installed"`
	Default   bool   `json:"default"`
}

// renderBackends renders the `backends list` view through the same JSON/human split as
// runRender, marshaling the local backendView slice for --json and the aligned table
// otherwise. It reads state off disk, so it needs no daemon.
func renderBackends(c *cobra.Command) error {
	rows, err := backendRows()
	if err != nil {
		return err
	}
	if jsonOutput(c) {
		views := make([]backendView, len(rows))
		for i, r := range rows {
			views[i] = backendView{Name: string(r.name), Installed: r.available, Default: r.isDefault}
		}
		b, err := json.Marshal(views)
		if err != nil {
			return err
		}
		return writeJSONLine(c.OutOrStdout(), b)
	}
	_, err = fmt.Fprint(c.OutOrStdout(), formatBackends(rows))
	return err
}

// renderFleetStatus renders cco.fleet.status for humans: a short KV summary head
// (counts, backends, the fleet subject and its resume seq, and the HTTP port) then a
// joined agent table resolving each agent's repo, workstream, and sprint names from the
// snapshot's own views.
func renderFleetStatus(w io.Writer, res fleetStatusResult) error {
	head := [][2]string{
		{"agents", strconv.Itoa(len(res.Agents))},
		{"repos", strconv.Itoa(len(res.Repos))},
		{"workstreams", strconv.Itoa(len(res.Workstreams))},
		{"sprints", strconv.Itoa(len(res.Sprints))},
		{"backend", distinctRepoBackends(res.Repos)},
		{"subject", res.FleetSubject},
		{"seq", strconv.FormatInt(res.Seq, 10)},
		{"port", strconv.Itoa(res.HTTPPort)},
	}
	if err := renderKV(w, head); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	sprints := make(map[string]sprintView, len(res.Sprints))
	for _, sp := range res.Sprints {
		sprints[sp.ID] = sp
	}
	workstreams := make(map[string]workstreamView, len(res.Workstreams))
	for _, ws := range res.Workstreams {
		workstreams[ws.ID] = ws
	}
	repos := make(map[string]repoView, len(res.Repos))
	for _, p := range res.Repos {
		repos[p.ID] = p
	}
	rows := make([][]string, len(res.Agents))
	for i, a := range res.Agents {
		repoName, wsName, spName := joinAgentNames(a, sprints, workstreams, repos)
		rows[i] = []string{agentLabel(a), repoName, wsName, spName, a.State, a.Status, strconv.Itoa(a.Tokens)}
	}
	_, err := fmt.Fprint(w, renderTable(
		[]string{"AGENT", "REPO", "WORKSTREAM", "SPRINT", "STATE", "STATUS", "TOKENS"}, rows))
	return err
}

// joinAgentNames resolves an agent's sprint, workstream, and repo names by walking the
// snapshot's own views up the hierarchy, degrading to the raw id when a link dangles so
// a mid-teardown snapshot still renders.
func joinAgentNames(a agentView, sprints map[string]sprintView, workstreams map[string]workstreamView, repos map[string]repoView) (repoName, wsName, spName string) {
	sp, ok := sprints[a.SprintID]
	if !ok {
		return "", "", a.SprintID
	}
	spName = nameOr(sp.Name, sp.ID)
	ws, ok := workstreams[sp.WorkstreamID]
	if !ok {
		return "", sp.WorkstreamID, spName
	}
	wsName = nameOr(ws.Name, ws.ID)
	if p, ok := repos[ws.RepoID]; ok {
		repoName = nameOr(p.Name, p.ID)
	} else {
		repoName = ws.RepoID
	}
	return repoName, wsName, spName
}

// agentLabel is the human handle for an agent: its name, or its id when unnamed.
func agentLabel(a agentView) string { return nameOr(a.Name, a.ID) }

func nameOr(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

// distinctRepoBackends is the sorted, comma-joined set of backends the repos run on —
// the "backend" line of the fleet status head.
func distinctRepoBackends(repos []repoView) string {
	seen := map[string]struct{}{}
	for _, p := range repos {
		if p.Backend != "" {
			seen[p.Backend] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// fleetFrameFields is the union of every fleet frame's payload fields, so formatFleetFrame
// can decode any frame off the wire and read the ones its type carries.
type fleetFrameFields struct {
	Type     string `json:"type"`
	TS       string `json:"ts"`
	AgentID  string `json:"agent_id"`
	Name     string `json:"name"`
	ID       string `json:"id"`
	State    string `json:"state"`
	Tool     string `json:"tool"`
	Target   string `json:"target"`
	Tokens   int    `json:"tokens"`
	Reason   string `json:"reason"`
	Attempt  int    `json:"attempt"`
	Attempts int    `json:"attempts"`
	Backend  string `json:"backend"`
	Path     string `json:"path"`
	Count    int    `json:"count"`
}

// formatFleetFrame renders one fleet-subject frame as a compact, timestamped, aligned
// line: the frame's ts, its type minus the "fleet." prefix, then the per-type detail.
// An unparseable payload passes through verbatim.
func formatFleetFrame(data string) string {
	var f fleetFrameFields
	if err := json.Unmarshal([]byte(data), &f); err != nil {
		return data
	}
	f.AgentID = sanitizeLine(f.AgentID)
	f.Name = sanitizeLine(f.Name)
	f.ID = sanitizeLine(f.ID)
	f.State = sanitizeLine(f.State)
	f.Tool = sanitizeLine(f.Tool)
	f.Target = sanitizeLine(f.Target)
	f.Reason = sanitizeLine(f.Reason)
	f.Backend = sanitizeLine(f.Backend)
	f.Path = sanitizeLine(f.Path)
	short := strings.TrimPrefix(f.Type, "fleet.")
	var detail string
	switch f.Type {
	case FrameAgentSpawned:
		detail = fmt.Sprintf("%s %s backend=%s", f.AgentID, f.Name, f.Backend)
	case FrameAgentStatus:
		detail = fmt.Sprintf("%s %s%s tokens=%d", f.AgentID, f.State, toolTargetSuffix(f.Tool, f.Target), f.Tokens)
	case FrameAgentMessage:
		detail = f.AgentID
	case FrameAgentReport:
		detail = strings.TrimSpace(f.AgentID + kvState(f.State))
	case FrameAgentExited:
		detail = fmt.Sprintf("%s reason=%s", f.AgentID, f.Reason)
	case FrameAgentRestarted:
		detail = fmt.Sprintf("%s attempt=%d", f.AgentID, f.Attempt)
	case FrameAgentAbandoned:
		detail = fmt.Sprintf("%s attempts=%d", f.AgentID, f.Attempts)
	case FrameSerialized, FrameRestored:
		detail = fmt.Sprintf("%s count=%d", f.Path, f.Count)
	default:
		detail = strings.TrimSpace(f.ID + " " + f.Name)
	}
	return fmt.Sprintf("%s  %-16s %s", f.TS, short, detail)
}

// agentEventFields is the union of every per-agent event payload's fields, so
// formatEventLine can decode any frame off an agent's own stream.
type agentEventFields struct {
	Type     string `json:"type"`
	State    string `json:"state"`
	Tool     string `json:"tool"`
	Target   string `json:"target"`
	Tokens   int    `json:"tokens"`
	Text     string `json:"text"`
	Backend  string `json:"backend"`
	Terminal string `json:"terminal"`
	Attempt  int    `json:"attempt"`
	Attempts int    `json:"attempts"`
}

// formatEventLine renders one per-agent event as a labeled, aligned human line, the
// agent-watch complement to formatFleetFrame. An unrecognized or unparseable payload
// passes through verbatim.
func formatEventLine(data string) string {
	var e agentEventFields
	if err := json.Unmarshal([]byte(data), &e); err != nil {
		return data
	}
	e.State = sanitizeLine(e.State)
	e.Tool = sanitizeLine(e.Tool)
	e.Target = sanitizeLine(e.Target)
	e.Text = sanitizeLine(e.Text)
	e.Backend = sanitizeLine(e.Backend)
	e.Terminal = sanitizeLine(e.Terminal)
	var label, detail string
	switch e.Type {
	case EventStatus:
		label, detail = "status", fmt.Sprintf("%s%s tokens=%d", e.State, toolTargetSuffix(e.Tool, e.Target), e.Tokens)
	case EventMessage:
		label, detail = "message", e.Text
	case EventReport:
		label, detail = "report", strings.TrimSpace(e.Text+kvState(e.State))
	case EventInbound:
		label, detail = "inbound", e.Text
	case EventExited:
		label = "exited"
	case EventSpawned:
		label, detail = "spawned", fmt.Sprintf("backend=%s terminal=%s", e.Backend, e.Terminal)
	case EventRestarted:
		label, detail = "restarted", fmt.Sprintf("terminal=%s attempt=%d", e.Terminal, e.Attempt)
	case EventAbandoned:
		label, detail = "abandoned", fmt.Sprintf("attempts=%d", e.Attempts)
	case EventRestored:
		label, detail = "restored", fmt.Sprintf("terminal=%s", e.Terminal)
	default:
		return data
	}
	if detail == "" {
		return label
	}
	return fmt.Sprintf("%-9s %s", label, detail)
}

// sanitizeLine collapses embedded line breaks in a free-text event/frame field into the
// literal two-character escape \n, so one logical event always renders as exactly one
// physical line — the invariant --all's agent-id prefix demux depends on.
func sanitizeLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\\n")
	return strings.ReplaceAll(s, "\n", "\\n")
}

// toolTargetSuffix renders a status frame's optional tool and target as a
// leading-spaced suffix, empty when the agent is not inside a tool call.
func toolTargetSuffix(tool, target string) string {
	switch {
	case tool == "":
		return ""
	case target == "":
		return " " + tool
	default:
		return " " + tool + " " + target
	}
}

// kvState renders an optional run state as a leading-spaced "state=<v>" suffix, empty
// when unset.
func kvState(state string) string {
	if state == "" {
		return ""
	}
	return " state=" + state
}
