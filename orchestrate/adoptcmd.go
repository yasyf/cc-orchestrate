package orchestrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// adoptPollInterval is the cadence `cco adopt`'s wait loop polls cco.adopt.list at
// while the chosen candidate is not yet ready to adopt.
var adoptPollInterval = 2 * time.Second

// adoptOptions bundles `cco adopt`'s flags for the RunE closure in cli.go.
type adoptOptions struct {
	cwd      string
	latest   bool
	name     string
	relocate bool
	pid      int
	timeout  time.Duration
}

// runAdopt is `cco adopt`'s RunE body. With neither a positional session-id nor
// --latest it lists adoptable candidates under --cwd; otherwise it resolves the
// target (a unique session-id prefix, or the newest candidate under --latest), waits
// for it to become ready, and adopts it — resuming the wait on a race-lost readiness
// re-check (a Conflict from cco.agent.adopt) rather than failing outright.
func runAdopt(c *cobra.Command, args []string, opt adoptOptions) error {
	if len(args) == 1 && opt.latest {
		return errors.New("pass exactly one of a session-id or --latest")
	}
	d := deps()
	if err := d.EnsureCurrent(c.Context()); err != nil {
		return err
	}
	cwd, err := filepath.Abs(opt.cwd)
	if err != nil {
		return fmt.Errorf("resolve --cwd %q: %w", opt.cwd, err)
	}

	if len(args) == 0 && !opt.latest {
		return runRender(c, mAdoptList, map[string]string{"cwd": cwd},
			func(w io.Writer, candidates []adoptCandidateView) error {
				return renderAdoptList(w, cwd, candidates)
			})
	}

	candidates, err := listAdoptCandidates(c.Context(), cwd)
	if err != nil {
		return err
	}
	var sessionID string
	if opt.latest {
		if len(candidates) == 0 {
			return fmt.Errorf("no adoptable sessions under %s", cwd)
		}
		sessionID = candidates[0].SessionID
	} else {
		sessionID, err = resolveAdoptPrefix(candidates, args[0])
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(c.Context(), opt.timeout)
	defer cancel()
	req := map[string]any{
		"session_id": sessionID, "cwd": cwd, "name": opt.name, "relocate": opt.relocate, "pid": opt.pid,
	}
	for {
		if err := waitAdoptReady(ctx, c, cwd, sessionID); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("timed out after %s waiting for session %s to become ready", opt.timeout, sessionID)
			}
			return err
		}
		err := runRender(c, mAgentAdopt, req, renderAdoptResult)
		if err == nil || !isAdoptNotReady(err) {
			return err
		}
	}
}

// listAdoptCandidates runs cco.adopt.list under cwd on ctx and decodes the raw candidate
// view slice — the shape prefix resolution, --latest selection, and the wait loop all
// need, distinct from runRender's json/human split that only the no-target list view
// uses. Taking ctx lets the wait loop's list RPCs ride the command's --timeout deadline.
func listAdoptCandidates(ctx context.Context, cwd string) ([]adoptCandidateView, error) {
	reply, err := runOpCtx(ctx, mAdoptList.op(), map[string]string{"cwd": cwd})
	if err != nil {
		return nil, err
	}
	var candidates []adoptCandidateView
	if err := json.Unmarshal(reply.Body, &candidates); err != nil {
		return nil, err
	}
	return candidates, nil
}

// resolveAdoptPrefix resolves a user-supplied session-id prefix to the one candidate
// it uniquely identifies. Zero matches and multiple matches both error, the latter
// listing every match so the caller can supply a longer prefix.
func resolveAdoptPrefix(candidates []adoptCandidateView, prefix string) (string, error) {
	var matches []string
	for _, cand := range candidates {
		if strings.HasPrefix(cand.SessionID, prefix) {
			matches = append(matches, cand.SessionID)
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no adoptable session matches %q", prefix)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("session id %q is ambiguous; matches %s", prefix, strings.Join(matches, ", "))
	}
}

// findAdoptCandidate looks up a session id within a freshly-listed candidate slice.
func findAdoptCandidate(candidates []adoptCandidateView, sessionID string) (adoptCandidateView, bool) {
	for _, cand := range candidates {
		if cand.SessionID == sessionID {
			return cand, true
		}
	}
	return adoptCandidateView{}, false
}

// waitAdoptReady polls cco.adopt.list every adoptPollInterval until the chosen
// candidate reports ready, printing one status line per readiness-reason change to
// stderr. It errors if the candidate drops out of a later list (adopted or killed
// from elsewhere, or the transcript moved) or if ctx is done — a --timeout deadline,
// or a cancellation — both of which leave nothing mutated since no adopt call has
// been made yet.
func waitAdoptReady(ctx context.Context, c *cobra.Command, cwd, sessionID string) error {
	lastReason := ""
	for {
		// Check the deadline first, so a candidate that is already ready still honors an
		// expired --timeout rather than racing past it into the adopt call.
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates, err := listAdoptCandidates(ctx, cwd)
		if err != nil {
			return err
		}
		cand, ok := findAdoptCandidate(candidates, sessionID)
		if !ok {
			return fmt.Errorf("session %s is no longer adoptable under %s", sessionID, cwd)
		}
		if cand.Ready {
			return nil
		}
		if cand.Reason != lastReason {
			_, _ = fmt.Fprintf(c.ErrOrStderr(), "waiting for turn end: %s\n", cand.Reason)
			lastReason = cand.Reason
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(adoptPollInterval):
		}
	}
}

// isAdoptNotReady reports whether err is the "NotReady: " opError text runOp reconstructs
// from a failed daemon reply — the not-quiescent-after-all race cco.agent.adopt's own
// re-verification can raise between the CLI's last readiness check and the call, and the
// only refusal the wait loop retries. Every other code (Conflict for an already-managed
// session, InvalidRequest, …) is fatal.
func isAdoptNotReady(err error) bool {
	return strings.HasPrefix(err.Error(), "NotReady: ")
}

// renderAdoptResult renders `cco adopt`'s human-mode success view: the adopted
// agent's id, backend, terminal, and hierarchy — mirroring how `agent spawn` renders
// its result — with any non-fatal warning surfaced prominently.
func renderAdoptResult(w io.Writer, res adoptResult) error {
	if err := renderKV(w, [][2]string{
		{"agent", res.AgentID},
		{"backend", res.Backend},
		{"terminal", res.Terminal},
		{"repo", res.RepoID},
		{"workstream", res.WorkstreamID},
	}); err != nil {
		return err
	}
	if res.Warning == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "\nWARNING: %s\n", res.Warning)
	return err
}

// renderAdoptList renders `cco adopt`'s no-target view: a table of adoptable
// candidates under cwd, or a clear message when there are none.
func renderAdoptList(w io.Writer, cwd string, candidates []adoptCandidateView) error {
	if len(candidates) == 0 {
		_, err := fmt.Fprintf(w, "no adoptable sessions under %s\n", cwd)
		return err
	}
	now := time.Now()
	rows := make([][]string, len(candidates))
	for i, cand := range candidates {
		age, err := adoptAge(cand.MTime, now)
		if err != nil {
			return err
		}
		rows[i] = []string{
			adoptShortID(cand.SessionID), age, yesNo(cand.Live), cand.State,
			adoptReadyCell(cand.Ready, cand.Reason), cand.GitBranch, cand.FirstPrompt,
		}
	}
	_, err := fmt.Fprint(w, renderTable([]string{"SESSION", "AGE", "LIVE", "STATE", "READY", "BRANCH", "PROMPT"}, rows))
	return err
}

// adoptShortID renders a session id's first 8 characters, the SESSION column's short
// form in `cco adopt`'s candidate table.
func adoptShortID(sid string) string {
	if len(sid) <= 8 {
		return sid
	}
	return sid[:8]
}

// adoptAge renders a candidate's mtime as a short relative age ("3m", "2h") against
// now, the AGE column of `cco adopt`'s candidate table.
func adoptAge(mtime string, now time.Time) (string, error) {
	t, err := time.Parse(time.RFC3339, mtime)
	if err != nil {
		return "", fmt.Errorf("parse candidate mtime %q: %w", mtime, err)
	}
	return humanizeAge(now.Sub(t)), nil
}

// humanizeAge collapses a duration into its coarsest single unit: seconds under a
// minute, minutes under an hour, hours under a day, days beyond that.
func humanizeAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// adoptReadyCell renders the READY column: "yes" when the candidate is ready to
// adopt, otherwise the daemon's not-ready reason.
func adoptReadyCell(ready bool, reason string) string {
	if ready {
		return "yes"
	}
	return reason
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
