package orchestrate

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
	"github.com/yasyf/cc-interact/subject"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ccnotes"
	"github.com/yasyf/cc-orchestrate/worktree"
)

const (
	// foreignKillTimeout bounds how long killForeignClaude waits for a SIGTERM'd
	// claude to exit before giving up. It never escalates to SIGKILL — a claude
	// still alive after this window is surfaced as an error so the caller aborts the
	// adoption rather than risk a half-flushed transcript.
	foreignKillTimeout = 10 * time.Second
	// foreignKillPoll is the ESRCH poll cadence while waiting for the process to exit.
	foreignKillPoll = 200 * time.Millisecond
)

// killForeignClaude stops the hand-started claude process at pid with a SIGTERM and
// waits for it to exit, polling kill(pid, 0) for ESRCH every foreignKillPoll up to
// foreignKillTimeout. Immediately before signalling it re-enumerates and re-checks the
// pid's identity (cwd and start time) so a pid recycled onto an unrelated process since
// identification is never signalled — a vanished pid is treated as already-dead, a changed
// identity is an error with no kill, and an ESRCH from the SIGTERM itself is success. It
// never escalates to SIGKILL, and it never routes through backend.Kill (superset's Kill
// pkills by --session-id and would silently no-op a bare foreign claude). It is a package
// var so a test can stub the signal without a real process.
var killForeignClaude = func(ctx context.Context, pid int, cwd string, start time.Time) error {
	procs, err := listClaudeProcs(ctx)
	if err != nil {
		return fmt.Errorf("re-enumerate claude processes before kill: %w", err)
	}
	cur, ok := procByPID(procs, pid)
	if !ok {
		return nil // gone since identification — already dead
	}
	if cur.cwd != cwd || !cur.start.Equal(start) {
		return fmt.Errorf("claude process %d changed identity before kill (now cwd %s start %s, expected %s / %s); refusing to signal", pid, cur.cwd, cur.start, cwd, start)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil // exited between the re-check and the signal
		}
		return fmt.Errorf("signal claude process %d: %w", pid, err)
	}
	deadline := time.Now().Add(foreignKillTimeout)
	for {
		if !processAlive(pid) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("claude process %d did not exit within %s of SIGTERM", pid, foreignKillTimeout)
		}
		time.Sleep(foreignKillPoll)
	}
}

// procByPID returns the process with the given pid from a snapshot.
func procByPID(procs []claudeProc, pid int) (claudeProc, bool) {
	for _, p := range procs {
		if p.pid == pid {
			return p, true
		}
	}
	return claudeProc{}, false
}

// processAlive reports whether pid is still a live process, via the kill(pid, 0)
// existence probe: ESRCH means it is gone, every other outcome (including EPERM) means
// it is still present.
func processAlive(pid int) bool {
	return !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}

// transcriptSize returns the byte size of the transcript at path, the offset the
// trailing-message check records around the foreign kill.
func transcriptSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat transcript %s: %w", path, err)
	}
	return info.Size(), nil
}

// scanAppendedTranscript inspects the transcript bytes written past offset. userMessage is
// a genuine user turn — a prompt that landed after adoption began and that the killed
// session never answered. torn is a non-empty appended region whose final line does not
// parse — a write cut mid-line, e.g. a just-submitted prompt the SIGTERM interrupted. A
// clean SIGTERM flushes only a non-user metadata line (type "last-prompt"), which trips
// neither, so the warnings fire on a real trailing message rather than on every live
// adoption.
func scanAppendedTranscript(path string, offset int64) (userMessage, torn bool, err error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from the Claude projects directory glob.
	if err != nil {
		return false, false, fmt.Errorf("reopen transcript %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return false, false, fmt.Errorf("stat transcript %s: %w", path, err)
	}
	if info.Size() <= offset {
		return false, false, nil
	}
	data, err := io.ReadAll(io.NewSectionReader(f, offset, info.Size()-offset))
	if err != nil {
		return false, false, fmt.Errorf("read appended transcript bytes: %w", err)
	}
	for _, raw := range bytes.Split(data, []byte{'\n'}) {
		var line struct {
			Type        string `json:"type"`
			IsSidechain *bool  `json:"isSidechain"`
		}
		if json.Unmarshal(raw, &line) != nil {
			continue
		}
		if line.Type == "user" && (line.IsSidechain == nil || !*line.IsSidechain) {
			userMessage = true
		}
	}
	return userMessage, !finalLineParses(data), nil
}

// transcriptTailTorn reports whether the transcript's final non-empty line fails to parse
// as JSON — a write torn mid-line at validation time, whose final entry may be lost. It
// reads a bounded tail rather than the whole file.
func transcriptTailTorn(path string) (bool, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from the Claude projects directory glob.
	if err != nil {
		return false, fmt.Errorf("reopen transcript %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("stat transcript %s: %w", path, err)
	}
	start := max(int64(0), info.Size()-adoptTailWindow)
	data, err := io.ReadAll(io.NewSectionReader(f, start, info.Size()-start))
	if err != nil {
		return false, fmt.Errorf("read transcript tail: %w", err)
	}
	return !finalLineParses(data), nil
}

// finalLineParses reports whether the final non-empty line of data is valid JSON. An empty
// region parses (nothing torn).
func finalLineParses(data []byte) bool {
	trimmed := bytes.TrimRight(data, "\n")
	if len(trimmed) == 0 {
		return true
	}
	if idx := bytes.LastIndexByte(trimmed, '\n'); idx >= 0 {
		trimmed = trimmed[idx+1:]
	}
	return json.Valid(trimmed)
}

// adoptListRequest lists adoptable hand-started sessions under a directory.
type adoptListRequest struct {
	Cwd string `json:"cwd"`
}

// adoptCandidateView is the JSON shape cco.adopt.list returns per candidate, flattened
// from adoptCandidate (which carries no json tags) with the timestamp rendered as an
// RFC3339 string like every other view. Ready mirrors adoptReadiness at list time so
// the CLI's poll loop reads the daemon's kill-gate verdict instead of re-deriving it.
type adoptCandidateView struct {
	SessionID   string `json:"session_id"`
	Cwd         string `json:"cwd"`
	GitBranch   string `json:"git_branch"`
	FirstPrompt string `json:"first_prompt"`
	MTime       string `json:"mtime"`
	State       string `json:"state"`
	Live        bool   `json:"live"`
	PID         int    `json:"pid"`
	Ready       bool   `json:"ready"`
	Reason      string `json:"reason,omitempty"`
}

func newAdoptCandidateView(c adoptCandidate) adoptCandidateView {
	ready, reason := adoptReadiness(c.State, c.Live, time.Since(c.MTime))
	return adoptCandidateView{
		SessionID: c.SessionID, Cwd: c.Cwd, GitBranch: c.GitBranch, FirstPrompt: c.FirstPrompt,
		MTime: c.MTime.UTC().Format(time.RFC3339), State: string(c.State), Live: c.Live, PID: c.PID,
		Ready: ready, Reason: reason,
	}
}

// handleAdoptList answers cco.adopt.list: it lists the hand-started Claude Code
// sessions under cwd that cc-orchestrate could adopt, the same discovery the CLI's poll
// loop reads.
func handleAdoptList(hc daemon.HandlerCtx, req adoptListRequest) ([]adoptCandidateView, error) {
	cwd, err := resolveAdoptCwd(hc, req.Cwd)
	if err != nil {
		return nil, err
	}
	candidates, err := listAdoptable(hc.Ctx, hc.DB, cwd)
	if err != nil {
		return nil, err
	}
	views := make([]adoptCandidateView, len(candidates))
	for i, c := range candidates {
		views[i] = newAdoptCandidateView(c)
	}
	return views, nil
}

// adoptRequest adopts one hand-started session by id. Cwd is the session's working
// directory; Name defaults like a spawn; Relocate moves the session into a fresh cco
// worktree; PID overrides foreign-process identification when the two-tier match is
// ambiguous.
type adoptRequest struct {
	SessionID string `json:"session_id"`
	Cwd       string `json:"cwd"`
	Name      string `json:"name,omitempty"`
	Relocate  bool   `json:"relocate,omitempty"`
	PID       int    `json:"pid,omitempty"`
}

// adoptResult reports the adopted agent, its subject and terminal, the hierarchy it
// landed in, and any non-fatal warning (a trailing user message caught mid-adopt, or a
// superset workspace that could not be matched).
type adoptResult struct {
	AgentID      string `json:"agent_id"`
	SubjectID    string `json:"subject_id"`
	Terminal     string `json:"terminal"`
	Backend      string `json:"backend"`
	RepoID       string `json:"repo_id"`
	WorkstreamID string `json:"workstream_id"`
	Warning      string `json:"warning,omitempty"`
}

// adoptPlacement is where an adopted session lands: the resolved hierarchy rows, the
// exact agent scope (the session cwd, or the relocated worktree), and any warning the
// resolution raised.
type adoptPlacement struct {
	repo    repoRow
	ws      workstreamRow
	sprint  sprintRow
	scope   string
	warning string
	// relocatedFrom/relocatedTo are a --relocate adopt's pre- and post-move transcript
	// paths; a launch failure renames the transcript back and tears down ws.
	relocatedFrom string
	relocatedTo   string
}

// handleAdopt answers cco.agent.adopt. Under agentLock(sid) for the whole op body — so
// a second concurrent adopt finds the inserted row and errors before spawning — it
// validates the transcript, re-verifies quiescence, stops the foreign process at a safe
// point, resolves (or creates) the session's place in the repo → workstream → sprint
// tree, and relaunches the same Claude session as a managed agent. The launch mirrors
// handleSpawn's spawn-then-insert ordering (never restoreAgent's insert-first): the
// supervisor lists DB rows before enumerating live handles, so a row inserted before its
// terminal exists has a window where the vanished lane respawns a duplicate --resume.
func handleAdopt(hc daemon.HandlerCtx, req adoptRequest) (adoptResult, error) {
	sid := req.SessionID
	if sid == "" {
		return adoptResult{}, opErr(codeInvalidRequest, fmt.Errorf("adopt requires a session_id"))
	}
	reqCwd, err := resolveAdoptCwd(hc, req.Cwd)
	if err != nil {
		return adoptResult{}, err
	}
	cwd, err := filepath.EvalSymlinks(reqCwd)
	if err != nil {
		return adoptResult{}, fmt.Errorf("resolve adoption directory %q: %w", reqCwd, err)
	}

	mu := agentLock(sid)
	mu.Lock()
	defer mu.Unlock()

	projectsDir, err := claudeProjectsDir()
	if err != nil {
		return adoptResult{}, err
	}

	// 1. Validate: the transcript is present, unique across project slugs, and not
	// already managed.
	matches, err := filepath.Glob(filepath.Join(projectsDir, "*", sid+".jsonl"))
	if err != nil {
		return adoptResult{}, fmt.Errorf("glob transcript for %s: %w", sid, err)
	}
	if len(matches) == 0 {
		return adoptResult{}, opErr(codeNotFound, fmt.Errorf("no adoptable transcript for session %s", sid))
	}
	if len(matches) > 1 {
		return adoptResult{}, opErr(codeConflict, fmt.Errorf("session %s is recorded under multiple project directories; cannot adopt unambiguously", sid))
	}
	transcriptPath := matches[0]
	exists, err := agentExists(hc.Ctx, hc.DB, sid)
	if err != nil {
		return adoptResult{}, err
	}
	if exists {
		return adoptResult{}, opErr(codeConflict, fmt.Errorf("agent %s is already managed — use cco agent respawn", sid))
	}

	// 2. Re-verify quiescence on a fresh discovery and identify the foreign process. The
	// re-read shrinks the idle→kill TOCTOU to this op's own execution, and the sibling
	// candidates in cwd give foreign-process attribution its context.
	candidates, err := discoverAdoptCandidates(hc.Ctx, hc.DB, cwd)
	if err != nil {
		return adoptResult{}, err
	}
	candidate, ok := findAdoptCandidateBySID(candidates, sid)
	if !ok {
		return adoptResult{}, opErr(codeConflict, fmt.Errorf("session %s is not adoptable from %s (cwd mismatch or sidechain transcript)", sid, cwd))
	}
	// Record the trailing-message offset before the readiness re-check and git preflights,
	// so anything the live session appends after the gate decision is caught by the
	// appended-message scan. Warn now if the transcript already ends mid-write.
	before, err := transcriptSize(transcriptPath)
	if err != nil {
		return adoptResult{}, err
	}
	var warnings []string
	if torn, err := transcriptTailTorn(transcriptPath); err != nil {
		return adoptResult{}, err
	} else if torn {
		warnings = append(warnings, "transcript ends mid-write; the final entry may be lost")
	}
	procs, err := listClaudeProcs(hc.Ctx)
	if err != nil {
		return adoptResult{}, fmt.Errorf("list live claude processes: %w", err)
	}
	foreign, live, err := identifyAdoptPID(procs, cwd, candidateMetas(candidates), sid, req.PID)
	if err != nil {
		return adoptResult{}, err
	}
	if ready, reason := adoptReadiness(candidate.State, live, time.Since(candidate.MTime)); !ready {
		return adoptResult{}, opErr(codeNotReady, errors.New(reason))
	}

	// Pre-flight the deterministic refusals (non-git cwd, and a dirty checkout under
	// --relocate) before the kill, so a rejection never leaves the original dead.
	toplevel, err := worktree.Toplevel(hc.Ctx, cwd)
	if err != nil {
		return adoptResult{}, opErr(codeInvalidRequest, fmt.Errorf("%s is not inside a git repository: %w", cwd, err))
	}
	toplevel, err = filepath.EvalSymlinks(toplevel)
	if err != nil {
		return adoptResult{}, fmt.Errorf("resolve worktree root %q: %w", toplevel, err)
	}
	if req.Relocate {
		dirty, err := worktree.Dirty(hc.Ctx, toplevel)
		if err != nil {
			return adoptResult{}, err
		}
		if dirty {
			return adoptResult{}, opErr(codeConflict, fmt.Errorf("checkout %s has uncommitted changes; commit or stash before adopting with --relocate", toplevel))
		}
	}

	// 3. Stop the foreign process and detect a message that landed mid-adopt.
	origin := "no live process"
	if live {
		origin = fmt.Sprintf("pid %d", foreign.pid)
		if err := killForeignClaude(hc.Ctx, foreign.pid, foreign.cwd, foreign.start); err != nil {
			return adoptResult{}, err
		}
		trailing, torn, err := scanAppendedTranscript(transcriptPath, before)
		if err != nil {
			return adoptResult{}, err
		}
		if trailing {
			warnings = append(warnings, "the original session recorded a trailing user message during adoption; the resumed agent will see it unanswered")
		}
		if torn {
			warnings = append(warnings, "the original session recorded a partial trailing write during adoption; the final entry may be lost")
		}
	}

	// 4/5. Resolve (and, for --relocate, create) the session's placement, serialized so
	// two concurrent adopts in the same unregistered cwd cannot both auto-create the repo.
	adoptPlacementMu.Lock()
	placement, movedNote, err := resolveAdoptPlacement(hc, cwd, toplevel, transcriptPath, projectsDir, req, sid)
	adoptPlacementMu.Unlock()
	if err != nil {
		return adoptResult{}, err
	}
	if placement.warning != "" {
		warnings = append(warnings, placement.warning)
	}

	// 6. Launch — spawn-then-insert, mirroring handleSpawn. A launch failure after a
	// --relocate move renames the transcript back and tears down the freshly created
	// workstream, so a refused adoption leaves no orphaned placement.
	res, err := launchAdopted(hc, req, sid, candidate, placement, movedNote, origin)
	if err != nil {
		if placement.relocatedFrom != "" {
			_ = os.Rename(placement.relocatedTo, placement.relocatedFrom)
			teardownAdoptWorkstream(hc, placement.ws.ID, sid)
		}
		return adoptResult{}, err
	}
	res.Warning = strings.Join(warnings, "; ")
	return res, nil
}

// adoptPlacementMu serializes handleAdopt's hierarchy resolution and creation so two
// concurrent adopts of different sessions in the same unregistered cwd cannot both
// auto-create the repo. It guards placement only, never the kill/wait/launch.
var adoptPlacementMu sync.Mutex

// teardownAdoptWorkstream best-effort tears down a workstream handleAdopt created but could
// not launch into. It passes skipAgentID (the in-flight sid, whose agentLock the caller
// holds for the whole op) to the kill cascade, so even a compensation that failed to flip
// the sid agent off Active cannot make the cascade re-lock agentLock(sid) and self-deadlock.
func teardownAdoptWorkstream(hc daemon.HandlerCtx, wsID, skipAgentID string) {
	ws, err := getWorkstream(hc.Ctx, hc.DB, wsID, "")
	if err != nil {
		return
	}
	_, _ = killWorkstreamResolved(hc, ws, skipAgentID)
}

// identifyAdoptPID resolves the foreign claude process to stop, returning it (and whether a
// live process was found). A caller-supplied pid overrides attribution but is still
// validated: it must be a claude in the fresh snapshot, running in the session's own cwd,
// and not resuming or serving a different identified session (unless its argv carries the
// target sid) — otherwise it errors naming what failed. With no pid, the cwd-context
// attribution runs; an ambiguous match refuses and points at --pid, and no match reports no
// live process so the caller adopts the finished transcript directly.
func identifyAdoptPID(procs []claudeProc, cwd string, candidates []candidateMeta, sid string, reqPID int) (claudeProc, bool, error) {
	if reqPID != 0 {
		p, ok := procByPID(procs, reqPID)
		if !ok {
			return claudeProc{}, false, opErr(codeInvalidRequest, fmt.Errorf("pid %d is not a running claude process", reqPID))
		}
		if p.cwd != cwd {
			return claudeProc{}, false, opErr(codeInvalidRequest, fmt.Errorf("pid %d runs in %s, not the session directory %s", reqPID, p.cwd, cwd))
		}
		if tier2Excluded(p.argv) && !argvHasSID(p.argv, sid) {
			return claudeProc{}, false, opErr(codeInvalidRequest, fmt.Errorf("pid %d is resuming or serving a different session, not %s; omit --pid or pass this session's claude", reqPID, sid))
		}
		return p, true, nil
	}
	pid, ambiguous, reason := attributeForeignClaude(procs, cwd, candidates, sid)
	if len(ambiguous) > 0 {
		return claudeProc{}, false, opErr(codeInvalidRequest, errors.New(reason))
	}
	if pid == 0 {
		return claudeProc{}, false, nil
	}
	p, _ := procByPID(procs, pid)
	return p, true, nil
}

// resolveAdoptPlacement decides where an adopted session lands. --relocate always mints
// a fresh workstream and moves the transcript into it; otherwise the session stays in
// place: a primary checkout rides the repo's primary workstream with the exact session
// cwd as its scope, a hand-made worktree gets an adopted workstream row of its own, and
// an unregistered checkout auto-creates the repo. It returns the placement and, for
// --relocate, the note appended to the resumed agent's brief.
func resolveAdoptPlacement(hc daemon.HandlerCtx, cwd, toplevel, transcriptPath, projectsDir string, req adoptRequest, sid string) (adoptPlacement, string, error) {
	repo, isPrimary, err := resolveAdoptRepo(hc, toplevel)
	if err != nil {
		return adoptPlacement{}, "", err
	}
	if req.Relocate {
		return relocateAdopt(hc, repo, toplevel, transcriptPath, projectsDir, req, sid)
	}
	if isPrimary {
		ws, err := getPrimaryWorkstream(hc.Ctx, hc.DB, repo.ID)
		if err != nil {
			return adoptPlacement{}, "", err
		}
		sp, err := getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
		if err != nil {
			return adoptPlacement{}, "", err
		}
		return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: cwd}, "", nil
	}
	placement, err := adoptWorktree(hc, repo, toplevel, cwd)
	return placement, "", err
}

// resolveAdoptRepo maps a session's git checkout to a registered repo, reporting whether
// the session sits in that repo's primary checkout. A toplevel equal to a repo's cwd is
// the primary; otherwise a hand-made worktree is mapped back to its main repo through
// the shared git common dir; an unregistered checkout auto-creates a repo rooted at the
// toplevel (handleRepoCreate mints its primary workstream over the main checkout).
func resolveAdoptRepo(hc daemon.HandlerCtx, toplevel string) (repoRow, bool, error) {
	repos, err := listRepos(hc.Ctx, hc.DB, StatusActive)
	if err != nil {
		return repoRow{}, false, err
	}
	if repo, ok := matchRepoByPath(repos, toplevel); ok {
		return repo, true, nil
	}
	// Not a repo's own checkout: group by the shared git common dir so a session in a
	// linked worktree maps back to whichever registered repo shares that git store — even
	// a repo whose own Cwd is itself a linked worktree.
	sessionCommon, err := resolveCommonDir(hc.Ctx, toplevel)
	if err != nil {
		return repoRow{}, false, err
	}
	for _, repo := range repos {
		repoCommon, err := resolveCommonDir(hc.Ctx, repo.Cwd)
		if err != nil {
			continue
		}
		if repoCommon == sessionCommon {
			return repo, false, nil
		}
	}
	// Unregistered: auto-create the repo at the main checkout (the directory holding the
	// common git dir), never at a linked worktree. The session is primary only when it
	// sits in that main checkout.
	mainRoot, err := filepath.EvalSymlinks(filepath.Dir(sessionCommon))
	if err != nil {
		return repoRow{}, false, fmt.Errorf("resolve main repo root %q: %w", filepath.Dir(sessionCommon), err)
	}
	created, err := handleRepoCreate(hc, repoCreateRequest{Name: filepath.Base(mainRoot), Cwd: mainRoot})
	if err != nil {
		return repoRow{}, false, err
	}
	repo, err := getRepo(hc.Ctx, hc.DB, created.RepoID)
	if err != nil {
		return repoRow{}, false, err
	}
	return repo, mainRoot == toplevel, nil
}

// resolveCommonDir returns the resolved git common dir shared by every worktree of dir's
// repository — the identity key that groups a main checkout with its linked worktrees.
func resolveCommonDir(ctx context.Context, dir string) (string, error) {
	commonDir, err := worktree.CommonDir(ctx, dir)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(commonDir)
	if err != nil {
		return "", fmt.Errorf("resolve git common dir %q: %w", commonDir, err)
	}
	return resolved, nil
}

// matchRepoByPath returns the repo whose cwd resolves to path. A repo whose checkout no
// longer resolves is skipped rather than matched.
func matchRepoByPath(repos []repoRow, path string) (repoRow, bool) {
	for _, repo := range repos {
		resolved, err := filepath.EvalSymlinks(repo.Cwd)
		if err != nil {
			continue
		}
		if resolved == path {
			return repo, true
		}
	}
	return repoRow{}, false
}

// findWorkstreamByWorktree returns repo's workstream whose worktree resolves to checkout,
// if one already wraps it. A workstream whose worktree no longer resolves is skipped.
func findWorkstreamByWorktree(ctx context.Context, db *sql.DB, repoID, checkout string) (workstreamRow, bool, error) {
	wss, err := listWorkstreams(ctx, db, repoID, "")
	if err != nil {
		return workstreamRow{}, false, err
	}
	for _, ws := range wss {
		resolved, err := filepath.EvalSymlinks(ws.Worktree)
		if err != nil {
			continue
		}
		if resolved == checkout {
			return ws, true, nil
		}
	}
	return workstreamRow{}, false, nil
}

// adoptWorktree places a session that lives in a hand-made worktree of a registered
// repo. A git-managed backend gets a fresh backend workspace wrapping the existing
// checkout and an adopted workstream row bound to it. A ManagesWorktree backend
// (superset) cannot wrap an arbitrary dir, so it matches an existing server-side
// workspace by worktree path, and otherwise falls back to the repo's primary workstream
// with the checkout as the agent scope, raising a warning.
func adoptWorktree(hc daemon.HandlerCtx, repo repoRow, checkout, cwd string) (adoptPlacement, error) {
	// Reuse an existing workstream already wrapping this checkout — a prior adopt of a
	// sibling session in the same hand-made worktree — instead of inserting a duplicate row
	// or re-creating a backend workspace.
	if ws, ok, err := findWorkstreamByWorktree(hc.Ctx, hc.DB, repo.ID, checkout); err != nil {
		return adoptPlacement{}, err
	} else if ok {
		sp, err := getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
		if err != nil {
			return adoptPlacement{}, err
		}
		return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: cwd}, nil
	}

	b, ok := backend.Get(repo.Backend)
	if !ok {
		return adoptPlacement{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", repo.Backend))
	}
	if err := b.EnsureReady(hc.Ctx); err != nil {
		return adoptPlacement{}, err
	}
	branch, err := worktree.CurrentBranch(hc.Ctx, checkout)
	if err != nil {
		return adoptPlacement{}, err
	}

	if b.Caps().Has(backend.ManagesWorktree) {
		handles, err := b.ListWorkstreams(hc.Ctx)
		if err != nil {
			return adoptPlacement{}, err
		}
		for _, h := range handles {
			resolved, err := filepath.EvalSymlinks(h.Worktree)
			if err != nil {
				continue
			}
			if resolved == checkout {
				ws, sp, err := insertAdoptedWorkstream(hc, repo, checkout, branch, h.ID)
				if err != nil {
					return adoptPlacement{}, err
				}
				return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: cwd}, nil
			}
		}
		ws, err := getPrimaryWorkstream(hc.Ctx, hc.DB, repo.ID)
		if err != nil {
			return adoptPlacement{}, err
		}
		sp, err := getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
		if err != nil {
			return adoptPlacement{}, err
		}
		warning := fmt.Sprintf("no %s workspace wraps %s; adopted into the primary workstream with scope %s", repo.Backend, checkout, checkout)
		return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: checkout, warning: warning}, nil
	}

	handle, err := b.CreateWorkstream(hc.Ctx, backend.WorkstreamSpec{Name: branch, Cwd: checkout, RepoCwd: repo.Cwd, Branch: branch})
	if err != nil {
		return adoptPlacement{}, err
	}
	ws, sp, err := insertAdoptedWorkstream(hc, repo, checkout, branch, handle.ID)
	if err != nil {
		return adoptPlacement{}, err
	}
	return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: cwd}, nil
}

// insertAdoptedWorkstream persists a non-primary workstream row that wraps an existing
// checkout (never via handleWorkstreamCreate, which mints a fresh branch and worktree),
// provisions its cc-notes bindings like a real workstream, and creates its default
// sprint. It mirrors handleWorkstreamCreate's row + sprint + frame writes for the dir
// cc-orchestrate did not create.
func insertAdoptedWorkstream(hc daemon.HandlerCtx, repo repoRow, checkout, branch, workspaceHandle string) (workstreamRow, sprintRow, error) {
	ccProject, ccSprint, err := provisionCCNotes(hc.Ctx, repo.Cwd, branch)
	if err != nil {
		return workstreamRow{}, sprintRow{}, err
	}
	ws := workstreamRow{
		ID: workstreamSlug(branch), RepoID: repo.ID, Name: branch, Backend: repo.Backend,
		WorkspaceHandle: workspaceHandle, Branch: branch, Worktree: checkout, IsPrimary: false,
		CCNotesProject: ccProject, Status: StatusActive, CreatedAt: nowStamp(),
	}
	if err := insertWorkstream(hc.Ctx, hc.DB, ws); err != nil {
		return workstreamRow{}, sprintRow{}, err
	}
	sprintID, err := createDefaultSprint(hc.Ctx, hc.DB, ws.ID, ccSprint)
	if err != nil {
		return workstreamRow{}, sprintRow{}, err
	}
	sp, err := getSprint(hc.Ctx, hc.DB, sprintID, "")
	if err != nil {
		return workstreamRow{}, sprintRow{}, err
	}
	fleetLog.emit(hc.Ctx, containerFrame(FrameWorkstreamCreated, ws.ID, ws.Name))
	fleetLog.emit(hc.Ctx, containerFrame(FrameSprintCreated, sp.ID, sp.Name))
	return ws, sp, nil
}

// relocateAdopt moves an adopted session into a fresh cco worktree: it mints a new
// workstream via handleWorkstreamCreate (a fresh branch + worktree), renames the
// transcript into the new cwd's project slug so `claude --resume` finds it there, and
// returns a note (appended to the resumed brief) telling the agent it was moved. The
// dirty-checkout refusal already ran before the foreign kill.
func relocateAdopt(hc daemon.HandlerCtx, repo repoRow, oldCheckout, transcriptPath, projectsDir string, req adoptRequest, sid string) (adoptPlacement, string, error) {
	wsName := cmp.Or(req.Name, "adopted-"+sid[:8])
	created, err := handleWorkstreamCreate(hc, workstreamCreateRequest{Repo: repo.ID, Name: wsName})
	if err != nil {
		return adoptPlacement{}, "", err
	}
	// Everything past the workstream creation tears it back down on failure, so a refused
	// relocate (a destination collision, say) never orphans the fresh workstream.
	placement, note, err := planRelocate(hc, repo, created.WorkstreamID, oldCheckout, transcriptPath, projectsDir, sid)
	if err != nil {
		teardownAdoptWorkstream(hc, created.WorkstreamID, sid)
		return adoptPlacement{}, "", err
	}
	return placement, note, nil
}

// planRelocate resolves the created workstream's sprint and transcript destination, runs
// every preflight (including the destination-collision check, which needs the new worktree
// path), and — as the last step, immediately before the caller launches — moves the
// transcript into the new worktree's project slug so `claude --resume` finds it there. The
// returned placement carries both transcript paths so a launch failure can rename it back.
func planRelocate(hc daemon.HandlerCtx, repo repoRow, workstreamID, oldCheckout, transcriptPath, projectsDir, sid string) (adoptPlacement, string, error) {
	ws, err := getWorkstream(hc.Ctx, hc.DB, workstreamID, "")
	if err != nil {
		return adoptPlacement{}, "", err
	}
	sp, err := getDefaultSprint(hc.Ctx, hc.DB, ws.ID)
	if err != nil {
		return adoptPlacement{}, "", err
	}
	newRoot, err := filepath.EvalSymlinks(ws.Worktree)
	if err != nil {
		return adoptPlacement{}, "", fmt.Errorf("resolve relocated worktree %q: %w", ws.Worktree, err)
	}
	destDir := filepath.Join(projectsDir, adoptSlug(newRoot))
	if err := os.MkdirAll(destDir, 0o700); err != nil {
		return adoptPlacement{}, "", fmt.Errorf("create relocated transcript dir %s: %w", destDir, err)
	}
	destPath := filepath.Join(destDir, sid+".jsonl")
	if _, err := os.Stat(destPath); err == nil {
		return adoptPlacement{}, "", opErr(codeConflict, fmt.Errorf("a transcript for %s already exists at %s", sid, destPath))
	} else if !errors.Is(err, os.ErrNotExist) {
		return adoptPlacement{}, "", fmt.Errorf("stat relocation target %s: %w", destPath, err)
	}
	// The rename is the last step, immediately before launch: both endpoints sit under the
	// same claudeProjectsDir, so there is no cross-filesystem case to handle.
	if err := os.Rename(transcriptPath, destPath); err != nil {
		return adoptPlacement{}, "", fmt.Errorf("relocate transcript to %s: %w", destPath, err)
	}
	note := fmt.Sprintf("\n\nRELOCATION: this session was moved from %s to %s during adoption. Absolute paths recorded earlier in the conversation still point at %s; do further work in your current directory %s.", oldCheckout, newRoot, oldCheckout, newRoot)
	return adoptPlacement{repo: repo, ws: ws, sprint: sp, scope: ws.Worktree, relocatedFrom: transcriptPath, relocatedTo: destPath}, note, nil
}

// launchAdopted resumes an adopted session into a managed backend terminal, mirroring
// handleSpawn's spawn-then-insert ordering: start the subject, provision the cc-notes
// task, spawn the resume command, insert the agent row (CreatedAt is adopt time, never
// the session's original start, so the recentlySpawned grace applies), re-check the
// hierarchy with compensation on failure, then append EventAdopted and start the tailer
// after the fleet frame.
func launchAdopted(hc daemon.HandlerCtx, req adoptRequest, sid string, candidate adoptCandidate, placement adoptPlacement, movedNote, origin string) (adoptResult, error) {
	ws := placement.ws
	sprint := placement.sprint
	scope := placement.scope
	b, ok := backend.Get(ws.Backend)
	if !ok {
		return adoptResult{}, opErr(codeUnsupported, fmt.Errorf("unknown backend: %s", ws.Backend))
	}
	if err := b.EnsureReady(hc.Ctx); err != nil {
		return adoptResult{}, err
	}
	self, err := os.Executable()
	if err != nil {
		return adoptResult{}, err
	}
	name := cmp.Or(req.Name, "agent-"+sid[:8])

	ccTask := ""
	if ws.CCNotesProject != "" && sprint.CCNotesSprint != "" && ccnotes.Enabled(hc.Ctx, ws.Worktree) {
		ccTask, err = ccnotes.CreateTask(hc.Ctx, ws.Worktree, cmp.Or(req.Name, agentSlug(sid)), ws.Branch, sprint.CCNotesSprint, ws.CCNotesProject)
		if err != nil {
			return adoptResult{}, err
		}
	}

	// Read, validate, and resolve the launcher config before Subjects.Start: an
	// unusable child.launcher must not leave an active subject behind.
	launcher, err := childLauncher(hc.Ctx, hc.DB)
	if err != nil {
		return adoptResult{}, err
	}
	spawnNonce := uuid.NewString()
	command, err := wrapForCapture(self, sid, spawnNonce, launcher, adoptResumeCommand(self, sid, scope, movedNote), b.Caps())
	if err != nil {
		return adoptResult{}, err
	}
	sub, _, err := hc.Subjects.Start(hc.Ctx, subject.Window{Session: sid}, scope, agentSlug(sid), lifecycle, true)
	if err != nil {
		return adoptResult{}, err
	}
	command = wrapScrubExec(self, command)
	handle, err := b.Spawn(hc.Ctx, backend.SpawnSpec{
		Workstream: backend.WorkstreamHandle{Backend: ws.Backend, ID: ws.WorkspaceHandle, Name: ws.Name, Cwd: ws.Worktree},
		Name:       name,
		Cwd:        scope,
		Command:    command,
		SessionID:  sid,
	})
	if err != nil {
		return adoptResult{}, err
	}

	ag := agentRow{
		ID: sid, SprintID: sprint.ID, Backend: ws.Backend, TerminalHandle: handle.ID,
		SessionID: sid, Scope: scope, Name: name, Prompt: candidate.FirstPrompt,
		SubjectID: sub.ID, CCNotesTask: ccTask, Status: StatusActive, State: StateUnknown,
		CreatedAt: nowStamp(), SpawnNonce: spawnNonce,
	}
	if err := insertAgent(hc.Ctx, hc.DB, ag); err != nil {
		// The terminal is live but has no row; the supervisor only reconciles agents that
		// have one, so tear it down rather than leak an unmanaged claude forever.
		_ = b.Kill(hc.Ctx, handle)
		return adoptResult{}, err
	}
	res, err := finishAdoptLaunch(hc, ag, sub.ID, placement, origin)
	if err != nil {
		// Any post-insert failure soft-exits the agent and kills its terminal, so no active
		// orphan lingers under a placement the caller may be about to tear down.
		if cerr := compensateSpawnLocked(hc.Ctx, hc.DB, hc.Append, ag); cerr != nil {
			return adoptResult{}, cerr
		}
		return adoptResult{}, err
	}
	return res, nil
}

// finishAdoptLaunch runs the post-insert half of an adopt launch: the hierarchy re-check
// that closes the container-kill orphan window (a kill racing this adopt either captured
// the insert or is observed here), the EventAdopted lifecycle event, the spawned frame, and
// the transcript tailer. Every error path leaves the caller to compensate the inserted row.
func finishAdoptLaunch(hc daemon.HandlerCtx, ag agentRow, subID string, placement adoptPlacement, origin string) (adoptResult, error) {
	fresh, err := getSprint(hc.Ctx, hc.DB, ag.SprintID, "")
	if err != nil {
		return adoptResult{}, err
	}
	if _, _, err := requireActiveHierarchy(hc, fresh); err != nil {
		return adoptResult{}, err
	}
	if _, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: subID, Origin: event.OriginSystem, Type: EventAdopted, Payload: adoptedPayload(ag, origin),
	}); err != nil {
		return adoptResult{}, err
	}
	// Announce before starting the tailer, so a fast status frame can never precede the
	// adopted frame on the stream.
	fleetLog.emit(hc.Ctx, spawnedFrame(ag))
	tailers.start(hc.DB, hc.Append, ag)

	return adoptResult{
		AgentID: ag.ID, SubjectID: subID, Terminal: ag.TerminalHandle, Backend: string(placement.ws.Backend),
		RepoID: placement.repo.ID, WorkstreamID: placement.ws.ID,
	}, nil
}

// adoptResumeCommand builds the resume argv for an adopted session. With no relocation
// note it is resumeCommand verbatim; with a note it appends that note to the
// orchestration brief so the resumed agent learns its recorded paths no longer match
// its cwd.
func adoptResumeCommand(self, sid, scope, relocateNote string) []string {
	if relocateNote == "" {
		return resumeCommand(self, sid, scope)
	}
	return append(claudeInvocation(),
		"--resume", sid,
		"--channels", channelPlugin.ChannelID(),
		"--settings", childSettings(self),
		"--append-system-prompt", spawnBrief(self, sid, scope)+relocateNote,
	)
}

// resolveAdoptCwd resolves a request cwd to an absolute path, joining a relative cwd
// against the caller's scope and refusing when there is none (an HTTP envelope, whose
// scope is the daemon's own cwd, never the caller's).
func resolveAdoptCwd(hc daemon.HandlerCtx, cwd string) (string, error) {
	if cwd == "" {
		return "", opErr(codeInvalidRequest, fmt.Errorf("adopt requires a cwd"))
	}
	if filepath.IsAbs(cwd) {
		return cwd, nil
	}
	if hc.Scope == "" {
		return "", opErr(codeInvalidRequest, fmt.Errorf("relative cwd %q requires an absolute path when called with no scope", cwd))
	}
	return filepath.Join(hc.Scope, cwd), nil
}
