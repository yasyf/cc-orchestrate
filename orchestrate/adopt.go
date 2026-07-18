package orchestrate

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	adoptHeadWindow  = int64(64 << 10)
	adoptTailWindow  = int64(256 << 10)
	adoptPromptLimit = 120
)

type adoptCandidate struct {
	SessionID   string
	Cwd         string
	GitBranch   string
	FirstPrompt string
	MTime       time.Time
	State       State
	Live        bool
	PID         int
}

type adoptDiscoveryLine struct {
	Type        string   `json:"type"`
	Cwd         string   `json:"cwd"`
	SessionID   string   `json:"sessionId"`
	GitBranch   string   `json:"gitBranch"`
	IsSidechain *bool    `json:"isSidechain"`
	Message     *message `json:"message"`
}

func adoptSlug(dir string) string {
	var slug strings.Builder
	slug.Grow(len(dir))
	for i := 0; i < len(dir); i++ {
		b := dir[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' {
			slug.WriteByte(b)
		} else {
			slug.WriteByte('-')
		}
	}
	return slug.String()
}

func listAdoptable(ctx context.Context, db *sql.DB, cwd string) ([]adoptCandidate, error) {
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return nil, fmt.Errorf("resolve adoption directory: %w", err)
	}
	candidates, err := discoverAdoptCandidates(ctx, db, resolved)
	if err != nil {
		return nil, err
	}
	procs, err := listClaudeProcs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list live claude processes: %w", err)
	}
	metas := candidateMetas(candidates)
	for i := range candidates {
		pid, ambiguous, _ := attributeForeignClaude(procs, resolved, metas, candidates[i].SessionID)
		candidates[i].PID = pid
		candidates[i].Live = pid > 0 || len(ambiguous) > 0
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].MTime.After(candidates[j].MTime)
	})
	return candidates, nil
}

// discoverAdoptCandidates reads every unmanaged, in-place adoptable session whose
// transcript lives under resolved's project slug — the shared discovery both listAdoptable
// and handleAdopt run before any live-process attribution.
func discoverAdoptCandidates(ctx context.Context, db *sql.DB, resolved string) ([]adoptCandidate, error) {
	projectsDir, err := claudeProjectsDir()
	if err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(projectsDir, adoptSlug(resolved), "*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob adoptable transcripts: %w", err)
	}
	managed, err := managedSessionIDs(ctx, db)
	if err != nil {
		return nil, err
	}
	candidates := make([]adoptCandidate, 0, len(paths))
	for _, path := range paths {
		candidate, ok, err := readAdoptCandidate(path, resolved, projectsDir, managed)
		if err != nil {
			return nil, err
		}
		if ok {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, nil
}

// candidateMetas projects the per-candidate attribution context (session id + transcript
// mtime) a foreign-process match needs.
func candidateMetas(candidates []adoptCandidate) []candidateMeta {
	metas := make([]candidateMeta, len(candidates))
	for i, c := range candidates {
		metas[i] = candidateMeta{sid: c.SessionID, mtime: c.MTime}
	}
	return metas
}

// findAdoptCandidateBySID returns the candidate with the given session id from a discovered
// set.
func findAdoptCandidateBySID(candidates []adoptCandidate, sid string) (adoptCandidate, bool) {
	for _, c := range candidates {
		if c.SessionID == sid {
			return c, true
		}
	}
	return adoptCandidate{}, false
}

func managedSessionIDs(ctx context.Context, db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT id FROM agents`)
	if err != nil {
		return nil, fmt.Errorf("list managed session ids: %w", err)
	}
	ids := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan managed session id: %w", err)
		}
		ids[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate managed session ids: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close managed session ids: %w", err)
	}
	return ids, nil
}

func readAdoptCandidate(path, cwd, projectsDir string, managed map[string]struct{}) (adoptCandidate, bool, error) {
	head, tail, mtime, err := readAdoptWindows(path)
	if err != nil {
		return adoptCandidate{}, false, fmt.Errorf("read adoption transcript %s: %w", path, err)
	}
	metadata, firstPrompt, sidechain := parseAdoptHead(head)
	if metadata.Cwd == "" {
		// The head window is one long line ahead of the metadata; stream past it,
		// bounded, for the first cwd-bearing line before giving up on the file.
		scanned, err := scanAdoptMetadata(path)
		if err != nil {
			return adoptCandidate{}, false, fmt.Errorf("scan adoption transcript %s: %w", path, err)
		}
		metadata = scanned
	}
	filenameID := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	if metadata.SessionID != filenameID || metadata.Cwd != cwd || sidechain {
		return adoptCandidate{}, false, nil
	}
	if _, ok := managed[metadata.SessionID]; ok {
		return adoptCandidate{}, false, nil
	}
	matches, err := filepath.Glob(filepath.Join(projectsDir, "*", metadata.SessionID+".jsonl"))
	if err != nil {
		return adoptCandidate{}, false, fmt.Errorf("glob transcript locations for %s: %w", metadata.SessionID, err)
	}
	if len(matches) > 1 {
		return adoptCandidate{}, false, nil
	}

	acc := newStatusAcc()
	for _, line := range tail {
		acc.feed(line)
	}
	return adoptCandidate{
		SessionID:   metadata.SessionID,
		Cwd:         metadata.Cwd,
		GitBranch:   metadata.GitBranch,
		FirstPrompt: firstPrompt,
		MTime:       mtime,
		State:       acc.status().State,
	}, true, nil
}

func parseAdoptHead(lines [][]byte) (adoptDiscoveryLine, string, bool) {
	var metadata adoptDiscoveryLine
	var firstPrompt string
	metadataSeen := false
	promptSeen := false
	sidechainSeen := false
	sidechain := false
	for _, raw := range lines {
		var line adoptDiscoveryLine
		if json.Unmarshal(raw, &line) != nil {
			continue
		}
		if !metadataSeen && line.Cwd != "" {
			metadata = line
			metadataSeen = true
		}
		if !sidechainSeen && line.IsSidechain != nil {
			sidechain = *line.IsSidechain
			sidechainSeen = true
		}
		if promptSeen || line.Type != "user" || line.Message == nil {
			continue
		}
		content := bytes.TrimSpace(line.Message.Content)
		if len(content) == 0 || content[0] != '"' || decodeBlocks(content) != nil {
			continue
		}
		var prompt string
		if json.Unmarshal(content, &prompt) == nil {
			firstPrompt = trimAdoptPrompt(prompt)
			promptSeen = true
		}
	}
	return metadata, firstPrompt, sidechain
}

func trimAdoptPrompt(prompt string) string {
	prompt = strings.Join(strings.Fields(prompt), " ")
	runes := []rune(prompt)
	if len(runes) > adoptPromptLimit {
		return string(runes[:adoptPromptLimit])
	}
	return prompt
}

func readAdoptWindows(path string) ([][]byte, [][]byte, time.Time, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from the Claude projects directory glob.
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, time.Time{}, err
	}

	headSize := min(info.Size(), adoptHeadWindow)
	head, err := io.ReadAll(io.NewSectionReader(f, 0, headSize))
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	tailStart := max(int64(0), info.Size()-adoptTailWindow)
	tail, err := io.ReadAll(io.NewSectionReader(f, tailStart, info.Size()-tailStart))
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	return splitTranscriptLines(head, false, info.Size() > headSize), splitTranscriptLines(tail, tailStart > 0, false), info.ModTime(), nil
}

func splitTranscriptLines(data []byte, discardFirst, discardLast bool) [][]byte {
	if discardFirst {
		firstNewline := bytes.IndexByte(data, '\n')
		if firstNewline < 0 {
			return nil
		}
		data = data[firstNewline+1:]
	}
	if discardLast && len(data) > 0 && data[len(data)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(data, '\n')
		if lastNewline < 0 {
			return nil
		}
		data = data[:lastNewline+1]
	}
	data = bytes.TrimSuffix(data, []byte{'\n'})
	if len(data) == 0 {
		return nil
	}
	return bytes.Split(data, []byte{'\n'})
}

// candidateMeta is the minimal per-candidate context foreign-process attribution needs:
// the session id and the transcript's last-write time.
type candidateMeta struct {
	sid   string
	mtime time.Time
}

// foreignAmbiguousReason is the refusal when a live cwd-only claude cannot be safely tied
// to the target session.
const foreignAmbiguousReason = "cannot attribute the live claude process to this session — pass --pid or exit it first"

// attributeForeignClaude resolves the live foreign claude process to stop for targetSID
// among the unmanaged candidates sharing cwd.
//
// Tier 1 matches a process whose argv carries the target session id as a token and runs in
// the session's own cwd. Tier 2 handles a bare claude in cwd not tied to any identified
// session: a single such process is attributed to the target when the target's transcript
// was written during the process's lifetime (T.MTime >= P.start), or — when no candidate
// wrote during its lifetime — to the newest candidate (the resumed-idle-never-wrote case).
// A process another candidate wrote under belongs to that candidate, leaving the target
// dead (no kill); anything else is ambiguous. Returns (pid, ambiguous, reason): pid>0 a
// unique process to stop, ambiguous non-empty a refusal carrying reason, both zero no live
// process.
func attributeForeignClaude(procs []claudeProc, cwd string, candidates []candidateMeta, targetSID string) (int, []int, string) {
	var tier1 []int
	for _, p := range procs {
		if p.cwd == cwd && argvHasSID(p.argv, targetSID) {
			tier1 = append(tier1, p.pid)
		}
	}
	if len(tier1) == 1 {
		return tier1[0], nil, ""
	}
	if len(tier1) > 1 {
		return 0, tier1, fmt.Sprintf("multiple claude processes match session %s (%v); re-run with --pid to choose one", targetSID, tier1)
	}

	bare := bareCwdClaudes(procs, cwd)
	if len(bare) == 0 {
		return 0, nil, ""
	}
	if len(bare) > 1 {
		return 0, pidsOf(bare), foreignAmbiguousReason
	}
	p := bare[0]
	target, ok := candidateByID(candidates, targetSID)
	if !ok {
		return 0, []int{p.pid}, foreignAmbiguousReason
	}
	// ps start times are floor-truncated to the second, but mtimes are sub-second, so the
	// truncated second [p.start, p.start+1s) is ambiguous: a write inside it might predate
	// the process. A candidate only *claims* P by writing strictly past that band.
	claimThreshold := p.start.Add(time.Second)
	if !target.mtime.Before(claimThreshold) {
		return p.pid, nil, ""
	}
	for _, c := range candidates {
		if c.sid != targetSID && !c.mtime.Before(claimThreshold) {
			// Another candidate wrote past the band → P is its live process; the target is a
			// dead session adopted without a kill.
			return 0, nil, ""
		}
	}
	// Nobody claims P. The resumed-idle fallback (attribute to the newest candidate) is only
	// safe when every candidate wrote strictly before the process started; a candidate whose
	// mtime lands inside the ambiguous band can't be ruled out, so refuse.
	for _, c := range candidates {
		if !c.mtime.Before(p.start) {
			return 0, []int{p.pid}, foreignAmbiguousReason
		}
	}
	if isNewestCandidate(target, candidates) {
		return p.pid, nil, ""
	}
	return 0, []int{p.pid}, foreignAmbiguousReason
}

// bareCwdClaudes returns the claude processes in cwd that are not resuming/running an
// identified session, not an mcp-serve child, and not a descendant of another cwd claude —
// the tier-2 attribution pool.
func bareCwdClaudes(procs []claudeProc, cwd string) []claudeProc {
	cwdPIDs := map[int]struct{}{}
	for _, p := range procs {
		if p.cwd == cwd {
			cwdPIDs[p.pid] = struct{}{}
		}
	}
	var bare []claudeProc
	for _, p := range procs {
		if p.cwd != cwd || tier2Excluded(p.argv) {
			continue
		}
		if _, descendant := cwdPIDs[p.ppid]; descendant {
			continue
		}
		bare = append(bare, p)
	}
	return bare
}

// tier2Excluded reports whether argv marks a process that belongs to an identified
// session (a --session-id/--resume argument) or is an mcp-serve child — never a bare
// hand-started claude eligible for tier-2 attribution. Matching is on real argv elements,
// so a session id or "--resume" mentioned inside a prompt never trips it.
func tier2Excluded(argv []string) bool {
	for i, a := range argv {
		if a == "--session-id" || a == "--resume" {
			return true
		}
		if a == "mcp" && i+1 < len(argv) && argv[i+1] == "serve" {
			return true
		}
	}
	return false
}

// argvHasSID reports whether argv references sid as a real argument: the bare id (the
// operand of a "--resume <sid>" pair) or the combined "--resume=<sid>" / "--session-id=<sid>"
// forms. A prompt element merely containing the id as a substring never matches.
func argvHasSID(argv []string, sid string) bool {
	for _, a := range argv {
		if a == sid || a == "--resume="+sid || a == "--session-id="+sid {
			return true
		}
	}
	return false
}

func pidsOf(procs []claudeProc) []int {
	pids := make([]int, len(procs))
	for i, p := range procs {
		pids[i] = p.pid
	}
	return pids
}

func candidateByID(candidates []candidateMeta, sid string) (candidateMeta, bool) {
	for _, c := range candidates {
		if c.sid == sid {
			return c, true
		}
	}
	return candidateMeta{}, false
}

// isNewestCandidate reports whether target has the strictly newest mtime among candidates.
// A tie leaves nobody newest, so a resumed-idle process stays ambiguous rather than being
// attributed to two sessions at once.
func isNewestCandidate(target candidateMeta, candidates []candidateMeta) bool {
	for _, c := range candidates {
		if c.sid == target.sid {
			continue
		}
		if !c.mtime.Before(target.mtime) {
			return false
		}
	}
	return true
}

// adoptScanCap bounds the head-window fallback stream in scanAdoptMetadata.
const adoptScanCap = int64(4 << 20)

// scanAdoptMetadata streams the transcript for the first line carrying a cwd — the
// fallback when the 64KB head window is one long line ahead of the metadata. It is bounded
// to adoptScanCap bytes (and lines up to that length); past that it returns a zero line, so
// the candidate is simply not adoptable rather than an error.
func scanAdoptMetadata(path string) (adoptDiscoveryLine, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from the Claude projects directory glob.
	if err != nil {
		return adoptDiscoveryLine{}, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(io.LimitReader(f, adoptScanCap))
	scanner.Buffer(make([]byte, 0, 64<<10), int(adoptScanCap))
	for scanner.Scan() {
		var line adoptDiscoveryLine
		if json.Unmarshal(scanner.Bytes(), &line) != nil {
			continue
		}
		if line.Cwd != "" {
			return line, nil
		}
	}
	return adoptDiscoveryLine{}, nil
}

func adoptReadiness(state State, live bool, mtimeAge time.Duration) (bool, string) {
	if !live {
		return true, ""
	}
	switch state {
	case StateIdle:
		return true, ""
	case StateAwaiting:
		return false, "session has a pending question — answer or dismiss it in the original terminal first"
	case StateUnknown:
		return false, "no recorded activity yet"
	case StateWorking:
		if mtimeAge >= 30*time.Second {
			return true, ""
		}
		return false, "session is mid-turn"
	default:
		panic(fmt.Sprintf("unexpected adopt state %q", state))
	}
}
