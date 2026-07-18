package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAdoptSlug(t *testing.T) {
	cases := []struct {
		name string
		dir  string
		want string
	}{
		{name: "root and dots become dashes", dir: "/Users/test/.cc-orchestrate/x", want: "-Users-test--cc-orchestrate-x"},
		{name: "underscores become dashes", dir: "/tmp/my_repo", want: "-tmp-my-repo"},
		{name: "digits stay unchanged", dir: "/tmp/repo123", want: "-tmp-repo123"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := adoptSlug(tc.dir); got != tc.want {
				t.Errorf("adoptSlug(%q) = %q, want %q", tc.dir, got, tc.want)
			}
		})
	}
}

func TestListAdoptableHappyPath(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	oldID := "11111111-1111-1111-1111-111111111111"
	newID := "22222222-2222-2222-2222-222222222222"
	oldPrompt := "  inspect the repo\nand report back  "
	newPrompt := strings.Repeat("x", 130)
	oldPath := writeAdoptTranscript(t, resolved, oldID, []string{
		adoptPromptFixture(t, oldPrompt, resolved, oldID, "main"),
		adoptFixture(t, lineText, resolved, oldID, "main"),
	})
	newPath := writeAdoptTranscript(t, resolved, newID, []string{
		adoptPromptFixture(t, newPrompt, resolved, newID, "feature/adopt"),
		adoptFixture(t, lineBash, resolved, newID, "feature/adopt"),
	})
	oldTime := time.Now().Add(-time.Minute).Round(time.Second)
	newTime := time.Now().Round(time.Second)
	setModTime(t, oldPath, oldTime)
	setModTime(t, newPath, newTime)
	calls := stubClaudeProcs(t, []claudeProc{
		{pid: 101, argv: []string{"claude", "--resume", oldID}, cwd: resolved},
		{pid: 202, argv: []string{"claude", "--resume", newID}, cwd: resolved},
		{pid: 203, argv: []string{"claude", "--resume", newID}, cwd: resolved},
	})

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("listClaudeProcs calls = %d, want 1", *calls)
	}
	if len(got) != 2 {
		t.Fatalf("listAdoptable() returned %d candidates, want 2: %+v", len(got), got)
	}
	want := []adoptCandidate{
		{
			SessionID: newID, Cwd: resolved, GitBranch: "feature/adopt",
			FirstPrompt: strings.Repeat("x", adoptPromptLimit), MTime: newTime,
			State: StateWorking, Live: true,
		},
		{
			SessionID: oldID, Cwd: resolved, GitBranch: "main",
			FirstPrompt: "inspect the repo and report back", MTime: oldTime,
			State: StateIdle, Live: true, PID: 101,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("listAdoptable() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestListAdoptableMetadataFirstLine(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	sid := "33333333-3333-3333-3333-333333333333"
	writeAdoptTranscript(t, resolved, sid, []string{
		adoptFixture(t, `{"type":"attachment"}`, resolved, sid, "metadata-branch"),
		`{"type":"user","isSidechain":false,"message":{"content":null}}`,
		adoptPromptFixture(t, "metadata came first", "", "", ""),
		lineText,
	})
	stubClaudeProcs(t, nil)

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("listAdoptable() returned %d candidates, want 1", len(got))
	}
	if got[0].SessionID != sid || got[0].GitBranch != "metadata-branch" || got[0].FirstPrompt != "metadata came first" || got[0].State != StateIdle {
		t.Errorf("candidate = %+v", got[0])
	}
}

func TestListAdoptableFilters(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(context.Context, *testing.T, *sql.DB, string, string)
	}{
		{
			name: "sidechain",
			prepare: func(_ context.Context, t *testing.T, _ *sql.DB, resolved, sid string) {
				writeAdoptTranscript(t, resolved, sid, []string{
					adoptFixture(t, `{"type":"attachment"}`, resolved, sid, "main"),
					adoptSidechainFixture(t, lineText, true),
				})
			},
		},
		{
			name: "managed session id",
			prepare: func(ctx context.Context, t *testing.T, db *sql.DB, resolved, sid string) {
				writeAdoptTranscript(t, resolved, sid, []string{adoptFixture(t, lineText, resolved, sid, "main")})
				if _, err := db.ExecContext(ctx, `INSERT INTO agents (id, sprint_id, backend, scope, status, created_at) VALUES (?, 'sprint', 'tmux', 'scope', 'killed', 'now')`, sid); err != nil {
					t.Fatalf("insert managed agent: %v", err)
				}
			},
		},
		{
			name: "cwd mismatch",
			prepare: func(_ context.Context, t *testing.T, _ *sql.DB, resolved, sid string) {
				writeAdoptTranscript(t, resolved, sid, []string{adoptFixture(t, lineText, resolved+"-other", sid, "main")})
			},
		},
		{
			name: "filename and session id mismatch",
			prepare: func(_ context.Context, t *testing.T, _ *sql.DB, resolved, sid string) {
				writeAdoptTranscript(t, resolved, sid, []string{adoptFixture(t, lineText, resolved, sid+"-other", "main")})
			},
		},
		{
			name: "session id in multiple project slugs",
			prepare: func(_ context.Context, t *testing.T, _ *sql.DB, resolved, sid string) {
				writeAdoptTranscript(t, resolved, sid, []string{adoptFixture(t, lineText, resolved, sid, "main")})
				writeAdoptTranscriptAtSlug(t, "another-project", sid, adoptFixture(t, lineText, "/another", sid, "main")+"\n")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db, cwd, resolved := newAdoptTest(t)
			sid := "44444444-4444-4444-4444-444444444444"
			tc.prepare(ctx, t, db, resolved, sid)
			stubClaudeProcs(t, nil)
			got, err := listAdoptable(ctx, db, cwd)
			if err != nil {
				t.Fatalf("listAdoptable() error: %v", err)
			}
			if len(got) != 0 {
				t.Errorf("listAdoptable() = %+v, want no candidates", got)
			}
		})
	}
}

func TestListAdoptableTornFinalLine(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	sid := "55555555-5555-5555-5555-555555555555"
	content := adoptFixture(t, lineText, resolved, sid, "main") + "\n" + `{"type":"assistant"`
	writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), sid, content)
	stubClaudeProcs(t, nil)

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if len(got) != 1 || got[0].State != StateIdle {
		t.Fatalf("listAdoptable() = %+v, want one idle candidate", got)
	}
}

func TestListAdoptableValidFinalLineWithoutNewline(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	sid := "77777777-7777-7777-7777-777777777777"
	writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), sid, adoptFixture(t, lineText, resolved, sid, "main"))
	stubClaudeProcs(t, nil)

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if len(got) != 1 || got[0].State != StateIdle {
		t.Fatalf("listAdoptable() = %+v, want one idle candidate", got)
	}
}

func TestListAdoptableBoundedTailState(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	sid := "66666666-6666-6666-6666-666666666666"
	content := adoptPromptFixture(t, "large transcript", resolved, sid, "main") + "\n" +
		strings.Repeat(`{"type":"summary","summary":"padding padding padding"}`+"\n", 7000) +
		adoptFixture(t, lineBash, resolved, sid, "main") + "\n"
	path := writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), sid, content)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	if info.Size() <= adoptTailWindow {
		t.Fatalf("fixture size = %d, want > %d", info.Size(), adoptTailWindow)
	}
	stubClaudeProcs(t, nil)

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if len(got) != 1 || got[0].FirstPrompt != "large transcript" || got[0].State != StateWorking {
		t.Fatalf("listAdoptable() = %+v, want one working candidate", got)
	}
}

func TestListAdoptableHeadWindowFallback(t *testing.T) {
	ctx, db, cwd, resolved := newAdoptTest(t)
	sid := "88888888-8888-8888-8888-888888888888"
	// A single line longer than the head window sits ahead of the metadata, so the head
	// scan finds no cwd and discovery must fall back to a bounded streaming scan.
	huge := `{"type":"summary","summary":"` + strings.Repeat("x", 70000) + `"}`
	content := huge + "\n" +
		adoptPromptFixture(t, "late metadata line", resolved, sid, "main") + "\n" +
		adoptFixture(t, lineText, resolved, sid, "main") + "\n"
	path := writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), sid, content)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	if info.Size() <= adoptHeadWindow {
		t.Fatalf("fixture head = %d, want > %d so the head window misses the metadata", info.Size(), adoptHeadWindow)
	}
	stubClaudeProcs(t, nil)

	got, err := listAdoptable(ctx, db, cwd)
	if err != nil {
		t.Fatalf("listAdoptable() error: %v", err)
	}
	if len(got) != 1 || got[0].SessionID != sid || got[0].GitBranch != "main" || got[0].State != StateIdle {
		t.Fatalf("listAdoptable() = %+v, want one idle candidate discovered via the head-window fallback", got)
	}
}

func TestAttributeForeignClaude(t *testing.T) {
	cwd := "/private/tmp/project"
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	t0, t1, t2 := base, base.Add(time.Minute), base.Add(2*time.Minute)
	// Sub-second stamps for the ps floor-truncation band [ps, ps+1s).
	ps := time.Date(2026, 7, 18, 12, 0, 5, 0, time.UTC)
	inBand, pastBand, preStart := ps.Add(500*time.Millisecond), ps.Add(time.Second), ps.Add(-time.Second)
	target := candidateMeta{sid: "target-sid", mtime: t0}
	cases := []struct {
		name          string
		procs         []claudeProc
		candidates    []candidateMeta
		wantPID       int
		wantAmbiguous []int
	}{
		{
			name:    "tier one matches the sid in the session cwd",
			procs:   []claudeProc{{pid: 1, argv: []string{"claude", "--resume", "target-sid"}, cwd: cwd}},
			wantPID: 1,
		},
		{
			name:    "tier one matches the combined --resume=sid form",
			procs:   []claudeProc{{pid: 26, argv: []string{"claude", "--resume=target-sid"}, cwd: cwd}},
			wantPID: 26,
		},
		{
			// Fix 1: a claude in another dir whose argv mentions the sid is not tier-1.
			name:  "tier one ignores a matching sid outside the cwd",
			procs: []claudeProc{{pid: 1, argv: []string{"claude", "--resume", "target-sid"}, cwd: "/elsewhere"}},
		},
		{
			name:          "tier one ambiguous in the cwd",
			procs:         []claudeProc{{pid: 1, argv: []string{"claude", "target-sid"}, cwd: cwd}, {pid: 2, argv: []string{"claude", "--resume", "target-sid"}, cwd: cwd}},
			wantAmbiguous: []int{1, 2},
		},
		{
			name:       "tier two attributes a bare cwd claude written during its lifetime",
			procs:      []claudeProc{{pid: 3, argv: []string{"claude"}, cwd: cwd, start: t1}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: t2}},
			wantPID:    3,
		},
		{
			// R3: a prompt element merely containing "--resume" is not the flag.
			name:       "prompt mentioning --resume stays a tier-two candidate",
			procs:      []claudeProc{{pid: 24, argv: []string{"claude", "tell me about the --resume flag"}, cwd: cwd, start: ps}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: pastBand}},
			wantPID:    24,
		},
		{
			name:          "tier two ambiguous with two bare cwd claudes",
			procs:         []claudeProc{{pid: 3, argv: []string{"claude"}, cwd: cwd, start: t1}, {pid: 4, argv: []string{"claude", "--model", "opus"}, cwd: cwd, start: t1}},
			wantAmbiguous: []int{3, 4},
		},
		{
			name:  "session id invocation excluded from tier two",
			procs: []claudeProc{{pid: 5, argv: []string{"claude", "--session-id", "managed-sid"}, cwd: cwd}},
		},
		{
			name:  "resume invocation for another session excluded from tier two",
			procs: []claudeProc{{pid: 6, argv: []string{"claude", "--resume", "other-sid"}, cwd: cwd}},
		},
		{
			name:  "mcp serve child excluded from tier two",
			procs: []claudeProc{{pid: 7, argv: []string{"claude", "mcp", "serve"}, cwd: cwd}},
		},
		{
			name:       "claude descendant excluded, parent attributed",
			procs:      []claudeProc{{pid: 8, argv: []string{"claude"}, cwd: cwd, start: t1}, {pid: 9, ppid: 8, argv: []string{"claude", "helper"}, cwd: cwd, start: t1}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: t2}},
			wantPID:    8,
		},
		{
			name:       "resumed-idle-never-wrote attributes to the sole newest candidate",
			procs:      []claudeProc{{pid: 3, argv: []string{"claude"}, cwd: cwd, start: t2}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: t1}},
			wantPID:    3,
		},
		{
			// Another candidate wrote during the process's lifetime → the process is its
			// live claude; the target is a dead session adopted without a kill.
			name:       "other candidate owns the process, target is dead",
			procs:      []claudeProc{{pid: 3, argv: []string{"claude"}, cwd: cwd, start: t1}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: t0}, {sid: "other-sid", mtime: t2}},
			wantPID:    0,
		},
		{
			// Nobody wrote during the lifetime and the target is not the newest → refuse.
			name:          "unattributable when the target is not newest",
			procs:         []claudeProc{{pid: 3, argv: []string{"claude"}, cwd: cwd, start: t2}},
			candidates:    []candidateMeta{{sid: "target-sid", mtime: t0}, {sid: "other-sid", mtime: t1}},
			wantAmbiguous: []int{3},
		},
		{
			// R2: a write at the claim threshold (start + 1s) claims the process.
			name:       "claims a write just past the truncation band",
			procs:      []claudeProc{{pid: 20, argv: []string{"claude"}, cwd: cwd, start: ps}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: pastBand}},
			wantPID:    20,
		},
		{
			// R2: the only write lands inside the ambiguous truncated second → refuse.
			name:          "refuses when the only write lands inside the band",
			procs:         []claudeProc{{pid: 21, argv: []string{"claude"}, cwd: cwd, start: ps}},
			candidates:    []candidateMeta{{sid: "target-sid", mtime: inBand}},
			wantAmbiguous: []int{21},
		},
		{
			// R2: a write strictly before the process started is safely resumed-idle.
			name:       "resumed-idle attributes a write strictly before start",
			procs:      []claudeProc{{pid: 22, argv: []string{"claude"}, cwd: cwd, start: ps}},
			candidates: []candidateMeta{{sid: "target-sid", mtime: preStart}},
			wantPID:    22,
		},
		{
			// R2: a sibling inside the band can't be ruled out → refuse even though the
			// target itself predates the process.
			name:          "refuses when a sibling write lands inside the band",
			procs:         []claudeProc{{pid: 23, argv: []string{"claude"}, cwd: cwd, start: ps}},
			candidates:    []candidateMeta{{sid: "target-sid", mtime: preStart}, {sid: "other-sid", mtime: inBand}},
			wantAmbiguous: []int{23},
		},
		{
			name:  "no live process in the cwd",
			procs: []claudeProc{{pid: 12, argv: []string{"claude"}, cwd: "/other"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidates := tc.candidates
			if candidates == nil {
				candidates = []candidateMeta{target}
			}
			pid, ambiguous, reason := attributeForeignClaude(tc.procs, cwd, candidates, "target-sid")
			if pid != tc.wantPID || !reflect.DeepEqual(ambiguous, tc.wantAmbiguous) {
				t.Errorf("attributeForeignClaude() = (%d, %v), want (%d, %v)", pid, ambiguous, tc.wantPID, tc.wantAmbiguous)
			}
			if (len(tc.wantAmbiguous) > 0) != (reason != "") {
				t.Errorf("reason = %q, want a refusal message iff ambiguous", reason)
			}
			if len(tc.wantAmbiguous) > 0 && !strings.Contains(reason, "--pid") {
				t.Errorf("ambiguous reason = %q, want it to point at --pid", reason)
			}
		})
	}
}

func TestAdoptReadiness(t *testing.T) {
	cases := []struct {
		name       string
		state      State
		live       bool
		mtimeAge   time.Duration
		wantReady  bool
		wantReason string
	}{
		{name: "idle live", state: StateIdle, live: true, wantReady: true},
		{name: "awaiting live", state: StateAwaiting, live: true, wantReason: "session has a pending question — answer or dismiss it in the original terminal first"},
		{name: "unknown live", state: StateUnknown, live: true, wantReason: "no recorded activity yet"},
		{name: "unknown dead", state: StateUnknown, live: false, wantReady: true},
		{name: "working stale", state: StateWorking, live: true, mtimeAge: 30 * time.Second, wantReady: true},
		{name: "working fresh", state: StateWorking, live: true, mtimeAge: 29 * time.Second, wantReason: "session is mid-turn"},
		{name: "dead any state", state: StateStuck, live: false, wantReady: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready, reason := adoptReadiness(tc.state, tc.live, tc.mtimeAge)
			if ready != tc.wantReady || reason != tc.wantReason {
				t.Errorf("adoptReadiness() = (%v, %q), want (%v, %q)", ready, reason, tc.wantReady, tc.wantReason)
			}
		})
	}
}

func newAdoptTest(t *testing.T) (context.Context, *sql.DB, string, string) {
	t.Helper()
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	cwd := t.TempDir()
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	ctx := context.Background()
	return ctx, newTestDB(ctx, t), cwd, resolved
}

func adoptFixture(t *testing.T, raw, cwd, sid, branch string) string {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if cwd != "" {
		record["cwd"] = cwd
	}
	if sid != "" {
		record["sessionId"] = sid
	}
	if branch != "" {
		record["gitBranch"] = branch
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return string(encoded)
}

func adoptPromptFixture(t *testing.T, prompt, cwd, sid, branch string) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"type":        "user",
		"isSidechain": false,
		"message": map[string]any{
			"role":    "user",
			"content": prompt,
		},
	})
	if err != nil {
		t.Fatalf("marshal prompt fixture: %v", err)
	}
	return adoptFixture(t, string(encoded), cwd, sid, branch)
}

func adoptSidechainFixture(t *testing.T, raw string, sidechain bool) string {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		t.Fatalf("unmarshal sidechain fixture: %v", err)
	}
	record["isSidechain"] = sidechain
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal sidechain fixture: %v", err)
	}
	return string(encoded)
}

func writeAdoptTranscript(t *testing.T, resolved, sid string, lines []string) string {
	t.Helper()
	return writeAdoptTranscriptAtSlug(t, adoptSlug(resolved), sid, strings.Join(lines, "\n")+"\n")
}

func writeAdoptTranscriptAtSlug(t *testing.T, slug, sid, content string) string {
	t.Helper()
	projectsDir, err := claudeProjectsDir()
	if err != nil {
		t.Fatalf("claudeProjectsDir: %v", err)
	}
	dir := filepath.Join(projectsDir, slug)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	path := filepath.Join(dir, sid+".jsonl")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	return path
}

func setModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set transcript mtime: %v", err)
	}
}

func stubClaudeProcs(t *testing.T, procs []claudeProc) *int {
	t.Helper()
	previous := listClaudeProcs
	calls := new(int)
	listClaudeProcs = func(context.Context) ([]claudeProc, error) {
		*calls++
		return procs, nil
	}
	t.Cleanup(func() { listClaudeProcs = previous })
	return calls
}
