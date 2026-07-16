package orchestrate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// pollInterval is the default cadence at which a tailer polls for the transcript
// to appear and for newly appended bytes. newTailerManager snapshots it once at
// construction and each tailer receives the value as a parameter, so the tailer
// goroutines never read this global — a test shortens it before constructing the
// manager (or passes an interval straight to runTailer) without racing them.
var pollInterval = 250 * time.Millisecond

// probeGrace is how long the prober waits for an agent's transcript to appear before
// it treats the agent as possibly blocked on an interactive prompt and probes its
// screen. newTailerManager snapshots it once, like pollInterval, so a test can
// shorten it race-free before constructing the manager.
var probeGrace = 5 * time.Second

// claudeProjectsDir resolves the directory holding Claude Code's per-project
// transcript folders. claude honors $CLAUDE_CONFIG_DIR over ~/.claude, so the
// tailer must too, or it never finds a child whose config dir is relocated. It
// errors only when the home directory is needed but unresolvable.
func claudeProjectsDir() (string, error) {
	if base := os.Getenv("CLAUDE_CONFIG_DIR"); base != "" {
		return filepath.Join(base, "projects"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".claude", "projects"), nil
}

// findTranscript locates a session's Claude Code transcript under
// <claudeProjectsDir>/<slug>/<sessionID>.jsonl, returning the newest by mtime when
// the session id collides across project slugs.
func findTranscript(sessionID string) (string, bool, error) {
	dir, err := claudeProjectsDir()
	if err != nil {
		return "", false, err
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*", sessionID+".jsonl"))
	if err != nil {
		return "", false, fmt.Errorf("glob transcript for %s: %w", sessionID, err)
	}
	if len(matches) == 0 {
		return "", false, nil
	}
	newest, newestMod := "", time.Time{}
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil {
			continue
		}
		if mod := info.ModTime(); mod.After(newestMod) {
			newest, newestMod = m, mod
		}
	}
	return newest, newest != "", nil
}

// runTailer waits for the session's transcript to appear, then tails it, calling
// onStatus with the derived Status whenever it changes (identical consecutive
// statuses are deduped). It returns nil when ctx is cancelled and propagates a
// callback error. scope is currently informational; interval is the poll cadence.
// The baseline status is the empty accumulator's StateUnknown, which matches the
// agents table default, so the first emission is the first meaningful change rather
// than a redundant unknown.
//
// The transcript is replayed from the start on every (re)start to rebuild status.
// onStatus carries a live flag: false while replaying history, true once the first
// read has caught up, so a consumer can tell a genuinely-new status from a replayed
// one.
func runTailer(ctx context.Context, sessionID, _ string, interval time.Duration, onStatus func(Status, bool) error) error {
	path, ok, err := waitForTranscript(ctx, sessionID, interval)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	acc := newStatusAcc()
	last := acc.status()
	var offset int64
	var buf []byte
	live := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		appended, next, err := readAppended(path, offset)
		if err != nil {
			return fmt.Errorf("tail transcript %s: %w", path, err)
		}
		offset = next
		buf = append(buf, appended...)
		for {
			i := bytes.IndexByte(buf, '\n')
			if i < 0 {
				break
			}
			acc.feed(buf[:i])
			buf = buf[i+1:]
		}
		if cur := acc.status(); cur != last {
			if err := onStatus(cur, live); err != nil {
				return err
			}
			last = cur
		}
		live = true
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// waitForTranscript polls findTranscript until it resolves or ctx is cancelled.
func waitForTranscript(ctx context.Context, sessionID string, interval time.Duration) (string, bool, error) {
	if p, ok, err := findTranscript(sessionID); err != nil || ok {
		return p, ok, err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", false, nil
		case <-ticker.C:
			if p, ok, err := findTranscript(sessionID); err != nil || ok {
				return p, ok, err
			}
		}
	}
}

// readAppended returns the bytes written past offset and the new end offset,
// leaving any trailing partial line for the caller to buffer.
func readAppended(path string, offset int64) ([]byte, int64, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is the session's own transcript resolved under the claude config dir, not user input
	if err != nil {
		return nil, offset, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, offset, err
	}
	return data, offset + int64(len(data)), nil
}
