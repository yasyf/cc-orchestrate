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

// claudeProjectsDir resolves the directory holding Claude Code's per-project
// transcript folders. claude honors $CLAUDE_CONFIG_DIR over ~/.claude, so the
// tailer must too, or it never finds a child whose config dir is relocated. It is
// empty only when the home directory is needed but unresolvable.
func claudeProjectsDir() string {
	if base := os.Getenv("CLAUDE_CONFIG_DIR"); base != "" {
		return filepath.Join(base, "projects")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".claude", "projects")
}

// findTranscript locates a session's Claude Code transcript under
// <claudeProjectsDir>/<slug>/<sessionID>.jsonl, returning the newest by mtime when
// the session id collides across project slugs.
func findTranscript(sessionID string) (string, bool) {
	dir := claudeProjectsDir()
	if dir == "" {
		return "", false
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*", sessionID+".jsonl"))
	if err != nil || len(matches) == 0 {
		return "", false
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
	return newest, newest != ""
}

// runTailer waits for the session's transcript to appear, then tails it, calling
// onStatus with the derived Status whenever it changes (identical consecutive
// statuses are deduped). It returns nil when ctx is cancelled and propagates an
// onStatus error. scope is currently informational; interval is the poll cadence.
// The baseline status is the empty accumulator's StateUnknown, which matches the
// agents table default, so the first emission is the first meaningful change
// rather than a redundant unknown.
func runTailer(ctx context.Context, sessionID, scope string, interval time.Duration, onStatus func(Status) error) error {
	path, ok := waitForTranscript(ctx, sessionID, interval)
	if !ok {
		return nil
	}

	acc := newStatusAcc()
	last := acc.status()
	var offset int64
	var buf []byte
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
			if err := onStatus(cur); err != nil {
				return err
			}
			last = cur
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// waitForTranscript polls findTranscript until it resolves or ctx is cancelled.
func waitForTranscript(ctx context.Context, sessionID string, interval time.Duration) (string, bool) {
	if p, ok := findTranscript(sessionID); ok {
		return p, true
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", false
		case <-ticker.C:
			if p, ok := findTranscript(sessionID); ok {
				return p, true
			}
		}
	}
}

// readAppended returns the bytes written past offset and the new end offset,
// leaving any trailing partial line for the caller to buffer.
func readAppended(path string, offset int64) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, offset, err
	}
	return data, offset + int64(len(data)), nil
}
