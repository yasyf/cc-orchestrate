package backend

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// supersetDaemonProtocol is the pty-daemon wire protocol version cc-orchestrate
// announces in its hello; the daemon answers hello-ack with the negotiated version.
const supersetDaemonProtocol = 2

// supersetMaxFrame caps a single daemon frame, matching the daemon's own 8 MB
// guard, so a malformed length prefix cannot make the client allocate unbounded.
const supersetMaxFrame = 8 << 20

// supersetDaemonTimeout bounds a whole list exchange so a wedged daemon cannot
// stall a supervisor tick; it mirrors the daemon's own list timeout.
const supersetDaemonTimeout = 5 * time.Second

// supersetSession is one live PTY session the superset host daemon owns, from a
// list-reply. Alive is the child process's real-time liveness (the daemon's
// !exited): a session whose claude has exited reports Alive=false even while its
// record persists for cold-restore, which is exactly the death signal the
// supervisor needs. ID is the terminalId Spawn returned, stored as a terminal handle.
type supersetSession struct {
	ID    string `json:"id"`
	PID   int    `json:"pid"`
	Alive bool   `json:"alive"`
}

// supersetFrame is the decoded JSON of one control frame, covering both the
// hello-ack (Type only) and the list-reply (Type plus Sessions).
type supersetFrame struct {
	Type     string            `json:"type"`
	Sessions []supersetSession `json:"sessions"`
}

// supersetDaemonSocketPath resolves the pty-daemon control socket for the local
// organization: $TMPDIR/superset-ptyd-<sha256(orgId)[:12]>.sock, where orgId is the
// single directory under ~/.superset/host. It is a package var so a test can point
// ListAgents at an in-process fake daemon without a real Superset install. The path
// is derived from the host directory rather than `superset status --json` so the
// supervisor resolves it without spawning the node CLI on every tick.
var supersetDaemonSocketPath = func() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	hostDir := filepath.Join(home, ".superset", "host")
	entries, err := os.ReadDir(hostDir)
	if err != nil {
		return "", fmt.Errorf("superset: read host dir %s: %w", hostDir, err)
	}
	orgID := ""
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if orgID != "" {
			return "", fmt.Errorf("superset: %s holds more than one organization; cannot resolve the pty-daemon socket", hostDir)
		}
		orgID = e.Name()
	}
	if orgID == "" {
		return "", fmt.Errorf("superset: no organization under %s", hostDir)
	}
	sum := sha256.Sum256([]byte(orgID))
	return filepath.Join(os.TempDir(), "superset-ptyd-"+hex.EncodeToString(sum[:])[:12]+".sock"), nil
}

// listSupersetSessions dials the pty-daemon control socket and runs the hello/list
// handshake (length-prefixed binary frames, protocol v2), returning the host's live
// PTY sessions. The socket is the daemon's authoritative real-time view and is
// guarded by filesystem permissions (0600), so no token is exchanged.
func listSupersetSessions(ctx context.Context, socket string) ([]supersetSession, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("superset: dial pty-daemon %s: %w", socket, err)
	}
	defer func() { _ = conn.Close() }()
	deadline := time.Now().Add(supersetDaemonTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)

	if err := writeSupersetFrame(conn, supersetHello{Type: "hello", Protocols: []int{supersetDaemonProtocol}}); err != nil {
		return nil, err
	}
	ack, err := readSupersetFrame(conn)
	if err != nil {
		return nil, err
	}
	if ack.Type != "hello-ack" {
		return nil, fmt.Errorf("superset: pty-daemon handshake: got frame %q, want hello-ack", ack.Type)
	}
	if err := writeSupersetFrame(conn, supersetListReq{Type: "list"}); err != nil {
		return nil, err
	}
	// The daemon answers a list with a single list-reply; tolerate a bounded run of
	// interleaved control frames before it rather than trusting strict ordering.
	for range 8 {
		frame, err := readSupersetFrame(conn)
		if err != nil {
			return nil, err
		}
		if frame.Type == "list-reply" {
			return frame.Sessions, nil
		}
	}
	return nil, fmt.Errorf("superset: pty-daemon sent no list-reply")
}

type supersetHello struct {
	Type      string `json:"type"`
	Protocols []int  `json:"protocols"`
}

type supersetListReq struct {
	Type string `json:"type"`
}

// writeSupersetFrame encodes msg as one control frame: a u32 big-endian total
// length, a u32 big-endian JSON length, then the JSON. Control frames carry no
// binary payload, so total == 4 + len(json).
func writeSupersetFrame(w io.Writer, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("superset: encode pty-daemon frame: %w", err)
	}
	total := 4 + len(body)
	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(total))     //nolint:gosec // G115: control frames are tiny; the length fits u32 by construction
	binary.BigEndian.PutUint32(out[4:8], uint32(len(body))) //nolint:gosec // G115: control frames are tiny; the length fits u32 by construction
	copy(out[8:], body)
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("superset: write pty-daemon frame: %w", err)
	}
	return nil
}

// readSupersetFrame reads one frame and returns its decoded JSON, discarding any
// binary payload tail (control replies have none).
func readSupersetFrame(r io.Reader) (supersetFrame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return supersetFrame{}, fmt.Errorf("superset: read pty-daemon frame header: %w", err)
	}
	total := binary.BigEndian.Uint32(hdr[:])
	if total < 4 || total > supersetMaxFrame {
		return supersetFrame{}, fmt.Errorf("superset: pty-daemon frame length %d out of range", total)
	}
	body := make([]byte, total)
	if _, err := io.ReadFull(r, body); err != nil {
		return supersetFrame{}, fmt.Errorf("superset: read pty-daemon frame body: %w", err)
	}
	jsonLen := binary.BigEndian.Uint32(body[0:4])
	if int64(jsonLen) > int64(len(body))-4 {
		return supersetFrame{}, fmt.Errorf("superset: pty-daemon frame json length %d exceeds body %d", jsonLen, len(body)-4)
	}
	var frame supersetFrame
	if err := json.Unmarshal(body[4:4+jsonLen], &frame); err != nil {
		return supersetFrame{}, fmt.Errorf("superset: parse pty-daemon frame: %w", err)
	}
	return frame, nil
}
