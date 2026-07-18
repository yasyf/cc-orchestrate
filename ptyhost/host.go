package ptyhost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

// Options configures a Run.
type Options struct {
	Socket string   // unix socket path to serve CAPTURE/KEYS on
	Argv   []string // the child command to run under the PTY
	// OnChildExit, when set, fires once the hosted child exits on its own — not when a
	// parent signal (SIGINT/SIGTERM/SIGHUP) tore it down — as the wrapper's last act,
	// AFTER the socket and grid are freed: the daemon's response is to kill this
	// wrapper and respawn the session, so by the time the hook hands control to the
	// daemon this wrapper's socket must already be gone and never touched again (the
	// daemon derives the replacement's socket per incarnation, so even an interleaved
	// replacement binds a different path and this wrapper's removal stays self-scoped).
	// A signal-driven teardown skips it, leaving that window to the supervisor's
	// membership/staleness fallbacks.
	OnChildExit func()
}

// Run hosts opts.Argv under a pseudo-terminal: it tees the child's output to this
// process's stdout (so the spawning terminal still shows it) and into a virtual
// screen grid, copies this process's stdin into the child, and serves a control
// socket answering CAPTURE with the rendered screen and KEYS by writing bytes to the
// child. It returns when the child exits, propagating the child's wait error, and
// removes the socket. A parent kill (SIGINT/SIGTERM/SIGHUP) cancels ctx and tears the
// child down.
func Run(ctx context.Context, opts Options) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	ws := ttySize()
	cmd := exec.CommandContext(ctx, opts.Argv[0], opts.Argv[1:]...) //nolint:gosec // G204: pty-host runs the caller-specified child command by design
	ptmx, err := pty.StartWithSize(cmd, ws)
	if err != nil {
		return fmt.Errorf("pty-host start %s: %w", opts.Argv[0], err)
	}

	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	go func() {
		for range winch {
			_ = pty.InheritSize(os.Stdin, ptmx)
		}
	}()

	// The grid drains the emulator's query replies back into the child PTY (see
	// grid_vt.go), so a TUI that probes for cursor position or device attributes
	// never stalls waiting for the answer.
	g := newGrid(int(ws.Cols), int(ws.Rows), ptmx)

	// Read loop: feed the grid first (in-memory, never blocks) then tee to our stdout,
	// until the PTY closes. readDone gates the grid's release in teardown.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.Copy(io.MultiWriter(gridWriter{g}, os.Stdout), ptmx)
	}()
	// Forward our stdin to the child. os.Stdin.Read is not cancellable, so this
	// goroutine outlives Run; it is bounded by the pty-host process, which exits as
	// soon as Run returns.
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()

	// teardown stops the read loop and frees the grid in order — closing the PTY wakes
	// the read loop's Read so it stops feeding before the grid is freed (close-before-
	// free ordering borrowed from headless-terminal) — for both the happy and error paths.
	teardown := func() {
		signal.Stop(winch)
		close(winch)
		_ = ptmx.Close()
		<-readDone
		g.Close()
	}

	srv, err := serve(opts.Socket, g, ptmx)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		teardown()
		return err
	}

	werr := cmd.Wait()
	_ = srv.Close() // stop accepting and drain in-flight handlers before freeing the grid
	_ = os.Remove(opts.Socket)
	teardown()
	// The child exited on its own (no parent signal cancelled ctx): fire the exit hook
	// as the wrapper's last act, strictly after the socket removal above, so the whole
	// teardown is complete before the hook hands control to the daemon — which answers
	// the report by killing this wrapper and respawning the session. The daemon derives
	// socket paths per incarnation, so the replacement binds its own path and this
	// wrapper's removal can never unlink it regardless of interleaving; the ordering
	// keeps the wrapper from ever touching the filesystem after the daemon has taken
	// over. A signal-driven teardown leaves ctx.Err() non-nil and skips it.
	if opts.OnChildExit != nil && ctx.Err() == nil {
		opts.OnChildExit()
	}
	return werr
}

// gridWriter adapts a grid to io.Writer so the PTY read loop can tee into it.
type gridWriter struct{ g grid }

func (w gridWriter) Write(p []byte) (int, error) {
	w.g.Feed(p)
	return len(p), nil
}

// clampUint16 maps a terminal dimension into uint16 range, guarding the int->uint16
// narrowing against a negative or absurdly large value rather than wrapping it.
func clampUint16(n int) uint16 {
	if n < 0 {
		return 0
	}
	if n > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(n)
}

// ttySize returns the controlling terminal's size, or a sane default when stdin is
// not a terminal (e.g. under a test harness).
func ttySize() *pty.Winsize {
	if rows, cols, err := pty.Getsize(os.Stdin); err == nil && rows > 0 && cols > 0 {
		return &pty.Winsize{Rows: clampUint16(rows), Cols: clampUint16(cols)}
	}
	return &pty.Winsize{Rows: 24, Cols: 80}
}

// ptyServer serves the control socket; Close stops accepting and waits for in-flight
// handlers, so no handler reads the grid after Run frees it. Live connections are
// tracked so Close can expire their deadlines: the drain is bounded by unblocking
// every handler, never by trusting a client to finish its request.
type ptyServer struct {
	ln      net.Listener
	wg      sync.WaitGroup
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

func serve(socket string, g grid, child io.Writer) (*ptyServer, error) {
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return nil, fmt.Errorf("pty-host socket dir: %w", err)
	}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("pty-host listen %s: %w", socket, err)
	}
	s := &ptyServer{ln: ln, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go s.accept(g, child)
	return s, nil
}

func (s *ptyServer) accept(g grid, child io.Writer) {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			s.track(conn)
			handleConn(conn, g, child)
		}()
	}
}

// track registers a live connection; a conn accepted while Close is already draining
// gets its deadline expired immediately, so it can never slip past the drain sweep.
func (s *ptyServer) track(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[conn] = struct{}{}
	if s.closing {
		_ = conn.SetDeadline(time.Now())
	}
}

func (s *ptyServer) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	_ = conn.Close()
}

// Close stops accepting, expires every in-flight connection's deadline so its handler
// unblocks with an I/O error, and waits for the handlers to drain. A wedged client
// that connected but never completes its request therefore cannot stall the wrapper's
// teardown (or the child-exit report behind it).
func (s *ptyServer) Close() error {
	err := s.ln.Close()
	s.mu.Lock()
	s.closing = true
	for conn := range s.conns {
		_ = conn.SetDeadline(time.Now())
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func handleConn(conn net.Conn, g grid, child io.Writer) {
	enc := json.NewEncoder(conn)
	var req request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		_ = enc.Encode(response{Err: "decode request: " + err.Error()})
		return
	}
	switch req.Op {
	case opCapture:
		_ = enc.Encode(response{Text: g.Text()})
	case opKeys:
		if _, err := child.Write(req.Data); err != nil {
			_ = enc.Encode(response{Err: "write keys: " + err.Error()})
			return
		}
		_ = enc.Encode(response{})
	default:
		_ = enc.Encode(response{Err: "unknown op: " + req.Op})
	}
}
