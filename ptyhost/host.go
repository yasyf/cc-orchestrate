package ptyhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yasyf/daemonkit"
	"golang.org/x/sys/unix"
)

const (
	ptyShutdown  = daemonkit.Grace(5 * time.Second)
	ptyHandshake = daemonkit.Grace(500 * time.Millisecond)

	// ptyStartup bounds the parked child's readiness signal and its adoption,
	// so a wrapper whose child never reaches the gate fails instead of hanging
	// the spawn.
	ptyStartup = 5 * time.Second

	// ptyLabelPrefix names this binary's per-incarnation pty daemons. Every
	// path one owns — socket, record file, state dir — derives from the label,
	// so the prefix is what makes an abandoned incarnation's residue greppable.
	ptyLabelPrefix = "com.yasyf.cco-pty."
)

// ptyDaemon is the one declaration the host and the prober client both read.
// The label carries 64 bits of the spawn nonce's SHA-256 — wide enough that two
// incarnations of one session can never collide — which is what makes a
// kill-driven respawn race-free: the replacement derives its own label, so
// settling the old incarnation's listener cannot disturb the replacement's
// socket. Program is unset, because the spawn wrapper's argv starts the host
// and nothing ever Ensures it.
func ptyDaemon(spawnNonce string) daemonkit.Daemon {
	if spawnNonce == "" {
		panic("pty daemon requires spawn nonce")
	}
	sum := sha256.Sum256([]byte(spawnNonce))
	return daemonkit.Daemon{
		Label:     daemonkit.Label(ptyLabelPrefix + hex.EncodeToString(sum[:8])),
		Schemas:   []daemonkit.Schema{ptyWireBuild},
		Shutdown:  ptyShutdown,
		Handshake: ptyHandshake,
		Trust:     daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
	}
}

// Options configures a Run.
type Options struct {
	SpawnNonce  string
	Argv        []string
	OnChildExit func()
}

// Run hosts opts.Argv under a pseudo-terminal and serves its exact v1 control
// protocol for this incarnation's lifetime.
func Run(parent context.Context, opts Options) error {
	if len(opts.Argv) == 0 {
		return errors.New("pty-host child argv is required")
	}
	d := ptyDaemon(opts.SpawnNonce)
	defer func() { _ = os.RemoveAll(filepath.Dir(d.RecordPath())) }()

	var product *ptyProduct
	_, err := daemonkit.Serve(parent, d, func(c daemonkit.Ctx) (daemonkit.Product, error) { //nolint:contextcheck // resources live until Product.Close, not until a ctx cancels
		started, startErr := startPTYProduct(c, opts.Argv)
		if startErr != nil {
			return nil, startErr
		}
		product = started
		return started, nil
	})
	if product == nil {
		return err
	}
	if opts.OnChildExit != nil && err == nil && parent.Err() == nil && product.natural.Load() {
		opts.OnChildExit()
	}
	return errors.Join(product.childResult(), err)
}

// ptyProduct is the hosted child and the screen it renders into. The child is
// adopted rather than spawned because creack/pty owns the fork; daemonkit
// never wait(2)s an adopted process, so the reap stays here.
type ptyProduct struct {
	tracked   *daemonkit.Tracked
	resources *ptyResources

	exited  chan struct{}
	waitErr error
	natural atomic.Bool

	drainOnce sync.Once
	drainErr  error
	closeOnce sync.Once
	closeErr  error
}

func (p *ptyProduct) Handle(_ context.Context, req daemonkit.Request) (daemonkit.Reply, error) {
	switch req.Op {
	case opCapture:
		if len(req.Body) != 0 {
			return daemonkit.Reply{}, errors.New("pty-host capture payload must be empty")
		}
		body, err := encodeMessage(captureResponse{Text: p.resources.grid.Text()})
		if err != nil {
			return daemonkit.Reply{}, err
		}
		return daemonkit.Reply{Body: body}, nil
	case opKeys:
		var message keysRequest
		if err := decodeMessage(req.Body, &message); err != nil {
			return daemonkit.Reply{}, err
		}
		if _, err := p.resources.ptmx.Write(message.Data); err != nil {
			return daemonkit.Reply{}, fmt.Errorf("pty-host write keys: %w", err)
		}
		return daemonkit.Reply{}, nil
	default:
		return daemonkit.Reply{}, fmt.Errorf("pty-host unknown op %q", req.Op)
	}
}

// Drain retires the child: the record is released when this process's own Wait
// already proved the exit, and signalled out of the process table otherwise.
// Retiring here rather than in Close is what keeps the screen the child is
// still writing into alive until the child is gone.
func (p *ptyProduct) Drain(budget daemonkit.Budget) error {
	p.drainOnce.Do(func() {
		ctx, cancel := budget.Context(context.Background())
		defer cancel()
		select {
		case <-p.exited:
			p.drainErr = p.tracked.Release()
			return
		default:
		}
		if _, err := p.tracked.Stop(ctx); err != nil {
			p.drainErr = err
			return
		}
		select {
		case <-p.exited:
		case <-ctx.Done():
			p.drainErr = ctx.Err()
		}
	})
	return p.drainErr
}

func (p *ptyProduct) Close(daemonkit.Budget) error {
	p.closeOnce.Do(func() { p.closeErr = p.resources.Close() })
	return p.closeErr
}

// childResult is the hosted child's own exit, reported only once this process
// reaped it. A drain that outran its budget leaves the child unproven, and its
// abandonment is already the failure Serve reports.
func (p *ptyProduct) childResult() error {
	select {
	case <-p.exited:
		return p.waitErr
	default:
		return nil
	}
}

func startPTYProduct(c daemonkit.Ctx, argv []string) (*ptyProduct, error) {
	gate, err := daemonkit.NewGate(argv)
	if err != nil {
		return nil, err
	}
	ws := ttySize()
	gateArgv := gate.Argv()
	cmd := exec.Command(gateArgv[0], gateArgv[1:]...) //nolint:gosec // the gate wrapper fixes argv[0]; the caller's argv is the parked target.
	cmd.ExtraFiles = gate.Files()
	ptmx, err := pty.StartWithSize(cmd, ws)
	if err != nil {
		_ = gate.Close()
		return nil, fmt.Errorf("pty-host start %s: %w", argv[0], err)
	}

	product := &ptyProduct{exited: make(chan struct{})}
	go func() {
		waitErr := cmd.Wait()
		product.waitErr = waitErr
		if c.Context.Err() == nil {
			product.natural.Store(true)
		}
		close(product.exited)
		c.Stop(nil)
	}()
	abort := func(cause error) (*ptyProduct, error) {
		_ = gate.Close()
		_ = cmd.Process.Kill()
		<-product.exited
		return nil, errors.Join(cause, product.waitErr, ptmx.Close())
	}

	startCtx, cancel := context.WithTimeout(c.Context, ptyStartup)
	defer cancel()
	if err := gate.Ready(startCtx); err != nil {
		return abort(fmt.Errorf("pty-host child readiness: %w", err))
	}
	tracked, err := c.Adopt(startCtx, cmd.Process.Pid)
	if err != nil {
		return abort(fmt.Errorf("pty-host adopt child: %w", err))
	}
	product.tracked = tracked

	resources, err := startPTYResources(ptmx, ws)
	if err != nil {
		return abort(err)
	}
	product.resources = resources
	if err := gate.Release(); err != nil {
		closeErr := resources.Close()
		_ = cmd.Process.Kill()
		<-product.exited
		return nil, errors.Join(fmt.Errorf("pty-host release child: %w", err), product.waitErr, closeErr)
	}
	return product, nil
}

func startPTYResources(ptmx *os.File, ws *pty.Winsize) (*ptyResources, error) {
	inputFD, err := unix.Dup(int(os.Stdin.Fd()))
	if err != nil {
		return nil, fmt.Errorf("pty-host duplicate stdin: %w", err)
	}
	if inputFD > math.MaxInt32 {
		_ = unix.Close(inputFD)
		return nil, errors.New("pty-host duplicated stdin exceeds poll descriptor range")
	}
	inputPollFD := int32(inputFD) //nolint:gosec // the descriptor is range-checked immediately above
	if err := unix.SetNonblock(inputFD, true); err != nil {
		_ = unix.Close(inputFD)
		return nil, fmt.Errorf("pty-host make stdin relay nonblocking: %w", err)
	}
	input := os.NewFile(uintptr(inputFD), "pty-host-stdin")

	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	go func() {
		for range winch {
			_ = pty.InheritSize(os.Stdin, ptmx)
		}
	}()

	g := newGrid(int(ws.Cols), int(ws.Rows), ptmx)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.Copy(io.MultiWriter(gridWriter{g}, os.Stdout), ptmx)
	}()
	inputCtx, cancelInput := context.WithCancel(context.Background())
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		copyPTYInput(inputCtx, ptmx, input, inputPollFD)
	}()
	return &ptyResources{
		winch: winch, ptmx: ptmx, readDone: readDone, grid: g,
		cancelInput: cancelInput, input: input, inputDone: inputDone,
	}, nil
}

type ptyResources struct {
	winch       chan os.Signal
	ptmx        *os.File
	readDone    <-chan struct{}
	grid        grid
	cancelInput context.CancelFunc
	input       *os.File
	inputDone   <-chan struct{}
	once        sync.Once
	err         error
}

func (r *ptyResources) Close() error {
	r.once.Do(func() {
		signal.Stop(r.winch)
		close(r.winch)
		r.cancelInput()
		r.err = errors.Join(r.ptmx.Close(), r.input.Close())
		<-r.readDone
		<-r.inputDone
		r.grid.Close()
	})
	return r.err
}

func copyPTYInput(ctx context.Context, dst, src *os.File, inputFD int32) {
	buffer := make([]byte, 32*1024)
	poll := []unix.PollFd{{Fd: inputFD, Events: unix.POLLIN}}
	for {
		if ctx.Err() != nil {
			return
		}
		ready, err := unix.Poll(poll, 100)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return
		}
		if ready == 0 {
			continue
		}
		if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return
		}
		if poll[0].Revents&unix.POLLIN == 0 {
			continue
		}
		count, readErr := src.Read(buffer)
		if count > 0 {
			if _, writeErr := dst.Write(buffer[:count]); writeErr != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

type gridWriter struct{ g grid }

func (w gridWriter) Write(p []byte) (int, error) {
	w.g.Feed(p)
	return len(p), nil
}

func clampUint16(n int) uint16 {
	if n <= 0 {
		return 1
	}
	if n > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(n)
}

func ttySize() *pty.Winsize {
	if rows, cols, err := pty.Getsize(os.Stdin); err == nil && rows > 0 && cols > 0 {
		return &pty.Winsize{Rows: clampUint16(rows), Cols: clampUint16(cols)}
	}
	return &pty.Winsize{Rows: 24, Cols: 80}
}
