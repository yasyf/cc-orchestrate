package ptyhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

const ptyShutdownTimeout = 5 * time.Second

// Options configures a Run.
type Options struct {
	Socket       string
	Argv         []string
	RuntimeBuild string
	DaemonRole   wire.ProtectedSessionClassifier
	StopVerifier wire.StopControlVerifier
	OnChildExit  func()
}

// Run hosts opts.Argv under a pseudo-terminal and serves its exact v1 control
// protocol through a daemonkit-owned persistent session runtime.
func Run(parent context.Context, opts Options) error {
	if len(opts.Argv) == 0 {
		return errors.New("pty-host child argv is required")
	}
	if opts.RuntimeBuild == "" {
		return errors.New("pty-host runtime build is required")
	}
	if opts.DaemonRole == nil {
		return errors.New("pty-host daemon role is required")
	}
	if opts.StopVerifier == nil {
		return errors.New("pty-host stop verifier is required")
	}

	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	ws := ttySize()
	cmd := exec.CommandContext(ctx, opts.Argv[0], opts.Argv[1:]...) //nolint:gosec // G204: the caller supplies the child command.
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

	g := newGrid(int(ws.Cols), int(ws.Rows), ptmx)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.Copy(io.MultiWriter(gridWriter{g}, os.Stdout), ptmx)
	}()
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()

	resources := &ptyResources{winch: winch, ptmx: ptmx, readDone: readDone, grid: g}
	worker := newChildWorker(cmd)

	runtime, err := newRuntime(opts, g, ptmx, worker, resources)
	if err != nil {
		worker.Cancel()
		settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), ptyShutdownTimeout)
		_ = worker.Wait(settleCtx)
		settleCancel()
		_ = resources.Close()
		return err
	}
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- runtime.Run(ctx) }()
	readyDone := make(chan error, 1)
	go func() { readyDone <- runtime.WaitReady(ctx) }()

	var childErr, runtimeErr error
	naturalExit := false
	select {
	case runtimeErr = <-runtimeDone:
		childErr = worker.Result()
	case readyErr := <-readyDone:
		if readyErr != nil {
			childErr = worker.Result()
			runtimeErr = <-runtimeDone
			runtimeErr = errors.Join(readyErr, runtimeErr)
			break
		}
		select {
		case <-worker.done:
			naturalExit = true
			childErr = worker.Result()
			runtimeErr = closeRuntime(ctx, runtime, runtimeDone)
		case runtimeErr = <-runtimeDone:
			childErr = worker.Result()
		}
	}

	if opts.OnChildExit != nil && naturalExit && ctx.Err() == nil && runtimeErr == nil {
		opts.OnChildExit()
	}
	return errors.Join(childErr, runtimeErr)
}

func newRuntime(
	opts Options,
	g grid,
	child io.Writer,
	worker *childWorker,
	resources *ptyResources,
) (*daemon.Runtime, error) {
	server := &wire.Server{
		WireBuild:   ptyWireBuild,
		MaxSessions: 8,
		Trust: func(ctx context.Context, peer wire.Peer) error {
			accepted, err := opts.DaemonRole.Classify(ctx, peer)
			if err != nil {
				return err
			}
			if !accepted {
				return wire.ErrUntrustedPeer
			}
			return nil
		},
	}
	server.RegisterConcurrent(opCapture, func(_ context.Context, request wire.Request) (any, error) {
		if len(request.Payload) != 0 {
			return nil, errors.New("pty-host capture payload must be empty")
		}
		return captureResponse{Text: g.Text()}, nil
	})
	server.RegisterConcurrent(opKeys, func(_ context.Context, request wire.Request) (any, error) {
		var message keysRequest
		if err := decodeMessage(request.Payload, &message); err != nil {
			return nil, err
		}
		if _, err := child.Write(message.Data); err != nil {
			return nil, fmt.Errorf("pty-host write keys: %w", err)
		}
		return struct{}{}, nil
	})
	intake := &drain.Intake{}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket:                    opts.Socket,
		RuntimeBuild:              opts.RuntimeBuild,
		RuntimeProtocol:           int(wire.ProtocolVersion),
		Wire:                      server,
		Classifier:                opts.DaemonRole,
		ReservedProtectedSessions: 1,
		StopVerifier:              opts.StopVerifier,
		Admission:                 intake,
		Workers:                   worker,
		State:                     idleCloser{},
		Resources:                 resources,
		Activate:                  func(daemon.Activation) error { return nil },
		ShutdownTimeout:           ptyShutdownTimeout,
		Signals:                   make(chan os.Signal),
	})
	if err != nil {
		return nil, fmt.Errorf("pty-host runtime: %w", err)
	}
	return runtime, nil
}

func closeRuntime(parent context.Context, runtime *daemon.Runtime, done <-chan error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), ptyShutdownTimeout)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		return errors.Join(err, <-done)
	}
	return <-done
}

type childWorker struct {
	cmd  *exec.Cmd
	done chan struct{}

	cancelOnce sync.Once
	mu         sync.Mutex
	err        error
}

func newChildWorker(cmd *exec.Cmd) *childWorker {
	w := &childWorker{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		close(w.done)
	}()
	return w
}

func (*childWorker) Close() {}

func (w *childWorker) Cancel() {
	w.cancelOnce.Do(func() {
		if w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
	})
}

func (w *childWorker) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return nil
	}
}

func (w *childWorker) Result() error {
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

type ptyResources struct {
	winch    chan os.Signal
	ptmx     *os.File
	readDone <-chan struct{}
	grid     grid
	once     sync.Once
	err      error
}

func (r *ptyResources) Close() error {
	r.once.Do(func() {
		signal.Stop(r.winch)
		close(r.winch)
		r.err = r.ptmx.Close()
		<-r.readDone
		r.grid.Close()
	})
	return r.err
}

type idleCloser struct{}

func (idleCloser) Close() error { return nil }

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
