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
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"golang.org/x/sys/unix"
)

const (
	ptyShutdownTimeout                 = 5 * time.Second
	ptyChildRecoveryID proc.RecoveryID = "cc-orchestrate.pty-child.v1"
	ptyPauseScript                     = `trap ':' TERM
printf r >&3
exec 3>&-
if ! IFS= read -r marker <&4 || [ "$marker" != start ]; then exit 125; fi
exec 4<&-
trap - TERM
exec "$@"`
)

// Options configures a Run.
type Options struct {
	Socket       string
	ProcessStore string
	Argv         []string
	RuntimeBuild string
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
	if opts.ProcessStore == "" {
		return errors.New("pty-host process store is required")
	}

	components, err := newRuntime(opts)
	if err != nil {
		return err
	}
	activation, err := components.runtime.Begin(parent)
	if err != nil {
		return err
	}
	if err := recoverPTYChild(activation, components.reaper); err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, components.runtime.Wait(context.Background()))
	}
	settlement, err := activation.ClaimProductSettlement()
	if err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, components.runtime.Wait(context.Background()))
	}
	product, err := startPTYProduct(parent, opts.Argv, components.reaper)
	if err != nil {
		_ = activation.Fail(err)
		<-activation.Context().Done()
		return errors.Join(err, settlement.Complete(), components.runtime.Wait(context.Background()))
	}
	settled := make(chan error, 1)
	go func() {
		<-activation.Context().Done()
		ctx, cancel := context.WithTimeout(context.Background(), ptyShutdownTimeout)
		defer cancel()
		if closeErr := product.Close(ctx); closeErr != nil {
			settled <- closeErr
			return
		}
		settled <- settlement.Complete()
	}()
	publication, err := components.products.Stage(activation, product)
	if err == nil {
		err = activation.CommitReady(publication)
	}
	if err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, <-settled, components.runtime.Wait(context.Background()))
	}
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- components.runtime.Wait(context.Background()) }()

	var childErr, runtimeErr error
	naturalExit := false
	select {
	case <-product.child.done:
		naturalExit = parent.Err() == nil && activation.Context().Err() == nil
		childErr = product.child.Result()
		runtimeErr = closeRuntime(parent, components.runtime, runtimeDone)
	case <-parent.Done():
		runtimeErr = closeRuntime(parent, components.runtime, runtimeDone)
		childErr = product.child.Result()
	case runtimeErr = <-runtimeDone:
		childErr = product.child.Result()
	}
	settlementErr := <-settled

	if opts.OnChildExit != nil && naturalExit && runtimeErr == nil && settlementErr == nil {
		opts.OnChildExit()
	}
	return errors.Join(childErr, runtimeErr, settlementErr)
}

type runtimeComponents struct {
	runtime  *daemon.Runtime
	products *daemon.PublicationSlot[*ptyProduct]
	reaper   *proc.Reaper
}

func newRuntime(opts Options) (runtimeComponents, error) {
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return runtimeComponents{}, fmt.Errorf("pty-host process generation: %w", err)
	}
	store := &proc.FileStore{Path: opts.ProcessStore}
	reaper := &proc.Reaper{
		Store: store, Generation: generation,
		Grace: 500 * time.Millisecond, Settlement: 2 * time.Second,
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 0, MaxTotalRun: ptyShutdownTimeout,
		MaxStdinBytes: 0, MaxStdoutBytes: 4096, MaxStderrBytes: 4096,
	}, reaper)
	if err != nil {
		return runtimeComponents{}, fmt.Errorf("pty-host worker pool: %w", err)
	}
	children, err := proc.NewManager(1, reaper)
	if err != nil {
		return runtimeComponents{}, fmt.Errorf("pty-host process manager: %w", err)
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
	})
	if err != nil {
		return runtimeComponents{}, fmt.Errorf("pty-host trust policy: %w", err)
	}
	server := &wire.Server{
		WireBuild: ptyWireBuild, MaxSessions: 8, HandshakeTimeout: 500 * time.Millisecond,
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: opts.Socket, RuntimeBuild: opts.RuntimeBuild, RuntimeProtocol: int(wire.ProtocolVersion),
		Wire: server, TrustPolicy: policy, StopControlStore: store, Workers: workers, Children: children,
		ShutdownTimeout: ptyShutdownTimeout,
	})
	if err != nil {
		return runtimeComponents{}, fmt.Errorf("pty-host runtime: %w", err)
	}
	products := daemon.NewPublicationSlot[*ptyProduct](runtime)
	server.Register(wire.HandlerSpec{Op: opCapture, Concurrent: true, Handler: func(_ context.Context, request wire.Request) (any, error) {
		if len(request.Payload) != 0 {
			return nil, errors.New("pty-host capture payload must be empty")
		}
		product, ok := products.LoadPinned(request.Publication)
		if !ok {
			return nil, daemon.ErrPublicationStale
		}
		return captureResponse{Text: product.resources.grid.Text()}, nil
	}})
	server.Register(wire.HandlerSpec{Op: opKeys, Concurrent: true, Handler: func(_ context.Context, request wire.Request) (any, error) {
		var message keysRequest
		if err := decodeMessage(request.Payload, &message); err != nil {
			return nil, err
		}
		product, ok := products.LoadPinned(request.Publication)
		if !ok {
			return nil, daemon.ErrPublicationStale
		}
		if _, err := product.resources.ptmx.Write(message.Data); err != nil {
			return nil, fmt.Errorf("pty-host write keys: %w", err)
		}
		return struct{}{}, nil
	}})
	return runtimeComponents{runtime: runtime, products: products, reaper: reaper}, nil
}

func closeRuntime(parent context.Context, runtime *daemon.Runtime, done <-chan error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), ptyShutdownTimeout)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	return <-done
}

func recoverPTYChild(activation daemon.Activation, reaper *proc.Reaper) error {
	capability, err := activation.RecoveryCapability(ptyChildRecoveryID)
	if err != nil {
		return fmt.Errorf("pty-host recovery capability: %w", err)
	}
	receipt := capability.Receipt()
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("pty-host recovery proof: %w", err)
	}
	settled := receipt.Settled()
	observed := make(map[proc.OwnerGeneration]struct{}, len(settled))
	if _, err := reaper.RecoverReapReceipts(
		activation.Context(),
		ptyChildRecoveryID,
		func(ctx context.Context, reap proc.ReapReceipt) error {
			if err := reaper.VerifyReapReceipt(ctx, reap); err != nil {
				return err
			}
			if !slices.Contains(settled, reap.Record.Generation) {
				return errors.New("pty-host reap receipt is outside the runtime recovery proof")
			}
			observed[reap.Record.Generation] = struct{}{}
			return nil
		},
	); err != nil {
		return fmt.Errorf("pty-host settle recovery receipts: %w", err)
	}
	for _, generation := range settled {
		if _, ok := observed[generation]; !ok {
			return errors.New("pty-host runtime recovery proof has no durable reap receipt")
		}
	}
	if err := capability.Consume(); err != nil {
		return fmt.Errorf("pty-host consume recovery capability: %w", err)
	}
	return nil
}

type ptyProduct struct {
	child     *childWorker
	resources *ptyResources
}

func (p *ptyProduct) Close(ctx context.Context) error {
	return errors.Join(p.child.Close(ctx), p.resources.Close())
}

func startPTYProduct(ctx context.Context, argv []string, reaper *proc.Reaper) (*ptyProduct, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ws := ttySize()
	cmd, ptmx, record, release, err := prepareTrackedPTY(ctx, argv, ws, reaper)
	if err != nil {
		return nil, err
	}
	child := newChildWorker(cmd, reaper, record)

	inputFD, err := unix.Dup(int(os.Stdin.Fd()))
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), ptyShutdownTimeout)
		defer cancel()
		return nil, errors.Join(fmt.Errorf("pty-host duplicate stdin: %w", err), child.Close(cleanupCtx), ptmx.Close())
	}
	if err := unix.SetNonblock(inputFD, true); err != nil {
		_ = unix.Close(inputFD)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), ptyShutdownTimeout)
		defer cancel()
		return nil, errors.Join(fmt.Errorf("pty-host make stdin relay nonblocking: %w", err), child.Close(cleanupCtx), ptmx.Close())
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
		copyPTYInput(inputCtx, ptmx, input)
	}()
	resources := &ptyResources{
		winch: winch, ptmx: ptmx, readDone: readDone, grid: g,
		cancelInput: cancelInput, input: input, inputDone: inputDone,
	}
	product := &ptyProduct{child: child, resources: resources}
	if _, err := io.WriteString(release, "start\n"); err != nil {
		_ = release.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), ptyShutdownTimeout)
		defer cancel()
		return nil, errors.Join(fmt.Errorf("pty-host release child: %w", err), product.Close(cleanupCtx))
	}
	if err := release.Close(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), ptyShutdownTimeout)
		defer cancel()
		return nil, errors.Join(fmt.Errorf("pty-host close child release: %w", err), product.Close(cleanupCtx))
	}
	return product, nil
}

func prepareTrackedPTY(
	ctx context.Context,
	argv []string,
	ws *pty.Winsize,
	reaper *proc.Reaper,
) (*exec.Cmd, *os.File, proc.Record, *os.File, error) {
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return nil, nil, proc.Record{}, nil, fmt.Errorf("pty-host create readiness pipe: %w", err)
	}
	startRead, startWrite, err := os.Pipe()
	if err != nil {
		_ = readyRead.Close()
		_ = readyWrite.Close()
		return nil, nil, proc.Record{}, nil, fmt.Errorf("pty-host create release pipe: %w", err)
	}
	closePipes := func() {
		_ = readyRead.Close()
		_ = readyWrite.Close()
		_ = startRead.Close()
		_ = startWrite.Close()
	}
	args := append([]string{"-c", ptyPauseScript, "cc-orchestrate-pty-child"}, argv...)
	cmd := exec.Command("/bin/sh", args...) //nolint:gosec // The caller supplies argv after the fixed readiness wrapper.
	cmd.ExtraFiles = []*os.File{readyWrite, startRead}
	ptmx, err := pty.StartWithSize(cmd, ws)
	if err != nil {
		closePipes()
		return nil, nil, proc.Record{}, nil, fmt.Errorf("pty-host start %s: %w", argv[0], err)
	}
	_ = readyWrite.Close()
	_ = startRead.Close()

	deadline := time.Now().Add(ptyShutdownTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := readyRead.SetReadDeadline(deadline); err != nil {
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		_ = readyRead.Close()
		_ = startWrite.Close()
		_ = ptmx.Close()
		return nil, nil, proc.Record{}, nil, errors.Join(fmt.Errorf("pty-host bound child readiness: %w", err), waitErr)
	}
	var ready [1]byte
	_, readErr := io.ReadFull(readyRead, ready[:])
	_ = readyRead.Close()
	if readErr != nil || ready[0] != 'r' {
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		_ = startWrite.Close()
		_ = ptmx.Close()
		if readErr == nil {
			readErr = errors.New("invalid readiness byte")
		}
		return nil, nil, proc.Record{}, nil, errors.Join(fmt.Errorf("pty-host child readiness: %w", readErr), waitErr)
	}
	record, err := reaper.TrackGroup(ctx, cmd.Process.Pid, ptyChildRecoveryID)
	if err != nil {
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		_ = startWrite.Close()
		_ = ptmx.Close()
		return nil, nil, proc.Record{}, nil, errors.Join(fmt.Errorf("pty-host track child: %w", err), waitErr)
	}
	return cmd, ptmx, record, startWrite, nil
}

type childWorker struct {
	cmd    *exec.Cmd
	reaper *proc.Reaper
	record proc.Record
	done   chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
	closeErr  error
}

func newChildWorker(cmd *exec.Cmd, reaper *proc.Reaper, record proc.Record) *childWorker {
	w := &childWorker{cmd: cmd, reaper: reaper, record: record, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		close(w.done)
	}()
	return w
}

func (w *childWorker) Close(ctx context.Context) error {
	w.closeOnce.Do(func() {
		select {
		case <-w.done:
			w.closeErr = w.reaper.Untrack(ctx, w.record)
		default:
			w.closeErr = w.reaper.Terminate(ctx, w.record)
			if w.closeErr == nil {
				select {
				case <-w.done:
				case <-ctx.Done():
					w.closeErr = ctx.Err()
				}
			}
		}
	})
	return w.closeErr
}

func (w *childWorker) Result() error {
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
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

func copyPTYInput(ctx context.Context, dst, src *os.File) {
	buffer := make([]byte, 32*1024)
	poll := []unix.PollFd{{Fd: int32(src.Fd()), Events: unix.POLLIN}}
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
