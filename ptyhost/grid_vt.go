//go:build !cgo

package ptyhost

import (
	"io"
	"sync"

	"github.com/charmbracelet/x/vt"
)

// vtGrid is the pure-Go grid backed by charmbracelet/x/vt. A single mutex serializes
// Feed (the PTY read-loop goroutine) against Text and Close: the emulator's plain-text
// accessor is not itself synchronized, and SafeEmulator only guards its ANSI Render,
// not the plain String we substring-match against.
//
// The emulator answers terminal query escapes (DA/DSR/cursor-position) by writing the
// reply into an internal io.Pipe; a drain goroutine copies that reply stream back to
// the child PTY so a TUI like claude's never blocks waiting for the answer — and the
// concurrent drain is also what lets a query reply unblock a Feed that is mid-parse.
// Close stops the drain by closing the reply pipe's writer (its EOF makes the drain's
// Read return); it deliberately avoids Emulator.Close, whose unsynchronized closed
// flag would data-race the drain's concurrent Read.
type vtGrid struct {
	mu      sync.Mutex
	e       *vt.Emulator
	pw      *io.PipeWriter
	drained chan struct{}
}

func newGrid(cols, rows int, reply io.Writer) grid {
	e := vt.NewEmulator(cols, rows)
	pw, ok := e.InputPipe().(*io.PipeWriter)
	if !ok {
		panic("ptyhost: vt.Emulator.InputPipe() is not *io.PipeWriter; x/vt internals changed")
	}
	g := &vtGrid{e: e, pw: pw, drained: make(chan struct{})}
	go func() {
		defer close(g.drained)
		_, _ = io.Copy(reply, e) // ends when Close closes pw → e.Read returns io.EOF
	}()
	return g
}

func (g *vtGrid) Feed(p []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, _ = g.e.Write(p)
}

func (g *vtGrid) Text() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.e.String()
}

func (g *vtGrid) Close() {
	g.mu.Lock()
	_ = g.pw.CloseWithError(io.EOF)
	g.mu.Unlock()
	<-g.drained
}
