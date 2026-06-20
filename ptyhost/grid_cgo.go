//go:build cgo

package ptyhost

import (
	"io"
	"sync"

	lg "go.mitchellh.com/libghostty"
)

// ghosttyGrid is the cgo grid backed by go-libghostty (Ghostty's libghostty-vt). It is
// the default whenever a C toolchain is present (the CI release binaries); the pure-Go
// x/vt grid in grid_vt.go backs CGO_ENABLED=0 builds. The terminal answers query
// escapes through the WithWritePty callback, which routes the reply straight back to
// the child PTY — so unlike the x/vt grid there is no separate drain goroutine. A
// single mutex serializes Feed against Text and Close.
type ghosttyGrid struct {
	mu   sync.Mutex
	term *lg.Terminal
}

func newGrid(cols, rows int, reply io.Writer) grid {
	term, err := lg.NewTerminal(
		lg.WithSize(clampUint16(cols), clampUint16(rows)),
		lg.WithWritePty(func(_ *lg.Terminal, data []byte) {
			_, _ = reply.Write(data) // query replies (DA/DSR/cursor-position) go to the child
		}),
	)
	if err != nil {
		panic("ptyhost: libghostty NewTerminal: " + err.Error())
	}
	return &ghosttyGrid{term: term}
}

func (g *ghosttyGrid) Feed(p []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, _ = g.term.Write(p)
}

func (g *ghosttyGrid) Text() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, err := lg.NewFormatter(g.term,
		lg.WithFormatterFormat(lg.FormatterFormatPlain),
		lg.WithFormatterTrim(true),
	)
	if err != nil {
		return ""
	}
	defer f.Close()
	s, _ := f.FormatString()
	return s
}

func (g *ghosttyGrid) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.term.Close()
}
