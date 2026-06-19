// Package ptyhost runs a child program under a pseudo-terminal it owns, mirrors the
// child's screen into a virtual grid, and serves a control socket so an out-of-band
// caller can read the rendered screen and inject keystrokes. cc-orchestrate uses it
// to drive a backend that cannot capture its own terminal natively (superset): the
// child claude runs under the host, its output still reaches the spawning terminal,
// and the prober answers the trust dialog by reading the grid and sending keys.
package ptyhost

// grid is a virtual terminal screen fed raw child-PTY bytes so the host can answer a
// CAPTURE with the rendered plain-text screen. newGrid is implemented per build tag:
// go-libghostty under cgo, charmbracelet/x/vt for pure-Go builds.
type grid interface {
	// Feed advances the screen by the given child-PTY output bytes.
	Feed(p []byte)
	// Text returns the current visible screen as plain text (no escape sequences).
	Text() string
	// Close releases the emulator.
	Close()
}
