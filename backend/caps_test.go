package backend

import "testing"

// TestCapsMatchSenderInterface enforces the invariant that a backend advertises
// CanSendText exactly when it implements Sender, so the startup prober never asks
// a backend to answer a blocked prompt through a path its driver cannot take.
func TestCapsMatchSenderInterface(t *testing.T) {
	for name, b := range registry {
		_, isSender := b.(Sender)
		if has := b.Caps().Has(CanSendText); has != isSender {
			t.Errorf("%s: Caps().Has(CanSendText) = %v but implements Sender = %v; they must agree", name, has, isSender)
		}
	}
}

// TestCapsMatchCapturerInterface enforces the same invariant for capture: a backend
// advertises CanCapture exactly when it implements Capturer, so the prober's
// `Has(CanCapture) && bk.(Capturer)` gate never promises a native screen read the
// driver cannot perform (nor leaves a Capturer method nobody dispatches to).
func TestCapsMatchCapturerInterface(t *testing.T) {
	for name, b := range registry {
		_, isCapturer := b.(Capturer)
		if has := b.Caps().Has(CanCapture); has != isCapturer {
			t.Errorf("%s: Caps().Has(CanCapture) = %v but implements Capturer = %v; they must agree", name, has, isCapturer)
		}
	}
}
