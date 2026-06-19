package backend

import "testing"

// TestCapsMatchSenderInterface enforces the invariant that a backend advertises
// CanSendText exactly when it implements Sender. Without this, a capability could
// promise a native path the driver cannot take (or a driver could grow a SendText
// method nobody dispatches to), and the send dispatcher's
// `Has(CanSendText) && bk.(Sender)` gate would silently fall back to the LCD.
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
