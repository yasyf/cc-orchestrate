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
