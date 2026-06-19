package ptyhost

// keyBytes maps a named key token to the bytes a terminal sends when it is pressed.
// The escape sequences follow the xterm/VT100 conventions used by
// montanaflynn/headless-terminal's internal/keys table (MIT). A token absent from
// this map is sent as its literal UTF-8 bytes, so encodeKeys covers both named keys
// (e.g. "Enter", "Down") and literal text in one sequence.
var keyBytes = map[string][]byte{
	"Enter":     {'\r'},
	"Tab":       {'\t'},
	"Escape":    {0x1b},
	"Space":     {' '},
	"Backspace": {0x7f},
	"Up":        {0x1b, '[', 'A'},
	"Down":      {0x1b, '[', 'B'},
	"Right":     {0x1b, '[', 'C'},
	"Left":      {0x1b, '[', 'D'},
	"Home":      {0x1b, '[', 'H'},
	"End":       {0x1b, '[', 'F'},
}

// IsNamedKey reports whether token is a named key (e.g. "Enter", "Down") rather than
// literal text, so a caller limited to sending literal text (a native Sender) can
// tell a bare named key has no representation on that path.
func IsNamedKey(token string) bool {
	_, ok := keyBytes[token]
	return ok
}

// encodeKeys turns a sequence of key tokens into the bytes to write to the child
// PTY: each token is a named key (see keyBytes) or, failing that, literal text.
func encodeKeys(keys []string) []byte {
	var out []byte
	for _, k := range keys {
		if b, ok := keyBytes[k]; ok {
			out = append(out, b...)
		} else {
			out = append(out, k...)
		}
	}
	return out
}
