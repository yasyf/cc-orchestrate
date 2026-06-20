package ptyhost

// The control socket speaks one JSON request and one JSON response per connection.
const (
	opCapture = "capture" // reply with the rendered screen text
	opKeys    = "keys"    // write Data to the child PTY, reply empty
)

// request is one control-socket call.
type request struct {
	Op   string `json:"op"`
	Data []byte `json:"data,omitempty"` // raw bytes to write to the child PTY for opKeys
}

// response is the reply to a request; Err is set instead of a result on failure.
type response struct {
	Text string `json:"text,omitempty"`
	Err  string `json:"err,omitempty"`
}
