package ptyhost

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
)

// Client talks to a pty-host's control socket to read the child's screen and inject
// keystrokes. Each call opens a short-lived connection, so a Client is reusable and
// needs no explicit close.
type Client struct{ socket string }

// Dial returns a Client for the pty-host listening at socket.
func Dial(socket string) *Client { return &Client{socket: socket} }

// Capture returns the child's current rendered screen as plain text.
func (c *Client) Capture(ctx context.Context) (string, error) {
	resp, err := c.do(ctx, request{Op: opCapture})
	if err != nil {
		return "", err
	}
	return resp.Text, nil
}

// SendKeys encodes the given key tokens — named keys like "Enter"/"Down" or literal
// text — to their terminal byte sequences and writes them to the child PTY.
func (c *Client) SendKeys(ctx context.Context, keys ...string) error {
	_, err := c.do(ctx, request{Op: opKeys, Data: encodeKeys(keys)})
	return err
}

func (c *Client) do(ctx context.Context, req request) (response, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socket)
	if err != nil {
		return response{}, fmt.Errorf("pty-host dial %s: %w", c.socket, err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return response{}, fmt.Errorf("pty-host send %s: %w", req.Op, err)
	}
	var resp response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return response{}, fmt.Errorf("pty-host recv %s: %w", req.Op, err)
	}
	if resp.Err != "" {
		return response{}, fmt.Errorf("pty-host %s: %s", req.Op, resp.Err)
	}
	return resp, nil
}
