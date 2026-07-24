package ptyhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

// Client owns one persistent multiplexed session to a pty-host.
type Client struct {
	socket string

	mu      sync.Mutex
	session *wire.Client
}

// Dial returns a lazy Client for the pty-host listening at socket.
func Dial(socket string) *Client { return &Client{socket: socket} }

// Capture returns the child's current rendered screen as plain text.
func (c *Client) Capture(ctx context.Context) (string, error) {
	var response captureResponse
	if err := c.call(ctx, opCapture, nil, &response); err != nil {
		return "", err
	}
	return response.Text, nil
}

// SendKeys encodes the given key tokens and writes them to the child PTY.
func (c *Client) SendKeys(ctx context.Context, keys ...string) error {
	payload, err := encodeMessage(keysRequest{Data: encodeKeys(keys)})
	if err != nil {
		return err
	}
	return c.call(ctx, opKeys, payload, nil)
}

// Close closes the persistent session when one was established.
func (c *Client) Close() error {
	c.mu.Lock()
	session := c.session
	c.session = nil
	c.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close()
}

func (c *Client) call(ctx context.Context, op wire.Op, payload []byte, dst any) error {
	session, err := c.getSession(ctx)
	if err != nil {
		return fmt.Errorf("pty-host %s: %w", op, err)
	}
	var result wire.Result
	for {
		result, err = session.Call(ctx, op, "", payload)
		if err != nil {
			c.retire(session, err)
			return fmt.Errorf("pty-host %s: %w", op, err)
		}
		rejection := result.Rejection()
		if !errors.Is(rejection, wire.ErrNotReady) {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("pty-host %s: %w", op, ctx.Err())
		case <-timer.C:
		}
	}
	if result.Response.Err != "" {
		return fmt.Errorf("pty-host %s: %s", op, result.Response.Err)
	}
	if result.Outcome != wire.Delivered {
		return fmt.Errorf("pty-host %s: %w", op, result.Rejection())
	}
	if dst == nil {
		if len(result.Response.Payload) != 0 && string(result.Response.Payload) != "{}" {
			return fmt.Errorf("pty-host %s returned unexpected payload", op)
		}
		return nil
	}
	if err := decodeMessage(result.Response.Payload, dst); err != nil {
		return fmt.Errorf("pty-host %s response: %w", op, err)
	}
	return nil
}

func (c *Client) getSession(ctx context.Context) (*wire.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		return c.session, nil
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:      wire.UnixDialer(c.socket),
		WireBuild: ptyWireBuild,
		Role:      trust.UnprotectedRole,
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.socket, err)
	}
	c.session = session
	return session, nil
}

func (c *Client) retire(session *wire.Client, cause error) {
	c.mu.Lock()
	if c.session != session {
		c.mu.Unlock()
		return
	}
	c.session = nil
	c.mu.Unlock()
	_ = session.Abort(errors.Join(errors.New("pty-host session failed"), cause))
}
