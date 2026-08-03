package ptyhost

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"
)

// callTimeout bounds one control call whose caller carries no deadline of its
// own, which daemonkit refuses outright.
const callTimeout = 10 * time.Second

// readyPoll is the cadence a call retries a host that has bound its socket but
// not yet published readiness.
const readyPoll = 10 * time.Millisecond

// Client owns one business lane to a pty-host incarnation.
type Client struct {
	business *daemonkit.Business
}

// Dial returns a lazy Client for the pty-host of the incarnation spawnNonce
// names. It performs no I/O: the lane attaches on the first call and verifies
// the accepting process on every session it acquires.
func Dial(spawnNonce string) (*Client, error) {
	dk, err := daemonkit.Open(ptyDaemon(spawnNonce))
	if err != nil {
		return nil, err
	}
	return &Client{business: dk.Business()}, nil
}

// Capture returns the child's current rendered screen as plain text.
func (c *Client) Capture(ctx context.Context) (string, error) {
	reply, err := c.call(ctx, opCapture, nil)
	if err != nil {
		return "", err
	}
	var response captureResponse
	if err := decodeMessage(reply.Body, &response); err != nil {
		return "", fmt.Errorf("pty-host %s response: %w", opCapture, err)
	}
	return response.Text, nil
}

// SendKeys encodes the given key tokens and writes them to the child PTY.
func (c *Client) SendKeys(ctx context.Context, keys ...string) error {
	payload, err := encodeMessage(keysRequest{Data: encodeKeys(keys)})
	if err != nil {
		return err
	}
	_, err = c.call(ctx, opKeys, payload)
	return err
}

// Close releases the lane; every later call is refused.
func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	return c.business.Close(ctx)
}

// call dispatches one op, retrying a host that is still starting: the prober
// dials as soon as the spawn wrapper binds, routinely before the hosted child
// has cleared its gate.
func (c *Client) call(ctx context.Context, op string, payload []byte) (daemonkit.Reply, error) {
	ctx, cancel := operationContext(ctx)
	defer cancel()
	for {
		reply, err := c.business.Call(ctx, op, payload)
		if err == nil {
			return reply, nil
		}
		if !errors.Is(err, daemonkit.ErrNotReady) {
			return daemonkit.Reply{}, fmt.Errorf("pty-host %s: %w", op, err)
		}
		timer := time.NewTimer(readyPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return daemonkit.Reply{}, fmt.Errorf("pty-host %s: %w", op, ctx.Err())
		case <-timer.C:
		}
	}
}

func operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, callTimeout)
}
