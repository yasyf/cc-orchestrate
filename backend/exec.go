package backend

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

// runner runs an external backend CLI and returns its stdout. It is the single
// seam driver tests stub to assert the argv a driver builds without invoking the
// real binary; production drivers hold execRunner.
type runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// execRunner runs name with args and returns stdout, wrapping a non-zero exit
// with the captured stderr so the failure carries the backend's own diagnostic.
func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...) //nolint:gosec // G204: orchestrator invokes backend CLIs (tmux/cmux/...) by design
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%s %v: %w: %s", name, args, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// installed reports whether bin resolves on PATH; it backs every driver's
// Available and never returns an error.
func installed(bin string) bool {
	_, err := exec.LookPath(bin)
	return err == nil
}
