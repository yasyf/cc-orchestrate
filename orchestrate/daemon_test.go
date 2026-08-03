package orchestrate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/daemonkit"
)

func testDaemonConfig(t *testing.T) daemon.Config {
	t.Helper()
	d, err := appDaemon()
	if err != nil {
		t.Fatal(err)
	}
	return daemon.Config{
		AppName: AppName, Paths: appPaths(), Daemon: d, RuntimeBuild: buildVersion(),
		ActiveStatuses: []string{string(StatusActive)}, StoreSchema: databaseStoreSchema(),
	}
}

// startTestDaemon serves cfg in-process and returns once it answers business
// requests. Readiness rides the business lane, not daemonkit's Client.WaitReady:
// that subscribes over the control lane, which Trust.Control gates on the
// release signing identity a test binary cannot carry.
func startTestDaemon(t *testing.T, s *daemon.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("daemon serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	client, err := newClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), time.Second)
		_, probeErr := client.Do(probeCtx, daemon.Envelope{Op: daemon.OpStatus, Session: AppName})
		probeCancel()
		if probeErr == nil {
			return
		}
		// ErrAbsent is the window before the socket binds; ErrNotReady is the one
		// before the product publishes. Anything else is an answer, not a state.
		if !errors.Is(probeErr, daemonkit.ErrAbsent) && !errors.Is(probeErr, daemonkit.ErrNotReady) {
			t.Fatalf("readiness probe: %v", probeErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not become ready")
		}
		select {
		case err := <-done:
			t.Fatalf("daemon exited before readiness: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
