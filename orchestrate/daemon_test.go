package orchestrate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-interact/daemon"
)

func testDaemonConfig(t *testing.T) daemon.Config {
	t.Helper()
	trustPolicy, err := appTrustPolicy()
	if err != nil {
		t.Fatal(err)
	}
	return daemon.Config{
		AppName: AppName, Paths: appPaths(), WireBuild: daemon.WireBuild, RuntimeBuild: buildVersion(),
		TrustPolicy: trustPolicy, Roles: appRoles(), ActiveStatuses: []string{string(StatusActive)}, StoreSchema: databaseStoreSchema(),
	}
}

func startTestDaemon(t *testing.T, s *daemon.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		client, err := newClient(probeCtx)
		if err == nil {
			health, healthErr := client.RuntimeHealth(probeCtx)
			if err := client.Close(); err != nil {
				t.Fatalf("close readiness client: %v", err)
			}
			probeCancel()
			if healthErr == nil && health.Ready {
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
				return
			}
		}
		probeCancel()
		select {
		case err := <-done:
			t.Fatalf("daemon exited before readiness: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	t.Fatalf("daemon did not become ready")
}
