package orchestrate

import (
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

// TestMain mirrors the consumer trust contract: tests that start the daemon make
// this test binary the runtime's trustExecutable, so it must dispatch the verifier
// child verb before anything else runs, or the serve-time self-probe refuses to
// start the daemon.
func TestMain(m *testing.M) {
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
