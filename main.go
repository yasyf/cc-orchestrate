// Command cc-orchestrate orchestrates fleets of Claude Code agents across
// pluggable backends, built as a consumer of the cc-interact substrate.
package main

import (
	"fmt"
	"os"

	"github.com/yasyf/daemonkit/trust"

	"github.com/yasyf/cc-orchestrate/orchestrate"
)

func main() {
	// Both hosted wire daemons (the cc-interact substrate daemon and the pty-host)
	// re-exec this binary as daemonkit's trust-verifier child for every connecting
	// peer; without this dispatch each daemon's serve-time self-probe fails and
	// every peer is rejected as untrusted.
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	root := orchestrate.Root()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
