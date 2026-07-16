// Command cc-orchestrate orchestrates fleets of Claude Code agents across
// pluggable backends, built as a consumer of the cc-interact substrate.
package main

import (
	"fmt"
	"os"

	"github.com/yasyf/cc-orchestrate/orchestrate"
)

func main() {
	root := orchestrate.Root()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
