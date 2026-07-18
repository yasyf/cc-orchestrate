package orchestrate

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// procArgv reads a pid's real argv vector from the kern.procargs2 sysctl. A pid that has
// exited (or whose args are unreadable) returns an error and is dropped by the caller.
func procArgv(pid int) ([]string, error) {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return nil, fmt.Errorf("sysctl kern.procargs2 for %d: %w", pid, err)
	}
	return parseProcArgs2(buf)
}
