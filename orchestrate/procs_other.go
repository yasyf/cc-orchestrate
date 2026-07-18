//go:build !darwin

package orchestrate

// procArgv is darwin-only: listDarwinClaudeProcs is never reached on other platforms (the
// linux enumerator reads /proc/<pid>/cmdline directly), so this stub only satisfies the
// compiler.
func procArgv(int) ([]string, error) {
	return nil, errUnsupportedPlatform
}
