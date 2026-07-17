// Package channelsetup approves the cc-orchestrate plugin as a Claude channel.
package channelsetup

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/yasyf/cc-orchestrate/backend"
)

const (
	// Marketplace identifies the cc-orchestrate plugin marketplace.
	Marketplace = "cc-orchestrate"
	// Plugin identifies the cc-orchestrate channel plugin.
	Plugin = "cc-orchestrate"
	// ChannelID is the Claude channel identifier for the cc-orchestrate plugin.
	ChannelID = "plugin:" + Plugin + "@" + Marketplace
	// ChannelSource is the source attribute Claude renders on this plugin's
	// channel tags: plugin:<plugin>:<mcp server name>.
	ChannelSource = "plugin:" + Plugin + ":cc-orchestrate"
)

// ManagedSettingsPath returns Claude's machine-wide managed settings path.
func ManagedSettingsPath() string {
	if runtime.GOOS == "darwin" {
		return "/Library/Application Support/ClaudeCode/managed-settings.json"
	}
	return "/etc/claude-code/managed-settings.json"
}

// MergeManaged enables channels and adds cc-orchestrate to the plugin allowlist.
func MergeManaged(existing []byte) ([]byte, error) {
	m, err := decodeObject(existing)
	if err != nil {
		return nil, err
	}
	list, err := allowlist(m)
	if err != nil {
		return nil, err
	}
	m["channelsEnabled"] = true
	if !allowlistHasEntry(list) {
		list = append(list, map[string]any{"marketplace": Marketplace, "plugin": Plugin})
	}
	m["allowedChannelPlugins"] = list
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode managed settings: %w", err)
	}
	return append(out, '\n'), nil
}

// ManagedHasEntry reports whether channel delivery is already approved: channels
// enabled and cc-orchestrate in the plugin allowlist.
func ManagedHasEntry(existing []byte) (bool, error) {
	m, err := decodeObject(existing)
	if err != nil {
		return false, err
	}
	list, err := allowlist(m)
	if err != nil {
		return false, err
	}
	return m["channelsEnabled"] == true && allowlistHasEntry(list), nil
}

func allowlist(m map[string]any) ([]any, error) {
	raw, ok := m["allowedChannelPlugins"]
	if !ok {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("managed settings: allowedChannelPlugins is not an array")
	}
	return list, nil
}

// ApplyManagedViaAdmin writes merged settings through a macOS admin prompt.
func ApplyManagedViaAdmin(merged []byte) (retErr error) {
	dest := ManagedSettingsPath()
	tmp, err := os.CreateTemp("", "cc-orchestrate-managed-*.json")
	if err != nil {
		return fmt.Errorf("create temp settings: %w", err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			if err := os.Remove(tmpPath); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("remove temp settings: %w", err))
			}
		}
	}()
	if _, err := tmp.Write(merged); err != nil {
		return errors.Join(fmt.Errorf("write temp settings: %w", err), closeTempSettings(tmp))
	}
	if err := closeTempSettings(tmp); err != nil {
		return err
	}
	if runtime.GOOS != "darwin" {
		removeTemp = false
		return fmt.Errorf("automatic managed-settings write is macOS-only; run: sudo install -d -m 755 %q && sudo install -m 644 %q %q", filepath.Dir(dest), tmpPath, dest)
	}
	if out, err := exec.Command("osascript", "-e", adminScript(dest, tmpPath)).CombinedOutput(); err != nil { //nolint:gosec // G204: adminScript shell-quotes + AppleScript-escapes every path (TestAdminScriptQuoting)
		return fmt.Errorf("admin write of %s (%s): %w", dest, strings.TrimSpace(string(out)), err)
	}
	return nil
}

// adminScript builds the privileged AppleScript, shell-quoting each path and
// escaping the whole command for the AppleScript string literal so no path
// (notably the $TMPDIR-derived staging file) can inject into the root shell.
func adminScript(dest, tmpPath string) string {
	shellCmd := fmt.Sprintf("mkdir -p %s && cp %s %s && chmod 644 %s",
		backend.ShellQuote(filepath.Dir(dest)), backend.ShellQuote(tmpPath), backend.ShellQuote(dest), backend.ShellQuote(dest))
	return `do shell script "` + appleScriptQuote(shellCmd) + `" with administrator privileges`
}

// appleScriptQuote escapes s for interpolation inside an AppleScript
// double-quoted string literal.
func appleScriptQuote(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s)
}

func closeTempSettings(tmp *os.File) error {
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp settings: %w", err)
	}
	return nil
}

func decodeObject(b []byte) (map[string]any, error) {
	m := map[string]any{}
	if len(b) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse managed settings: %w", err)
	}
	return m, nil
}

func allowlistHasEntry(list []any) bool {
	for _, entry := range list {
		obj, ok := entry.(map[string]any)
		if ok && obj["marketplace"] == Marketplace && obj["plugin"] == Plugin {
			return true
		}
	}
	return false
}
