package channelsetup

import (
	"encoding/json"
	"testing"
)

func TestMergeManaged(t *testing.T) {
	tests := []struct {
		name        string
		existing    string
		wantOther   bool
		wantDiscord bool
	}{
		{name: "empty"},
		{name: "preserves unrelated keys", existing: `{"otherKey":"keep","permissions":{"allow":["Bash"]}}`, wantOther: true},
		{name: "already approved", existing: `{"channelsEnabled":true,"allowedChannelPlugins":[{"marketplace":"cc-orchestrate","plugin":"cc-orchestrate"}]}`},
		{name: "appends beside another plugin", existing: `{"allowedChannelPlugins":[{"marketplace":"claude-plugins-official","plugin":"discord"}]}`, wantDiscord: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			merged, err := MergeManaged([]byte(tc.existing))
			if err != nil {
				t.Fatalf("MergeManaged: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(merged, &got); err != nil {
				t.Fatalf("decode merged settings: %v", err)
			}
			if got["channelsEnabled"] != true {
				t.Errorf("channelsEnabled = %v, want true", got["channelsEnabled"])
			}
			has, err := ManagedHasEntry(merged)
			if err != nil {
				t.Fatalf("ManagedHasEntry: %v", err)
			}
			if !has {
				t.Errorf("merged settings missing cc-orchestrate entry: %s", merged)
			}
			list, _ := got["allowedChannelPlugins"].([]any)
			if countEntry(list, Marketplace, Plugin) != 1 {
				t.Errorf("cc-orchestrate entry count = %d, want 1", countEntry(list, Marketplace, Plugin))
			}
			if tc.wantOther && got["otherKey"] != "keep" {
				t.Errorf("otherKey = %v, want keep", got["otherKey"])
			}
			if tc.wantDiscord && countEntry(list, "claude-plugins-official", "discord") != 1 {
				t.Errorf("discord entry was not preserved: %s", merged)
			}
			twice, err := MergeManaged(merged)
			if err != nil {
				t.Fatalf("second MergeManaged: %v", err)
			}
			if string(twice) != string(merged) {
				t.Errorf("merge is not idempotent:\nfirst=%s\nsecond=%s", merged, twice)
			}
		})
	}
}

func TestManagedHasEntry(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		want     bool
	}{
		{name: "missing", existing: `{}`},
		{name: "allowlisted but channels not enabled", existing: `{"allowedChannelPlugins":[{"marketplace":"cc-orchestrate","plugin":"cc-orchestrate"}]}`},
		{name: "enabled but not allowlisted", existing: `{"channelsEnabled":true,"allowedChannelPlugins":[]}`},
		{name: "enabled and allowlisted", existing: `{"channelsEnabled":true,"allowedChannelPlugins":[{"marketplace":"cc-orchestrate","plugin":"cc-orchestrate"}]}`, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ManagedHasEntry([]byte(tc.existing))
			if err != nil {
				t.Fatalf("ManagedHasEntry: %v", err)
			}
			if got != tc.want {
				t.Errorf("ManagedHasEntry = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestAllowlistWrongTypeErrors(t *testing.T) {
	bad := []byte(`{"allowedChannelPlugins":"not-a-list"}`)
	if _, err := MergeManaged(bad); err == nil {
		t.Error("MergeManaged on non-array allowlist: want error, got nil")
	}
	if _, err := ManagedHasEntry(bad); err == nil {
		t.Error("ManagedHasEntry on non-array allowlist: want error, got nil")
	}
}

// TestAdminScriptQuoting proves a hostile staging path (quotes from $TMPDIR)
// cannot break out of the privileged shell command.
func TestAdminScriptQuoting(t *testing.T) {
	got := adminScript("/L S/managed.json", `/t'mp/f".json`)
	want := `do shell script "mkdir -p '/L S' && cp '/t'\\''mp/f\".json' '/L S/managed.json' && chmod 644 '/L S/managed.json'" with administrator privileges`
	if got != want {
		t.Fatalf("adminScript =\n%s\nwant\n%s", got, want)
	}
}

func countEntry(list []any, marketplace, plugin string) int {
	count := 0
	for _, entry := range list {
		obj, ok := entry.(map[string]any)
		if ok && obj["marketplace"] == marketplace && obj["plugin"] == plugin {
			count++
		}
	}
	return count
}
