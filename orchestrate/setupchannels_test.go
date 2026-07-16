package orchestrate

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSetupChannelsStateMachine(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	setManagedSettingsPath(t, filepath.Join(t.TempDir(), "managed-settings.json"))

	falseValue := false
	trueValue := true
	tests := []struct {
		name       string
		args       []string
		wantOffer  *bool
		wantReason string
		wantStatus string
	}{
		{name: "offer", wantOffer: &trueValue, wantReason: "channel not yet approved"},
		{name: "decline", args: []string{"--decline"}, wantStatus: "declined"},
		{name: "no further offer", wantOffer: &falseValue, wantReason: "already offered"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			cmd := setupChannelsCmd()
			cmd.SetOut(&out)
			cmd.SetArgs(tc.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("execute setup-channels: %v", err)
			}
			if tc.wantOffer != nil {
				var got struct {
					Offer  bool   `json:"offer"`
					Reason string `json:"reason"`
				}
				if err := json.Unmarshal(out.Bytes(), &got); err != nil {
					t.Fatalf("decode check output %q: %v", out.String(), err)
				}
				if got.Offer != *tc.wantOffer || got.Reason != tc.wantReason {
					t.Errorf("check = {offer:%t reason:%q}, want {offer:%t reason:%q}", got.Offer, got.Reason, *tc.wantOffer, tc.wantReason)
				}
			}
			if tc.wantStatus != "" {
				body, err := os.ReadFile(channelSetupMarker())
				if err != nil {
					t.Fatalf("read channel marker: %v", err)
				}
				var got channelSetupState
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("decode channel marker: %v", err)
				}
				if got.Status != tc.wantStatus || got.Version == "" {
					t.Errorf("marker = %+v, want status %q and a version", got, tc.wantStatus)
				}
			}
		})
	}
}

func TestSetupChannelsApprovalCheck(t *testing.T) {
	for _, tc := range []struct {
		name       string
		managed    string
		wantOffer  bool
		wantReason string
	}{
		{
			name:       "enabled and allowlisted is approved",
			managed:    `{"channelsEnabled":true,"allowedChannelPlugins":[{"marketplace":"cc-orchestrate","plugin":"cc-orchestrate"}]}`,
			wantOffer:  false,
			wantReason: "already approved",
		},
		{
			name:       "allowlisted but channels disabled still offers",
			managed:    `{"allowedChannelPlugins":[{"marketplace":"cc-orchestrate","plugin":"cc-orchestrate"}]}`,
			wantOffer:  true,
			wantReason: "channel not yet approved",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			managedPath := filepath.Join(t.TempDir(), "managed-settings.json")
			setManagedSettingsPath(t, managedPath)
			if err := os.WriteFile(managedPath, []byte(tc.managed), 0o600); err != nil {
				t.Fatalf("write managed settings: %v", err)
			}

			var out bytes.Buffer
			cmd := setupChannelsCmd()
			cmd.SetOut(&out)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("execute setup-channels: %v", err)
			}
			var got struct {
				Offer  bool   `json:"offer"`
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatalf("decode check output %q: %v", out.String(), err)
			}
			if got.Offer != tc.wantOffer || got.Reason != tc.wantReason {
				t.Errorf("check = {offer:%t reason:%q}, want {offer:%t reason:%q}", got.Offer, got.Reason, tc.wantOffer, tc.wantReason)
			}
		})
	}
}

func TestSetupChannelsFlagsMutuallyExclusive(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cmd := setupChannelsCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"--apply", "--decline"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("execute setup-channels --apply --decline: want error, got nil")
	}
}

func setManagedSettingsPath(t *testing.T, path string) {
	t.Helper()
	old := managedSettingsPath
	managedSettingsPath = func() string { return path }
	t.Cleanup(func() { managedSettingsPath = old })
}
