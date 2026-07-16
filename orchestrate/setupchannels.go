package orchestrate

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-orchestrate/channelsetup"
)

var managedSettingsPath = channelsetup.ManagedSettingsPath

type channelSetupState struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

func setupChannelsCmd() *cobra.Command {
	var check, apply, decline bool
	c := &cobra.Command{
		Use:    "setup-channels",
		Hidden: true,
		Short:  "Approve cc-orchestrate channel delivery",
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch {
			case apply:
				return runChannelsApply(cmd.OutOrStdout())
			case decline:
				return writeChannelMarker("declined")
			default:
				return runChannelsCheck(cmd.OutOrStdout())
			}
		},
	}
	c.Flags().BoolVar(&check, "check", false, "print the first-run channel offer (default)")
	c.Flags().BoolVar(&apply, "apply", false, "approve channel delivery (prompts for admin)")
	c.Flags().BoolVar(&decline, "decline", false, "decline the channel delivery offer")
	c.MarkFlagsMutuallyExclusive("check", "apply", "decline")
	return c
}

func runChannelsCheck(out io.Writer) error {
	offer, reason, err := channelsOffer()
	if err != nil {
		return err
	}
	if err := json.NewEncoder(out).Encode(map[string]any{"offer": offer, "reason": reason}); err != nil {
		return fmt.Errorf("write channel offer: %w", err)
	}
	return nil
}

func channelsOffer() (bool, string, error) {
	if _, err := os.Stat(channelSetupMarker()); err == nil {
		return false, "already offered", nil
	} else if !os.IsNotExist(err) {
		return false, "", fmt.Errorf("stat channel marker: %w", err)
	}
	managed, err := readFileOrEmpty(managedSettingsPath())
	if err != nil {
		return false, "", err
	}
	approved, err := channelsetup.ManagedHasEntry(managed)
	if err != nil {
		return false, "", err
	}
	if approved {
		return false, "already approved", nil
	}
	return true, "channel not yet approved", nil
}

func runChannelsApply(out io.Writer) error {
	managed, err := readFileOrEmpty(managedSettingsPath())
	if err != nil {
		return err
	}
	merged, err := channelsetup.MergeManaged(managed)
	if err != nil {
		return err
	}
	if err := channelsetup.ApplyManagedViaAdmin(merged); err != nil {
		return err
	}
	if err := writeChannelMarker("done"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "Channel delivery is enabled. New agent spawns will now load the cc-orchestrate channel."); err != nil {
		return fmt.Errorf("write channel setup hint: %w", err)
	}
	return nil
}

func channelSetupMarker() string {
	return filepath.Join(appPaths().StateDir(), "channels-setup.json")
}

func writeChannelMarker(status string) error {
	if err := appPaths().EnsureStateDir(); err != nil {
		return fmt.Errorf("create app state dir: %w", err)
	}
	body, err := json.Marshal(channelSetupState{Status: status, Version: buildVersion()})
	if err != nil {
		return fmt.Errorf("encode channel marker: %w", err)
	}
	if err := os.WriteFile(channelSetupMarker(), body, 0o600); err != nil {
		return fmt.Errorf("write channel marker: %w", err)
	}
	return nil
}

func readFileOrEmpty(path string) ([]byte, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is the tool-owned Claude managed settings file.
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return b, nil
}
