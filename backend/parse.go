package backend

import (
	"encoding/json"
	"fmt"
	"strings"
)

// decodeJSON unmarshals b into a T, wrapping a parse failure with who (the
// driver name) and what (the noun being parsed) so the error stays specific.
func decodeJSON[T any](b []byte, who, what string) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("%s: cannot parse %s: %w", who, what, err)
	}
	return v, nil
}

// nonEmptyLines splits b on newlines, trims each line, and drops the empties.
func nonEmptyLines(b []byte) []string {
	lines := []string{}
	for _, line := range strings.Split(string(b), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}
