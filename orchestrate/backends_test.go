package orchestrate

import "testing"

func TestFormatBackends(t *testing.T) {
	cases := []struct {
		name string
		rows []backendRow
		want string
	}{
		{
			name: "mixed availability marks first available as default",
			rows: []backendRow{
				{name: "herd", available: true, isDefault: true},
				{name: "superset", available: false},
				{name: "cmux", available: true},
				{name: "zellij", available: false},
				{name: "tmux", available: false},
			},
			want: "BACKEND   INSTALLED  DEFAULT\n" +
				"herd      yes        *\n" +
				"superset  no\n" +
				"cmux      yes\n" +
				"zellij    no\n" +
				"tmux      no\n",
		},
		{
			name: "none available has no default marker",
			rows: []backendRow{
				{name: "herd", available: false},
				{name: "superset", available: false},
				{name: "cmux", available: false},
				{name: "zellij", available: false},
				{name: "tmux", available: false},
			},
			want: "BACKEND   INSTALLED  DEFAULT\n" +
				"herd      no\n" +
				"superset  no\n" +
				"cmux      no\n" +
				"zellij    no\n" +
				"tmux      no\n",
		},
		{
			name: "narrow set recomputes column widths",
			rows: []backendRow{
				{name: "herd", available: true, isDefault: true},
				{name: "tmux", available: false},
			},
			want: "BACKEND  INSTALLED  DEFAULT\n" +
				"herd     yes        *\n" +
				"tmux     no\n",
		},
		{
			name: "empty renders header only",
			rows: nil,
			want: "BACKEND  INSTALLED  DEFAULT\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatBackends(tc.rows); got != tc.want {
				t.Errorf("formatBackends() mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}
