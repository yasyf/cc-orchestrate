package orchestrate

import (
	"bytes"
	"encoding/binary"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParsePSOutput(t *testing.T) {
	start1 := time.Date(2025, time.July, 5, 9, 15, 30, 0, time.Local)
	start2 := time.Date(2025, time.July, 6, 10, 0, 0, 0, time.Local)
	// Build the lstart column the way ps emits it, so parse round-trips the same instant.
	lstart := func(tm time.Time) string { return tm.Format(psStartLayout) }
	cases := []struct {
		name    string
		input   string
		want    []procMeta
		wantErr bool
	}{
		{
			name: "keeps claude rows with pid, ppid, and start time",
			input: "  101   1 " + lstart(start1) + " /Users/test/.local/bin/claude --resume sid prompt with spaces\n" +
				"102 1 " + lstart(start2) + " claude --model opus another prompt\n" +
				"103 1 " + lstart(start1) + " /usr/local/bin/claude-worker --resume sid\n" +
				"104 1 " + lstart(start1) + " node /usr/local/bin/claude\n",
			want: []procMeta{
				{pid: 101, ppid: 1, start: start1},
				{pid: 102, ppid: 1, start: start2},
			},
		},
		{
			name:    "row missing the start time fails",
			input:   "101 1 claude --resume sid\n",
			wantErr: true,
		},
		{
			name:    "malformed row fails",
			input:   "101 only-two-fields\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePSOutput([]byte(tc.input))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parsePSOutput() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parsePSOutput() = %+v, want %+v", got, tc.want)
			}
			for i, w := range tc.want {
				g := got[i]
				if g.pid != w.pid || g.ppid != w.ppid || !g.start.Equal(w.start) {
					t.Errorf("parsePSOutput()[%d] = %+v, want %+v", i, g, w)
				}
			}
		})
	}
}

func TestParseProcArgs2(t *testing.T) {
	// build synthesizes a kern.procargs2 buffer: LE int32 argc, exec path + NUL, pad NULs,
	// then the argv strings and (ignored) env strings, each NUL-terminated.
	build := func(argc int32, execPath string, pad int, args, env []string) []byte {
		var b bytes.Buffer
		_ = binary.Write(&b, binary.LittleEndian, argc)
		b.WriteString(execPath)
		b.WriteByte(0)
		for range pad {
			b.WriteByte(0)
		}
		for _, a := range args {
			b.WriteString(a)
			b.WriteByte(0)
		}
		for _, e := range env {
			b.WriteString(e)
			b.WriteByte(0)
		}
		return b.Bytes()
	}
	t.Run("parses argv, skipping exec path, padding, and env", func(t *testing.T) {
		got, err := parseProcArgs2(build(3, "/path/to/claude", 4, []string{"claude", "--resume", "the sid"}, []string{"PATH=/bin"}))
		if err != nil {
			t.Fatalf("parseProcArgs2: %v", err)
		}
		want := []string{"claude", "--resume", "the sid"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("parseProcArgs2() = %q, want %q", got, want)
		}
	})
	t.Run("errors on fewer argv strings than argc", func(t *testing.T) {
		if _, err := parseProcArgs2(build(5, "/path/to/claude", 0, []string{"claude", "--flag"}, nil)); err == nil {
			t.Error("parseProcArgs2 on a truncated argv = nil, want an error")
		}
	})
	t.Run("errors on a too-short buffer", func(t *testing.T) {
		if _, err := parseProcArgs2([]byte{0, 0}); err == nil {
			t.Error("parseProcArgs2 on a 2-byte buffer = nil, want an error")
		}
	})
}

func TestParseLSOFCWD(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  map[int]string
	}{
		{
			name: "groups cwd records by pid",
			input: "p101\nfcwd\nn/private/tmp/one\n" +
				"p202\nfcwd\nn/Users/test/project with spaces\n" +
				"p303\nf1\nn/not-a-cwd\n",
			want: map[int]string{101: "/private/tmp/one", 202: "/Users/test/project with spaces"},
		},
		{
			name:  "pid without cwd is absent",
			input: "p404\n",
			want:  map[int]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLSOFCWD([]byte(tc.input))
			if err != nil {
				t.Fatalf("parseLSOFCWD() error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseLSOFCWD() = %v, want %v", got, tc.want)
			}
		})
	}
}

// exitCode runs a trivial shell command that exits with code and returns the
// resulting *exec.ExitError, the real error type lsofCWDs must recognize.
func exitCode(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("sh", "-c", "exit "+strconv.Itoa(code)).Run() //nolint:gosec // G204: fixed test command producing a deterministic exit code
	if err == nil {
		t.Fatalf("sh -c exit %d: succeeded, want a non-zero exit", code)
	}
	return err
}

func TestLsofCWDs(t *testing.T) {
	cases := []struct {
		name    string
		out     string
		err     error
		want    map[int]string
		wantErr bool
	}{
		{
			name: "nil error parses normally",
			out:  "p101\nfcwd\nn/private/tmp/one\n",
			want: map[int]string{101: "/private/tmp/one"},
		},
		{
			name: "exit 1 with partial output drops the vanished pid, keeps the rest",
			out:  "p101\nfcwd\nn/private/tmp/one\n",
			err:  exitCode(t, 1),
			want: map[int]string{101: "/private/tmp/one"},
		},
		{
			name: "exit 1 with empty output yields an empty snapshot, no error",
			out:  "",
			err:  exitCode(t, 1),
			want: map[int]string{},
		},
		{
			name:    "exit 1 with unparseable output stays fatal",
			out:     "pXYZ\n",
			err:     exitCode(t, 1),
			wantErr: true,
		},
		{
			name:    "exit code other than 1 stays fatal",
			out:     "p101\nfcwd\nn/private/tmp/one\n",
			err:     exitCode(t, 2),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lsofCWDs([]byte(tc.out), tc.err, "boom")
			if (err != nil) != tc.wantErr {
				t.Fatalf("lsofCWDs() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), "boom") {
					t.Errorf("lsofCWDs() error = %v, want it to include stderr %q", err, "boom")
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("lsofCWDs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseProcStatPPID(t *testing.T) {
	cases := []struct {
		name    string
		stat    string
		want    int
		wantErr bool
	}{
		{name: "ordinary comm", stat: "42 (claude) S 7 1 2 3", want: 7},
		{name: "comm containing close and open parens", stat: "43 (claude ) (worker) R 88 1 2 3", want: 88},
		{name: "missing comm terminator", stat: "44 (claude S 9", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProcStatPPID(tc.stat)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseProcStatPPID() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseProcStatPPID() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseProcCmdline(t *testing.T) {
	argv, ok := parseProcCmdline([]byte("/usr/local/bin/claude\x00--resume\x00session id\x00"))
	want := []string{"/usr/local/bin/claude", "--resume", "session id"}
	if !ok || !reflect.DeepEqual(argv, want) {
		t.Fatalf("parseProcCmdline() = %q, %v, want %q", argv, ok, want)
	}
}
