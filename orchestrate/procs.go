package orchestrate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// psStartLayout parses macOS ps lstart ("Www Mmm D HH:MM:SS YYYY") after cutField has
// collapsed its internal whitespace. It carries no zone, so it is read in time.Local to
// match the transcript mtimes it is compared against.
const psStartLayout = "Mon Jan 2 15:04:05 2006"

// claudeProc is one live claude process. argv is the real argument vector (never a
// space-joined string), so prompt text containing "--resume" can never be mistaken for the
// flag.
type claudeProc struct {
	pid, ppid int
	argv      []string
	cwd       string
	start     time.Time
}

// procMeta is the pid/ppid/start-time trio ps yields per claude process; the accurate argv
// vector is fetched separately (sysctl on darwin, /proc/<pid>/cmdline on linux).
type procMeta struct {
	pid, ppid int
	start     time.Time
}

var errUnsupportedPlatform = errors.New("unsupported platform")

var listClaudeProcs = func(ctx context.Context) ([]claudeProc, error) {
	switch runtime.GOOS {
	case "darwin":
		return listDarwinClaudeProcs(ctx)
	case "linux":
		return listLinuxClaudeProcs()
	default:
		return nil, fmt.Errorf("list claude processes on %s: %w", runtime.GOOS, errUnsupportedPlatform)
	}
}

func listDarwinClaudeProcs(ctx context.Context) ([]claudeProc, error) {
	cmd := exec.CommandContext(ctx, "ps", "-xo", "pid=,ppid=,lstart=,args=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list current-user processes: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	metas, err := parsePSOutput(out)
	if err != nil {
		return nil, fmt.Errorf("parse current-user processes: %w", err)
	}
	if len(metas) == 0 {
		return nil, nil
	}

	// The accurate argv comes from kern.procargs2, not the space-flattened ps args. A pid
	// whose sysctl read fails raced an exit (or is unreadable) and is dropped.
	procs := make([]claudeProc, 0, len(metas))
	for _, m := range metas {
		argv, err := procArgv(m.pid)
		if err != nil {
			continue
		}
		procs = append(procs, claudeProc{pid: m.pid, ppid: m.ppid, start: m.start, argv: argv})
	}
	if len(procs) == 0 {
		return nil, nil
	}

	pids := make([]string, len(procs))
	for i, proc := range procs {
		pids[i] = strconv.Itoa(proc.pid)
	}
	cmd = exec.CommandContext(ctx, "lsof", "-a", "-p", strings.Join(pids, ","), "-d", "cwd", "-Fn") //nolint:gosec // G204: argv is built from internally-enumerated numeric pids
	stderr.Reset()
	cmd.Stderr = &stderr
	out, err = cmd.Output()
	cwds, err := lsofCWDs(out, err, strings.TrimSpace(stderr.String()))
	if err != nil {
		return nil, err
	}

	resolved := make([]claudeProc, 0, len(procs))
	for _, proc := range procs {
		cwd, ok := cwds[proc.pid]
		if !ok {
			continue
		}
		proc.cwd = cwd
		resolved = append(resolved, proc)
	}
	return resolved, nil
}

// lsofCWDs interprets one lsof -Fn run's stdout/error pair. Exit status 1 (a
// listed pid vanished mid-scan) still parses whatever -Fn records lsof emitted;
// any other exit status, or an exit-1 body that doesn't parse, is fatal.
func lsofCWDs(out []byte, err error, stderr string) (map[int]string, error) {
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return nil, fmt.Errorf("list claude process working directories: %w: %s", err, stderr)
		}
	}
	cwds, parseErr := parseLSOFCWD(out)
	if parseErr != nil {
		if err != nil {
			return nil, fmt.Errorf("list claude process working directories: %w: %s", err, stderr)
		}
		return nil, fmt.Errorf("parse claude process working directories: %w", parseErr)
	}
	return cwds, nil
}

func parsePSOutput(out []byte) ([]procMeta, error) {
	var metas []procMeta
	for i, raw := range bytes.Split(out, []byte{'\n'}) {
		line := strings.TrimSpace(string(raw))
		if line == "" {
			continue
		}
		pidText, rest, ok := cutField(line)
		if !ok {
			return nil, fmt.Errorf("parse ps row %d: missing pid", i+1)
		}
		ppidText, rest, ok := cutField(rest)
		if !ok {
			return nil, fmt.Errorf("parse ps row %d: missing ppid", i+1)
		}
		// lstart is five whitespace-separated fields: Www Mmm D HH:MM:SS YYYY.
		var lstart [5]string
		for j := range lstart {
			field, r, ok := cutField(rest)
			if !ok {
				return nil, fmt.Errorf("parse ps row %d: missing start time", i+1)
			}
			lstart[j], rest = field, r
		}
		argv := rest
		if argv == "" {
			return nil, fmt.Errorf("parse ps row %d: missing argv", i+1)
		}
		pid, err := strconv.Atoi(pidText)
		if err != nil {
			return nil, fmt.Errorf("parse ps row %d pid: %w", i+1, err)
		}
		ppid, err := strconv.Atoi(ppidText)
		if err != nil {
			return nil, fmt.Errorf("parse ps row %d ppid: %w", i+1, err)
		}
		start, err := time.ParseInLocation(psStartLayout, strings.Join(lstart[:], " "), time.Local)
		if err != nil {
			return nil, fmt.Errorf("parse ps row %d start time: %w", i+1, err)
		}
		// Filter to claude by the argv[0] basename only — a prompt can never be argv[0], so
		// the flattened ps args are safe here; accurate flag parsing uses the real vector.
		argv0, _, _ := cutField(argv)
		if filepath.Base(argv0) != "claude" {
			continue
		}
		metas = append(metas, procMeta{pid: pid, ppid: ppid, start: start})
	}
	return metas, nil
}

// parseProcArgs2 decodes a macOS kern.procargs2 buffer into the process's argv vector. The
// layout is a little-endian int32 argc, the NUL-terminated executable path, NUL padding,
// then argc NUL-separated argv strings (env strings follow and are ignored).
func parseProcArgs2(buf []byte) ([]string, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("procargs2 buffer too short: %d bytes", len(buf))
	}
	argc := int(binary.LittleEndian.Uint32(buf[:4]))
	rest := buf[4:]
	sep := bytes.IndexByte(rest, 0)
	if sep < 0 {
		return nil, fmt.Errorf("procargs2 missing exec path terminator")
	}
	rest = rest[sep+1:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	args := make([]string, 0, argc)
	for len(args) < argc && len(rest) > 0 {
		if sep := bytes.IndexByte(rest, 0); sep >= 0 {
			args = append(args, string(rest[:sep]))
			rest = rest[sep+1:]
			continue
		}
		args = append(args, string(rest))
		rest = nil
	}
	if len(args) < argc {
		return nil, fmt.Errorf("procargs2: parsed %d of %d argv strings", len(args), argc)
	}
	return args, nil
}

func parseLSOFCWD(out []byte) (map[int]string, error) {
	cwds := map[int]string{}
	var pid int
	cwdRecord := false
	for i, raw := range bytes.Split(out, []byte{'\n'}) {
		line := string(raw)
		if line == "" {
			continue
		}
		switch line[0] {
		case 'p':
			parsed, err := strconv.Atoi(line[1:])
			if err != nil {
				return nil, fmt.Errorf("parse lsof row %d pid: %w", i+1, err)
			}
			pid = parsed
			cwdRecord = false
		case 'f':
			cwdRecord = pid != 0 && line[1:] == "cwd"
		case 'n':
			if pid != 0 && cwdRecord {
				cwds[pid] = line[1:]
			}
		}
	}
	return cwds, nil
}

func listLinuxClaudeProcs() ([]claudeProc, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read proc filesystem: %w", err)
	}
	self, err := os.Stat("/proc/self")
	if err != nil {
		return nil, fmt.Errorf("stat current process: %w", err)
	}
	uid := statUID(self)
	var procs []claudeProc
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		procDir := filepath.Join("/proc", entry.Name())
		info, err := os.Stat(procDir)
		if err != nil || statUID(info) != uid {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(procDir, "cmdline")) //nolint:gosec // G304: fixed /proc entry under an enumerated pid dir
		if err != nil {
			continue
		}
		argv, ok := parseProcCmdline(cmdline)
		if !ok || filepath.Base(argv[0]) != "claude" {
			continue
		}
		stat, err := os.ReadFile(filepath.Join(procDir, "stat")) //nolint:gosec // G304: fixed /proc entry under an enumerated pid dir
		if err != nil {
			continue
		}
		ppid, err := parseProcStatPPID(string(stat))
		if err != nil {
			return nil, fmt.Errorf("parse /proc/%d/stat: %w", pid, err)
		}
		cwd, err := os.Readlink(filepath.Join(procDir, "cwd"))
		if err != nil {
			continue
		}
		// The /proc/<pid> directory's mtime approximates the process start time.
		procs = append(procs, claudeProc{pid: pid, ppid: ppid, argv: argv, cwd: cwd, start: info.ModTime()})
	}
	return procs, nil
}

// parseProcCmdline splits a /proc/<pid>/cmdline NUL-separated buffer into its argv elements,
// keeping them separate — never joined — so a real flag is distinguishable from prompt text.
func parseProcCmdline(raw []byte) ([]string, bool) {
	raw = bytes.TrimSuffix(raw, []byte{0})
	if len(raw) == 0 {
		return nil, false
	}
	parts := bytes.Split(raw, []byte{0})
	argv := make([]string, len(parts))
	for i, part := range parts {
		argv[i] = string(part)
	}
	return argv, true
}

func statUID(info os.FileInfo) uint64 {
	return reflect.ValueOf(info.Sys()).Elem().FieldByName("Uid").Uint()
}

func parseProcStatPPID(stat string) (int, error) {
	closeParen := strings.LastIndexByte(stat, ')')
	if closeParen < 0 {
		return 0, fmt.Errorf("missing comm terminator")
	}
	fields := strings.Fields(stat[closeParen+1:])
	if len(fields) < 2 {
		return 0, fmt.Errorf("missing state or ppid")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, fmt.Errorf("parse ppid: %w", err)
	}
	return ppid, nil
}

func cutField(s string) (string, string, bool) {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)
	if s == "" {
		return "", "", false
	}
	i := strings.IndexFunc(s, unicode.IsSpace)
	if i < 0 {
		return s, "", true
	}
	return s[:i], strings.TrimLeftFunc(s[i:], unicode.IsSpace), true
}
