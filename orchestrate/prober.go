package orchestrate

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/cc-orchestrate/ptyhost"
)

const (
	// maxProbeRounds bounds the capture→answer rounds the prober drives before it
	// surfaces the agent as stuck — enough to clear a short sequence of startup prompts
	// and then poll for the transcript, but not so many that a truly stuck agent lingers.
	maxProbeRounds = 24
	// probeOpTimeout bounds a single capture or answer so a wedged backend or socket
	// never blocks the prober (and the tailer behind it) indefinitely.
	probeOpTimeout = 3 * time.Second
	// probePolicyKey is the config key selecting which prompts the prober auto-answers.
	probePolicyKey = "prober.policy"
)

// promptPolicy classifies how sensitive auto-answering a prompt is, so the configured
// policy can permit folder-trust while still gating broader capability grants.
type promptPolicy int

const (
	policyTrust promptPolicy = iota // trusting a folder cc-orchestrate created itself
	policyGrant                     // a broader capability grant (bypass-permissions)
)

// tuiPrompt is one interactive prompt claude can stop on before it writes any
// transcript: match identifies it in the rendered screen, keys answer it, policy
// classifies the risk.
type tuiPrompt struct {
	name   string
	match  *regexp.Regexp
	keys   []string
	policy promptPolicy
}

// tuiPrompts is the table of known blocking prompts, matched in order against the
// captured screen. A malformed pattern fails at package load (MustCompile).
var tuiPrompts = []tuiPrompt{
	{
		name: "trust",
		// claude 2.1.x: "Quick safety check: Is this a project you created or one you
		// trust?" with options "1. Yes, I trust this folder" / "2. No, exit". The option
		// label is the stable, single-line phrase to match (verified against the binary
		// and a live capture).
		match:  regexp.MustCompile(`(?i)trust this folder`),
		keys:   []string{"Enter"}, // "Yes, I trust this folder" is the default (❯); Enter confirms
		policy: policyTrust,
	},
	{
		name: "external-claude-md",
		// claude 2.1.x: "Allow external CLAUDE.md file imports?" with options
		// "Yes, allow external imports" / "No, disable external imports". Fires when a
		// loaded CLAUDE.md @-imports a file outside the cwd (e.g. the user's global
		// config). The option label is the stable phrase to match.
		match:  regexp.MustCompile(`(?i)allow external imports`),
		keys:   []string{"Enter"}, // "Yes, allow external imports" is the default; Enter confirms
		policy: policyTrust,       // importing the orchestrator's own config; same trust class
	},
	{
		name:   "bypass-permissions",
		match:  regexp.MustCompile(`(?i)bypass permissions mode`),
		keys:   []string{"Down", "Enter"}, // default is "No, exit"; move to the accept option
		policy: policyGrant,
	},
}

// matchPrompt returns the first table entry present in the rendered screen, or nil.
func matchPrompt(screen string) *tuiPrompt {
	for i := range tuiPrompts {
		if tuiPrompts[i].match.MatchString(screen) {
			return &tuiPrompts[i]
		}
	}
	return nil
}

// promptScreen reads an agent's terminal and answers a prompt, via the backend's
// native capture/send when it advertises CanCapture, else the pty-host control socket.
type promptScreen interface {
	capture(ctx context.Context) (string, error)
	answer(ctx context.Context, keys ...string) error
}

// nativeScreen drives a capturing backend: Capturer reads the screen and Sender types
// the answer. SendText appends Enter, so a trust answer of {"Enter"} sends a bare
// Enter (empty text) to accept the default.
type nativeScreen struct {
	cap    backend.Capturer
	snd    backend.Sender
	handle backend.AgentHandle
}

func (n nativeScreen) capture(ctx context.Context) (string, error) {
	return n.cap.Capture(ctx, n.handle)
}

func (n nativeScreen) answer(ctx context.Context, keys ...string) error {
	text, ok := nativeText(keys)
	if !ok {
		return fmt.Errorf("native backend cannot send keys %v over SendText", keys)
	}
	return n.snd.SendText(ctx, n.handle, text)
}

// nativeText maps a key sequence to the literal text a Sender.SendText delivers (it
// already appends Enter): the sequence must end in "Enter" — the implicit submit —
// and every preceding token must be literal text. A bare named key (e.g. "Down") has
// no SendText representation, so it reports unsupported and the prober surfaces the
// prompt for a human. (Such keys only ever auto-resolve on a non-capturing backend
// driven through the pty-host socket, which writes raw key bytes; capturing backends
// have no SendKey path yet.)
func nativeText(keys []string) (string, bool) {
	if len(keys) == 0 || keys[len(keys)-1] != "Enter" {
		return "", false
	}
	var text strings.Builder
	for _, k := range keys[:len(keys)-1] {
		if ptyhost.IsNamedKey(k) {
			return "", false
		}
		text.WriteString(k)
	}
	return text.String(), true
}

// ptyScreen drives a non-capturing backend through its pty-host control socket.
type ptyScreen struct{ client *ptyhost.Client }

func (p ptyScreen) capture(ctx context.Context) (string, error) { return p.client.Capture(ctx) }

func (p ptyScreen) answer(ctx context.Context, keys ...string) error {
	return p.client.SendKeys(ctx, keys...)
}

// resolveScreen picks how to read and drive the agent's terminal: a backend that
// advertises CanCapture is read and typed natively; any other backend (superset) is
// driven through the pty-host socket the spawn wrapper started.
func resolveScreen(ctx context.Context, db *sql.DB, ag agentRow) (promptScreen, func() error, error) {
	b, ok := backend.Get(ag.Backend)
	if !ok {
		return nil, nil, fmt.Errorf("unknown backend %q", ag.Backend)
	}
	if capturer, isCap := b.(backend.Capturer); isCap && b.Caps().Has(backend.CanCapture) {
		snd, isSnd := b.(backend.Sender)
		if !isSnd {
			return nil, nil, fmt.Errorf("backend %q captures but cannot send", ag.Backend)
		}
		handle, err := backendAgentHandle(ctx, db, ag)
		if err != nil {
			return nil, nil, err
		}
		return nativeScreen{cap: capturer, snd: snd, handle: handle}, func() error { return nil }, nil
	}
	if ag.SpawnNonce == "" {
		return nil, nil, fmt.Errorf("agent %q has no spawn nonce", ag.ID)
	}
	client := ptyhost.Dial(ptySocketPath(ag.SessionID, ag.SpawnNonce))
	return ptyScreen{client: client}, client.Close, nil
}

// resolveProbePolicy reads the configured answer policy. The default (unset or
// "auto-answer-all") answers every known prompt. A config-read error returns the
// least-permissive policy (answer nothing) alongside the error, so a transient DB
// failure can never silently grant a broad capability the operator may have gated.
func resolveProbePolicy(ctx context.Context, db *sql.DB) (func(promptPolicy) bool, error) {
	v, _, err := getConfig(ctx, db, probePolicyKey)
	if err != nil {
		return func(promptPolicy) bool { return false }, err
	}
	switch v {
	case "auto-answer-trust-only":
		return func(p promptPolicy) bool { return p == policyTrust }, nil
	case "detect-and-surface-only":
		return func(promptPolicy) bool { return false }, nil
	default:
		return func(promptPolicy) bool { return true }, nil
	}
}

// runProber drives a spawned agent past a blocking interactive prompt before its
// transcript exists. It first waits a grace period for the transcript to appear on
// its own (claude initialized without a prompt); failing that, it captures the screen
// and, per the configured policy, answers a known prompt, confirming it cleared. It
// surfaces an unrecognized screen or an unclearable prompt as stuck, and a
// policy-gated prompt as blocked, so a blocked agent is never silently invisible.
// Capture/answer errors are logged and hand control back to the tailer unchanged.
func runProber(ctx context.Context, db *sql.DB, ag agentRow, emit func(Status) error, interval, grace time.Duration) {
	appeared := func() bool { return waitTranscript(ctx, ag.SessionID, interval, grace) }
	if appeared() {
		return
	}
	screen, closeScreen, err := resolveScreen(ctx, db, ag)
	if err != nil {
		log.Printf("cc-orchestrate: prober for agent %s: %v", ag.ID, err)
		return
	}
	defer func() {
		if err := closeScreen(); err != nil {
			log.Printf("cc-orchestrate: close prober screen for agent %s: %v", ag.ID, err)
		}
	}()
	answer, err := resolveProbePolicy(ctx, db)
	if err != nil {
		log.Printf("cc-orchestrate: prober policy for agent %s (failing safe to surface-only): %v", ag.ID, err)
	}
	exists := func() bool { _, ok, _ := findTranscript(ag.SessionID); return ok }
	driveProbe(ctx, ag.ID, screen, answer, emit, exists, interval)
}

// driveProbe drives an agent past its startup prompts. Each round it first checks
// whether the transcript has appeared — the definitive sign claude got past every
// prompt, far more robust than any on-screen regex — and otherwise captures the
// screen and answers a known prompt. It loops so a sequence of prompts (e.g. the
// trust dialog then the external-CLAUDE.md-import dialog) is cleared one after
// another. A policy-gated prompt surfaces as blocked; once the round budget is spent
// without a transcript, an unrecognized non-blank screen (or a silent pane) surfaces
// as stuck. Splitting it from runProber lets a test drive the loop with a fake screen
// and an injected transcript signal, with no daemon, DB, or filesystem.
func driveProbe(ctx context.Context, id string, screen promptScreen, answer func(promptPolicy) bool, emit func(Status) error, transcript func() bool, interval time.Duration) {
	drove, sawScreen := false, false
	for round := 0; round < maxProbeRounds; round++ {
		if transcript() {
			if drove {
				// Reset to the tailer's baseline so its first real status overwrites
				// cleanly rather than leaving the row at the transient blocked.
				if err := emit(Status{State: StateUnknown}); err != nil {
					log.Printf("cc-orchestrate: prober reset for agent %s: %v", id, err)
				}
			}
			return
		}
		pane, err := captureWithTimeout(ctx, screen)
		if err != nil {
			// A transient capture error (e.g. the pty-host socket not up yet) must not
			// abandon a possibly-blocked agent invisibly; pace and retry within the
			// bounded loop, and let the trailing stuck surface a persistent failure.
			log.Printf("cc-orchestrate: probe capture for agent %s: %v", id, err)
			if !sleepInterval(ctx, interval) {
				return
			}
			continue
		}
		switch p := matchPrompt(pane); {
		case p != nil && !answer(p.policy):
			emitState(emit, id, StateBlocked, p.name) // policy forbids auto-answer; a human decides
			return
		case p != nil:
			emitState(emit, id, StateBlocked, p.name)
			if err := answerWithTimeout(ctx, screen, p.keys...); err != nil {
				log.Printf("cc-orchestrate: probe answer %q for agent %s: %v", p.name, id, err)
				return // already surfaced as blocked; a human can take it from here
			}
			drove = true
		case strings.TrimSpace(pane) != "":
			sawScreen = true // an unrecognized non-blank screen — possibly a prompt we don't know
		}
		if !sleepInterval(ctx, interval) {
			return
		}
	}
	if sawScreen {
		emitState(emit, id, StateStuck, "unrecognized")
	} else {
		emitState(emit, id, StateStuck, "no output")
	}
}

// emitState records a prober-synthesized status (state + a "prompt: <detail>"
// activity) through the same applyStatus + EventStatus path the tailer uses.
func emitState(emit func(Status) error, id string, state State, detail string) {
	if err := emit(Status{State: state, Tool: "prompt", Target: detail}); err != nil {
		log.Printf("cc-orchestrate: prober emit for agent %s: %v", id, err)
	}
}

// waitTranscript polls findTranscript until it resolves, the budget elapses, or ctx
// is cancelled; it reports whether the transcript appeared.
func waitTranscript(ctx context.Context, sessionID string, interval, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, ok, _ := findTranscript(sessionID); ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}

// sleepInterval waits one interval, returning false if ctx was cancelled first.
func sleepInterval(ctx context.Context, interval time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(interval):
		return true
	}
}

func captureWithTimeout(ctx context.Context, s promptScreen) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, probeOpTimeout)
	defer cancel()
	return s.capture(cctx)
}

func answerWithTimeout(ctx context.Context, s promptScreen, keys ...string) error {
	cctx, cancel := context.WithTimeout(ctx, probeOpTimeout)
	defer cancel()
	return s.answer(cctx, keys...)
}
