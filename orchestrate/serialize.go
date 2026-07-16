package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"

	"github.com/yasyf/cc-orchestrate/backend"
)

// serializeBundleVersion is the on-disk bundle schema version, recorded so a reader
// can reject an incompatible bundle. Single-user same-machine restore only ever reads
// bundles this build wrote, so there is no version-branching beyond this stamp.
const serializeBundleVersion = 1

// serializeBundle is the on-disk snapshot a restore recreates a wiped DB from: the
// repo → workstream → sprint hierarchy every active agent descends from, then the
// active agents themselves (identity plus captured screen). The hierarchy is carried
// because a ~/.cc-orchestrate wipe deletes the single SQLite DB holding every table,
// so respawnAgent could not otherwise resolve the parents it resumes an agent into.
type serializeBundle struct {
	Version     int               `json:"version"`
	CreatedAt   string            `json:"created_at"`
	Repos       []repoRow         `json:"repos"`
	Workstreams []workstreamRow   `json:"workstreams"`
	Sprints     []sprintRow       `json:"sprints"`
	Agents      []serializedAgent `json:"agents"`
}

// serializedAgent is one active agent's restorable identity — the fields insertAgent
// recreates the row from — plus the captured terminal Screen. Restore reattaches the
// live Claude session via --resume (its transcript in ~/.claude survives a
// ~/.cc-orchestrate wipe); Screen is preserved for inspection only and is never replayed.
type serializedAgent struct {
	ID             string       `json:"id"`
	SprintID       string       `json:"sprint_id"`
	Backend        backend.Name `json:"backend"`
	TerminalHandle string       `json:"terminal_handle"`
	SessionID      string       `json:"session_id"`
	Scope          string       `json:"scope"`
	Name           string       `json:"name"`
	Prompt         string       `json:"prompt"`
	SubjectID      string       `json:"subject_id"`
	CCNotesTask    string       `json:"ccnotes_task"`
	RestartCount   int          `json:"restart_count"`
	LastRestartAt  string       `json:"last_restart_at"`
	Screen         string       `json:"screen"`
}

// serializedEvent is the EventSerialized body appended per agent when a snapshot is
// written; Type discriminates the frame.
type serializedEvent struct {
	Type    string `json:"type"`
	AgentID string `json:"agent_id"`
	Bundle  string `json:"bundle"`
}

func serializedPayload(id, bundle string) json.RawMessage {
	b, _ := json.Marshal(serializedEvent{Type: EventSerialized, AgentID: id, Bundle: bundle})
	return b
}

// restoredEvent is the EventRestored body appended per agent restored from a bundle:
// the fresh backend terminal handle respawnAgent minted. Type discriminates the frame.
type restoredEvent struct {
	Type     string `json:"type"`
	AgentID  string `json:"agent_id"`
	Terminal string `json:"terminal"`
}

func restoredPayload(id, terminal string) json.RawMessage {
	b, _ := json.Marshal(restoredEvent{Type: EventRestored, AgentID: id, Terminal: terminal})
	return b
}

// serializeDir is the root under which snapshot bundles are written, one JSON file per
// serialize: ~/.cc-orchestrate/serialize/<stamp>.json.
func serializeDir() string { return filepath.Join(appPaths().StateDir(), "serialize") }

// handleSerialize answers the serialize op: it snapshots the repo/workstream/sprint
// hierarchy and every active agent's restorable identity and captured terminal screen
// into a bundle JSON under serializeDir (or an explicit out path), appending an
// EventSerialized per agent.
// fleetSerializeRequest writes the snapshot to Out, or to a stamped file in the
// default serialize dir when Out is empty.
type fleetSerializeRequest struct {
	Out string `json:"out,omitempty"`
}

// fleetSerializeResult reports the bundle path and the number of agents captured.
type fleetSerializeResult struct {
	Path  string `json:"path"`
	Count int    `json:"count"`
}

func handleSerialize(hc daemon.HandlerCtx, req fleetSerializeRequest) (fleetSerializeResult, error) {
	path := req.Out
	if path == "" {
		path = filepath.Join(serializeDir(), time.Now().UTC().Format("20060102T150405Z")+".json")
	}
	repos, err := listRepos(hc.Ctx, hc.DB, "")
	if err != nil {
		return fleetSerializeResult{}, err
	}
	workstreams, err := listWorkstreams(hc.Ctx, hc.DB, "", "")
	if err != nil {
		return fleetSerializeResult{}, err
	}
	sprints, err := listSprints(hc.Ctx, hc.DB, "", "")
	if err != nil {
		return fleetSerializeResult{}, err
	}
	agents, err := listActiveAgents(hc.Ctx, hc.DB)
	if err != nil {
		return fleetSerializeResult{}, err
	}
	bundle := serializeBundle{
		Version: serializeBundleVersion, CreatedAt: nowStamp(),
		Repos: repos, Workstreams: workstreams, Sprints: sprints,
		Agents: make([]serializedAgent, 0, len(agents)),
	}
	for _, ag := range agents {
		sa, err := captureAgent(hc.Ctx, hc.DB, ag)
		if err != nil {
			return fleetSerializeResult{}, err
		}
		bundle.Agents = append(bundle.Agents, sa)
	}
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return fleetSerializeResult{}, fmt.Errorf("marshal bundle: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fleetSerializeResult{}, fmt.Errorf("create serialize dir: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fleetSerializeResult{}, fmt.Errorf("write bundle %q: %w", path, err)
	}
	for _, sa := range bundle.Agents {
		if _, err := hc.Append(hc.Ctx, &event.Event{
			SubjectID: sa.SubjectID, Origin: event.OriginSystem, Type: EventSerialized, Payload: serializedPayload(sa.ID, path),
		}); err != nil {
			return fleetSerializeResult{}, err
		}
	}
	fleetLog.emit(hc.Ctx, bundleFrame(FrameSerialized, path, len(bundle.Agents)))
	return fleetSerializeResult{Path: path, Count: len(bundle.Agents)}, nil
}

// captureScreenText reads one active agent's terminal screen, holding agentLock so a
// concurrent restart cannot swap the handle mid-read. An unresolvable screen is tagged
// Unsupported; a resolved screen's capture failing is left untagged (InternalError).
// Shared by captureAgent and the cco.agent.capture handler.
func captureScreenText(ctx context.Context, db *sql.DB, ag agentRow) (string, error) {
	mu := agentLock(ag.ID)
	mu.Lock()
	defer mu.Unlock()
	screen, err := resolveScreen(ctx, db, ag)
	if err != nil {
		return "", opErr(codeUnsupported, fmt.Errorf("resolve screen for agent %q: %w", ag.ID, err))
	}
	text, err := captureWithTimeout(ctx, screen)
	if err != nil {
		return "", fmt.Errorf("capture screen for agent %q: %w", ag.ID, err)
	}
	return text, nil
}

// captureAgent captures one active agent's terminal screen and copies its restorable
// identity into a serializedAgent. The EventSerialized audit frame is appended by
// handleSerialize only after the bundle is durably written, so a failed write leaves
// no event referencing an absent bundle.
func captureAgent(ctx context.Context, db *sql.DB, ag agentRow) (serializedAgent, error) {
	text, err := captureScreenText(ctx, db, ag)
	if err != nil {
		return serializedAgent{}, err
	}
	return serializedAgent{
		ID: ag.ID, SprintID: ag.SprintID, Backend: ag.Backend, TerminalHandle: ag.TerminalHandle,
		SessionID: ag.SessionID, Scope: ag.Scope, Name: ag.Name, Prompt: ag.Prompt,
		SubjectID: ag.SubjectID, CCNotesTask: ag.CCNotesTask,
		RestartCount: ag.RestartCount, LastRestartAt: ag.LastRestartAt, Screen: text,
	}, nil
}

// handleRestore answers the restore op: it strict-parses a bundle, recreates the
// repo/workstream/sprint hierarchy it carries, then recreates every agent in it. A
// malformed bundle fails the whole op, so nothing is restored partially.
// fleetRestoreRequest names the bundle to restore.
type fleetRestoreRequest struct {
	Path string `json:"path"`
}

// fleetRestoreResult reports the number of agents restored.
type fleetRestoreResult struct {
	Count int `json:"count"`
}

func handleRestore(hc daemon.HandlerCtx, req fleetRestoreRequest) (fleetRestoreResult, error) {
	if req.Path == "" {
		return fleetRestoreResult{}, opErr(codeInvalidRequest, fmt.Errorf("restore requires a bundle path"))
	}
	bundle, err := readBundle(req.Path)
	if err != nil {
		return fleetRestoreResult{}, err
	}
	if err := restoreHierarchy(hc.Ctx, hc.DB, bundle); err != nil {
		return fleetRestoreResult{}, err
	}
	for _, sa := range bundle.Agents {
		if err := restoreAgent(hc.Ctx, hc.DB, hc.Append, sa); err != nil {
			return fleetRestoreResult{}, err
		}
	}
	fleetLog.emit(hc.Ctx, bundleFrame(FrameRestored, req.Path, len(bundle.Agents)))
	return fleetRestoreResult{Count: len(bundle.Agents)}, nil
}

// restoreHierarchy re-inserts the bundle's repo, workstream, and sprint rows that are
// absent, in repo → workstream → sprint order, so a restore into a fully-wiped DB
// recreates the parents respawnAgent resolves before it can resume an agent. A row a
// live DB still holds is left as-is — restore recreates a wiped hierarchy, it never
// rewrites an existing one.
func restoreHierarchy(ctx context.Context, db *sql.DB, bundle serializeBundle) error {
	for _, p := range bundle.Repos {
		exists, err := rowExists(ctx, db, "repos", p.ID)
		if err != nil {
			return err
		}
		if !exists {
			if err := insertRepo(ctx, db, p); err != nil {
				return err
			}
		}
	}
	for _, w := range bundle.Workstreams {
		exists, err := rowExists(ctx, db, "workstreams", w.ID)
		if err != nil {
			return err
		}
		if !exists {
			if err := insertWorkstream(ctx, db, w); err != nil {
				return err
			}
		}
	}
	for _, sp := range bundle.Sprints {
		exists, err := rowExists(ctx, db, "sprints", sp.ID)
		if err != nil {
			return err
		}
		if !exists {
			if err := insertSprint(ctx, db, sp); err != nil {
				return err
			}
		}
	}
	return nil
}

// readBundle strict-parses a serialize bundle: an unknown field, trailing garbage, or
// malformed JSON fails loud, so a corrupt bundle restores nothing rather than partially.
func readBundle(path string) (serializeBundle, error) {
	f, err := os.Open(path) //nolint:gosec // G304: user supplies the bundle path to restore by design
	if err != nil {
		return serializeBundle{}, fmt.Errorf("open bundle %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var bundle serializeBundle
	if err := dec.Decode(&bundle); err != nil {
		return serializeBundle{}, fmt.Errorf("decode bundle %q: %w", path, err)
	}
	if dec.More() {
		return serializeBundle{}, fmt.Errorf("decode bundle %q: trailing data after the bundle object", path)
	}
	return bundle, nil
}

// restoreAgent recreates one agent from its bundle entry under agentLock so the row
// ends active with a freshly-resumed terminal: an absent row (a wiped DB) is inserted
// fresh — active, default state, zero tokens, fresh timestamps; a present row is
// reactivated, and if it was still active its live terminal is killed first so the
// re-attach never leaks it (a present exited row's terminal is already gone). Either
// way respawnAgent resumes the session into a fresh backend terminal and rewrites the
// handle, then an EventRestored records the new terminal. The reactivated-or-inserted
// row is re-read so respawnAgent and the event both act on the authoritative row.
func restoreAgent(ctx context.Context, db *sql.DB, appendFn daemon.AppendFunc, sa serializedAgent) error {
	mu := agentLock(sa.ID)
	mu.Lock()
	defer mu.Unlock()
	exists, err := agentExists(ctx, db, sa.ID)
	if err != nil {
		return err
	}
	if exists {
		prior, err := getAgent(ctx, db, sa.ID)
		if err != nil {
			return err
		}
		if prior.Status == StatusActive {
			if err := killAgentTerminal(ctx, db, prior); err != nil {
				return err
			}
		}
		if err := setAgentLifecycle(ctx, db, sa.ID, StatusActive); err != nil {
			return err
		}
	} else {
		stamp := nowStamp()
		if err := insertAgent(ctx, db, agentRow{
			ID: sa.ID, SprintID: sa.SprintID, Backend: sa.Backend, TerminalHandle: sa.TerminalHandle,
			SessionID: sa.SessionID, Scope: sa.Scope, Name: sa.Name, Prompt: sa.Prompt,
			SubjectID: sa.SubjectID, CCNotesTask: sa.CCNotesTask, Status: StatusActive, State: StateUnknown,
			UpdatedAt: stamp, CreatedAt: stamp, RestartCount: sa.RestartCount, LastRestartAt: sa.LastRestartAt,
		}); err != nil {
			return err
		}
	}
	cur, err := getAgent(ctx, db, sa.ID)
	if err != nil {
		return err
	}
	// Announce from the committed (revived) row state, before respawnAgent starts the
	// tailer, so a fast status frame can't precede the spawned announcement.
	fleetLog.emit(ctx, spawnedFrame(cur))
	handle, err := respawnAgent(ctx, db, appendFn, cur)
	if err != nil {
		return err
	}
	if _, err := appendFn(ctx, &event.Event{
		SubjectID: cur.SubjectID, Origin: event.OriginSystem, Type: EventRestored, Payload: restoredPayload(cur.ID, handle.ID),
	}); err != nil {
		return err
	}
	return nil
}
