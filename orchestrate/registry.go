package orchestrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/yasyf/cc-interact/daemon"
)

// opCode is a method failure's machine-readable class. The socket adapter serializes
// it as the "<Code>: <message>" prefix on Reply.Error, a parseable contract an HTTP
// bridge maps to a status code.
type opCode string

const (
	codeInvalidRequest opCode = "InvalidRequest"
	codeNotFound       opCode = "NotFound"
	codeConflict       opCode = "Conflict"
	codeUnsupported    opCode = "Unsupported"
	codeInternalError  opCode = "InternalError"
)

// opError tags an error with an opCode so the transports map it to a wire status. An
// untagged error, and a store not-found (errNotFound), are mapped by replyError.
type opError struct {
	Code opCode
	Err  error
}

func (e *opError) Error() string { return string(e.Code) + ": " + e.Err.Error() }
func (e *opError) Unwrap() error { return e.Err }

// opErr tags err with code.
func opErr(code opCode, err error) error { return &opError{Code: code, Err: err} }

// expose is the bit-set of transports a method is reachable on.
type expose uint8

const (
	exposeSocket expose = 1 << iota
	exposeXRPC
	exposeMCP
)

func (e expose) has(bit expose) bool { return e&bit != 0 }

// The transport combinations the methods table draws from: every method is on the
// socket; report is the child channel's tool and stays off the parent surfaces.
const (
	full = exposeSocket | exposeXRPC | exposeMCP // parent-facing: socket, xrpc, and mcp
	rpc  = exposeSocket | exposeXRPC             // socket and xrpc, off the parent mcp list
	sock = exposeSocket                          // socket only
)

// methodKind discriminates a read (query) from a mutation (procedure) — the GET vs
// POST split the XRPC layer keys on.
type methodKind string

const (
	kindQuery     methodKind = "query"
	kindProcedure methodKind = "procedure"
)

// method is one entry of the daemon's op surface: its cco.<noun>.<verb> name (the
// socket op string verbatim), its kind, description, the transports it is exposed on,
// the captured request/response reflect types for schema generation, and the socket
// adapter closure the generic constructor built.
type method struct {
	name    string
	kind    methodKind
	desc    string
	exposes expose
	reqType reflect.Type
	resType reflect.Type
	adapt   daemon.HandlerFunc
}

// op is the method's daemon op string — identical to its name.
func (m method) op() daemon.Op { return daemon.Op(m.name) }

// query builds a read method from a typed handler.
func query[Req, Res any](name, desc string, exposes expose, fn func(daemon.HandlerCtx, Req) (Res, error)) method {
	return newMethod(name, kindQuery, desc, exposes, fn)
}

// procedure builds a mutating method from a typed handler.
func procedure[Req, Res any](name, desc string, exposes expose, fn func(daemon.HandlerCtx, Req) (Res, error)) method {
	return newMethod(name, kindProcedure, desc, exposes, fn)
}

// newMethod captures a typed handler's request/response types for schema generation
// and wraps it in the socket adapter. Both schemas are generated eagerly, so an
// unschemable field panics at package init rather than on a live request.
func newMethod[Req, Res any](name string, kind methodKind, desc string, exposes expose, fn func(daemon.HandlerCtx, Req) (Res, error)) method {
	reqType := reflect.TypeFor[Req]()
	resType := reflect.TypeFor[Res]()
	_ = schemaFor(reqType)
	_ = schemaFor(resType)
	return method{
		name: name, kind: kind, desc: desc, exposes: exposes,
		reqType: reqType, resType: resType,
		adapt: func(hc daemon.HandlerCtx) daemon.Reply { return runTyped(fn, hc) },
	}
}

// runTyped is the socket adapter every registered method shares: strict-decode the
// request body, run the typed handler, and marshal the result — mapping a decode
// failure to InvalidRequest and a handler error through replyError.
func runTyped[Req, Res any](fn func(daemon.HandlerCtx, Req) (Res, error), hc daemon.HandlerCtx) daemon.Reply {
	req, err := decodeBody[Req](hc.Env.Body)
	if err != nil {
		return replyError(opErr(codeInvalidRequest, err))
	}
	res, err := fn(hc, req)
	if err != nil {
		return replyError(err)
	}
	body, err := json.Marshal(res)
	if err != nil {
		return replyError(fmt.Errorf("marshal response: %w", err))
	}
	return daemon.Reply{OK: true, Body: body}
}

// decodeBody strict-decodes a request body into Req: an empty body yields the zero
// Req (an op that takes no arguments), a present body is decoded with unknown fields
// rejected so a mistyped key never silently no-ops.
func decodeBody[Req any](body json.RawMessage) (Req, error) {
	var req Req
	if len(body) == 0 {
		return req, nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, err
	}
	return req, nil
}

// replyError renders err as a failed Reply, deriving its "<Code>: <message>" prefix
// from an opError tag, a store not-found (NotFound), or otherwise InternalError.
func replyError(err error) daemon.Reply {
	var oe *opError
	switch {
	case errors.As(err, &oe):
		return daemon.Reply{OK: false, Error: oe.Error()}
	case errors.Is(err, errNotFound):
		return daemon.Reply{OK: false, Error: opErr(codeNotFound, err).Error()}
	default:
		return daemon.Reply{OK: false, Error: opErr(codeInternalError, err).Error()}
	}
}

// registerOps registers every socket-exposed method's adapter under its op name. It
// replaces the legacy const/Register blocks; cc-interact's Server.Register panics on
// a duplicate, so this is the sole registration site.
func registerOps(s *daemon.Server) {
	for _, m := range methods {
		if m.exposes.has(exposeSocket) {
			s.Register(m.op(), m.adapt)
		}
	}
}

// The method registry: the single source of truth for the daemon's op surface, the
// XRPC catalog, the generated MCP tool list, and the CLI/channel call sites. Naming
// is cco.<noun>.<verb> (camelCase verbs); the socket op string is the name verbatim.
var (
	mRepoCreate   = procedure("cco.repo.create", "Create an orchestration repo and its backend workspace.", full, handleRepoCreate)
	mRepoList     = query("cco.repo.list", "List the orchestration repos.", full, handleRepoList)
	mRepoShow     = query("cco.repo.show", "Show one repo by id or name.", rpc, handleRepoShow)
	mRepoActivate = procedure("cco.repo.activate", "Mark a repo active.", full, handleRepoActivate)
	mRepoKill     = procedure("cco.repo.kill", "Kill a repo, its backend workspace, and all of its agents.", full, handleRepoKill)

	mWorkstreamCreate   = procedure("cco.workstream.create", "Create a workstream — a branch and its worktree — in a repo, backed by a fresh backend workspace.", full, handleWorkstreamCreate)
	mWorkstreamList     = query("cco.workstream.list", "List the workstreams, optionally filtered by repo.", full, handleWorkstreamList)
	mWorkstreamShow     = query("cco.workstream.show", "Show one workstream by id or name.", rpc, handleWorkstreamShow)
	mWorkstreamActivate = procedure("cco.workstream.activate", "Mark a workstream active and record it as the default an agent spawn lands in.", full, handleWorkstreamActivate)
	mWorkstreamKill     = procedure("cco.workstream.kill", "Kill a workstream: tear down its backend workspace and worktree and exit its agents.", full, handleWorkstreamKill)

	mSprintCreate   = procedure("cco.sprint.create", "Create a sprint — a planning group an agent spawns into — in a workstream.", full, handleSprintCreate)
	mSprintList     = query("cco.sprint.list", "List the sprints, optionally filtered by workstream.", full, handleSprintList)
	mSprintShow     = query("cco.sprint.show", "Show one sprint by id or name.", rpc, handleSprintShow)
	mSprintActivate = procedure("cco.sprint.activate", "Mark a sprint active and record it as the default an agent spawn lands in.", full, handleSprintActivate)

	mAgentSpawn       = procedure("cco.agent.spawn", "Spawn a child Claude Code agent into a sprint, a workstream's or repo's default sprint, or the active sprint. An empty prompt starts the agent idle and interactive; API callers normally pass one.", full, handleSpawn)
	mAgentList        = query("cco.agent.list", "List agents and their derived status, optionally filtered by repo.", full, handleList)
	mAgentShow        = query("cco.agent.show", "Show one agent's derived status.", full, handleStatus)
	mAgentSendMessage = procedure("cco.agent.sendMessage", "Send a message (a new instruction) to a running agent.", full, handleSendMessage)
	mAgentKill        = procedure("cco.agent.kill", "Kill a running agent.", full, handleAgentKill)
	mAgentReport      = procedure("cco.agent.report", "Append a child agent's report to its subject log (the child channel's tool).", sock, handleReport)

	mConfigGet   = query("cco.config.get", "Read one persisted config key's value.", full, handleConfigGet)
	mConfigSet   = procedure("cco.config.set", "Upsert one persisted config key.", full, handleConfigSet)
	mConfigList  = query("cco.config.list", "List the persisted config key-value pairs.", rpc, handleConfigList)
	mConfigUnset = procedure("cco.config.unset", "Delete one persisted config key.", rpc, handleConfigUnset)

	mFleetSerialize = procedure("cco.fleet.serialize", "Snapshot every active agent into a restorable bundle.", full, handleSerialize)
	mFleetRestore   = procedure("cco.fleet.restore", "Restore agents from a serialized bundle.", full, handleRestore)

	methods = []method{
		mRepoCreate, mRepoList, mRepoShow, mRepoActivate, mRepoKill,
		mWorkstreamCreate, mWorkstreamList, mWorkstreamShow, mWorkstreamActivate, mWorkstreamKill,
		mSprintCreate, mSprintList, mSprintShow, mSprintActivate,
		mAgentSpawn, mAgentList, mAgentShow, mAgentSendMessage, mAgentKill, mAgentReport,
		mConfigGet, mConfigSet, mConfigList, mConfigUnset,
		mFleetSerialize, mFleetRestore,
	}
)
