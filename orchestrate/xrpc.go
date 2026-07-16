package orchestrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-interact/daemon"
)

// xrpcTimeout bounds a dispatched HTTP call at parity with the socket connection
// deadline, so a wedged backend cannot park an HTTP connection indefinitely.
const xrpcTimeout = 35 * time.Second

// maxXRPCBody caps a POST body at 1 MiB; a larger body is rejected before decode.
const maxXRPCBody = 1 << 20

// fleetEventSchemas holds the JSON schemas for the fleet SSE frame taxonomy, emitted
// as the catalog's events.types. Phase 3's fleet.go populates it; until then the
// catalog advertises an empty object. This is the cross-phase contract — the var name
// is load-bearing.
var fleetEventSchemas = map[string]any{}

// mountXRPC registers the /xrpc HTTP surface on the daemon's mux, already behind
// cc-interact's authHandler (the whole mux is wrapped at Serve time). The literal
// describe route, the GET/POST method wildcards, and a verb-less method wildcard are
// registered. Go's mux prefers the literal over a wildcard and a method-qualified
// wildcard over the bare one, so a GET/POST lands on its verb handler while every
// other verb (PUT/DELETE/PATCH/…) falls through to the bare wildcard. Every /xrpc/*
// request therefore stays inside serveXRPC and answers a JSON envelope — the verb-less
// route is what keeps an unsupported verb from reaching the mux's own plain-text 405.
func mountXRPC(s *daemon.Server) {
	mux := s.Mux()
	mux.HandleFunc("GET /xrpc/cco.server.describe", func(w http.ResponseWriter, _ *http.Request) {
		writeCatalog(w)
	})
	h := func(w http.ResponseWriter, r *http.Request) { serveXRPC(w, r, s) }
	mux.HandleFunc("GET /xrpc/{method}", h)
	mux.HandleFunc("POST /xrpc/{method}", h)
	mux.HandleFunc("/xrpc/{method}", h)
}

// serveXRPC resolves the {method} path value to an xrpc-exposed method, enforces the
// query/procedure verb split (a query is reachable by GET or HEAD, a procedure by
// POST), decodes the request, and dispatches it through the daemon's op table under a
// bounded context.
func serveXRPC(w http.ResponseWriter, r *http.Request, s *daemon.Server) {
	name := r.PathValue("method")
	m, ok := lookupXRPCMethod(name)
	if !ok {
		writeXRPCError(w, http.StatusNotFound, "MethodNotFound", fmt.Sprintf("no method %q", name))
		return
	}
	if !methodAllowed(m.kind, r.Method) {
		want := verbFor(m.kind)
		w.Header().Set("Allow", want)
		writeXRPCError(w, http.StatusMethodNotAllowed, string(codeInvalidRequest),
			fmt.Sprintf("method %q is a %s; use %s", name, m.kind, want))
		return
	}
	body, err := decodeXRPCRequest(r, m)
	if err != nil {
		writeXRPCError(w, http.StatusBadRequest, string(codeInvalidRequest), err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), xrpcTimeout)
	defer cancel()
	reply := s.Dispatch(ctx, daemon.Envelope{
		Proto:   daemon.ProtocolVersion,
		Op:      m.op(),
		Session: AppName,
		Body:    body,
	})
	writeXRPCReply(w, reply)
}

// verbFor maps a method kind to the canonical HTTP verb the Allow header advertises: a
// query is GET, a procedure is POST.
func verbFor(kind methodKind) string {
	if kind == kindQuery {
		return http.MethodGet
	}
	return http.MethodPost
}

// methodAllowed reports whether an HTTP verb may reach a method of the given kind: a
// query by GET or HEAD (net/http serves HEAD by running the GET handler and discarding
// the body), a procedure only by POST.
func methodAllowed(kind methodKind, verb string) bool {
	if kind == kindQuery {
		return verb == http.MethodGet || verb == http.MethodHead
	}
	return verb == http.MethodPost
}

// lookupXRPCMethod finds the xrpc-exposed method with the given name. A socket-only
// method (report) and an unknown name both resolve to false, so both surface as a
// MethodNotFound rather than leaking a non-routable op onto the HTTP surface.
func lookupXRPCMethod(name string) (method, bool) {
	for _, m := range methods {
		if m.name == name && m.exposes.has(exposeXRPC) {
			return m, true
		}
	}
	return method{}, false
}

// decodeXRPCRequest builds the Envelope body for a request, keyed off the method kind
// the verb check already validated: a query projects the URL query params through the
// method's request schema (so a HEAD query decodes its params exactly as a GET does,
// never falling through to a body read), a procedure reads the raw JSON object body.
func decodeXRPCRequest(r *http.Request, m method) (json.RawMessage, error) {
	if m.kind == kindQuery {
		return decodeQueryParams(r, m)
	}
	return readJSONBody(r)
}

// readJSONBody reads a size-capped POST body: an empty (or whitespace-only) body yields
// an absent Envelope body, a non-JSON-object body is rejected early, and anything past
// the size cap is rejected before it is decoded. The strict field check happens in the
// socket adapter's DisallowUnknownFields decode.
func readJSONBody(r *http.Request) (json.RawMessage, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxXRPCBody+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(raw) > maxXRPCBody {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxXRPCBody)
	}
	body := bytes.TrimSpace(raw)
	if len(body) == 0 {
		return nil, nil
	}
	if body[0] != '{' {
		return nil, errors.New("request body must be a JSON object")
	}
	return json.RawMessage(body), nil
}

// decodeQueryParams projects a GET request's URL query params into a JSON object keyed
// by the method's request schema: a string field passes through, an int/bool/number
// field is parsed to its typed value, a param absent from the schema is rejected, and a
// param given more than once is rejected rather than silently collapsed to its first
// value (both mirroring the socket adapter's strict DisallowUnknownFields decode).
func decodeQueryParams(r *http.Request, m method) (json.RawMessage, error) {
	q := r.URL.Query()
	if len(q) == 0 {
		return nil, nil
	}
	props := requestProps(m)
	obj := make(map[string]any, len(q))
	for key, vals := range q {
		prop, ok := props[key]
		if !ok {
			return nil, fmt.Errorf("unknown query parameter %q", key)
		}
		if len(vals) != 1 {
			return nil, fmt.Errorf("query parameter %q given %d times; provide it exactly once", key, len(vals))
		}
		v, err := parseQueryValue(prop, vals[0])
		if err != nil {
			return nil, fmt.Errorf("query parameter %q: %w", key, err)
		}
		obj[key] = v
	}
	return json.Marshal(obj)
}

// requestProps returns the property schemas of a method's request object — the map an
// object schema keys each field's schema under. Every request type is a struct, so the
// object schema always carries a (possibly empty) properties map.
func requestProps(m method) map[string]any {
	schema := schemaFor(m.reqType)
	props, _ := schema["properties"].(map[string]any)
	return props
}

// parseQueryValue coerces a raw query-string value to the scalar type its schema names.
// A field the schema types as a non-scalar (object, array) cannot be expressed as a
// query param and is rejected.
func parseQueryValue(prop any, raw string) (any, error) {
	pm, _ := prop.(map[string]any)
	switch pm["type"] {
	case "string":
		return raw, nil
	case "integer":
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("not an integer: %q", raw)
		}
		return n, nil
	case "boolean":
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("not a boolean: %q", raw)
		}
		return b, nil
	case "number":
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("not a number: %q", raw)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("cannot decode a %v field from a query parameter", pm["type"])
	}
}

// writeXRPCReply maps a daemon Reply onto the HTTP response: an ok reply becomes 200
// with its body emitted verbatim (never re-marshaled — a re-marshal would drop any
// daemon-added field; an empty body becomes {}), and a failed reply is split back into
// its "<Code>: <message>" prefix and mapped to a status code and JSON error envelope.
func writeXRPCReply(w http.ResponseWriter, reply daemon.Reply) {
	if reply.OK {
		body := reply.Body
		if len(body) == 0 {
			body = json.RawMessage("{}")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	code, msg := splitErrorCode(reply.Error)
	writeXRPCError(w, statusForCode(code), string(code), msg)
}

// splitErrorCode parses a failed reply's "<Code>: <message>" prefix back into its
// opCode and message. An error with no recognized code prefix is an InternalError
// carrying the raw error as its message.
func splitErrorCode(errStr string) (opCode, string) {
	if prefix, rest, found := strings.Cut(errStr, ": "); found {
		if code, ok := knownCode(prefix); ok {
			return code, rest
		}
	}
	return codeInternalError, errStr
}

// knownCode reports whether s names one of the method failure codes the socket adapter
// serializes as a reply-error prefix.
func knownCode(s string) (opCode, bool) {
	switch opCode(s) {
	case codeInvalidRequest, codeNotFound, codeConflict, codeUnsupported, codeInternalError:
		return opCode(s), true
	}
	return "", false
}

// statusForCode maps a method failure code to its HTTP status.
func statusForCode(code opCode) int {
	switch code {
	case codeInvalidRequest:
		return http.StatusBadRequest
	case codeNotFound:
		return http.StatusNotFound
	case codeConflict:
		return http.StatusConflict
	case codeUnsupported:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

// writeXRPCError emits a JSON error envelope with the given status. Any Allow header a
// caller set for a verb mismatch survives, since it is written before the status line.
func writeXRPCError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": message})
}

// writeCatalog answers GET /xrpc/cco.server.describe with the typed method catalog: the
// app identity, every xrpc-exposed method (sorted by name) with its input/output JSON
// schema, and the fleet event stream descriptor.
func writeCatalog(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(buildCatalog())
}

// buildCatalog assembles the describe payload: the xrpc-exposed methods sorted by name,
// each with its generated request/response schema, plus the fleet SSE stream and its
// (Phase-3-populated) frame-type schemas.
func buildCatalog() map[string]any {
	var exposed []method
	for _, m := range methods {
		if m.exposes.has(exposeXRPC) {
			exposed = append(exposed, m)
		}
	}
	sort.Slice(exposed, func(i, j int) bool { return exposed[i].name < exposed[j].name })
	entries := make([]map[string]any, 0, len(exposed))
	for _, m := range exposed {
		entries = append(entries, map[string]any{
			"name":        m.name,
			"type":        string(m.kind),
			"description": m.desc,
			"input":       schemaFor(m.reqType),
			"output":      schemaFor(m.resType),
		})
	}
	return map[string]any{
		"app":     AppName,
		"version": buildVersion(),
		"methods": entries,
		"events": map[string]any{
			"stream": "/events?session=fleet",
			"types":  fleetEventSchemas,
		},
	}
}
