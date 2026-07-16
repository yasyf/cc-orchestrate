package orchestrate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
)

// newXRPCServer builds a registered daemon with the /xrpc routes mounted and an
// httptest server serving its raw mux (no auth wrapper — auth is cc-interact's, tested
// there). It returns the daemon so a test can compare an HTTP response against a direct
// Dispatch of the same envelope.
func newXRPCServer(t *testing.T) (*daemon.Server, *httptest.Server) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	s, err := daemon.New(daemon.Config{
		AppName:        AppName,
		Paths:          appPaths(),
		Version:        Version,
		ActiveStatuses: []string{string(StatusActive)},
		Migrate:        migrate,
	})
	if err != nil {
		t.Fatalf("daemon.New: %v", err)
	}
	registerOps(s)
	mountXRPC(s)
	ts := httptest.NewServer(s.Mux())
	t.Cleanup(ts.Close)
	return s, ts
}

// doReq issues method to ts's URL+path with an optional string body, returning the
// response for the caller to inspect and close.
func doReq(t *testing.T, ts *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, r)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	return resp
}

type catalogMethod struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description"`
	Input       map[string]any `json:"input"`
	Output      map[string]any `json:"output"`
}

type catalogDoc struct {
	App     string          `json:"app"`
	Version string          `json:"version"`
	Methods []catalogMethod `json:"methods"`
	Events  struct {
		Stream string         `json:"stream"`
		Types  map[string]any `json:"types"`
	} `json:"events"`
}

type errEnvelope struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func TestXRPCCatalog(t *testing.T) {
	_, ts := newXRPCServer(t)

	resp := doReq(t, ts, http.MethodGet, "/xrpc/cco.server.describe", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var cat catalogDoc
	if err := json.NewDecoder(resp.Body).Decode(&cat); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	if cat.App != AppName {
		t.Errorf("app = %q, want %q", cat.App, AppName)
	}
	if cat.Version == "" || cat.Version != Version {
		t.Errorf("version = %q, want %q", cat.Version, Version)
	}
	if cat.Events.Stream != "/events?session=fleet" {
		t.Errorf("events.stream = %q, want /events?session=fleet", cat.Events.Stream)
	}
	if cat.Events.Types == nil {
		t.Error("events.types is null, want an object (the Phase-3 fleet frame schemas)")
	}

	byName := map[string]catalogMethod{}
	var names []string
	for _, m := range cat.Methods {
		byName[m.Name] = m
		names = append(names, m.Name)
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] > names[i] {
			t.Fatalf("methods not sorted by name: %q before %q", names[i-1], names[i])
		}
	}

	list, ok := byName["cco.repo.list"]
	if !ok {
		t.Fatal("catalog missing cco.repo.list")
	}
	if list.Type != string(kindQuery) {
		t.Errorf("cco.repo.list type = %q, want query", list.Type)
	}
	if list.Input["type"] != "object" {
		t.Errorf("cco.repo.list input schema type = %v, want object", list.Input["type"])
	}
	if list.Output["type"] != "object" && list.Output["type"] != "array" {
		t.Errorf("cco.repo.list output schema type = %v, want object/array", list.Output["type"])
	}
	if spawn, ok := byName["cco.agent.spawn"]; !ok || spawn.Type != string(kindProcedure) {
		t.Errorf("cco.agent.spawn = %+v, ok=%v, want a procedure", spawn, ok)
	}
	if _, ok := byName["cco.agent.report"]; ok {
		t.Error("catalog exposes cco.agent.report, which is socket-only")
	}
}

func TestXRPCPostProcedureRoundTrip(t *testing.T) {
	_, ts := newXRPCServer(t)

	resp := doReq(t, ts, http.MethodPost, "/xrpc/cco.config.set", `{"key":"k1","value":"v1"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Key != "k1" || out.Value != "v1" {
		t.Fatalf("round trip = %+v, want key=k1 value=v1", out)
	}
}

func TestXRPCGetQueryRoundTrip(t *testing.T) {
	_, ts := newXRPCServer(t)

	// Set a key over POST, then read it back over GET with a query param, proving the
	// URL param is projected into the request body the handler decodes.
	set := doReq(t, ts, http.MethodPost, "/xrpc/cco.config.set", `{"key":"host","value":"local"}`)
	set.Body.Close()
	if set.StatusCode != http.StatusOK {
		t.Fatalf("set status = %d, want 200", set.StatusCode)
	}

	resp := doReq(t, ts, http.MethodGet, "/xrpc/cco.config.get?key=host", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Found || out.Value != "local" {
		t.Fatalf("get = %+v, want value=local found=true", out)
	}
}

// TestXRPCEmptyPostBody proves an empty POST body is admitted (becomes an absent
// Envelope body → the zero request), not rejected as a malformed body.
func TestXRPCEmptyPostBody(t *testing.T) {
	_, ts := newXRPCServer(t)
	resp := doReq(t, ts, http.MethodPost, "/xrpc/cco.config.set", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (empty body → zero request)", resp.StatusCode)
	}
}

func TestXRPCErrors(t *testing.T) {
	_, ts := newXRPCServer(t)

	oversize := `{"key":"k","value":"` + strings.Repeat("a", maxXRPCBody) + `"}`

	for _, tc := range []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantError  string
		wantAllow  string
	}{
		{"unknown method", http.MethodGet, "/xrpc/cco.nope.nope", "", http.StatusNotFound, "MethodNotFound", ""},
		{"unknown method post", http.MethodPost, "/xrpc/cco.nope.nope", "{}", http.StatusNotFound, "MethodNotFound", ""},
		{"socket-only method not routable", http.MethodPost, "/xrpc/cco.agent.report", "{}", http.StatusNotFound, "MethodNotFound", ""},
		{"get on procedure", http.MethodGet, "/xrpc/cco.config.set", "", http.StatusMethodNotAllowed, "InvalidRequest", http.MethodPost},
		{"post on query", http.MethodPost, "/xrpc/cco.config.get", "{}", http.StatusMethodNotAllowed, "InvalidRequest", http.MethodGet},
		{"unknown query param", http.MethodGet, "/xrpc/cco.config.get?bogus=1", "", http.StatusBadRequest, "InvalidRequest", ""},
		{"non-object body", http.MethodPost, "/xrpc/cco.config.set", "[1,2,3]", http.StatusBadRequest, "InvalidRequest", ""},
		{"oversize body", http.MethodPost, "/xrpc/cco.config.set", oversize, http.StatusBadRequest, "InvalidRequest", ""},
		{"handler not found", http.MethodGet, "/xrpc/cco.agent.show?agent_id=missing", "", http.StatusNotFound, "NotFound", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := doReq(t, ts, tc.method, tc.path, tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			if tc.wantAllow != "" {
				if got := resp.Header.Get("Allow"); got != tc.wantAllow {
					t.Errorf("Allow = %q, want %q", got, tc.wantAllow)
				}
			}
			var env errEnvelope
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				t.Fatalf("decode error envelope: %v", err)
			}
			if env.Error != tc.wantError {
				t.Errorf("error = %q, want %q", env.Error, tc.wantError)
			}
			if env.Message == "" {
				t.Error("error envelope has empty message")
			}
		})
	}
}

// TestXRPCHandlerNotFoundStripsPrefix asserts the "<Code>: " prefix is parsed out of a
// handler's reply error and never leaks into the JSON envelope's message.
func TestXRPCHandlerNotFoundStripsPrefix(t *testing.T) {
	_, ts := newXRPCServer(t)
	resp := doReq(t, ts, http.MethodGet, "/xrpc/cco.agent.show?agent_id=missing", "")
	defer resp.Body.Close()
	var env errEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if strings.HasPrefix(env.Message, string(codeNotFound)+": ") {
		t.Errorf("message %q still carries the code prefix", env.Message)
	}
	if env.Message == "" {
		t.Error("expected the underlying handler message, got empty")
	}
}

// TestXRPCVerbatimBody proves the ok-reply body is written byte-for-byte from the
// daemon Reply, never re-marshaled.
func TestXRPCVerbatimBody(t *testing.T) {
	s, ts := newXRPCServer(t)

	resp := doReq(t, ts, http.MethodGet, "/xrpc/cco.config.get?key=absent", "")
	defer resp.Body.Close()
	httpBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read http body: %v", err)
	}

	reply := s.Dispatch(context.Background(), daemon.Envelope{
		Proto:   daemon.ProtocolVersion,
		Op:      mConfigGet.op(),
		Session: AppName,
		Body:    json.RawMessage(`{"key":"absent"}`),
	})
	if !reply.OK {
		t.Fatalf("dispatch failed: %s", reply.Error)
	}
	if !bytes.Equal(httpBody, reply.Body) {
		t.Fatalf("http body %q != reply body %q (response was re-marshaled)", httpBody, reply.Body)
	}
}

// TestParseQueryValue covers the GET param coercion in isolation — the int/bool/number
// parse-failure paths in particular, which no current xrpc query field (all strings)
// reaches over HTTP.
func TestParseQueryValue(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     string
		raw     string
		want    any
		wantErr bool
	}{
		{"string passes through", "string", "hi", "hi", false},
		{"integer parses", "integer", "42", int64(42), false},
		{"integer parse failure", "integer", "nope", nil, true},
		{"boolean parses", "boolean", "true", true, false},
		{"boolean parse failure", "boolean", "maybe", nil, true},
		{"number parses", "number", "3.5", 3.5, false},
		{"number parse failure", "number", "x", nil, true},
		{"non-scalar rejected", "array", "1", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseQueryValue(map[string]any{"type": tc.typ}, tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}
