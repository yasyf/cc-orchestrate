# HTTP API reference

The daemon serves an XRPC-style HTTP API for programmatic clients — TUIs,
dashboards, and scripts that outgrow shelling out to `cco`. Every daemon op is a
method named `cco.<noun>.<verb>`, invoked as `GET` or `POST /xrpc/<method>`, and
the API describes itself: one catalog fetch returns every method with its JSON
schemas, ready for client-side type generation.

## Discovery and auth

The daemon publishes its listen port in `~/.cc-orchestrate/http.json`:

```json
{"port": 49213}
```

The daemon binds loopback by default and starts lazily — any `cco` command
brings it up. Loopback requests with no `Origin` header (or a loopback one)
need no credentials. A request carrying a foreign `Origin`, or arriving on a
non-loopback bind, must present the daemon's bearer token via
`Authorization: Bearer <token>` or the `?token=` query fallback.

## Methods

```
GET  /xrpc/<method>    queries    (parameters as query string)
POST /xrpc/<method>    procedures (parameters as a JSON object body)
GET  /xrpc/cco.server.describe    the catalog
```

A query is a read; a procedure mutates. `HEAD` is accepted wherever `GET` is.
The wrong verb answers `405` with an `Allow` header; an unknown or socket-only
method answers `404 MethodNotFound`. Both are JSON envelopes — every `/xrpc/*`
response is JSON, regardless of verb or outcome.

The method surface spans five namespaces:

| Namespace | Verbs |
| --- | --- |
| `cco.repo.*`, `cco.workstream.*`, `cco.sprint.*` | `create`, `list`, `show`, `activate`, `kill` |
| `cco.agent.*` | `spawn`, `list`, `show`, `sendMessage`, `kill`, `respawn`, `capture` |
| `cco.config.*` | `get`, `set`, `list`, `unset` |
| `cco.fleet.*` | `status`, `serialize`, `restore` |

`cco.agent.report` exists on the daemon socket only — it is the child agent's
reporting channel, not a parent-facing method. The catalog is the canonical
machine-readable list; this table is orientation.

### Requests

Request decoding is strict on every transport:

- A `POST` body is one JSON object, at most 1 MiB. An unknown field is a `400`,
  never a silent no-op. An empty body invokes a no-argument method.
- `GET` query parameters are typed against the method's request schema: strings
  pass through, integers and booleans parse. An unknown parameter, an
  unparseable value, or a duplicated key is a `400`.
- Paths in request bodies must be absolute — an HTTP caller has no working
  directory for the daemon to resolve against.

A request times out after 35 seconds and reports `InternalError`.

### Responses

Success is `200` with the method's result verbatim — the same bytes
`cco --json` prints. Failure is a JSON envelope:

```json
{"error": "NotFound", "message": "no agent a1f3c2"}
```

| `error` | HTTP status |
| --- | --- |
| `InvalidRequest` | 400 |
| `NotFound` / `MethodNotFound` | 404 |
| `Conflict` | 409 |
| `InternalError` | 500 |
| `Unsupported` | 501 |

### The catalog

`GET /xrpc/cco.server.describe` returns the app name and version, every
HTTP-exposed method with its kind, description, and request/response JSON
schemas, and the event-stream section:

```bash
curl "http://127.0.0.1:<port>/xrpc/cco.server.describe"
```

```json
{
  "app": "cc-orchestrate",
  "version": "<version>",
  "methods": [
    {"name": "cco.agent.spawn", "type": "procedure", "description": "…",
     "input": {"type": "object", "properties": {"prompt": {"type": "string"}}},
     "output": {"type": "object", "properties": {"agent_id": {"type": "string"}}}}
  ],
  "events": {"stream": "/events?session=fleet", "types": {"fleet.agent.spawned": {"type": "object"}}}
}
```

## The fleet stream

`GET /events?session=fleet` is a server-sent-event stream mirroring every
lifecycle change across the fleet as compact typed frames. Each SSE event's
`id` is a per-stream sequence number; resume a dropped connection with the
`Last-Event-ID` header (or the `?last_event_id=` query fallback for native
`EventSource`) and the replay is gap-free. Every frame carries `type` and `ts`
(RFC 3339 UTC) plus the identifiers below:

| Frame | Fields | Emitted when |
| --- | --- | --- |
| `fleet.agent.spawned` | `agent_id`, `name`, `sprint_id`, `backend`, `subject` | an agent spawns, or restore revives one |
| `fleet.agent.status` | `agent_id`, `state`, `tool`, `target`, `tokens` | the agent's transcript advances |
| `fleet.agent.message` | `agent_id` | a message is delivered to the agent |
| `fleet.agent.report` | `agent_id`, `state` | the agent reports back |
| `fleet.agent.exited` | `agent_id`, `reason` (`killed` \| `exited`) | the agent stops, directly or via a cascade |
| `fleet.agent.restarted` | `agent_id`, `attempt` | a respawn (`attempt` 0) or supervisor restart (1+) |
| `fleet.agent.abandoned` | `agent_id`, `attempts` | the restart budget runs out |
| `fleet.{repo,workstream,sprint}.{created,activated,killed}` | `id`, `name` | container lifecycle changes |
| `fleet.serialized`, `fleet.restored` | `path`, `count` | a bundle is written or restored |

Status frames coalesce: a state, tool, or target change emits immediately, but
token-only updates emit at most once per agent per three seconds. Exact live
token counts stay on the per-agent stream. Frames mirror committed state — a
kill whose backend teardown fails still emits its `exited` frame. A cascade
kill emits the directly-killed container's `killed` frame plus one `exited`
per agent; intermediate containers get no individual frames. Ordering is
guaranteed per stream, not across concurrent handlers.

The `subject` field on a spawned frame (also `subject_id` on every agent view)
keys the agent's own event stream: `GET /events?session=<subject>` carries that
agent's full status, message, and report detail.

## Client lifecycle

A client bootstraps in three requests, then stays current on one stream:

```
read ~/.cc-orchestrate/http.json            → the port
GET /xrpc/cco.server.describe               → identity, schemas
GET /xrpc/cco.fleet.status                  → snapshot + resume cursor
GET /events?session=fleet&last_event_id=<seq> → live frames from the snapshot on
```

`cco.fleet.status` returns the full tree (`repos`, `workstreams`, `sprints`,
`agents`), the fleet subject id, the daemon's `http_port`, and `seq` — the
stream cursor captured before the snapshot was read, so resuming at `seq`
re-delivers at worst a frame the snapshot already reflects, and never skips
one. On a connection error, re-read the handshake file (the port changes when
the daemon restarts) and resume from the last seen event id.
