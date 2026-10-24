# REST API Guide

This is the narrative companion to [`api/openapi.yaml`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/api/openapi.yaml). It
describes how to drive the go-saga-orchestration engine over HTTP: the base URL and
ports, the saga lifecycle, the live stream format, the error conventions, and a
runnable `curl` example per endpoint group.

All schemas in this guide are derived directly from the Go source
(`internal/api/handler_*.go`, `internal/api/response.go`, `internal/domain/*.go`,
`internal/store/store.go`, `internal/rules/rules.go`) and the handler tests.

## Base URL and ports

The REST API is served by the `api` binary (`cmd/api`). It listens on
`:8080` by default. The port is configurable via the `WORKFLOW_API_PORT`
environment variable.

```
http://localhost:8080
```

(The engine binary, `cmd/engine`, exposes a separate gRPC port — default `9090`,
env `WORKFLOW_ENGINE_GRPC_PORT` — and is not part of this REST surface.)

There is **no authentication** wired today. The stream endpoint explicitly
notes that auth middleware will land in a later batch.

## Endpoint overview

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe |
| GET | `/api/v1/sagas` | List/filter saga runs (paginated) |
| POST | `/api/v1/sagas/start` | Start a saga run |
| GET | `/api/v1/sagas/{id}` | Get one saga run |
| POST | `/api/v1/sagas/{run_id}/signal/{name}` | Deliver an external signal |
| POST | `/api/v1/sagas/{run_id}/user_task/{task_id}/submit` | Submit a user task result |
| POST | `/api/v1/sagas/{run_id}/actions/{step_id}/result` | Report an action result (http/rmq workers) |
| GET | `/api/v1/sagas/{run_id}/stream` | Live run inspector (WebSocket) |
| POST | `/api/v1/registry/register` | Register a service's actions |
| GET | `/api/v1/registry/actions` | List registered actions |
| POST | `/api/v1/rules/{rule_id}/evaluate` | Evaluate a rule |
| POST | `/api/v1/triggers` | Create a trigger |
| GET | `/api/v1/triggers` | List triggers |
| GET | `/api/v1/triggers/{id}` | Get one trigger |
| DELETE | `/api/v1/triggers/{id}` | Delete a trigger |
| GET | `/api/v1/workflows/{wf_id}/stats` | Aggregate workflow stats |

## The saga lifecycle

A saga is one running instance of a workflow definition. The typical lifecycle:

1. **Start** — `POST /api/v1/sagas/start` with a `workflow_id` and `inputs`.
   The engine resolves the published definition, creates a run in `pending`
   state, publishes a `saga.advance` message, and returns `202` with
   `{ "saga_run_id": "<uuid>" }`. The run then progresses through states:
   `pending → running → (paused) → succeeded | failed | cancelled`
   (with `compensating` during rollback). See the `RunState` enum.

2. **Wait points** — when a workflow reaches a `wait_for_signal`,
   `manual_approval`, or `collect_input` step, the run moves to `paused` and
   records what it is awaiting (`awaited_signal`, etc.).

3. **Resume** — there are two ways to wake a paused run:
   - **Signal**: `POST /api/v1/sagas/{run_id}/signal/{name}`. The signal is
     always recorded. If the run was paused awaiting exactly this signal name,
     the server consumes it and publishes `saga.advance`, returning `202`.
     If the run was not paused-and-awaiting this name, it returns `409`
     (the signal is still recorded, but nothing advances).
   - **User task submit**: `POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit`.
     This persists the task result, then internally appends a signal named
     `user_task.{task_id}.submitted` carrying the result as its payload, and
     advances the saga the same way a signal would. Always returns `202` on
     success.

4. **Observe** — `GET /api/v1/sagas/{run_id}/stream` (WebSocket) tails the run
   live; `GET /api/v1/sagas/{id}` fetches the current snapshot; and
   `GET /api/v1/sagas` lists/filters runs.

### The saga run object

`GET /api/v1/sagas/{id}` returns a `SagaRun` (the full Go struct, JSON-tagged):

```json
{
  "id": "f1e2d3c4-0000-0000-0000-000000000000",
  "workflow_id": "example_workflow_v1",
  "definition_id": "a1b2c3d4-0000-0000-0000-000000000000",
  "tenant_id": null,
  "state": "paused",
  "current_step": "await_approval",
  "inputs": { "order_id": "ORD-123" },
  "variables": {},
  "started_at": "2026-05-29T12:00:00Z",
  "last_event_at": "2026-05-29T12:00:05Z",
  "requires_manual_review": false,
  "awaited_signal": "approval.decided",
  "current_attempt": 0
}
```

Optional fields (`terminal_at`, `trigger_id`, `parent_run_id`, `wakeup_at`,
`feature_overrides`, `dry_run`, etc.) are omitted when empty. On a terminal
`failed` or `cancelled` run, `last_error` carries the failing step's error
message (or the cancel reason), so the run is self-describing without
replaying its event log; pair it with the `has_error` list filter to surface
failures.

### Listing runs

`GET /api/v1/sagas` is paginated (`limit` 1–500, default 50; `offset` >= 0,
default 0) and supports filters: `workflow_id`, `state`, `trigger_type`,
`since` (RFC3339), `has_error` (bool), `requires_review` (bool). The response
always includes a non-null `sagas` array plus `total`, `limit`, `offset`.

### `X-Feature-Override` header (start only)

`POST /api/v1/sagas/start` accepts an optional `X-Feature-Override` header to
override license feature flags on a per-request basis. This is valid in any
environment — standalone, on-prem, dev, or QA — not a QA-only facility. Format:
comma-separated `feature=value` pairs, e.g. `wf.parallel=on,wf.timers=off`.
Values `on`/`true`/`1` => true; `off`/`false`/`0` => false; anything else is
silently ignored.

## The stream event format

> **Note:** Despite being listed as an HTTP endpoint, `stream` upgrades to a
> **WebSocket** using `gorilla/websocket`. The documentation below reflects
> the actual handler implementation.

`GET /api/v1/sagas/{run_id}/stream` validates the run exists (returning a
JSON `400` for a bad UUID or `404` for an unknown run **before** upgrading,
using the standard `{"error": code, "message": msg}` envelope), then upgrades
the HTTP connection to a WebSocket. Messages are JSON text frames of the shape:

```json
{ "type": "run",   "data": { /* SagaRun snapshot */ } }
{ "type": "event", "data": { /* SagaRunEvent */ } }
```

On connect the server sends, in order:

1. one `run` frame — the current `SagaRun` snapshot,
2. one `event` frame per existing audit event for the run,
3. then a live `event` frame for each new event as it is recorded (tailed via
   Postgres LISTEN/NOTIFY on a per-run channel).

A `SagaRunEvent` looks like:

```json
{
  "id": "...",
  "run_id": "...",
  "step_id": "await_approval",
  "attempt": 0,
  "event_type": "step.paused",
  "actor": "engine",
  "recorded_at": "2026-05-29T12:00:05Z"
}
```

`event_type` is one of: `saga.started`, `step.dispatched`, `step.started`,
`step.succeeded`, `step.failed`, `step.skipped`, `step.paused`, `run.succeeded`,
`run.failed`, `run.cancelled`, `compensation.started`, `log`, `metric`,
`rule.evaluated`, `license.gate.rejected`.

## Error conventions

All API errors use a single structured JSON envelope:

```json
{ "error": "saga_not_found", "message": "f1e2d3c4-..." }
```

`error` is a stable, machine-readable code; `message` is human-readable detail.
On `5xx` responses the real error is logged server-side and only a generic
`"internal error"` message is returned to the client — never raw internal detail.
On `4xx` the message may include safe client-input context (e.g. the offending
field name).

Machine-readable codes include: `bad_request`, `not_found`, `internal`,
`invalid_config`, `publish_failed`, `unprocessable`, `conflict`,
`workflow_not_found`, `saga_not_found`, `trigger_not_found`.

## curl examples by group

### Health

```bash
curl -s http://localhost:8080/health/live
# {"status":"live"}

curl -s http://localhost:8080/health/ready
# {"status":"ready"}
```

### Sagas

```bash
# Start a saga
curl -s -X POST http://localhost:8080/api/v1/sagas/start \
  -H 'Content-Type: application/json' \
  -H 'X-Feature-Override: wf.parallel=on' \
  -d '{"workflow_id":"example_workflow_v1","version":"latest","inputs":{"order_id":"ORD-123"}}'
# 202 {"saga_run_id":"<uuid>"}

# Get one run
curl -s http://localhost:8080/api/v1/sagas/<run_id>

# List/filter runs
curl -s 'http://localhost:8080/api/v1/sagas?workflow_id=example_workflow_v1&state=paused&limit=20'

# Deliver a signal (202 if it advanced a paused run, 409 otherwise)
curl -s -X POST http://localhost:8080/api/v1/sagas/<run_id>/signal/approval.decided \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"approved":true}}'

# Submit a user task
curl -s -X POST http://localhost:8080/api/v1/sagas/<run_id>/user_task/<task_id>/submit \
  -H 'Content-Type: application/json' \
  -d '{"submitted_by":"alice@example.com","result":{"decision":"approve"}}'

# Stream (WebSocket) — use a WS client, e.g. websocat
websocat ws://localhost:8080/api/v1/sagas/<run_id>/stream
```

### Registry

```bash
# Register actions (idempotent; call on service startup)
# An action may carry an optional dispatch descriptor — "transport"
# (grpc | http | rmq) and "address" (callback URL for http, queue name for
# rmq; required only for http/rmq). Omit it for the gRPC default.
curl -s -X POST http://localhost:8080/api/v1/registry/register \
  -H 'Content-Type: application/json' \
  -d '{
        "service":"example",
        "service_version":"0.18.2",
        "actions":[
          {"action_name":"set_state","version":1,"category":"record_lifecycle","compensable":true,"input_schema":{},"output_schema":{}},
          {"action_name":"send_email","version":1,"input_schema":{},"output_schema":{},"transport":"http","address":"https://worker.example.com/actions/send_email"}
        ]
      }'
# 200 {"service":"example","service_version":"0.18.2","registered":2}

# List actions (descriptor is echoed back)
curl -s 'http://localhost:8080/api/v1/registry/actions?service=example&category=record_lifecycle'
# {"actions":[ ... ]}
```

### Action result callback (http / rmq workers)

`POST /api/v1/sagas/{run_id}/actions/{step_id}/result`

gRPC workers reply over the `ExecuteStep` stream. Workers reached over the
`http` or `rmq` transport have no return stream, so they report their result
asynchronously here. The endpoint applies the same `CompleteAction` /
`FailAction` semantics as the gRPC path: success merges the result into the
run's variables and resumes the saga; failure transitions the run to `failed`.
Attempt handling and idempotency are preserved — a stale `attempt` is a no-op.

Send **exactly one** of `result` or `error`. `attempt` is optional; omitted,
it defaults to the run's current attempt (the only in-flight dispatch).

```bash
# Success — completes the action and advances the saga.
curl -s -X POST http://localhost:8080/api/v1/sagas/<run_id>/actions/<step_id>/result \
  -H 'Content-Type: application/json' \
  -d '{"result":{"ticket_number":"INC-999"}}'
# 202

# Failure — transitions the run to failed.
curl -s -X POST http://localhost:8080/api/v1/sagas/<run_id>/actions/<step_id>/result \
  -H 'Content-Type: application/json' \
  -d '{"error":{"code":"ERR_WORKER_CRASH","message":"worker panicked","retryable":false}}'
# 202
```

### Rules

```bash
curl -s -X POST http://localhost:8080/api/v1/rules/triage/evaluate \
  -H 'Content-Type: application/json' \
  -d '{"inputs":{"priority":"p1"}}'
# 200 {"output":{"branch":"high"},"audit":[{"index":0,"when":"priority == 'p1'","matched":true}]}
```

### Triggers

```bash
# Create
curl -s -X POST http://localhost:8080/api/v1/triggers \
  -H 'Content-Type: application/json' \
  -d '{
        "trigger_type":"record_transition",
        "workflow_id":"example_workflow_v1",
        "version":1,
        "config":{"record_type":"order","from_state":"created","to_state":"pending_review"},
        "enabled":true,
        "created_by":"admin"
      }'
# 201 — body uses PascalCase field names (see note below)

# List (optional ?type= and ?enabled=true|false|1|0)
curl -s 'http://localhost:8080/api/v1/triggers?type=record_transition&enabled=true'
# {"triggers":[ ... ]}

# Get one
curl -s http://localhost:8080/api/v1/triggers/<id>

# Delete (204 on success, 404 if missing)
curl -s -X DELETE http://localhost:8080/api/v1/triggers/<id>
```

### Workflows

```bash
curl -s http://localhost:8080/api/v1/workflows/example_workflow_v1/stats
# {"workflow_id":"example_workflow_v1","success_rate_24h":0.83,"last_run_at":"2026-05-29T12:00:00Z","in_flight":2}
```

## Assumptions and ambiguities resolved

- **Stream is WebSocket, not SSE.** The brief said SSE; the code
  (`handler_stream.go`) uses `gorilla/websocket` and emits `{type, data}` JSON
  frames over a WebSocket. Documented as WebSocket. The OpenAPI spec models the
  endpoint with a `101 Switching Protocols` response and documents the
  `StreamFrame` schema, since OpenAPI 3.0/3.1 cannot natively describe WebSocket
  message streams.

- **`SagaTrigger` is serialized with PascalCase keys.** The `domain.SagaTrigger`
  struct has **no JSON tags**, so Go's `encoding/json` emits the exact Go field
  names: `ID`, `TriggerType`, `WorkflowID`, `Version`, `Config`, `Enabled`,
  `TenantID`, `CreatedAt`, `CreatedBy`. The handler tests confirm this by
  round-tripping responses into `domain.SagaTrigger`. The **request** body
  (`TriggerCreateRequest`), by contrast, is a separate struct with snake_case
  JSON tags. Both shapes are documented faithfully and differ on purpose.

- **Single JSON error contract.** All handlers use `WriteError` producing
  `{"error": code, "message": msg}`. The `PlainTextError` OpenAPI component and
  the legacy `http.Error` call sites have been removed.

- **`config` map values for triggers and rule `inputs`/`output` / action
  `input_schema`/`output_schema` are free-form JSON objects** (Go
  `map[string]any`), so they are modeled as objects with
  `additionalProperties: true`. For `trigger_type: record_transition` the
  server additionally requires `config.record_type`, `config.from_state`, and
  `config.to_state` to be non-empty strings (validated server-side; returns
  `422` `invalid_config` on failure).

- **`tenant_id` typing differs by endpoint.** On `POST /sagas/start` it is a
  UUID (`*uuid.UUID`). On `POST /triggers` and `POST /rules/.../evaluate` it is a
  string in the request body that the server attempts to parse as a UUID
  (invalid values are silently dropped rather than rejected). Modeled per the
  actual struct types.

- **`success_rate_24h` and `last_run_at` are nullable** in `WorkflowStats`
  (pointers in Go): `success_rate_24h` is null when there were no runs in the
  last 24h; `last_run_at` is null when there have been no runs at all.

- **Signal/user-task success responses have empty bodies** (the handlers only
  call `w.WriteHeader`), so no response schema is defined for their 2xx/4xx
  status codes beyond the status itself.
