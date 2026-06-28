# gRPC Worker Protocol

External workers report progress on dispatched saga actions to the engine over a
gRPC stream. This document describes that wire protocol for SDK authors and
integrators.

The schema source of truth is [`proto/liveness.proto`](../proto/liveness.proto)
(proto package `saga.v1`, Go package `livenesspb`). This doc explains it;
the `.proto` file remains authoritative for exact wire layout.

## Endpoint

The engine (`cmd/engine`) serves gRPC on **`:9090`** by default
(`WORKFLOW_ENGINE_GRPC_PORT`, see `internal/config/config.go`). The transport is
plaintext (`insecure` credentials) — TLS is expected to be terminated by the
service mesh / ingress, not by the engine itself.

A worker's gRPC address is configured via `BootstrapConfig.GrpcURL`, e.g.
`go-saga-orchestration-engine.platform.svc.cluster.local:9090`.

## Service: `WorkerLiveness`

```proto
service WorkerLiveness {
  rpc ExecuteStep(stream WorkerEvent) returns (stream EngineEvent);
}
```

`ExecuteStep` is a **bidirectional stream**. The worker is the client: it opens
one stream per action execution, sends `WorkerEvent` messages, and receives
`EngineEvent` messages. The full method name is
`/saga.v1.WorkerLiveness/ExecuteStep`.

## Messages

### `WorkerEvent` (worker → engine)

A `oneof event` carrying exactly one of:

| Variant | Type | Meaning |
|---|---|---|
| `start` | `StartJob` | Opens the job. Must be the first message. |
| `heartbeat` | `Heartbeat` | Optional liveness/progress signal while executing. |
| `complete` | `Complete` | Terminal success. |
| `error` | `Error` | Terminal failure. |

**`StartJob`**
- `run_id` (string) — saga run UUID.
- `step_id` (string) — the step/action being executed.
- `attempt` (int32) — attempt number (matches the dispatched `ActionPayload.attempt`).

**`Heartbeat`**
- `progress_pct` (int32) — 0–100 progress hint.
- `note` (string) — free-text status.

Heartbeats cause no state change today; the engine logs them at debug level.
They exist as a hook for long-action timeout extension.

**`Complete`**
- `result_json` (bytes) — JSON object merged into the saga's variables on success.
- `would_change_json` (bytes) — if non-empty, marks this as a dry-run preview
  (structured "what would change" rather than an applied side effect).

**`Error`**
- `code` (string) — stable error code.
- `message` (string) — human-readable detail.
- `retryable` (bool) — whether the engine should allow a retry.

### `EngineEvent` (engine → worker)

A `oneof event` carrying exactly one of:

| Variant | Type | Meaning |
|---|---|---|
| `ack` | `Acknowledged` | Engine accepted the `StartJob`. |
| `cancel` | `CancelRequested` | Engine asks the worker to abandon the job. |

**`Acknowledged`** — empty message.

**`CancelRequested`** — `reason` (string). (Defined in the schema; the current
engine implementation does not yet emit it.)

## Lifecycle / handshake

One `ExecuteStep` stream maps to one action execution. The frame protocol
(enforced server-side in `internal/grpc/server.go`):

1. **Worker → `StartJob`** with `run_id`, `step_id`, `attempt`. A second
   `StartJob` on the same stream is rejected (`duplicate StartJob`).
2. **Engine → `Acknowledged`**.
3. **Worker → `Heartbeat`** (zero or more, optional) while the handler runs.
4. **Worker → `Complete`** (success) **or `Error`** (failure). This is terminal.
   Sending `Complete`/`Error` before `StartJob` is rejected
   (`complete/error without start`).
5. **Engine** processes the terminal message and ends the stream (returns from
   the RPC). The worker then closes its send side.

On the terminal message the engine:
- `Complete` → parses `run_id` (UUID), JSON-decodes `result_json` (a non-JSON
  body is preserved under `_raw_result`), calls `store.CompleteAction(run, attempt,
  result)`, then publishes `saga.advance` via the `AdvancePublisher` to wake the
  paused saga.
- `Error` → calls `store.FailAction(run, attempt, code, message, retryable)`,
  which transitions the run to failed. No advance is published.

### Failure / disconnect semantics

Errors at the gRPC layer itself (network drop, decode failures, EOF before a
terminal message) leave the saga in its awaiting-action state. They are **not**
treated as action failures. Recovery comes from RabbitMQ redelivery of the
dispatch plus the worker's idempotency wrapper — not from the gRPC stream. A
handler error is reported as an `Error` message (engine fails the action), which
is distinct from a transport error (stream just breaks and the delivery is
retried).

## How the Go worker runtime drives it

The reference client lives in `clients/go/worker`. Workers don't talk to the
proto directly; they register `Handler`s and the runtime drives the stream.

Flow (`runtime.go`):

1. `Bootstrap` registers actions over REST, declares a per-service RabbitMQ
   queue (`<service>.actions`), and opens one long-lived gRPC client
   (`pb.NewWorkerLivenessClient`) to `GrpcURL`.
2. For each RabbitMQ delivery, `processDelivery` decodes an `ActionPayload`
   (`run_id`, `step_id`, `attempt`, `action`, `inputs`, `dry_run`), resolves the
   handler by the action-name suffix, and calls `driveStream`.
3. `driveStream` opens an `ExecuteStep` stream and performs the handshake:
   sends `StartJob`, waits for the `Acknowledged`, runs `Handler.Execute`, then
   sends `Complete{result_json}` on success or `Error{code, message, retryable}`
   on failure (codes come from errors implementing the `coded` interface, e.g.
   `worker.Errorf`). It always `CloseSend`s the stream.
4. RabbitMQ ack policy: handler/engine success → `Ack`; a transport-level stream
   failure → `Nack` with requeue (retry via redelivery); an undecodable payload
   or unknown action → `Nack` to the DLQ. A handler `Error` is reported on the
   stream but is *not* a transport failure, so the delivery is acked.

In v1, the runtime sends no `Heartbeat`s and does not yet act on
`CancelRequested`; those parts of the schema are forward-looking.
