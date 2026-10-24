# gRPC Worker Protocol

External workers report progress on dispatched saga actions to the engine over a
gRPC stream. This document describes that wire protocol for SDK authors and
integrators.

The schema source of truth is [`proto/liveness.proto`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/proto/liveness.proto)
(proto package `saga.v1`, Go package `livenesspb`). This doc explains it;
the `.proto` file remains authoritative for exact wire layout.

## Endpoint

The engine (`cmd/engine`) serves gRPC on **`:9090`** by default
(`WORKFLOW_ENGINE_GRPC_PORT`, see `internal/config/config.go`). The transport is
plaintext (`insecure` credentials) — TLS is expected to be terminated by the
service mesh / ingress, not by the engine itself.

A worker's gRPC address is configured via `BootstrapConfig.GrpcURL`, e.g.
`go-saga-orchestration-engine.platform.svc.cluster.local:9090`. To reach it with
TLS or per-RPC credentials, pass `BootstrapConfig.DialOptions`.

An application that embeds the engine serves the same service from its own gRPC
server with the public `liveness` package (see
[Serving the stream from your own application](#serving-the-stream-from-your-own-application)).

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
(enforced server-side by `liveness.Server`):

1. **Worker → `StartJob`** with `run_id`, `step_id`, `attempt`. A second
   `StartJob` on the same stream is rejected with `INVALID_ARGUMENT`, and so is
   a `run_id` that isn't a UUID.
2. **Engine → `Acknowledged`**, but only when the run is paused on that step,
   at that attempt, waiting for an action. Otherwise the engine ends the stream
   with `NOT_FOUND` (unknown run) or `FAILED_PRECONDITION` (the run isn't
   waiting for this step and attempt: a stale, duplicate or misrouted
   dispatch). A worker should acknowledge such a dispatch and drop it, since
   redelivering it can never succeed.
3. **Worker → `Heartbeat`** (zero or more, optional) while the handler runs.
4. **Worker → `Complete`** (success) **or `Error`** (failure). This is terminal.
   Sending `Complete`/`Error` before `StartJob` is rejected with
   `INVALID_ARGUMENT`.
5. **Engine** processes the terminal message and ends the stream (returns from
   the RPC). The worker then closes its send side.

On the terminal message the engine checks again that the run is waiting, then:
- `Complete` → JSON-decodes `result_json` (a non-JSON body is preserved under
  `_raw_result`), calls `store.CompleteAction(run, attempt, result)`, then
  publishes `saga.advance` to wake the paused saga.
- `Error` → calls `store.FailAction(run, attempt, code, message, retryable)`,
  which transitions the run to failed, then compensates the steps that
  finished before it (newest first), the same rollback a step failing inside
  the engine gets. No advance is published.

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

1. `Bootstrap` registers actions over REST (unless `SkipRegistration` is set),
   declares a durable RabbitMQ queue (`<service>.actions`, or `Queue`) bound
   to `action.direct` once per action name, and opens one long-lived gRPC
   client (`pb.NewWorkerLivenessClient`) to `GrpcURL`.
2. For each RabbitMQ delivery, `processDelivery` decodes an `ActionPayload`
   (`run_id`, `step_id`, `attempt`, `action`, `inputs`, `dry_run`,
   `compensation`) and resolves the handler by the full action name. A
   compensation runs its handler directly: there is no awaited step, so no
   stream. An action calls `driveStream`.
3. `driveStream` opens an `ExecuteStep` stream and performs the handshake:
   sends `StartJob`, waits for the `Acknowledged`, runs `Handler.Execute`, then
   sends `Complete{result_json}` on success or `Error{code, message, retryable}`
   on failure (codes come from errors implementing the `coded` interface, e.g.
   `worker.Errorf`). It always `CloseSend`s the stream.
4. RabbitMQ ack policy: handler/engine success → `Ack`; the engine refusing
   the report (`NOT_FOUND`, `FAILED_PRECONDITION`) → `Ack` and drop; any other
   stream failure → `Nack` with requeue (retry via redelivery); an undecodable
   payload, unknown action or failed compensation → `Nack` to the DLQ. A
   handler `Error` is reported on the stream but is *not* a transport failure,
   so the delivery is acked.

In v1, the runtime sends no `Heartbeat`s and does not yet act on
`CancelRequested`; those parts of the schema are forward-looking.

## Queues and routing

The `action` verb publishes each dispatch to the durable direct exchange
`action.direct` with the step's full action name as the routing key, for
example `billing.charge`. A compensation goes the same way, with the
compensation's action name and `"compensation": true` in the body. Direct
exchanges match routing keys exactly, so a worker binds its queue once per
action name; a wildcard such as `billing.*` never matches. The public
`topology` package names the exchanges and queues and declares them:

```go
// The engine's own exchanges and queues.
err := topology.Declare(ch)

// A worker's queue, bound once per action it handles.
err = topology.DeclareActionQueue(ch, "billing.actions", "billing.charge", "billing.refund")
```

## Serving the stream from your own application

An application that embeds the engine (see [Embedding](./embedding.md)) serves
`WorkerLiveness` itself with the `liveness` package. `liveness.StoreReporter`
is the default behaviour described above:

```go
srv := grpc.NewServer()
liveness.Register(srv, liveness.NewServer(&liveness.StoreReporter{
    Store:       st,                // the store the engine runs on
    Advance:     pub,               // wakes a run once its step completes
    Compensator: s.Coordinator(),   // rolls back a run whose step failed
    Logger:      logger,
}, logger))
```

To add your own bookkeeping (an audit record, a registry of live modules, a
different advance path), implement `liveness.Reporter` yourself, usually by
wrapping a `StoreReporter`. Return an error wrapping `liveness.ErrRunNotFound`
or `liveness.ErrStepNotAwaited` to refuse a report; the server maps them to
`NOT_FOUND` and `FAILED_PRECONDITION`, and any other error to `INTERNAL`
without sending its text to the worker.

When a failure is recorded some other way (a step that timed out waiting for
its worker, for example), `Coordinator.CompensateFailedStep(ctx, runID,
stepID)` runs the same rollback.
