---
sidebar_position: 2
---

# 🧩 Embedding Guide

This guide walks you through adding `go-saga-orchestration` as an in-process library to your Go service — from a thirty-second hello world all the way to production wiring.

---

## 🚀 Quickstart

The fastest path is `saga.InMemory()`: an in-process engine backed by a thread-safe in-memory store. No database, no message broker, no external processes required.

```go
import (
    "context"
    "fmt"

    "github.com/Bugs5382/go-saga-orchestration/saga"
    "github.com/Bugs5382/go-saga-orchestration/domain"
)

sc := saga.InMemory()

sc.Register(domain.WorkflowDefinition{
    ID: "hello", Version: 1, Start: "greet", Published: true,
    Steps: []domain.Step{
        {ID: "greet", Type: "noop", Next: "done"},
        {ID: "done",  Type: domain.StepTypeEnd},
    },
})

runID, err := sc.Start(context.Background(), "hello", map[string]any{"name": "world"})
if err != nil {
    panic(err)
}

run, _ := sc.Get(context.Background(), runID)
fmt.Println(run.State) // succeeded
```

`sc.Start` creates the run **and** advances it synchronously to the first pause or terminal state, so for an all-synchronous workflow the run is already complete by the time `Start` returns.

> 💡 See [`examples/basic`](https://github.com/Bugs5382/go-saga-orchestration/tree/main/examples/basic) for a runnable standalone example.

---

## 🧩 Custom verbs

Register your own step type with a closure. The return value is a `map[string]any` that gets **merged into** `run.Variables`.

```go
import "github.com/Bugs5382/go-saga-orchestration/engine/verbs"

sc.RegisterVerb(
    "charge_card",   // step type name used in workflow JSON/Go
    "common",        // license group — "common" means no gate
    verbs.HandlerFunc(func(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
        total, _ := run.Variables["total"].(float64)
        if total <= 0 {
            return nil, fmt.Errorf("charge_card: invalid total")
        }
        // ... call your payment service ...
        return map[string]any{"charge_id": "ch_abc123", "charged": total}, nil
    }),
)
```

The keys returned (`charge_id`, `charged`) land directly in `Variables` and are visible to every subsequent step. Use `set_var` or `transform` steps after the custom verb to rename or reshape them if needed.

---

## 🔌 Custom actions (worker round-trip)

For steps that need to run in a **separate process** (e.g. a microservice that owns its own business logic), use `type: "action"` in your workflow definition:

```json
{
  "id": "charge",
  "type": "action",
  "action": "payments.charge_card",
  "inputs": {"total": 4200},
  "next": "confirm"
}
```

The engine dispatches `payments.charge_card` over the configured publisher (RabbitMQ in service mode, in-process in embedded mode). A worker process built with the [Go worker SDK](https://github.com/Bugs5382/go-saga-orchestration/tree/main/clients/go/worker) connects over the gRPC `ExecuteStep` stream, registers a handler for `payments.charge_card`, and returns a result map that is merged into `Variables`.

> ⚠️ Pure `saga.InMemory()` with no worker goroutine will leave an `action` step paused indefinitely — the action verb pauses the saga and waits for a worker reply. You need either a worker process (service mode) or a registered `RegisterVerb` handler with the same step type to handle it in-process.

See [`docs/grpc.md`](grpc.md) for the worker protocol and [`clients/go/worker`](https://github.com/Bugs5382/go-saga-orchestration/tree/main/clients/go/worker) for the SDK.

---

## 🪢 Data flow between steps

Each step operates on exactly one verb, and all data flows through `run.Variables`. CEL verbs (e.g. `transform`, `filter`, `switch`) read from `Variables` — **not** from `step.Inputs` directly.

**Pattern:** use `set_var` to seed a variable from a literal or a previous step's output, then reference it in downstream CEL expressions.

```
start → set_var (out_var: "items", value: [...]) → filter → transform → end
```

**Two worked scenarios in [`examples/workflows/`](https://github.com/Bugs5382/go-saga-orchestration/tree/main/examples/workflows):**

- [`scenario_action_to_setvar.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/scenario_action_to_setvar.json) — an `action` step returns a result, then a `set_var` step reads the worker's output key and assigns it to a clean variable name for downstream steps.
- [`scenario_parallel_setvars.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/scenario_parallel_setvars.json) — parallel branches each write to distinct variables, which are available after the join.

> 💡 Prefer descriptive `out_var` names. The `http_request` and `webhook_emit` verbs default to `http_result` / `webhook_result` — override `out_var` to avoid collisions when you call multiple endpoints in the same workflow.

---

## 🚪 Entry points / call tree

Every `WorkflowDefinition` has a default entry point (`Start` field). You can define **named entry points** with the `Entrypoints` map:

```go
domain.WorkflowDefinition{
    ID:      "order",
    Version: 1,
    Start:   "charge",
    Entrypoints: map[string]string{
        "refund":  "start_refund",
        "cancel":  "start_cancel",
    },
    Steps: []domain.Step{ /* ... */ },
}
```

Start a run at a named entry point with `StartAt`:

```go
runID, err := sc.StartAt(ctx, "order", "refund", map[string]any{"order_id": "ord_99"})
```

`sub_saga` and `spawn_saga` steps also accept an `entrypoint` input so a parent workflow can invoke a specific slice of a child workflow without a separate definition.

REST triggers (service mode) accept an `entrypoint` field in the trigger configuration — see [`docs/api.md`](api.md).

---

## 🏭 Production wiring

Replace `InMemory()` with `saga.New(opts)` and provide your own store and infrastructure:

```go
import (
    "github.com/Bugs5382/go-saga-orchestration/saga"
    "github.com/Bugs5382/go-saga-orchestration/store/postgres"
)

pgStore, err := postgres.New(ctx, databaseDSN)
if err != nil {
    log.Fatal(err)
}

sc, err := saga.New(saga.Options{
    Store:     pgStore,           // durable Postgres store (see store/postgres)
    Licensing: myLicenseResolver, // licensing.Resolver — controls feature groups
    Secrets:   mySecretsResolver, // secrets.Resolver — for http_request/webhook_emit
    Publisher: rabbitPublisher,   // engine.Publisher — RabbitMQ-backed
    Logger:    &logger,           // *zerolog.Logger
    Context:   appCtx,            // base context for background advances
})
```

Key option notes:
- **`Store`** is the only required field. All others have in-process defaults.
- **`Licensing`**: omit (or pass `nil`) for `StubAllowAll` (all groups permitted). Provide your own `licensing.Resolver` to gate feature groups in production.
- **`Secrets`**: omit for an in-memory store seeded from a map. Provide a Vault-backed (or similar) resolver for production.
- **`Publisher`**: omit for in-process fan-out. Provide a RabbitMQ publisher to enable multi-process workers and the `action` round-trip.
- See [`store/postgres`](https://github.com/Bugs5382/go-saga-orchestration/tree/main/store/postgres) for the Postgres store implementation and SQL migrations.

---

## ♻️ Lifecycle

When your application shuts down, call `Shutdown` to drain in-flight background advances:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := sc.Shutdown(shutdownCtx); err != nil {
    log.Warn("saga shutdown timed out", "err", err)
}
```

`Shutdown` cancels the internal background context (pausing new work between steps) and waits for all in-flight goroutines (from `parallel`, `foreach`, and `spawn_saga` child advances) to drain. If they don't finish before `shutdownCtx` expires, `ctx.Err()` is returned.

> ⚠️ After `Shutdown` returns, the `Saga` instance should not be reused.

---

## 🛰️ Service mode

For multi-process deployments, the repo ships two reference binaries:

- **`cmd/api`** — REST API on `:8080`. Handles workflow publishing, run lifecycle, signals, user tasks, and triggers. See [`docs/api.md`](api.md).
- **`cmd/engine`** — Saga coordinator + gRPC worker server on `:9090`. Reads from the `saga.advance` RabbitMQ queue and drives runs. Hosts the `ExecuteStep` gRPC stream that workers connect to. See [`docs/grpc.md`](grpc.md).

Both binaries require Postgres (`DATABASE_DSN`) and RabbitMQ (`RABBITMQ_URL`). The overall architecture is documented in [`docs/architecture.md`](architecture.md).

```bash
go run ./cmd/api     # REST API on :8080
go run ./cmd/engine  # coordinator + gRPC on :9090
```
