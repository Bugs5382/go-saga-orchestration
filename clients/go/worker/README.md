# worker — go-saga-orchestration Go SDK

`worker` is the Go SDK that every action-worker service imports to run as a
saga-action worker for the go-saga-orchestration engine. You declare your service's
actions and provide a handler function for each; the SDK does the rest of the
plumbing so your code only contains business logic.

A single `worker.Bootstrap` call handles:

- POSTing the service's action declarations to `go-saga-orchestration/api/v1/registry/register`.
- Declaring the per-service RabbitMQ queue `<service>.actions` bound to `action.direct`.
- Opening a gRPC `ExecuteStep` stream per dispatch and driving it (Start → run handler → Complete/Error).
- Calling the host service's `Handler.Execute(ctx, payload)` (or `Preview` if dry-run).
- ACK/NACK of the RabbitMQ message based on the handler outcome.

Optional idempotency dedupe is available by wrapping handlers with
`worker.Wrap` (see below).

## Install / import

This package is its own Go module (it has its own `go.mod`), so add it
directly:

```sh
go get github.com/Bugs5382/go-saga-orchestration/clients/go/worker
```

```go
import "github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
```

## End-to-end usage

```go
package main

import (
    "context"
    "log"

    "github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
)

func main() {
    // Each action gets a handler. The payload carries the run/step IDs and
    // the step inputs; the returned Result is merged into the saga's
    // variables by the engine on completion.
    setState := worker.HandlerFunc(func(ctx context.Context, p worker.ActionPayload) (worker.Result, error) {
        to, _ := p.Inputs["to"].(string)
        if to == "" {
            // A stable, non-retryable domain error.
            return nil, worker.Errorf("bad_input", false, "missing 'to' input")
        }
        // ... call service.TransitionRecord(...) ...
        return worker.Result{"new_state": to}, nil
    })

    cfg := worker.BootstrapConfig{
        Service:        "example",
        ServiceVersion: "1.0.0",
        RegistryURL:    "http://go-saga-orchestration-api:8080",
        RmqURL:         "amqp://guest:guest@rabbitmq:5672/",
        GrpcURL:        "go-saga-orchestration-engine:9090",
        Actions: []worker.Action{
            {
                Name:        "set_state",
                Description: "Transition a record to a new state",
                Category:    "record_lifecycle",
                Compensable: true,
                Handler:     setState,
            },
        },
    }

    // Bootstrap registers the actions, declares the queue, and runs the
    // consumer loop until ctx is cancelled.
    if err := worker.Bootstrap(context.Background(), cfg); err != nil {
        log.Fatal(err)
    }
}
```

See `example_test.go` for runnable, self-contained examples (handler results,
`CodedError`, and idempotency) that don't require a live broker or engine.

## Idempotency

Dispatches can be redelivered (RabbitMQ requeue, engine retries), so handlers
with side effects should be idempotent. The SDK ships a helper:

```go
store := worker.NewMemoryIdempotencyStore() // test impl; production uses a durable store
handler := worker.Wrap(store, setState)
```

`worker.Wrap` returns a `Handler` that, when the incoming
`ActionPayload.IdempotencyKey` is non-empty:

- looks the key up in the store first and returns the cached `Result` on a hit
  (the inner handler is **not** re-run);
- on a miss, runs the inner handler and caches a successful `Result` under the key.

Errors are never cached, so a failed dispatch can still be retried by the
engine. `IdempotencyStore` is an interface — `MemoryIdempotencyStore` is
intended for tests; in production a host service implements it over a durable
backing store (e.g. a Postgres `idempotency_keys` table).

## Error handling: CodedError

A handler can return any `error`, but returning a `worker.CodedError` lets you
control how the engine records and retries the failure. Construct one with
`worker.Errorf`:

```go
return nil, worker.Errorf("insufficient_funds", false, "account %s lacks funds", acctID)
//                          │ code             │ retryable │ message (fmt)
```

- **code** — a stable error code recorded against the step.
- **retryable** — whether the engine should retry the step.

If a handler returns a plain error that is not coded, the engine defaults to
code `"handler_error"` with `retryable=true`.
