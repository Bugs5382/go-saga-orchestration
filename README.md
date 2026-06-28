# go-saga-orchestration

A standalone, solution-agnostic **saga orchestrator + synchronous CEL rule evaluator** you can embed as a Go library or run as a two-binary service.

---

## ✨ Features

- **31 saga step types** — data transforms, HTTP/webhooks, timers, signals, events, parallel fan-out, foreach, loops, try/catch, human tasks, sub-sagas, and more (see [`docs/verbs.md`](docs/verbs.md)).
- **Embed or deploy** — run in-process with zero infrastructure, or deploy as two Docker-friendly binaries backed by Postgres + RabbitMQ.
- **CEL expressions** — [Google Common Expression Language](https://cel.dev) for conditions, transforms, filters, and routing, all evaluated against live run variables.
- **Named entrypoints** — `Entrypoints map[string]string` on a `WorkflowDefinition` lets a single workflow serve multiple start scenarios; triggers and `sub_saga`/`spawn_saga` accept an `entrypoint` input.
- **gRPC workers** — microservices connect over bidirectional gRPC streams to handle `action` steps and return results without polling.
- **Durable audit trail** — every step transition, rule evaluation, signal, and metric is written as an immutable event row.
- **License-gated verbs** — feature groups (`waits`, `parallel_control`, `human_interaction`, …) are checked at publish and runtime so environments only use the features they are licensed for.

---

## 🚀 30-second embed quickstart

```go
import (
    "context"

    "github.com/Bugs5382/go-saga-orchestration/saga"
    "github.com/Bugs5382/go-saga-orchestration/domain"
    "github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

sc := saga.InMemory() // in-memory store + in-process advance

// Register your own verb as a closure:
sc.RegisterVerb("charge_card", "common",
    verbs.HandlerFunc(func(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
        return map[string]any{"ok": true}, nil
    }))

// Define + publish a workflow, then start it:
sc.Register(domain.WorkflowDefinition{
    ID: "checkout", Version: 1, Start: "charge", Published: true,
    Steps: []domain.Step{
        {ID: "charge", Type: "charge_card", Next: "done"},
        {ID: "done", Type: domain.StepTypeEnd},
    },
})
runID, _ := sc.Start(context.Background(), "checkout", map[string]any{"total": 4200})
run, _ := sc.Get(context.Background(), runID)
_ = run.State // succeeded
```

See [`examples/basic`](examples/basic) for a runnable standalone example, and [`docs/embedding.md`](docs/embedding.md) for the full embedding guide including production wiring.

---

## 🧭 Which mode? Embedded vs service

| | Embedded library | Service mode |
|---|---|---|
| **Infrastructure** | None | Postgres + RabbitMQ |
| **Workers** | `RegisterVerb` closures | Separate processes via gRPC |
| **Scale** | Single process | Horizontally scalable |
| **Best for** | Tests, simple automations, CLIs | Production multi-tenant deployments |

**Embedded:** `saga.InMemory()` for tests; `saga.New(saga.Options{Store: pgStore, ...})` for in-process production use with a durable store.

**Service:** `cmd/api` (REST, `:8080`) + `cmd/engine` (coordinator + gRPC, `:9090`). Workers connect via the gRPC `ExecuteStep` stream; clients use the REST API.

---

## 📚 Docs

| Doc | What it covers |
|---|---|
| [`docs/verbs.md`](docs/verbs.md) | Complete reference for all 31 step types — inputs, outputs, license groups, and example links |
| [`docs/embedding.md`](docs/embedding.md) | Quickstart, custom verbs, data flow, entry points, production wiring, lifecycle, service mode |
| [`docs/stores.md`](docs/stores.md) | Store backend selection (`STORE_TYPE`), env vars, Redis/Valkey durability, `REDIS_RUN_TTL`, and the stream-requires-postgres limitation |
| [`docs/caveats.md`](docs/caveats.md) | Limitations and common gotchas with workarounds |
| [`docs/architecture.md`](docs/architecture.md) | Engine internals, coordinator, MQ topology, stores, request flow, CEL rules |
| [`docs/api.md`](docs/api.md) + [`api/openapi.yaml`](api/openapi.yaml) | REST API reference (17 endpoints) and OpenAPI 3 spec |
| [`docs/grpc.md`](docs/grpc.md) | The `WorkerLiveness.ExecuteStep` worker protocol |
| [`docs/deployment.md`](docs/deployment.md) | Container images (GHCR) and Helm chart deployment |
| [`clients/go/worker/README.md`](clients/go/worker/README.md) | Go worker SDK |
| [`examples/`](examples/) | Basic embed example and 31 per-verb workflow JSON files |

---

## Local development

```bash
go run ./cmd/api     # REST API on :8080
go run ./cmd/engine  # coordinator + gRPC on :9090 (needs Postgres + RabbitMQ)
go build ./...       # build everything
go vet ./...         # vet
```

End-to-end tests under `test/e2e` require Postgres + RabbitMQ.

---

## Configuration

All configuration is via environment variables (`internal/config/config.go`):

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `WORKFLOW_API_PORT` | `8080` | api | REST API listen port |
| `WORKFLOW_ENGINE_GRPC_PORT` | `9090` | engine | gRPC worker server port |
| `DATABASE_DSN` | _(empty)_ | both | Postgres connection string (durable store) |
| `RABBITMQ_URL` | _(empty)_ | both | RabbitMQ connection URL (step dispatch) |
| `STORE_TYPE` | `postgres` | both | Store backend: `postgres` (default) \| `redis` \| `valkey` \| `memory` — see [`docs/stores.md`](docs/stores.md) |
| `REDIS_URL` | _(empty)_ | both | Redis/Valkey connection URL (required when `STORE_TYPE` is `redis` or `valkey`) |
| `REDIS_RUN_TTL` | `0s` | both | Go duration; auto-expire terminal-run keys after this window (default `0s` = keep forever) |

---

## Layout

**Public importable packages** (the library surface):
- `saga` — facade (`saga.InMemory()`, `saga.New(saga.Options{...})`, `*saga.Saga`).
- `domain` — core types (`WorkflowDefinition`, `SagaRun`, `Step`, `RuleDefinition`, etc.).
- `engine`, `engine/verbs` — coordinator + the 31 saga step implementations + `verbs.HandlerFunc`.
- `store`, `store/memory`, `store/postgres` — `Store` interface, in-memory impl, Postgres impl + migrations.
- `api` — REST handlers, router, and OpenAPI spec (`api/openapi.yaml`).
- `licensing`, `secrets`, `clock` — resolver interfaces and stubs.

**Infrastructure (not for direct import)**:
- `internal/mq` — RabbitMQ topology, publisher, consumer.
- `internal/cel`, `internal/rules` — CEL evaluator + decision-table rule evaluation.
- `internal/grpc` — gRPC worker liveness server.
- `internal/config`, `internal/logging` — environment config + structured logging.

**Binaries and supporting dirs**:
- `cmd/api`, `cmd/engine` — the two service binaries (reference service-mode apps).
- `clients/go/worker` — Go worker SDK (nested module) for consuming services.
- `proto/` — gRPC worker liveness service + generated code.
- `test/e2e` — end-to-end tests (require Postgres + RabbitMQ).
- `deployments/helm` — Helm chart deploying the api and engine (see [`docs/deployment.md`](docs/deployment.md)).
- `ui/` — reserved for the future reusable UI framework (outside the Go module; planned).

---

## History

Built as a standalone, solution-agnostic saga engine. The orchestrator and the CEL rule evaluator are deliberately decoupled from any single application so the project can be embedded as a library or run as a service across unrelated solutions.
