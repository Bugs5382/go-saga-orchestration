# Deployment

go-saga-orchestration ships two stateless services — the HTTP **api** and the
gRPC **engine** — as container images, plus a Helm chart attached to each release.

## Container images

Published to GHCR on every release (multi-arch amd64/arm64):

- `ghcr.io/bugs5382/go-saga-orchestration/api`
- `ghcr.io/bugs5382/go-saga-orchestration/engine`

Tags: `vX.Y.Z`, `latest`, and the commit SHA.

## Prerequisites

Both services require, at runtime:

- A reachable **RabbitMQ** (`RABBITMQ_URL`) — both the api and the engine connect
  on startup and exit if it is unavailable.
- A **store**: `postgres` (default), `redis`/`valkey`, or `memory` (single-process,
  dev only). postgres needs `DATABASE_DSN`; redis/valkey needs `REDIS_URL`.

Supply the connection strings through a Secret and point the chart at it:

```bash
kubectl create secret generic gosaga-conn \
  --from-literal=rabbitmq-url='amqp://user:pass@rabbitmq:5672/' \
  --from-literal=database-dsn='postgres://user:pass@postgres:5432/saga?sslmode=require'
```

## Install the Helm chart

The chart is attached to each GitHub Release as `go-saga-orchestration-<version>.tgz`:

```bash
helm install go-saga \
  https://github.com/Bugs5382/go-saga-orchestration/releases/download/v0.2.2/go-saga-orchestration-0.2.2.tgz \
  --set store.type=postgres \
  --set connectionSecret=gosaga-conn
```

## Configuration

| Value | Default | Description |
|-------|---------|-------------|
| `store.type` | `postgres` | `postgres` / `redis` / `valkey` / `memory` (→ `STORE_TYPE`) |
| `connectionSecret` | `""` | Secret with `rabbitmq-url` (required) + `database-dsn` or `redis-url` |
| `api.replicas` / `engine.replicas` | `1` | Replica counts (both stateless) |
| `api.port` | `8080` | API HTTP port (`WORKFLOW_API_PORT`) |
| `engine.grpcPort` | `9090` | Engine gRPC port (`WORKFLOW_ENGINE_GRPC_PORT`) |
| `ingress.enabled` | `false` | Expose the api via Ingress |

See `chart/values.yaml` for the full set, including probes, resources, and security context.
