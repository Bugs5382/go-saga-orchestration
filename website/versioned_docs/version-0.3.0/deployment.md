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

The chart is attached to each GitHub Release as `go-saga-orchestration-<version>.tgz`.
Replace `<version>` below with the release you want (see the
[Releases](https://github.com/Bugs5382/go-saga-orchestration/releases) page):

```bash
helm install go-saga \
  https://github.com/Bugs5382/go-saga-orchestration/releases/download/v<version>/go-saga-orchestration-<version>.tgz \
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
| `cron.dedicated` | `false` | Run the cron dispatcher on a dedicated pod and off the main engines (see below) |
| `cron.replicas` | `1` | Replicas for the dedicated cron pod (only when `cron.dedicated`) |
| `ingress.enabled` | `false` | Expose the api via Ingress |

See `deployments/helm/values.yaml` for the full set, including probes, resources, and security context.

## Cron dispatcher topology

The engine's cron dispatcher polls for due cron triggers and fires them. Firing
is **exactly-once across every engine pod** — each fire is claimed with a
compare-and-swap on the trigger's `next_fire_at` (`ClaimCronFire`), so even if
many pods run the loop at once, only one wins each window. Running cron on every
engine pod is therefore always safe.

A single Helm switch, `cron.dedicated`, picks the topology:

- **`cron.dedicated: false` (default)** — no extra pod. Every main engine pod
  runs the cron dispatcher (`WORKFLOW_CRON_DISPATCHER=true`); the CAS keeps firing
  single-shot. Simplest single-deployment setup.
- **`cron.dedicated: true`** — one dedicated single-replica cron pod runs the
  dispatcher, and it is turned **off** on every (scaled) main engine pod. This
  gives predictable scheduling, resource isolation, and one obvious cron writer
  while the main engines scale out purely for saga processing. The cron pod
  reuses the engine image, ConfigMap, and connection Secret. Enabling the
  dedicated pod is the only switch — there is no separate per-engine cron flag;
  the chart derives `WORKFLOW_CRON_DISPATCHER` for both deployments from it.

```mermaid
flowchart TB
  client([Clients]) --> api

  subgraph k8s["Kubernetes (cron.dedicated: true)"]
    api["api Deployment\nN replicas\nHTTP :8080"]
    engine["engine Deployment\nN replicas\ncron OFF\n(saga processing)"]
    cron["cron Deployment\n1 replica\ncron ON\n(WORKFLOW_CRON_DISPATCHER=true)"]
  end

  store[("Store\n(Postgres / Redis)")]
  rmq[["RabbitMQ"]]

  api --> store
  api --> rmq
  engine --> store
  engine --> rmq
  cron --> store
  cron --> rmq

  cron -. "fires cron triggers\n(single writer)" .-> rmq
```

Even with `cron.dedicated: true` you may raise `cron.replicas` above 1 for
availability — the `ClaimCronFire` CAS still guarantees each window fires once,
so duplicate cron pods do not double-fire.
