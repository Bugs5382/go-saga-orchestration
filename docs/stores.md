# Store Backends

go-saga-orchestration persists saga state to a pluggable `store.Store`. The backend is selected at startup via the `STORE_TYPE` environment variable and cannot be changed at runtime.

---

## Backend selection

Set `STORE_TYPE` to one of the values below. If the variable is absent the engine defaults to `postgres`.

| `STORE_TYPE` | Backend | Notes |
|---|---|---|
| `postgres` _(default)_ | PostgreSQL | Fully durable; LISTEN/NOTIFY powers the live-stream endpoint. Requires `DATABASE_DSN`. |
| `redis` | Redis / Valkey | Durable within Redis AOF/RDB persistence settings. `redis` and `valkey` are wire-compatible aliases — the same code path handles both. Requires `REDIS_URL`. |
| `valkey` | Valkey / Redis | Alias for `redis` above. |
| `memory` | In-process map | No persistence; all state is lost on restart. For tests and local development only. |

### Environment variables

| Variable | Backends | Default | Purpose |
|---|---|---|---|
| `DATABASE_DSN` | `postgres` | _(empty)_ | Postgres connection string (`postgres://user:pass@host/db`). |
| `REDIS_URL` | `redis`, `valkey` | _(empty)_ | Redis/Valkey connection URL (`redis://host:6379/0` or `rediss://` for TLS). Required when the store type is `redis` or `valkey`. |
| `REDIS_RUN_TTL` | `redis`, `valkey` | `0s` (disabled) | Go duration string (e.g. `168h`, `72h`). When non-zero, the engine calls `EXPIRE` on all keys belonging to a saga run once it reaches a terminal state (succeeded, failed, or cancelled). Keys affected: run blob, event list, signals list, user-task index. Default `0s` keeps terminal runs forever. |

---

## Redis / Valkey durability

Redis and Valkey use the same RESP protocol and no proprietary modules. The go-saga-orchestration redis backend is pure-RESP; either server can be used interchangeably behind `REDIS_URL`.

### Persistence modes and data-loss windows

Redis and Valkey support three common persistence configurations:

| Mode | How it works | Worst-case data-loss window |
|---|---|---|
| **AOF `appendfsync everysec`** (default) | Writes are flushed to the AOF file once per second. | Up to ~1 second of committed writes. |
| **AOF `appendfsync always`** | Every write command is fsynced before the client gets a reply. | Near-zero (bounded by disk flush latency). |
| **RDB snapshots only** (no AOF) | Point-in-time snapshots at a configured interval. | Up to the snapshot interval (often minutes). |

For production use with saga state, AOF `everysec` is the standard trade-off between throughput and durability. Use `always` if the ~1 s loss window is unacceptable for your use case. RDB-only is not recommended for saga workloads where every state transition matters.

### Memory-bound storage

Redis and Valkey keep all data in RAM (with optional disk persistence). The size of the saga keyspace grows with the number of runs, events, signals, and user tasks stored. Monitor memory usage and configure `maxmemory` and an eviction policy appropriate for your workload. Note that `noeviction` will cause write errors rather than silent data loss if memory is exhausted; `allkeys-lru` will silently discard saga keys.

### Run retention with `REDIS_RUN_TTL`

By default, terminal-run keys are kept forever. Set `REDIS_RUN_TTL` to a Go duration (e.g. `168h` for one week) to have the engine auto-expire all keys for a run once it finishes. This bounds keyspace growth at the cost of losing historical run data after the TTL expires.

---

## Limitation: live saga-stream requires postgres

The `GET /api/v1/sagas/{run_id}/stream` endpoint tails audit events in real time using PostgreSQL `LISTEN/NOTIFY`. Under the `redis`, `valkey`, or `memory` backends, the handler returns **HTTP 501 Not Implemented** because the required Postgres connection pool is not available. All other API endpoints work with any backend.
