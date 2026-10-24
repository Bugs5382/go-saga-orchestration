# Benchmarks

Benchmarks for the saga coordinator's hot path: step advancement
(`Coordinator.Advance`) and verb dispatch. They exist to (1) establish a
baseline and (2) guard the allocation-reduction work that follows. This page
records the **baseline** captured before any tuning.

This is issue #19. The baseline is below; the [After — PR2](#after--pr2-cel-program-cache)
section records the tuning deltas.

## What is measured

All benchmarks run against the in-memory store (`store/memory`) with a
`SystemClock`, so they isolate the engine's own CPU and allocation cost. In
service mode the dominant cost is store and message-queue I/O (Postgres,
RabbitMQ); that latency is deliberately **out of scope** here — these numbers
measure engine overhead, not deployed throughput.

| Area | Benchmark | Package |
|------|-----------|---------|
| Step advancement (serial) | `BenchmarkAdvance` | `engine` |
| Step advancement (concurrent) | `BenchmarkAdvanceParallel` | `engine` |
| Registry lookup | `BenchmarkRegistryLookup` | `engine/verbs` |
| Verb dispatch (per verb) | `BenchmarkVerbExecute` | `engine/verbs` |
| Verb dispatch (concurrent) | `BenchmarkVerbExecuteParallel` | `engine/verbs` |
| CEL env / compile / eval | `BenchmarkNewEnv`, `BenchmarkCompile`, `BenchmarkEval`, `BenchmarkNewEnvCompileEval`, `BenchmarkNewEnvParallel` | `internal/cel` |
| Audit event creation | `BenchmarkNewEvent` | `domain` |

## How to run

```sh
# All hot-path benchmarks with allocation stats.
go test -run='^$' -bench=. -benchmem ./engine/... ./internal/cel/... ./domain/...

# A single area.
go test -run='^$' -bench=BenchmarkAdvance -benchmem ./engine/...
```

`allocs/op` and `B/op` are deterministic and are the primary signal for this
work. `ns/op` varies with host load (these were captured on a shared machine);
treat the timings as indicative, not absolute, and always compare old vs new on
the same host in one sitting.

## Baseline

Captured with `-benchtime=200ms`, `GOMAXPROCS=4`, Go 1.26, Intel Xeon Gold
6426Y, Linux/amd64. Timings are indicative; allocation columns are the stable
baseline.

### `engine` — step advancement

A single `Advance` call drives every synchronous step of a run, so
`multi_step_N` is the cost of an N-step linear saga end to end.

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `Advance/trivial` | 3,804 | 1,314 | 7 |
| `Advance/single_verb` | 12,061 | 3,316 | 15 |
| `Advance/multi_step_10` | 31,212 | 15,550 | 94 |
| `Advance/multi_step_100` | 303,304 | 142,082 | 1,003 |
| `AdvanceParallel/trivial` | 6,099 | 1,312 | 7 |
| `AdvanceParallel/single_verb` | 8,798 | 3,328 | 15 |
| `AdvanceParallel/multi_step_10` | 34,995 | 15,601 | 94 |
| `AdvanceParallel/multi_step_100` | 301,307 | 142,090 | 1,003 |

Per step the loop costs roughly **~9 allocs** (the delta between consecutive
`multi_step` sizes is ~10 allocs/step), driven by audit-event creation and
per-step state/variable writes through the store.

### `engine/verbs` — dispatch

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `RegistryLookup` | 7.2 | 0 | 0 |
| `VerbExecute/noop` | 42 | 48 | 1 |
| `VerbExecute/set_var_literal` | 200 | 336 | 2 |
| `VerbExecute/set_var_cel` | 113,260 | 68,444 | 1,021 |
| `VerbExecute/transform` | 111,778 | 68,442 | 1,021 |
| `VerbExecute/map_10` | 185,184 | 117,267 | 1,545 |
| `VerbExecute/filter_10` | 205,035 | 120,935 | 1,610 |
| `VerbExecute/map_100` | 218,094 | 153,130 | 1,728 |
| `VerbExecute/filter_100` | 235,462 | 156,798 | 1,793 |
| `VerbExecute/decision` | 103,523 | 59,399 | 807 |
| `VerbExecute/parallel_2` | 8,734 | 2,854 | 54 |
| `VerbExecute/parallel_4` | 18,403 | 5,735 | 108 |
| `VerbExecuteParallel/set_var_cel` | 65,044 | 68,447 | 1,021 |
| `VerbExecuteParallel/transform` | 66,860 | 68,445 | 1,021 |
| `VerbExecuteParallel/map_100` | 146,907 | 153,132 | 1,728 |

The registry lookup and literal `set_var` are effectively free. **Every
CEL-bearing verb is ~500x more expensive** — `set_var_cel` and `transform`
each cost ~1,021 allocs/op even though they evaluate a trivial expression.

### `internal/cel` — expression primitives

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `NewEnv/vars_0` | 24,038 | 20,837 | 261 |
| `NewEnv/vars_5` | 25,002 | 21,518 | 272 |
| `NewEnv/vars_20` | 24,447 | 23,615 | 305 |
| `Compile` | 92,978 | 51,720 | 1,020 |
| `Eval` | 245 | 0 | 0 |
| `NewEnvCompileEval` | 133,653 | 82,146 | 1,331 |
| `NewEnvParallel` | 14,659 | 21,518 | 272 |

This isolates the headline finding: a compiled program **evaluates in 245 ns
with zero allocations**, but the verbs rebuild the environment and recompile
the expression on every single dispatch (`NewEnvCompileEval`: ~1,331
allocs/op). `NewEnv` and `Compile` together account for essentially all of the
allocation cost seen in the CEL verbs above.

### `domain` — audit events

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `NewEvent` | 490 | 16 | 1 |

`NewEvent`'s cost is a `uuid.New()` plus a `time.Now()`; the hot loop emits two
to three per step. The UUID and timestamp are audit-critical, so this is a
documented floor rather than a tuning target.

## After — PR2 (CEL program cache)

The tuning adds `cel.CompiledProgram`, a thread-safe cache that memoises the
compiled CEL program for a given (declared variable set, expression) pair, so
the verbs no longer rebuild and recompile the environment on every dispatch.
The map/filter verbs additionally reuse a single activation map across
elements instead of cloning `run.Variables` per element.

Allocation reductions on the CEL-bearing verbs, measured with
`benchstat old.txt new.txt` over `-count=10 -benchtime=100ms` runs taken back
to back on the same host:

| Benchmark | allocs/op (before → after) | B/op (before → after) |
|-----------|:--------------------------:|:---------------------:|
| `VerbExecute/set_var_cel` | 1,021 → 6 (−99.4%) | 68,444 → 456 (−99.3%) |
| `VerbExecute/transform` | 1,021 → 6 (−99.4%) | 68,444 → 456 (−99.3%) |
| `VerbExecute/map_10` | 1,545 → 21 (−98.6%) | 114.5Ki → 1.58Ki (−98.6%) |
| `VerbExecute/map_100` | 1,728 → 24 (−98.6%) | 149.5Ki → 7.05Ki (−95.3%) |
| `VerbExecute/filter_10` | 1,610 → 21 (−98.7%) | 118.1Ki → 1.58Ki (−98.7%) |
| `VerbExecute/filter_100` | 1,793 → 24 (−98.7%) | 153.1Ki → 7.05Ki (−95.4%) |
| `VerbExecute/decision` | 807 → 9 (−98.9%) | 57.9Ki → 1.34Ki (−97.7%) |
| `VerbExecuteParallel/set_var_cel` | 1,021 → 6 (−99.4%) | 68,445 → 456 (−99.3%) |
| `VerbExecuteParallel/transform` | 1,021 → 6 (−99.4%) | 68,442 → 456 (−99.3%) |
| `VerbExecuteParallel/map_100` | 1,728 → 24 (−98.6%) | 149.5Ki → 7.05Ki (−95.3%) |
| **`engine/verbs` geomean** | **323 → 12 (−96.4%)** | **23.2Ki → 1.19Ki (−94.9%)** |

Wall-clock falls in step with the allocations once the cache is warm
(`set_var_cel` ~136µs → ~0.8µs, the `engine/verbs` sec/op geomean −94%), but
timings vary with host load — the allocation columns are the authoritative
result.

What is intentionally unchanged:

- **`Advance/*`** uses literal `set_var` (no CEL), so its allocs/op are
  identical before and after; the small sec/op wobble is host noise.
- **`VerbExecute/parallel_*`** passes literal branch lists (not a CEL string),
  so its path is untouched (54 / 108 allocs/op unchanged).
- **`internal/cel` `NewEnv` / `Compile` / `NewEnvCompileEval`** call the raw
  primitives directly and remain the reference cost of an *uncached* build —
  they show what the cache now avoids.
- **`NewEvent`** is unchanged: its UUID + timestamp are audit-critical, so it
  stays a documented floor rather than a tuning target.

## Comparing runs (benchstat)

```sh
go test -run='^$' -bench=. -benchmem -count=10 ./engine/... ./internal/cel/... ./domain/... > old.txt
# ...make a change...
go test -run='^$' -bench=. -benchmem -count=10 ./engine/... ./internal/cel/... ./domain/... > new.txt
benchstat old.txt new.txt
```

Run `old.txt` and `new.txt` back to back on the same idle host so the `ns/op`
comparison is meaningful; the `allocs/op` delta is reliable regardless.
