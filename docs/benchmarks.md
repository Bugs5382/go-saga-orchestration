# Benchmarks — coordinator hot path

Benchmarks for the saga coordinator's hot path: step advancement
(`Coordinator.Advance`) and verb dispatch. They exist to (1) establish a
baseline and (2) guard the allocation-reduction work that follows. This page
records the **baseline** captured before any tuning.

This is issue #19. The follow-up tuning lands its before/after deltas in an
"After" section below.

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

## Comparing runs (benchstat)

```sh
go test -run='^$' -bench=. -benchmem -count=10 ./engine/... ./internal/cel/... ./domain/... > old.txt
# ...make a change...
go test -run='^$' -bench=. -benchmem -count=10 ./engine/... ./internal/cel/... ./domain/... > new.txt
benchstat old.txt new.txt
```

Run `old.txt` and `new.txt` back to back on the same idle host so the `ns/op`
comparison is meaningful; the `allocs/op` delta is reliable regardless.
