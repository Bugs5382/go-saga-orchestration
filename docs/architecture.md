# go-saga-orchestration — Architecture & Concepts

Module: `github.com/Bugs5382/go-saga-orchestration`

This document explains how the go-saga-orchestration engine works end to end. It is written to be read by a human or an AI assistant who needs to understand the system before changing it. Everything below is derived from the actual source; where the code's intent is ambiguous, the observable behavior is described rather than guessed.

---

## 1. Overview & the two binaries

The system is a **saga / workflow orchestration engine**. Workflows are declarative graphs of *steps* (each step has a *verb*). Running instances are *saga runs*. The engine walks each run forward one step at a time, persisting state to Postgres and coordinating asynchronous work over RabbitMQ. Long-running or external work is delegated to *worker* services that connect over gRPC.

There are two process binaries; both load the same `config.Config` (`internal/config/config.go`) from environment variables and both run Postgres migrations on boot.

### `cmd/api` — REST surface (`:8080`)
Source: `cmd/api/main.go`. Boot sequence:
1. `postgres.Open` + `postgres.Migrate` (embedded migrations applied on every boot, idempotent).
2. Connect to RabbitMQ, declare topology (`mq.DeclareTopology`), open a `mq.Publisher`.
3. Construct the chi router (`api/router.go`) wiring the handlers below.
4. Serve HTTP on `cfg.API.Port` (default `8080`, env `WORKFLOW_API_PORT`), with graceful shutdown on SIGINT/SIGTERM.

Routes (`api/router.go`):
- `GET /health/live`, `GET /health/ready`
- `GET /api/v1/sagas` — list/filter runs (paginated)
- `POST /api/v1/sagas/start` — start a run (returns `202` + `saga_run_id`)
- `GET /api/v1/sagas/{id}` — fetch one run
- `POST /api/v1/sagas/{run_id}/signal/{name}` — deliver an external signal
- `POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit` — submit a user task
- `GET /api/v1/sagas/{run_id}/stream` — WebSocket run inspector (tails audit events via Postgres LISTEN/NOTIFY)
- `POST /api/v1/registry/register`, `GET /api/v1/registry/actions` — worker action registry
- `POST /api/v1/rules/{rule_id}/evaluate` — evaluate a stored rule
- `POST|GET|GET|DELETE /api/v1/triggers...` — trigger CRUD
- `GET /api/v1/workflows/{wf_id}/stats` — per-workflow aggregate stats

The API process does **not** advance sagas itself — it only persists rows and publishes `saga.advance` messages.

### `cmd/engine` — coordinator + gRPC (`:9090`)
Source: `cmd/engine/main.go`. This is the worker process that actually runs sagas. Boot sequence:
1. Postgres open + migrate.
2. RabbitMQ connect + `mq.Publisher`.
3. Construct the `engine.Coordinator` with a `SystemClock`, an in-memory secrets resolver, and `licensing.StubAllowAll{}` (dev/test license resolver — approves everything).
4. Start the **timer dispatcher** goroutine (`engine.Timer`) — polls for due wakeups every second.
5. Construct (but, in the committed code, leave un-started) the `TriggerDispatcher` + `EventSubscriber`. The comment notes `RunRMQ` wiring is deferred until the prod env has RMQ; `_ = sub` keeps it referenced.
6. Start the gRPC server on `cfg.Engine.GRPCPort` (default `9090`, env `WORKFLOW_ENGINE_GRPC_PORT`) so workers can open `ExecuteStep` streams.
7. Block in `mq.ConsumeSagaAdvance`, dispatching each `saga.advance` message to `coord.HandleAdvance`.

---

## 2. Core concepts: sagas, runs, definitions, coordinator

### Definitions (`domain/definition.go`)
A `WorkflowDefinition` is one version of one workflow: an `ID` (stable workflow id), `Version`, optional `TenantID`, a `Start` step id, an optional `Entrypoints` map (see below), and a list of `Step`s. A `Step` has an `ID`, a `Type` (the verb / `StepType`), an optional `Next` (the default outgoing edge), an optional `Action` string, an `Inputs` map (verb-specific config), optional `Compensation`, optional `Retry` policy, and optional `Branches` (named outgoing edges used by `decision`/`while`/`parallel`). `StepByID` looks up a step within a definition.

### Entrypoints / call tree
`WorkflowDefinition.Entrypoints` is an optional `map[string]string` that maps entry names to step IDs. The empty string `""` and the name `"default"` always resolve to `Start` regardless of the map. Any other name must appear in `Entrypoints`; an unknown name is a runtime error (returned by `ResolveEntry`). The `ValidateDefinition` function checks that every step ID referenced in `Entrypoints` actually exists in the definition.

`saga.Start` is unchanged and is equivalent to `saga.StartAt(ctx, workflowID, "", inputs)`. `saga.StartAt(ctx, workflowID, entrypoint, inputs)` resolves the entry point before creating the run, setting `run.CurrentStep` to the resolved step ID instead of `def.Start`.

Triggers honor the entry point via `config.entrypoint` in the trigger's config map — `TriggerDispatcher.Dispatch` calls `def.ResolveEntry(trigEntrypoint)` to determine the starting step. The `sub_saga` and `spawn_saga` verbs accept an `"entrypoint"` input key that is resolved against the child definition the same way.

`RuleDefinition` (`domain/rule.go`) is a separately versioned, published artifact used by the `decision` verb (see §4).

### Runs (`domain/run.go`)
A `SagaRun` is one executing instance. Key fields:
- `State` (`RunState`): `pending`, `running`, `paused`, `compensating`, `succeeded`, `failed`, `cancelled`. `IsTerminal()` is true for `succeeded`/`failed`/`cancelled`.
- `CurrentStep`: the step id the run is at.
- `Inputs` (immutable start inputs) and `Variables` (mutable working state that verbs read and write).
- Pause markers: `WakeupAt`, `AwaitedSignal`, `AwaitedEventTopic`, `AwaitedEventHeaders`, `AwaitedActionDispatch`, `CurrentAttempt`.
- Composition links: `ParentRunID`, `ParentStepID`, `ParentBranchID` (set on child runs spawned by `parallel`/`foreach`/`sub_saga`/`spawn_saga`).
- `TryCatchStack` (`[]TryCatchFrame`), `DryRun`, `FeatureOverrides` (per-run feature overrides), `RequiresManualReview`, `TriggerID`.

### The coordinator (`engine/coordinator.go`)
`Coordinator` owns a `store.Store`, a `Publisher` (re-enqueues `saga.advance`), the verb `Registry`, a clock, a secrets resolver, and a license resolver. `NewCoordinator` builds the verb registry via `verbs.Default(...)`. `HandleAdvance` is the consumer callback; it simply calls `Advance(runID)` and wraps any error (a non-nil return NACKs the RabbitMQ message → requeue).

### How a run advances (`engine/advance.go`)
`Advance` is a synchronous loop over one run. Each iteration **re-reads the run from the store** so it sees variable updates the previous step wrote. Per iteration:

1. **Terminal?** If `run.State.IsTerminal()`, return (ACK).
2. **Paused?** If state is `paused`, decide whether a wakeup condition holds:
   - *Time-based*: `WakeupAt != nil && WakeupAt <= now` (from `wait_duration`/`wait_until`, or `WakeFromExternal` which sets `wakeup_at=now`).
   - *External*: no pending await markers (`AwaitedSignal`/`AwaitedEventTopic` nil) **and** `WakeupAt == nil` (a parallel/sub-saga join wake).
   If a wakeup condition holds, it clears pause (`ClearPause`), emits `step.succeeded` for the paused step, transitions to `step.Next`, and continues the loop. The paused step is treated as already-succeeded because wait verbs persist their pause marker and return `ErrSagaPaused` *after* succeeding. If neither wakeup condition holds, the run is still legitimately paused → return (ACK; the message arrived prematurely).
3. **Resolve the step.** Load the definition, pick `run.CurrentStep` (or `def.Start` on first entry, emitting `saga.started`). Emit `step.dispatched` and set state `running`.
4. **`end` step** short-circuits to `completeRun` (emits `step.succeeded` + `run.succeeded`, sets state `succeeded`, then checks any parent join). `end` is intentionally NOT in the verb registry.
5. **License gate.** Look up the verb's `LicenseGroup` (with `LicenseGroupForStep` dynamic override), map to a feature flag (`GroupToFeature`), and call `licensing.IsFeatureEnabled`. On rejection: emit `license.gate.rejected`, set state `failed`, return error. (In the engine binary the resolver is `StubAllowAll`, so this never rejects in dev.)
6. **Execute the verb.** `entry.Handler.Execute(ctx, run, step)` returns `(result map, error)`.
   - If `error == ErrSagaPaused`: emit `step.paused`, return (ACK). The verb has already persisted its pause marker; a timer/signal/event/join wake will republish `saga.advance`.
   - If another error: **try_catch handling** — `PopTryCatch`; if a frame was popped, write `_error` (`{step_id, message, verb}`) into Variables, emit `step.failed` (actor `engine-caught`), set `CurrentStep` to the frame's `CatchStep`, and continue the loop. Otherwise emit `step.failed`, set state `failed`, run `checkParentJoin`, return error.
   - On success: if `result` is non-empty, merge it into Variables (`UpdateRunVariables` — supports dotted keys for nested writes). Emit `step.succeeded`.
7. **Pick the next step.** Default is `step.Next`. If `result["branch"]` is a non-empty string and `step.Branches[branch]` exists, follow that branch instead. For `decision`/`while`, a missing branch is an error. A run with no `next` that is not `end` is an error. Set state `running` + new `CurrentStep`; loop.

### Joins (`checkParentJoin`, `aggregateChildResults`)
After any terminal write of a child run, `checkParentJoin` runs. It only proceeds if the parent is still `paused` on the exact step that spawned the child (`parent.CurrentStep == *run.ParentStepID`) — this guards fire-and-forget `spawn_saga` children from prematurely waking a parent. Join strategy comes from the parent step's `inputs.join_strategy`:
- `"all"` (default): wake only when **every** sibling is terminal.
- `"quorum"`: wake when `quorum_n` siblings reach `succeeded` (`quorum_n` may be a literal int or a CEL string evaluated against the parent's variables; non-numeric/invalid falls back to `"all"`). Remaining branches keep running but no longer gate the parent.

On wake it aggregates child results into `Variables._parallel.<step_id>.branches` (per child: `key`, `variables`, terminal `state`, and the first submitted `_user_task` if any), calls `WakeFromExternal(parent)` (clears await markers, sets `wakeup_at=now`), and publishes `saga.advance` for the parent.

### Retry (`engine/retry.go`)
`DefaultRetryPolicy` is max 3 attempts, 1s initial backoff, 60s cap, ×2 multiplier, jitter on. `Backoff(policy, attempt, jitter)` computes exponential backoff with optional ±25% jitter. NOTE: these helpers exist and are tested, but the `Advance` loop in the committed code does not itself re-dispatch failed verbs through them — step-level retry is the worker/action concern (RabbitMQ redelivery + worker idempotency), and `current_attempt` is bumped by the `action` verb. This is a notable gap between the helper and its wiring.

---

## 3. The verb catalog

All 31 step types live in `engine/verbs/` (30 in the registry plus `end`, which is handled inline by the coordinator). They implement `Handler.Execute(ctx, run, step) (map[string]any, error)`. The returned map is merged into `run.Variables`; returning `ErrSagaPaused` suspends the run; returning `ErrSagaCancelled` transitions the run to `cancelled` (terminal); any other error fails the step (subject to try_catch). The registry (`verbs.Default`, `registry.go`) maps each `StepType` to a handler plus a license group.

| Verb | Purpose | Key fields / notes |
|------|---------|--------------------|
| `action` | Dispatch a registered action to a worker over RabbitMQ, then pause awaiting the worker's gRPC reply. | `step.Action` = `"<service>.<name>"` (must contain a dot); `step.Inputs` forwarded verbatim. Bumps `current_attempt`, computes a SHA-256 idempotency key from (run, step, attempt), calls `MarkAwaitingAction`, publishes `ActionPayload` to `action.direct` with routing key = the action, returns `ErrSagaPaused`. License group `common`. Resumed by gRPC `CompleteAction`/`FailAction`. |
| `cancel` | Cancel a run. | `run_id` (optional string). No `run_id` (or `run_id` equal to the current run) → self-cancel: returns `ErrSagaCancelled` and the engine sets state `cancelled` (terminal); the run ends immediately. With a different `run_id` → cancels that target run (`UpdateRunState` → `cancelled`, appends `run.cancelled` event) and the current run continues to `Next`. `reason` (optional string) is available for logging. Group `loops_and_recovery`. |
| `assert` | Fail the saga if a CEL expression is not true. | `expr` (required), `code` (optional, default `assertion_failed`). Evaluates against `run.Variables`; non-true → error `"<code>: <expr> is false"`. Group `common`. |
| `collect_input` | Create a user task that **requires** a form and pause until submitted. | `assignee` (required), `form_schema` (required, non-empty), `due_in` (optional Go duration). Creates a `UserTask`, sets paused awaiting signal `user_task.<task_id>.submitted`, returns `ErrSagaPaused`. Group `human_interaction`. |
| `decision` | Evaluate a stored rule and branch on its output. | `rule_id` (required), `inputs_map` (optional map ruleKey→variableName narrowing the inputs; otherwise all of `run.Variables`). Calls `rules.Evaluate`, emits `rule.evaluated` with the audit trail, returns the rule output (engine reads `result["branch"]` to pick `step.Branches`). Group `common`. |
| `emit_event` | Publish an event via the configured `EventEmitter`. | `topic` (required string), `headers` (optional map — values stringified), `payload` (optional map). Delegates to `verbs.EventEmitter.EmitEvent`; fails if no emitter is configured. See below for in-process vs. service-mode behavior. Group `events_and_signals`. |
| `emit_signal` | Send a signal to a target run (the send-side complement of `wait_for_signal`). | `run_id` (required string UUID), `name` (required string), `payload` (optional map). Appends a `SagaSignal` row via `AppendSignal`, then calls `TryConsumeAwaitedSignal`; if the target was paused awaiting that signal, clears its markers and publishes `saga.advance` to resume it immediately. Group `events_and_signals`. |
| `end` | Terminate the saga successfully. | No inputs. Dispatched inline by the coordinator (`completeRun`), NOT via the registry — sets run state `succeeded`. |
| `error` | Halt the saga with a non-retryable error. | `code` (required), `message` (optional). Always returns an error. Group `common`. |
| `filter` | Keep list elements where a CEL predicate is truthy. | `list` (CEL → `[]any`), `expr` (predicate; current element bound as `_`), `out_var` (destination). All required. Group `common`. |
| `foreach` | Fan out one child run per list element (parallel only in v1). | `list` (CEL → list), `body` (`[]any` of step objects), `start` (first step id in body), `parallel` (optional, default true; `false` is rejected — "use `while`"). Each child gets `_foreach_item` + `_foreach_index` in inputs. Empty list → advance to `Next` without spawning. Pauses parent; woken by the join hook. Group `parallel_control`. |
| `http_request` | Synchronous outbound HTTP request; merge response into Variables. | `method` (default GET), `url` (required), `headers`, `body` (JSON), `timeout_s` (default 30), `secret_ref` (→ `Authorization` header via secrets resolver), `out_var` (default `http_result`). Writes `<out_var>`, `<out_var>_status`, `<out_var>_headers`. Group dynamically `common` for GET-no-auth, else `external_io_advanced` (see `LicenseGroupForStep`). |
| `log` | Append a `log` audit event. | `message` (required), `level` (optional, default `info`). No variable output. Group `common`. |
| `manual_approval` | Create a user task (form optional) and pause until submitted. | `assignee` (required), `due_in` (optional duration), `form_schema` (optional). Same await-signal mechanism as `collect_input`. Group `human_interaction`. |
| `map` | Transform each list element via a CEL expression. | `list`, `expr` (element bound as `_`), `out_var` — all required. Writes the mapped list to `out_var`. Group `common`. |
| `merge` | Deep-merge a CEL-evaluated map into a target variable. | `from` (CEL → map), `into` (target var, dotted). Flattens nested maps into dotted keys rooted at `into` (last-write-wins). Group `common`. |
| `metric_emit` | Append a `metric` audit event. | `name` (required), `value` (required), `labels` (optional map). Prometheus side-channel deferred; the event is the record. Group `observability`. |
| `noop` | Do nothing (placeholder / authoring aid). | No inputs. Returns empty result. Group `common`. |
| `parallel` | Fan out N branch child-runs and pause the parent until the join is satisfied. | `branches` (`[]any` of `{start, steps}` objects, short-form `{type, inputs}` normalized, or a CEL string → list); `join_strategy` `all` (default) or `quorum`; `quorum_n` required for quorum (literal or CEL, must be ≤ branch count). Each branch becomes a synthetic published definition + child run. Pauses parent; woken by `checkParentJoin`. Group `parallel_control`. |
| `set_var` | Write a literal or CEL-evaluated value to a variable. | `out_var` (required, dotted ok); exactly one of `value` (literal) or `expr` (CEL); `expr` wins if both present. Group `common`. |
| `spawn_saga` | Start a named workflow as a **fire-and-forget** child; parent continues immediately. | `workflow_id` (required), `inputs` (optional), `entrypoint` (optional — same semantics as `sub_saga`). Resolves the published workflow for the parent's tenant, `SpawnChildRun` (carries `ParentRunID` for audit), publishes child advance, returns empty result (no pause). The parent never pauses, so the join guard never wakes it. Group `compositions`. |
| `sub_saga` | Start a named workflow as a child and **pause** until the child terminates. | `workflow_id` (required), `inputs` (optional), `entrypoint` (optional — name of a named entry point in the child definition; `""` / `"default"` → child's `Start`). Same spawn mechanism as `parallel` with a single branch; pauses parent (`ErrSagaPaused`), woken by `checkParentJoin`. Group `compositions`. |
| `switch` | Evaluate a CEL expression to a string key and branch on it. | `expr` (required CEL expression). The expression must evaluate to a `string`; returns `{"branch": <result>}`. The engine routes the run to `step.Branches[key].Next`. A missing branch key is a runtime error (same behavior as `decision`/`while`). Group `common`. |
| `transform` | Evaluate a CEL expression and write the result to a variable. | `expr` (required), `out_var` (required, dotted ok). Group `common`. |
| `try_catch` | Push a try/catch frame so an error inside the try body jumps to a catch step. | `try` (`[]any` of step ids — used by the publish-time validator to forbid parallel-in-try; not consumed at runtime), `catch` (required step id). Author wires `step.Next` to the first try step. Frame stays on the stack until the saga terminates. On error inside the block, the coordinator pops the frame, writes `_error`, and jumps to the catch step. Group `loops_and_recovery`. |
| `wait_duration` | Pause for a fixed relative duration. | `duration` (required Go duration; negative rejected). Sets `wakeup_at = now + d` via `SetPausedWithWakeup`, returns `ErrSagaPaused`. Timer dispatcher republishes advance when due. Group `waits`. |
| `wait_for_event` | Pause until a matching RabbitMQ event arrives. | `topic` (required routing key), `headers` (optional subset that the incoming event must match, values stringified). `SetPausedAwaitingEvent`, returns `ErrSagaPaused`. Woken by `EventSubscriber`. Group `events_and_signals`. |
| `wait_for_signal` | Pause until a named external signal arrives via REST. | `name` (required), `timeout_s` (optional; sets a deadline, else waits indefinitely). `SetPausedAwaitingSignal`, returns `ErrSagaPaused`. The signal handler's `TryConsumeAwaitedSignal` clears markers + sets `wakeup_at=now`. Group `events_and_signals`. |
| `wait_until` | Pause until an absolute wall-clock instant. | `timestamp` (required RFC3339). Past timestamps clamp to `now` (wake next tick). `SetPausedWithWakeup`, returns `ErrSagaPaused`. Group `waits`. |
| `webhook_emit` | POST a payload to an external URL (optionally async / HMAC-signed). | `url` (required), `body` (required, JSON), `secret_ref` (optional → `X-Webhook-Sig: sha256=<hmac>`), `timeout_s` (default 15), `headers`, `async` (default false — fire in a goroutine, ignore result), `out_var` (default `webhook_result`; sync writes `<out_var>_status`, async writes `<out_var>_async`). Group `external_io_advanced`. |
| `while` | Evaluate a CEL condition and branch `continue`/`exit`, with a loop cap. | `condition` (required CEL), `max_iterations` (optional, default 100, hard cap 10000). Returns `{branch, _while.<step_id>.iter}`. Author wires `branches.continue` → loop body and `branches.exit` → after-loop; the body's last step loops back to this step. Group `loops_and_recovery`. |

License groups → feature flags are defined in `engine/verbs/license_groups.go` (`GroupToFeature`): `common`→(none), `observability`→`wf.observability`, `external_io_advanced`→`wf.external_io`, `waits`→`wf.timers`, `events_and_signals`→`wf.event_driven`, `human_interaction`→`wf.user_tasks`, `parallel_control`→`wf.parallel`, `loops_and_recovery`→`wf.loops_recovery`, `compositions`→`wf.compositions`.

### EventEmitter (`verbs.EventEmitter`, `saga/event_emitter.go`, `cmd/engine`)
`emit_event` delegates to a `verbs.EventEmitter` interface injected at coordinator construction. In **embedded mode** (`saga.InMemory()` / `saga.New(...)`), the `saga` package wires an `InProcessEventEmitter` that calls `store.FindRunsByAwaitedEvent`, applies the same header-subset match as `EventSubscriber`, calls `WakeFromExternal` on each match, and publishes `saga.advance` in-process — no broker needed. In **service mode** (`cmd/engine`), the coordinator is given an `mqEventEmitter` that calls `mq.Publisher.PublishEvent`, putting the event on the `workflow.events` exchange so that all engine pods receive it via their `EventSubscriber` goroutine. Note: starting *new* runs from event-triggers fired via `emit_event` is a follow-up; in the current implementation only already-paused runs are woken.

### Timeout / escalation branch
Any wait step (`wait_for_signal`, `wait_for_event`, or any step that sets a deadline via `timeout_s`) can optionally define a `"timeout"` key in `step.Branches`. When the timer dispatcher fires the deadline (`WakeupAt <= now`) and the run's await markers (`AwaitedSignal` / `AwaitedEventTopic`) are still set — meaning a real signal or event did not arrive first — the coordinator routes the run to `step.Branches["timeout"].Next` instead of `step.Next`. If no `"timeout"` branch is defined, `step.Next` is used as normal (backward-compatible). A real signal/event always clears the await markers before the `saga.advance` message arrives, so `timedOut` is false in that case.

---

## 4. CEL rules: expression and rule evaluation

### CEL (`internal/cel`)
`internal/cel/cel.go` wraps `google/cel-go`. `NewEnv(varNames...)` builds an environment where every supplied variable name is declared as `dyn` (any JSON-shaped value), then applies the v1 subset. `Compile(expr)` parses + type-checks once into a reusable `Program`; `Eval(vars)` runs it and **deep-converts** the result back to Go-native types (`[]any`, `map[string]any`) via `refValueToNative` — this is why verbs like `parallel`/`filter`/`map` can rely on getting `[]any` back. Map keys that are not strings cause an error.

`internal/cel/subset.go` defines the allow-list: only the CEL **stdlib** (arithmetic, string ops, equality, `&&`, `||`, `in`) and the **strings extension** are enabled. Native Go host functions, and file/time/network functions, are deliberately NOT exposed — this is the single chokepoint for widening the surface later.

Verbs that take CEL (`assert`, `transform`, `set_var.expr`, `filter`, `map`, `merge.from`, `while.condition`, `parallel.branches`/`quorum_n`, `foreach.list`) build the env from the keys of `run.Variables`. List-iterating verbs additionally bind the current element as `_`.

### Rules (`internal/rules` + `domain/rule.go`)
A `RuleDefinition` (type `decision_table`, hit policy `first` — the only supported variants in v1) holds an ordered list of `DecisionTableRow{When (CEL), Then (output map)}` plus an optional `DefaultOutput`. `rules.Evaluate(def, inputs)` builds a CEL env from the input keys, evaluates each row's `When` in order, and returns the first matched row's `Then` (with an audit trail of `{index, when, matched}`). If nothing matches and there is a `DefaultOutput`, that is returned; otherwise it errors `no_decision_row_matched`. The `decision` verb calls this and records the audit in a `rule.evaluated` event.

---

## 5. Triggers, signals, and user tasks

These are the three ways external activity drives runs.

### Triggers (`domain/trigger.go`, `engine/trigger_dispatcher.go`)
A `SagaTrigger` binds an external event to a workflow. v1 supports one `TriggerType`: `record_transition`. The `TriggerDispatcher.Dispatch` inspects a RabbitMQ delivery: it only acts on routing keys shaped `*.record.transitioned.*`, decodes the body for `record_type`/`from_state`/`to_state`, lists enabled `record_transition` triggers, and for each whose config matches all three it:
1. builds saga inputs from the body via the trigger's `input_mapping` (v1 supports only top-level `$.field` references; unmapped values pass through as literals; empty mapping → the body itself),
2. resolves tenant (trigger's tenant wins, else body `tenant_id`, else nil),
3. resolves + upserts the published workflow definition,
4. creates a `SagaRun`, injects startup variables, and publishes `saga.advance`.

Per-trigger failures are logged and skipped; the first store/publish error is returned. CRUD is exposed via `/api/v1/triggers`; some first-party triggers are seeded by migrations.

### Cron triggers (`domain/trigger.go`, `engine/cron_dispatcher.go`)
A cron trigger is a `SagaTrigger` with `trigger_type: cron`. Its `config` map carries exactly one of:
- `schedule` — a standard five-field cron expression (`* * * * *`) or a `@`-descriptor (`@hourly`, `@daily`, etc.). Granularity is one minute.
- `interval` — a Go duration string (e.g. `"30s"`, `"5m"`) enabling sub-minute cadences. `next_fire_at` is advanced by the interval on each claim.

Exactly one of `schedule` or `interval` must be present; supplying both or neither is rejected at create time with HTTP 400. The config also accepts:
- `entrypoint` (optional) — a named entry point in the target workflow definition; resolves the same way as `config.entrypoint` on event triggers.
- `input` (optional) — a JSON-compatible map injected as the run's start inputs.

The `CronDispatcher` polls on a ~1-second tick. On each tick it calls `ListDueCronTriggers` (triggers whose `next_fire_at ≤ now`) and attempts to claim each one via `ClaimCronFire` — a compare-and-swap on `next_fire_at` guarded by the row's current value. Exactly one engine pod wins the claim per window; pods that lose the race silently skip. On a successful claim the dispatcher creates a `SagaRun` and publishes `saga.advance`. The `next_fire_at` is advanced to the next schedule slot and `last_fired_at` is recorded.

**Missed-fire behavior.** If all engine pods are down when a scheduled window elapses, the trigger fires once on the next tick after they come back. There is no backfill of missed windows.

**License gate.** Cron triggers are gated by the `wf.cron_triggers` feature flag, checked at both create time (REST) and fire time (dispatcher). The `wf.cron_triggers` feature is not part of the verb `GroupToFeature` map; it is checked directly against the license resolver.

**Management.** Cron triggers are created, listed, and deleted through the existing `/api/v1/triggers` REST endpoints. `POST /api/v1/triggers` with `trigger_type: cron` and a valid `config.schedule` initializes `next_fire_at` to the next schedule tick after the current time (so the first run fires on schedule, not immediately) and enables the trigger.

### Signals (`domain/signal.go`, `api/handler_signals.go`)
A `SagaSignal` is an external message addressed to a specific run by name. `POST /api/v1/sagas/{run_id}/signal/{name}` appends the signal row, then calls `TryConsumeAwaitedSignal(run, name)`. If the run was paused awaiting exactly that signal, it consumes it (clearing markers, setting `wakeup_at=now`) and publishes `saga.advance` → `202`. If the run was not awaiting it → `409` (recorded but not matched). Signals wake `wait_for_signal`, and (indirectly) `manual_approval`/`collect_input`, which await the synthetic signal `user_task.<task_id>.submitted`.

### User tasks (`domain/user_task.go`, `api/handler_user_tasks.go`)
A `UserTask` is created by `manual_approval` or `collect_input` and pauses the run awaiting its submission. `POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit` (1) records `submitted_at`/`submitted_by`/`result` on the task, (2) appends a signal named `user_task.<task_id>.submitted` carrying the result as payload, (3) tries to consume the awaited signal and, on match, publishes `saga.advance` → `202`. This means user-task completion reuses the signal machinery exactly.

---

## 6. Package layout and stores

### Public importable packages
The engine is structured as an embeddable library. The top-level packages form the importable surface for consuming applications:

| Package | Purpose |
|---------|---------|
| `saga` | Facade: `saga.InMemory()` and `saga.New(saga.Options{...})` return `*saga.Saga` — the entry point for embedding the engine. |
| `domain` | Core types: `WorkflowDefinition`, `SagaRun`, `Step`, `RuleDefinition`, `SagaSignal`, `UserTask`, `SagaTrigger`, etc. |
| `engine` | `Coordinator`, `Timer`, `Advance` — the saga execution engine. |
| `engine/verbs` | The 31 built-in step implementations (30 in the registry + `end`) plus `verbs.HandlerFunc` for custom verbs. |
| `store` | The `Store` interface (see below) and `ErrNotFound`. |
| `store/memory` | In-memory `Store` implementation (tests and embedded in-process use). |
| `store/postgres` | Production Postgres `Store` + embedded SQL migrations. |
| `licensing` | `Resolver` interface + `StubAllowAll` for dev/test. |
| `secrets` | Secrets resolver interface used by HTTP/webhook verbs. |
| `clock` | `Clock` interface (`SystemClock` + test stub). |
| `api` | REST handlers, router (`api/router.go`), and the OpenAPI spec (`api/openapi.yaml`). |

Infrastructure that backs the public interfaces but is not part of the importable surface lives under `internal/`: `internal/mq` (RabbitMQ topology, publisher, consumer), `internal/cel` (CEL evaluator), `internal/rules` (decision-table evaluation), `internal/grpc` (gRPC worker liveness server), `internal/config` (environment-variable config), `internal/logging`. Consumers should not import these directly.

### Embedding the engine

An application can run the saga engine entirely in-process without a separate service or message broker. Import `saga` and call `saga.InMemory()` to get a `*saga.Saga` backed by an in-memory store; register `domain.WorkflowDefinition` values with `Register`, add custom verb handlers with `RegisterVerb`, and drive runs with `Start`, `Signal`, and `Get`. For production use, pass a Postgres store and other options to `saga.New(saga.Options{Store: pgStore, ...})`. The `cmd/api` and `cmd/engine` binaries are reference apps for service-mode deployment (Postgres + RabbitMQ), not a prerequisite for library use.

**Lifecycle.** Workflows that fan out (`parallel`/`foreach`/`spawn_saga`) advance their child runs on background goroutines via an in-process publisher. These run on a context derived from `Options.Context` (default `context.Background()`) and are tracked so they can be drained. Call `sc.Shutdown(ctx)` to cancel that context (the engine's advance loop stops between steps) and wait for in-flight background advances to finish, bounded by the passed `ctx` (it returns `ctx.Err()` if the drain exceeds the deadline). Linear workflows advance synchronously inside `Start` and need no draining.

### The `Store` interface
`store/store.go` defines the `Store` interface that both the engine and API depend on (neither depends on a concrete implementation). It covers: workflow + rule definitions, run CRUD + listing + stats, audit events, run-variable merges (dotted-key aware), pause/resume helpers (`SetPausedWithWakeup`, `SetPausedAwaitingSignal`, `SetPausedAwaitingEvent`, `ClearPause`, `WakeFromExternal`, `FindRunsByDueWakeup`, `FindRunsByAwaitedEvent`, `TryConsumeAwaitedSignal`, `AppendSignal`), child runs + try/catch stack (`SpawnChildRun`, `ListChildrenByParent`, `PushTryCatch`/`PopTryCatch`), user tasks, the action registry, saga triggers, and action-dispatch tracking (`MarkAwaitingAction`/`CompleteAction`/`FailAction`, which are no-ops if `attempt` doesn't match `current_attempt`, handling late deliveries). `ErrNotFound` is the standard not-found error.

### `store/memory`
In-memory implementation (`store.go`, plus `triggers.go`). Used by unit/e2e tests and by `saga.InMemory()`. Note: in the memory store, `UpsertWorkflowDefinition` may mint a new UUID per call (acceptable because runs only need *some* `definition_id` pointer).

### `store/postgres`
Production implementation, split by concern: `pool.go` (pgx pool / `Open`), `definitions.go`, `runs.go`, `events.go`, `rules.go`, `registry.go`, `triggers.go`, `user_tasks.go`, and `migrate.go`. `Open` connects; `Migrate(dsn)` applies embedded migrations on every boot via golang-migrate's pgx5 driver (idempotent, no-ops at head). The postgres `UpsertWorkflowDefinition` keeps a stable id per `(workflow_id, version)`.

### Migrations (`store/postgres/migrations`)
Numbered `NNN_*.up.sql` / `.down.sql`, embedded in the binary. They establish three schemas — `definitions`, `runtime`, `audit` — and evolve them:
- `001_init` — `workflow_definitions`, `rule_definitions`, `action_registry`; `saga_runs`, `saga_dlq_items`, `saga_trigger_fires`, `saga_signals`, `saga_user_tasks`; `saga_run_events` (audit, unique on `(run_id, step_id, attempt, event_type)`).
- `002_add_wait_columns` — `wakeup_at`, `awaited_signal`, `awaited_event_topic`, `awaited_event_headers` on `saga_runs` + partial indexes for due-wakeup and awaited-topic lookups.
- `003_child_runs_and_try_catch` — `parent_step_id`, `parent_branch_id`, `try_catch_stack` + parent index.
- `004_license_groups` — license-group support.
- `005_action_dispatch` — `awaited_action_dispatch`, `current_attempt` + partial index.
- `006_saga_triggers` — trigger persistence.
- `007_saga_event_notify` — a Postgres trigger on `audit.saga_run_events` that `pg_notify`s on channel `saga_event_<run_id_no_dashes>`; this powers the WebSocket run inspector (`api/handler_stream.go`) via LISTEN/NOTIFY.

What persists: workflow/rule/action definitions (`definitions.*`); live run state, inputs, variables, pause/await markers, parent links, try/catch stack (`runtime.saga_runs`); signals, user tasks, trigger-fire records, DLQ items (`runtime.*`); and the full append-only event log (`audit.saga_run_events`).

---

## 7. Messaging: RabbitMQ topology (`internal/mq`)

`mq.DeclareTopology` (declared by the go-saga-orchestration processes, idempotent) sets up:
- **Exchange `action.direct`** (direct, durable) — step dispatch. The `action` verb publishes `ActionPayload` here with routing key `<service>.<action_name>`. Workers declare their own per-service queue (`<service>.actions`) bound with `<service>.*` and consume it.
- **Exchange `workflow.events`** (topic, durable) — inbound events. `EventSubscriber.RunRMQ` binds a per-pod queue with `#` to consume all events (auto-ack, fire-and-forget); each delivery feeds both the `EventSubscriber` (wake paused sagas) and the `TriggerDispatcher` (start new sagas).
- **Queues `saga.advance`, `saga.dlq`, `action.dlq`** (durable). `saga.advance` is the core work queue.

Publishing (`mq/publisher.go`): `PublishSagaAdvance(runID)` sends `{"saga_run_id": ...}` JSON to `saga.advance` via the default exchange (persistent). `PublishActionDispatch(routingKey, payload)` publishes to `action.direct`. A `Publisher` owns one channel (channels are not concurrency-safe).

Consuming (`mq/consumer.go`): `ConsumeSagaAdvance` is a competing consumer on `saga.advance` with `prefetch=1` (fairness), manual ack. Per delivery: malformed JSON → `Reject(false)` (→ DLQ); handler error → `Nack(requeue=true)`; success → `Ack`.

---

## 8. Request-flow narrative (API → engine → store/MQ → completion; workers)

**Start.** A client calls `POST /api/v1/sagas/start` (`api/handler_sagas.go`). The API resolves the published `WorkflowDefinition` for the workflow id + tenant, upserts it to obtain a `definition_id`, creates a `pending` `SagaRun` (carrying `dry_run` and any `X-Feature-Override` flags), injects startup variables via any registered `StartupVariableProvider`s (none ship by default), publishes `saga.advance`, and returns `202` with the `saga_run_id`. (Triggers reach the same state via `TriggerDispatcher`.)

**Advance.** The engine's `saga.advance` consumer hands the message to `Coordinator.HandleAdvance` → `Advance`. The loop (see §2) re-reads the run, dispatches the current step's verb through the license gate, merges the verb result into Variables, emits audit events at each transition, and moves to the next step (or branch). It keeps looping in-process for as long as steps complete synchronously.

**Pause / async work.** When a verb suspends the run it persists a pause marker and returns `ErrSagaPaused`; the coordinator emits `step.paused` and ACKs. The run resumes when something republishes `saga.advance`:
- **Timer** (`engine.Timer`, running in `cmd/engine`) polls `FindRunsByDueWakeup` every tick and republishes advance for `wait_duration`/`wait_until` (and any `wakeup_at=now` set by `WakeFromExternal`).
- **Signals / user tasks** — the REST handlers consume the awaited signal and publish advance.
- **Events** — `EventSubscriber` matches `workflow.events` deliveries to runs awaiting a topic + header subset and publishes advance.
- **Joins** — `checkParentJoin` wakes a parent after its children's join condition is met.

**Workers via gRPC + `clients/go/worker`.** The `action` verb publishes an `ActionPayload` to `action.direct` and pauses the run awaiting the action. A worker built with `clients/go/worker`:
1. registers its actions over REST (`/api/v1/registry/register`),
2. declares + binds its `<service>.actions` queue to `action.direct` and consumes it (`prefetch=1`, manual ack),
3. holds a long-lived gRPC client to the engine.
On each delivery it deserializes the `ActionPayload`, resolves the handler by action name, and **drives the `ExecuteStep` bidi stream** (`internal/grpc/server.go`, proto `proto/liveness.proto`): worker sends `StartJob{run_id, step_id, attempt}` → engine replies `Acknowledged` → worker may stream `Heartbeat`s → worker sends `Complete{result_json}` or `Error{code, message, retryable}`. The engine bridges `Complete`→`store.CompleteAction` (merge result, then publish `saga.advance` to resume) and `Error`→`store.FailAction` (transition the run to failed; no advance). `CompleteAction`/`FailAction` are no-ops when `attempt` doesn't match the run's `current_attempt`, so late/duplicate deliveries are safe; combined with the worker's idempotency key and RabbitMQ redelivery, this gives at-least-once delivery with idempotent completion.

**Completion.** When the loop reaches an `end` step (or a verb pushes the run terminal), `completeRun` emits `step.succeeded` + `run.succeeded`, sets state `succeeded`, and runs `checkParentJoin` so any waiting parent advances. A failed step with no try/catch frame sets state `failed` and likewise notifies the parent. Throughout, every transition appends an immutable `audit.saga_run_events` row, which (via the `010` NOTIFY trigger) streams live to any connected run-inspector WebSocket.

---

## Notable findings & ambiguities

- **Retry is defined but not wired into `Advance`.** `engine/retry.go` provides a full backoff policy + default, but the coordinator's verb-error path does not re-attempt steps using it; on error it either jumps to a try/catch catch step or fails the run. Step retry is effectively delegated to workers (RabbitMQ redelivery + idempotency) and the `current_attempt` counter the `action` verb maintains.
- **Engine does not start the event/trigger consumers in the committed code.** `cmd/engine/main.go` constructs `EventSubscriber`/`TriggerDispatcher` but leaves `sub.RunRMQ` commented out (`_ = sub`), with a note that prod-RMQ wiring is deferred. So in this build, `wait_for_event` and `record_transition` triggers will not fire until that goroutine is started. The timer dispatcher and the `saga.advance` consumer **are** started.
- **Timer leader election is a no-op.** `Timer.AcquireLeaderLock` is documented as a stub; every engine pod runs the timer. With 2+ replicas this could double-publish `saga.advance` — harmless because `Advance` is idempotent (a premature advance on a still-paused run just ACKs), but worth knowing.
- **`ValidateDefinition` is not called at publish time.** `engine/validate.go` implements structural checks (forbids `parallel` inside `try_catch`) and license-gate validation, but its own TODO notes the publish handler does not yet invoke it. Runtime is still gated by the per-step license check in `Advance`.
- **`foreach` is parallel-only.** Sequential mode is explicitly rejected with guidance to use `while` + a counter.
- **`license.StubAllowAll` in the engine binary** means license gates never reject in this deployment; the gate logic is fully present and exercised by tests, just disabled by the resolver `cmd/engine` wires.
- **`try_catch` frames are never popped on success** — they remain on the stack until the saga terminates (documented as acceptable given the max-nesting-depth-3 rule the validator would enforce).
