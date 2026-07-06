# 📖 Verb Reference

This page documents all 31 saga step types ("verbs") supported by the engine.

## Quick-reference table

| Verb | License group | One-liner |
|---|---|---|
| `action` | common | Dispatch a step to an external worker and pause until it replies. |
| `decision` | common | Evaluate a stored rule table and branch on its output. |
| `switch` | common | Evaluate a CEL expression and branch on the string result. |
| `error` | common | Immediately fail the saga with a code and message. |
| `noop` | common | Placeholder — do nothing and advance. |
| `end` | _(terminal)_ | End the run as `succeeded`. |
| `set_var` | common | Write a literal or CEL-computed value into a variable. |
| `transform` | common | Evaluate a CEL expression and store the result. |
| `merge` | common | Merge a CEL-evaluated map into an existing variable. |
| `filter` | common | Keep list elements that satisfy a CEL predicate. |
| `map` | common | Transform each element of a list with a CEL expression. |
| `assert` | common | Fail the saga if a CEL expression is not truthy. |
| `log` | common | Emit a structured log line at a chosen level. |
| `metric_emit` | observability | Append a named metric event to the run's audit stream. |
| `http_request` | common / external_io_advanced¹ | Issue a synchronous outbound HTTP request. |
| `webhook_emit` | external_io_advanced | POST a JSON payload to an external URL. |
| `wait_duration` | waits | Pause for a Go duration (e.g. `"5s"`, `"1h30m"`). |
| `wait_until` | waits | Pause until an RFC3339 absolute timestamp. |
| `wait_for_signal` | events_and_signals | Pause until a named external signal arrives (with optional timeout). |
| `wait_for_event` | events_and_signals | Pause until a matching event topic + header subset arrives. |
| `emit_signal` | events_and_signals | Send a signal to another (or the same) run. |
| `emit_event` | events_and_signals | Publish an event via the configured EventEmitter. |
| `while` | loops_and_recovery | Loop while a CEL condition holds; exit via branching. |
| `try_catch` | loops_and_recovery | Push an error-handler frame; jump to `catch` on any step error. |
| `cancel` | loops_and_recovery | Cancel a run (self or a target). |
| `parallel` | parallel_control | Fan out N branches and join when a strategy is satisfied. |
| `foreach` | parallel_control | Fan out one branch per element of a CEL-evaluated list. |
| `sub_saga` | compositions | Start a child workflow and pause the parent until it finishes. |
| `spawn_saga` | compositions | Fire-and-forget: start a child workflow and continue immediately. |
| `manual_approval` | human_interaction | Create a user task; pause until an assignee submits it. |
| `collect_input` | human_interaction | Like `manual_approval` but `form_schema` is required. |

> ¹ `http_request` is `common` when method is `GET` with no `secret_ref`; all other configurations require the `external_io_advanced` group.

---

> 💡 **License groups** gate verbs at publish time and at runtime. In development the `StubAllowAll` licensing resolver (used by `saga.InMemory()` and `saga.New` with no `Licensing` set) permits every group. In production, override per-request with the `X-Feature-Override` header or the `FeatureOverrides` field on the run start request.

---

## Core / Control

### `action`

Dispatches the step to a named worker process and **pauses the saga** until the worker replies.

| Input | Required | Notes |
|---|---|---|
| `step.Action` | ✅ | `"service.name"` — must contain a dot. Identifies the worker queue. |
| `step.Inputs` | optional | Forwarded verbatim to the worker as `inputs`. |

**Output:** The worker's result map is merged directly into `Variables`. There is no `out_var` — whatever keys the worker returns become top-level variables.

> ⚠️ In embedded mode (`saga.InMemory()`) the action verb publishes to an in-process publisher. You still need a worker goroutine (or service mode) to actually handle the dispatch; a plain in-memory saga with no registered worker handler will leave the run paused indefinitely.

**Example:** [`examples/workflows/action.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/action.json)

---

### `decision`

Evaluates a stored decision-table rule and returns its output map. The engine reads `result["branch"]` to pick a route from `step.Branches`.

| Input | Required | Notes |
|---|---|---|
| `rule_id` | ✅ | Stable ID of a published `RuleDefinition`. |
| `inputs_map` | optional | `map[string]string` — maps rule input keys to variable names. Omit to pass `Variables` directly. |

**Output:** The rule's full output map (including `branch`) is merged into `Variables`.

**Example:** [`examples/workflows/decision.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/decision.json)

---

### `switch`

Evaluates a CEL expression to a string and routes via `step.Branches`. Simpler than `decision` when no rule table is needed.

| Input | Required | Notes |
|---|---|---|
| `expr` | ✅ | CEL expression over `Variables`; must produce a string. |

**Output:** `{"branch": "<result>"}` — the engine picks `step.Branches[<result>].Next`. An unknown branch value is a runtime error.

**Example:** [`examples/workflows/switch.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/switch.json)

---

### `error`

Immediately fails the saga as a non-retryable error.

| Input | Required | Notes |
|---|---|---|
| `code` | ✅ | Error code string surfaced in the run's error record. |
| `message` | optional | Human-readable description. |

**Output:** None — the run terminates as `failed`.

**Example:** [`examples/workflows/error.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/error.json)

---

### `noop`

Does nothing. Advances to `step.Next`. Useful as a placeholder during development or as a join point for multiple branches.

**Inputs:** none. **Output:** empty map.

**Example:** [`examples/workflows/noop.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/noop.json)

---

### `end`

Marks the run as `succeeded`. Every workflow needs at least one `end` step. It is dispatched inline by the engine (not via a queue) so no license group applies.

**Inputs:** none. **Output:** none — the saga terminates.

---

## Data

### `set_var`

Writes a value to a variable. Use it to seed variables before CEL verbs read them.

| Input | Required | Notes |
|---|---|---|
| `out_var` | ✅ | Destination variable name. Dotted keys (`a.b.c`) write into nested maps. |
| `value` | one of `value`/`expr` | Literal value — passed through unchanged. |
| `expr` | one of `value`/`expr` | CEL expression over `Variables`; result is written. `expr` wins when both are present. |

**Output:** `{out_var: <value>}`.

**Example:** [`examples/workflows/set_var.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/set_var.json)

---

### `transform`

Evaluates a CEL expression and writes the result to a named variable. Equivalent to `set_var` with `expr`.

| Input | Required | Notes |
|---|---|---|
| `expr` | ✅ | CEL expression over `Variables`. |
| `out_var` | ✅ | Destination variable name. |

**Output:** `{out_var: <result>}`.

**Example:** [`examples/workflows/transform.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/transform.json)

---

### `merge`

Evaluates a CEL expression that must produce a map, then **deep-merges** it into an existing variable.

| Input | Required | Notes |
|---|---|---|
| `from` | ✅ | CEL expression → `map`. |
| `into` | ✅ | Name of the destination variable. Dotted paths are supported. |

**Output:** The merged variable value under its existing key.

**Example:** [`examples/workflows/merge.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/merge.json)

---

### `filter`

Keeps list elements where a CEL predicate is truthy.

| Input | Required | Notes |
|---|---|---|
| `list` | ✅ | CEL expression → list. |
| `expr` | ✅ | CEL predicate; the current element is bound as `_`. |
| `out_var` | ✅ | Variable to write the filtered list to. |

**Output:** `{out_var: [filtered list]}`.

**Example:** [`examples/workflows/filter.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/filter.json)

---

### `map`

Transforms every element of a list with a CEL expression.

| Input | Required | Notes |
|---|---|---|
| `list` | ✅ | CEL expression → list. |
| `expr` | ✅ | CEL transform; element bound as `_`. |
| `out_var` | ✅ | Variable to write the mapped list to. |

**Output:** `{out_var: [mapped list]}`.

**Example:** [`examples/workflows/map.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/map.json)

---

### `assert`

Fails the saga if a CEL expression is not truthy. Use it for invariant checks mid-workflow.

| Input | Required | Notes |
|---|---|---|
| `expr` | ✅ | CEL boolean expression. |
| `code` | optional | Error code emitted on failure (default `"assertion_failed"`). |

**Output:** Empty map on success; non-retryable error on failure.

**Example:** [`examples/workflows/assert.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/assert.json)

---

## Observability

### `log`

Emits a structured log line via the engine's logger.

| Input | Required | Notes |
|---|---|---|
| `message` | ✅ | Log message string. |
| `level` | optional | `"info"` (default) \| `"warn"` \| `"error"`. |

**Output:** Empty map.

**Example:** [`examples/workflows/log.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/log.json)

---

### `metric_emit`

Appends a named metric event to the run's audit stream. _(Prometheus side-channel wiring is planned for a future release.)_

| Input | Required | Notes |
|---|---|---|
| `name` | ✅ | Metric name string. |
| `value` | ✅ | Numeric value. |
| `labels` | optional | `map[string]string` of label key/value pairs. |

**Output:** Empty map.

**Example:** [`examples/workflows/metric_emit.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/metric_emit.json)

---

## I/O

### `http_request`

Issues a synchronous outbound HTTP request and merges the response into `Variables`.

| Input | Required | Notes |
|---|---|---|
| `url` | ✅ | Target URL. |
| `method` | optional | HTTP method (default `"GET"`). |
| `headers` | optional | `map[string]any` → request headers. |
| `body` | optional | Any value; JSON-marshalled into the request body. |
| `timeout_s` | optional | Request timeout in seconds (default `30`). |
| `secret_ref` | optional | Secret key resolved via the Secrets resolver → set as `Authorization` header. |
| `out_var` | optional | Output prefix (default `"http_result"`). |

**Output keys** (with `out_var = "http_result"`):
- `http_result` — parsed JSON body (or raw string if non-JSON).
- `http_result_status` — `int64` HTTP status code.
- `http_result_headers` — `map[string]string` of response headers.

> ⚠️ License group is `common` only for `GET` with no `secret_ref`. Any other method or authenticated request requires the `external_io_advanced` group.

**Example:** [`examples/workflows/http_request.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/http_request.json)

---

### `webhook_emit`

POSTs a JSON payload to an external URL, with optional HMAC-SHA256 request signing.

| Input | Required | Notes |
|---|---|---|
| `url` | ✅ | Target URL. |
| `body` | ✅ | Any value; JSON-marshalled. |
| `secret_ref` | optional | Secret key → `X-Webhook-Sig: sha256=<hex>` header. |
| `timeout_s` | optional | Timeout in seconds (default `15`). |
| `headers` | optional | Additional request headers. |
| `async` | optional | `bool`; default `false`. When `true`, fires the request in a goroutine and returns immediately (failures are logged only). |
| `out_var` | optional | Output prefix (default `"webhook_result"`). |

**Output (sync mode):** `{out_var}_status` — `int64` HTTP status code.  
**Output (async mode):** `{out_var}_async: true`.

**Example:** [`examples/workflows/webhook_emit.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/webhook_emit.json)

---

## Timing

### `wait_duration`

Pauses the saga for a duration expressed as a Go duration string.

| Input | Required | Notes |
|---|---|---|
| `duration` | ✅ | Go duration string e.g. `"5s"`, `"1h30m"`, `"72h"`. |

The engine's timer dispatcher wakes the run when the deadline passes.

**Example:** [`examples/workflows/wait_duration.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/wait_duration.json)

---

### `wait_until`

Pauses the saga until an absolute point in time.

| Input | Required | Notes |
|---|---|---|
| `timestamp` | ✅ | RFC3339 datetime string, e.g. `"2026-01-01T09:00:00Z"`. |

**Example:** [`examples/workflows/wait_until.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/wait_until.json)

---

## Events & Signals

### `wait_for_signal`

Pauses the saga until a named external signal arrives via `POST /api/v1/sagas/{run_id}/signal/{name}`.

| Input | Required | Notes |
|---|---|---|
| `name` | ✅ | Signal name to await. |
| `timeout_s` | optional | Max seconds to wait. Omit to wait indefinitely. |

**Timeout routing:** when the deadline fires while the signal has not yet arrived, the engine checks `step.Branches["timeout"].Next` first. If that branch exists, the run routes there instead of `step.Next` — handy for escalation paths.

> 💡 Wire a `timeout` branch to an escalation step to handle missed approvals or SLA breaches without any extra polling.

**Example:** [`examples/workflows/wait_for_signal.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/wait_for_signal.json)

---

### `wait_for_event`

Pauses the saga until an event with a matching topic (and optional header subset) arrives via the event bus.

| Input | Required | Notes |
|---|---|---|
| `topic` | ✅ | Event topic / RabbitMQ routing key to await. |
| `headers` | optional | `map[string]any` — incoming event headers must contain all of these key/value pairs (string equality). |
| `timeout_s` | optional | Number of seconds to wait. On timeout the run routes to the step's `timeout` branch if defined, else to `next`. Omitted = wait indefinitely. |

> 💡 Add a `"timeout"` entry to the step's `branches` to escalate when no matching event arrives before `timeout_s` (same pattern as `wait_for_signal`).

**Example:** [`examples/workflows/wait_for_event.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/wait_for_event.json)

---

### `emit_signal`

Sends a signal to another run (the send-side complement of `wait_for_signal`). If the target is currently paused awaiting that signal, it is consumed and the target advances immediately.

| Input | Required | Notes |
|---|---|---|
| `run_id` | ✅ | UUID of the target run. |
| `name` | ✅ | Signal name. |
| `payload` | optional | `map[string]any` carried with the signal. |

**Output:** Empty map.

**Example:** [`examples/workflows/emit_signal.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/emit_signal.json)

---

### `emit_event`

Publishes an event via the configured `EventEmitter` (in-process when embedded; RabbitMQ in service mode).

| Input | Required | Notes |
|---|---|---|
| `topic` | ✅ | Event topic / routing key. |
| `headers` | optional | `map[string]any` → `map[string]string`. |
| `payload` | optional | `map[string]any` event payload. |

**Output:** Empty map.

> 💡 In embedded mode the in-process emitter both wakes runs awaiting the topic **and** runs the trigger dispatcher, so matching `record_transition` triggers start new runs — parity with service mode (no broker needed).

**Example:** [`examples/workflows/emit_event.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/emit_event.json)

---

## Loops & Recovery

### `while`

Loops while a CEL condition evaluates to `true`.

| Input | Required | Notes |
|---|---|---|
| `condition` | ✅ | CEL boolean expression over `Variables`. |
| `max_iterations` | optional | Default `100`; hard cap `10000`. Prevents runaway loops. |

**Output:** `{"branch": "continue" | "exit"}`.

**Wiring pattern:**
- `step.Branches.continue → next` — first step of the loop body.
- `step.Branches.exit → next` — first step after the loop.
- The body's last step sets `next` back to the `while` step.

An iteration counter is maintained at `Variables._while.<step_id>.iter`.

**Example:** [`examples/workflows/while.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/while.json)

---

### `try_catch`

Pushes an error-handler frame. If any step inside the protected region errors, the saga jumps to the `catch` step instead of failing. The error context is written to `Variables._error`.

| Input | Required | Notes |
|---|---|---|
| `try` | ✅ | `[]string` of step IDs in the protected region. Used by `ValidateDefinition` to reject disallowed nesting (e.g. `parallel` inside `try`). |
| `catch` | ✅ | Step ID to jump to on error. |

**Wiring:** set `step.Next` to the first step inside the `try` body. The body's last step sets `next` to whatever comes after the protected region.

**Example:** [`examples/workflows/try_catch.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/try_catch.json)

---

### `cancel`

Cancels a run.

| Input | Required | Notes |
|---|---|---|
| `run_id` | optional | UUID of the target run. Omit (or set to the current run's ID) to self-cancel — the current run ends as `cancelled`. |
| `reason` | optional | Human-readable reason string. |

**Self-cancel:** returns `ErrSagaCancelled`; the run ends immediately as `cancelled`.  
**Target-cancel:** cancels the target run and the current run **continues** to `step.Next`.

> ⚠️ When you cancel a run that is a child of a `parallel` join, the join may be left waiting if the join strategy expects all children. The cancelled child is counted as terminal, so with `join_strategy: "all"` the parent will eventually time out or remain paused unless all other children also complete.

**Example:** [`examples/workflows/cancel.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/cancel.json)

---

## Parallelism

### `parallel`

Fans out N branches as child runs and pauses the parent until the join strategy is satisfied.

| Input | Required | Notes |
|---|---|---|
| `branches` | ✅ | `[]any` of branch objects **or** a CEL string → list. Each branch: long-form `{"start": "step_id", "steps": [...]}` or short-form `{"type": "...", "inputs": {...}}`. |
| `join_strategy` | optional | `"all"` (default) — wait for every branch. `"quorum"` — wake after `quorum_n` successes. |
| `quorum_n` | required when `quorum` | Positive integer ≤ branch count. Can also be a CEL string evaluated at runtime. |

**Output:** Each branch's variables are aggregated into `Variables._parallel.<step_id>.branches` when the parent wakes.

> ⚠️ Remaining branches continue to run after a quorum wake — they are not cancelled.

**Example:** [`examples/workflows/parallel.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/parallel.json)

---

### `foreach`

Fans out one child run per element of a CEL-evaluated list (parallel mode only in v1).

| Input | Required | Notes |
|---|---|---|
| `list` | ✅ | CEL expression → list. |
| `body` | ✅ | `[]any` of step objects forming the loop body. |
| `start` | ✅ | ID of the first step inside `body`. |
| `parallel` | optional | `bool`; default `true`. Sequential mode is not yet supported — use `while` with an index counter for sequential loops. |

Each child run receives `Variables._foreach_item` (the element) and `Variables._foreach_index` (zero-based index). An empty list advances without spawning.

**Example:** [`examples/workflows/foreach.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/foreach.json)

---

## Composition (Call Tree)

### `sub_saga`

Starts a named workflow as a child saga and **pauses the parent** until the child reaches a terminal state.

| Input | Required | Notes |
|---|---|---|
| `workflow_id` | ✅ | Stable ID of the child `WorkflowDefinition`. |
| `inputs` | optional | `map[string]any` passed as the child's initial inputs. |
| `entrypoint` | optional | Named entry point on the child definition (see `Entrypoints`). Defaults to `Start`. |

**Output:** Empty map on parent resume (child variables are not automatically merged; wire a `set_var`/`transform` after if needed).

**Example:** [`examples/workflows/sub_saga.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/sub_saga.json)

---

### `spawn_saga`

Starts a named workflow as a **fire-and-forget** child. The parent continues immediately to `step.Next` without waiting.

| Input | Required | Notes |
|---|---|---|
| `workflow_id` | ✅ | Stable ID of the child workflow. |
| `inputs` | optional | `map[string]any` passed as the child's initial inputs. |
| `entrypoint` | optional | Named entry point on the child. Defaults to `Start`. |

**Output:** Empty map; parent is not paused.

**Example:** [`examples/workflows/spawn_saga.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/spawn_saga.json)

---

## Human Interaction

### `manual_approval`

Creates a user task and pauses the saga until the assignee submits it via `POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit`.

| Input | Required | Notes |
|---|---|---|
| `assignee` | ✅ | User ID or role expected to submit. |
| `due_in` | optional | Go duration string — sets `due_at = now + due_in`. |
| `form_schema` | optional | `map[string]any` rendered in the admin UI. |

**Output:** None on pause; the submitted form data is available after the run resumes.

**Example:** [`examples/workflows/manual_approval.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/manual_approval.json)

---

### `collect_input`

Like `manual_approval` but `form_schema` is **required**. Use this when the workflow needs structured data from the user (remediation notes, parameters, etc.) rather than a simple approve/reject.

| Input | Required | Notes |
|---|---|---|
| `assignee` | ✅ | User ID or role. |
| `form_schema` | ✅ | `map[string]any` schema — must be non-empty. |
| `due_in` | optional | Go duration deadline. |

**Output:** None on pause.

**Example:** [`examples/workflows/collect_input.json`](https://github.com/Bugs5382/go-saga-orchestration/blob/main/examples/workflows/collect_input.json)
