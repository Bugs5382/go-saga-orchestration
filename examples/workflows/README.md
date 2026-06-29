# Per-Verb Workflow Examples

One minimal, self-contained `WorkflowDefinition` JSON per verb. Each file is a
2–4-step flow that demonstrates the verb's key inputs and ends at an `end` step.

> See the [verb reference](https://bugs5382.github.io/go-saga-orchestration/docs/verbs) on the documentation site for each verb's full inputs/outputs.
All examples are validated automatically by `examples_test.go`.

---

## Control Flow

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [decision.json](decision.json) | `decision` | Evaluates a stored rule and routes to an approved or rejected branch | `rule_id`, `inputs_map`, `branches` |
| [switch.json](switch.json) | `switch` | Seeds a channel variable and routes to the matching handler branch via CEL | `expr`, `branches` |
| [while.json](while.json) | `while` | Loops while a retry counter is below 3, incrementing each iteration | `condition`, `max_iterations`, `branches.continue`, `branches.exit` |
| [try_catch.json](try_catch.json) | `try_catch` | Wraps a risky step in an error frame; on failure routes to a compensating step | `try`, `catch`, `next` |

---

## Variables and Data

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [set_var.json](set_var.json) | `set_var` | Sets `order_status` to a literal and `tax_amount` to a numeric literal | `out_var`, `value` |
| [transform.json](transform.json) | `transform` | Evaluates a CEL expression to compute a discounted price | `expr`, `out_var` |
| [merge.json](merge.json) | `merge` | Deep-merges a CEL-evaluated map into a context variable | `from`, `into` |
| [filter.json](filter.json) | `filter` | Keeps only order amounts above a threshold from a list | `list`, `expr`, `out_var` |
| [map.json](map.json) | `map` | Applies a CEL expression to every element of a price list | `list`, `expr`, `out_var` |
| [assert.json](assert.json) | `assert` | Asserts an order total is within an acceptable range before proceeding | `expr`, `code` |

---

## Actions and External I/O

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [action.json](action.json) | `action` | Dispatches a `notifications.send_email` action to a worker service | `action` (`service.name`), step `inputs` |
| [http_request.json](http_request.json) | `http_request` | POSTs an order payload to an external fulfillment REST API | `method`, `url`, `headers`, `body`, `timeout_s`, `out_var` |
| [webhook_emit.json](webhook_emit.json) | `webhook_emit` | POSTs a signed order.completed notification to a partner webhook | `url`, `body`, `headers`, `timeout_s` |

---

## Waits and Timers

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [wait_duration.json](wait_duration.json) | `wait_duration` | Pauses for 30 minutes as a cooling-off period | `duration` (Go syntax, e.g. `"30m"`) |
| [wait_until.json](wait_until.json) | `wait_until` | Pauses until a specific RFC3339 wall-clock timestamp | `timestamp` |
| [wait_for_signal.json](wait_for_signal.json) | `wait_for_signal` | Awaits a `payment_confirmed` signal with a 24-hour timeout escalation | `name`, `timeout_s`, `branches.timeout` |
| [wait_for_event.json](wait_for_event.json) | `wait_for_event` | Pauses until a RabbitMQ `inventory.updated` event matching header filters arrives | `topic`, `headers` |

---

## Parallel and Fan-Out

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [parallel.json](parallel.json) | `parallel` | Fans out email and SMS notification branches; waits for both | `branches` (list of `{key, start, steps}`), `join_strategy` |
| [foreach.json](foreach.json) | `foreach` | Spawns one notification child run per recipient in a list | `list`, `start`, `body` |

---

## Composition

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [sub_saga.json](sub_saga.json) | `sub_saga` | Starts a `kyc_verification` child workflow and blocks until it completes | `workflow_id`, `inputs` |
| [spawn_saga.json](spawn_saga.json) | `spawn_saga` | Fire-and-forgets an `audit_trail_recorder` child workflow then continues immediately | `workflow_id`, `inputs` |

---

## Human Interaction

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [manual_approval.json](manual_approval.json) | `manual_approval` | Pauses for a manager's approve/reject decision with a 48-hour deadline | `assignee`, `due_in`, `form_schema` |
| [collect_input.json](collect_input.json) | `collect_input` | Collects a structured remediation plan from an on-call engineer | `assignee`, `form_schema` (required), `due_in` |

---

## Events and Signals

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [emit_signal.json](emit_signal.json) | `emit_signal` | Sends a `payment_confirmed` signal to a target saga run | `run_id`, `name`, `payload` |
| [emit_event.json](emit_event.json) | `emit_event` | Publishes an `order.shipped` domain event to RabbitMQ | `topic`, `headers`, `payload` |

---

## Observability

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [log.json](log.json) | `log` | Emits an info log and a warning log at two severity levels | `message`, `level` |
| [metric_emit.json](metric_emit.json) | `metric_emit` | Emits a labelled counter metric after an order is fulfilled | `name`, `value`, `labels` |

---

## Lifecycle

| File | Verb | Description | Key inputs |
|------|------|-------------|------------|
| [noop.json](noop.json) | `noop` | Placeholder step with no side effects | — |
| [error.json](error.json) | `error` | Halts the saga immediately with a non-retryable error | `code`, `message` |
| [cancel.json](cancel.json) | `cancel` | Logs a reason then self-cancels the current saga run | `reason` |

---

## Validation

The test file `examples_test.go` (package `examples_test`) validates every JSON
in this directory on every `go test` run:

```
go test ./examples/workflows/...
```

It checks JSON parse, `engine.ValidateDefinition`, all known verbs, and every
`next`/`branch.next` edge reference against the step ID index.

## Scenarios (multi-verb data flow)

These show how data flows between steps. Verbs run **one per step**; a step
cannot embed another verb. Dependencies flow through `run.Variables`, which each
verb's result is merged into.

- **`scenario_action_to_setvar.json`** — an `action` step's worker result is
  merged into `Variables` (by the engine's `CompleteAction`), then a later
  `set_var` reads it via a CEL `expr`. Shows the cross-step hand-off (the action
  and set_var are separate steps, not one node).
- **`scenario_parallel_setvars.json`** — a `parallel` step fans out **four
  `set_var` branches**; on join the engine aggregates each branch's variables
  into `_parallel.<step>.branches`, which a downstream `set_var` reads via CEL
  (`size(_parallel.fanout.branches)` → 4). `scenario_test.go` runs this one
  in-memory and asserts the aggregate, proving the flow end-to-end.
