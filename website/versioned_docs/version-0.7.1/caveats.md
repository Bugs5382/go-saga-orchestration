# ⚠️ Caveats & Gotchas

A friendly list of things that trip people up. Each item has a one-line workaround where relevant.

---

- **One verb per step, no inline composition.** A step executes exactly one verb. You cannot chain a CEL transform and an HTTP call in the same step. _Workaround: add a second step._

- **`action` has no `out_var` — the worker controls output keys.** Whatever keys the worker returns are merged directly into `Variables`. If two `action` steps return overlapping keys, the later step's values overwrite the earlier ones. _Workaround: prefix output keys in your worker, or use a `set_var`/`transform` step immediately after to rename them._

- **CEL verbs read `Variables`, not `step.Inputs`.** `transform`, `filter`, `map`, `switch`, `while`, `assert`, and similar verbs compile their `expr` against `run.Variables`. If the value you need is in the run's initial inputs and not yet in `Variables`, it is not automatically visible. _Workaround: add a `set_var` step at the start of your workflow to promote input values into named variables._

- **Embedded `action` steps need a worker or service mode.** `saga.InMemory()` dispatches `action` steps to its in-process publisher, which pauses the saga. Without a registered worker goroutine to reply, the run will pause indefinitely. _Workaround: use `RegisterVerb` for in-process handlers, or run `cmd/engine` + a gRPC worker for true worker round-trips._

- 💡 **Embedded `emit_event` matches in-process (no broker).** The in-process `EventEmitter` both wakes runs awaiting the topic (header-subset match) **and** runs the trigger dispatcher, so matching `record_transition` triggers start new runs — parity with service mode. (Payload-CEL trigger matching beyond `record_transition` is not implemented in either mode yet.)

- 💡 **Waits support `timeout_s`.** Both `wait_for_signal` and `wait_for_event` accept an optional `timeout_s`; on timeout the run routes to the step's `timeout` branch if defined, else to `next`. (`wait_duration`/`wait_until` are scheduled waits — their firing *is* the intended path, so no timeout branch.)

- **Cancelling a parallel child re-checks the parent join.** `cancel` on a child that is a branch of a `parallel` join immediately re-evaluates the parent's join and wakes the parent if it is now satisfied (e.g. the cancelled child was the last non-terminal branch, or a `quorum` is met). Under `join_strategy: "all"` with other branches still running, the parent correctly stays paused until they finish. _For partial completion, use `join_strategy: "quorum"`._

- **`ValidateDefinition` is not auto-called by the REST publish path.** The engine provides `engine.ValidateDefinition(def)` to catch structural problems (missing steps, circular references, `parallel`-inside-`try_catch`, excessive nesting depth) before a workflow goes live. The REST `PUT /api/v1/workflows` endpoint does not call it automatically. _Workaround: call `ValidateDefinition` in your CI pipeline or publishing tooling before pushing definitions to production._

- **The module path is internal.** The Go module is hosted at `github.com/Bugs5382/go-saga-orchestration`. It is not published to the public Go module proxy. _Workaround: add a `GONOSUMCHECK` / `GONOSUMDB` / `GOFLAGS` directive in your environment, or vendor the module, per your organisation's private module setup._
