# Examples

Runnable examples of embedding the saga orchestration engine as a library.

## basic

A minimal end-to-end embed: an in-memory engine (no Postgres/RabbitMQ), a custom
verb registered as a closure, and a workflow that combines the custom verb with
the built-in `switch` and `set_var` verbs to route an order by amount.

```
go run ./examples/basic
```

Expected output:

```
amount=50    -> state=succeeded tier=standard message="standard handling"
amount=5000  -> state=succeeded tier=priority message="VIP handling"
```

It demonstrates the core embedding API:

- `saga.InMemory()` — an engine that runs in-process with no external infrastructure.
- `sc.RegisterVerb(stepType, licenseGroup, verbs.HandlerFunc(...))` — add your own verb.
- `sc.Register(domain.WorkflowDefinition{...})` — publish a workflow.
- `sc.Start(ctx, workflowID, inputs)` — start a run (use `StartAt` for a named entry point).
- `sc.Get(ctx, runID)` — read the run's state and variables.

## workflows

A per-verb catalog of 30 minimal `WorkflowDefinition` JSON examples — one for
every verb the engine ships. Each is a 2–4-step flow ending at an `end` step and
demonstrating the verb's exact required inputs.

See [`examples/workflows/README.md`](workflows/README.md) for the full index
grouped by category, plus one-line descriptions and key inputs for each verb.

All examples are validated automatically:

```
go test ./examples/workflows/...
```
