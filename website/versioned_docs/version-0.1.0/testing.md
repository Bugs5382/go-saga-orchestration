---
sidebar_position: 3
---

# 🧪 Testing Sagas

The in-memory store makes workflows trivial to unit-test: `saga.InMemory()` runs
the coordinator **in-process and synchronously**, with no Postgres, RabbitMQ, or
worker processes. By the time `Start` returns, an embedded run has advanced to a
terminal state, so a test is just *register → publish → start → assert*.

`InMemory()` wires `StubAllowAll` licensing by default, so every verb — including
license-gated ones — is available in tests. (To exercise real gating, pass your
own resolver via `saga.New(saga.Options{Licensing: ...})`.)

---

## A first unit test

This test registers a custom verb, publishes a one-step workflow, starts it, and
asserts both the terminal state and the variables the verb produced.

```go
package checkout_test

import (
	"context"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
)

func TestCheckout_ChargesCard(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()

	// Register the custom verb under test.
	sc.RegisterVerb("charge_card", "common",
		verbs.HandlerFunc(func(_ context.Context, run domain.SagaRun, _ domain.Step) (map[string]any, error) {
			total, _ := run.Variables["total"].(float64)
			return map[string]any{"charge_id": "ch_123", "charged": total}, nil
		}))

	// Define and publish the workflow.
	if err := sc.Register(domain.WorkflowDefinition{
		ID: "checkout", Version: 1, Start: "charge", Published: true,
		Steps: []domain.Step{
			{ID: "charge", Type: "charge_card", Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	// Start it and read the finished run back.
	runID, err := sc.Start(ctx, "checkout", map[string]any{"total": 4200.0})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	run, err := sc.Get(ctx, runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state = %q, want succeeded", run.State)
	}
	if got := run.Variables["charge_id"]; got != "ch_123" {
		t.Fatalf("charge_id = %v, want ch_123", got)
	}
}
```

Run it with the rest of the suite:

```bash
task test          # go test ./...
# or, for just this package:
go test ./checkout/...
```

:::tip Numeric literals
Inputs flow through the engine as `map[string]any`, and JSON-style numbers are
`float64`. Pass `4200.0` (not `4200`) so the `run.Variables["total"].(float64)`
type assertion in the verb succeeds.
:::

---

## Asserting failure and compensation

A handler that returns an error fails the step. With no `try`/`catch` or
compensation wired, the run lands in `RunStateFailed` — assert that directly:

```go
sc.RegisterVerb("charge_card", "common",
	verbs.HandlerFunc(func(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
		return nil, fmt.Errorf("gateway declined")
	}))

// ... Start + Get ...

if run.State != domain.RunStateFailed {
	t.Fatalf("state = %q, want failed", run.State)
}
```

To test the **happy path through compensation**, build a workflow with a
`try`/`catch` branch (see the [verb reference](./verbs.md)) and assert the run
reaches `RunStateSucceeded` after the catch branch runs, or
`RunStateCompensating`/`RunStateFailed` mid-flight if you drive it with a fake
clock.

---

## Testing `action` steps in-process

An `action` step normally pauses the run and waits for an external worker to
reply over gRPC — so in a pure `InMemory()` test it would hang indefinitely. To
test a workflow that contains an `action` without standing up a worker, register
an in-process verb **with the same step type** to stand in for the worker:

```go
// Stub the worker: handle the action's type in-process so the run completes.
sc.RegisterVerb("action", "common",
	verbs.HandlerFunc(func(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
		// Inspect step.Inputs / run.Variables and return the result the real
		// worker would have produced.
		return map[string]any{"ok": true}, nil
	}))
```

See [Custom actions](./embedding.md#-custom-actions-worker-round-trip) for how
action dispatch works in service mode, and the
[gRPC worker protocol](./grpc.md) for testing a real worker against the engine.

---

## Time-dependent verbs

Verbs that wait (`wait_for`, `wait_for_event`, timers) read the clock through the
injected `clock.Clock`. Inject a `clock.NewFakeClock(...)` via
`saga.New(saga.Options{Clock: fc, Store: memory.New()})` and advance it in the
test to drive timeouts deterministically, instead of sleeping. The engine's own
follow-up tests under `test/e2e` use exactly this pattern.
