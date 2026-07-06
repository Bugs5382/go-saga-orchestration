---
sidebar_position: 1
title: Getting started
---

# Getting started

This tutorial builds a realistic multi-step saga from empty, in embedded mode (no infrastructure).

## 1. Install

```bash
go get github.com/Bugs5382/go-saga-orchestration
```

## 2. A minimal saga

```go
package main

import (
	"context"
	"fmt"

	"github.com/Bugs5382/go-saga-orchestration/saga"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

func main() {
	sc := saga.InMemory() // in-memory store + in-process advance

	sc.RegisterVerb("charge_card", "common",
		verbs.HandlerFunc(func(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
			return map[string]any{"ok": true}, nil
		}))

	sc.Register(domain.WorkflowDefinition{
		ID: "checkout", Version: 1, Start: "charge", Published: true,
		Steps: []domain.Step{
			{ID: "charge", Type: "charge_card", Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	})

	runID, _ := sc.Start(context.Background(), "checkout", map[string]any{"total": 4200})
	run, _ := sc.Get(context.Background(), runID)
	fmt.Println(run.State) // succeeded
}
```

## 3. Recovering from errors with `try_catch`

When a step returns an error, the run normally transitions to `failed`. To recover
instead, wrap the risky steps in a `try_catch` frame: if any step inside the
protected region errors, the saga jumps to your `catch` step rather than failing,
and the error is written to `Variables._error`.

```go
sc := saga.InMemory()

// A step that fails.
sc.RegisterVerb("charge_card", "common",
	verbs.HandlerFunc(func(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
		return nil, fmt.Errorf("gateway declined")
	}))

// The catch handler — it can read the error context off Variables._error.
sc.RegisterVerb("notify_ops", "common",
	verbs.HandlerFunc(func(_ context.Context, run domain.SagaRun, _ domain.Step) (map[string]any, error) {
		errInfo, _ := run.Variables["_error"].(map[string]any)
		return map[string]any{"recovered": true, "failed_step": errInfo["step_id"]}, nil
	}))

sc.Register(domain.WorkflowDefinition{
	ID: "checkout", Version: 1, Start: "protect", Published: true,
	Steps: []domain.Step{
		// The frame: protect "charge", jump to "recover" on any error.
		{ID: "protect", Type: "try_catch",
			Inputs: map[string]any{"try": []string{"charge"}, "catch": "recover"},
			Next:   "charge"},
		{ID: "charge", Type: "charge_card", Next: "done"},
		{ID: "recover", Type: "notify_ops", Next: "done"},
		{ID: "done", Type: domain.StepTypeEnd},
	},
})

runID, _ := sc.Start(context.Background(), "checkout", nil)
run, _ := sc.Get(context.Background(), runID)
fmt.Println(run.State)                  // succeeded — the error was caught
fmt.Println(run.Variables["recovered"]) // true
```

The `try_catch` step's `Next` points at the **first step inside** the protected
region; the protected step's `Next` points past it. See the
[`try_catch` verb](verbs#try_catch) for the nesting rules, and [Testing](testing)
for asserting both the caught and uncaught (`failed`) paths.

### Step-level retry and compensation

`try_catch` is one of three recovery mechanisms, and they compose:

- **`Step.Retry`** — set a `RetryPolicy` on a step and the engine re-runs it on
  error, up to `MaxAttempts`, with exponential backoff (`InitialBackoffMS`,
  `MaxBackoffMS`, `Multiplier`, optional `Jitter`). Retries are exhausted before
  a `try_catch` frame catches the error or the run fails.
- **`Step.Compensation`** — give a completed step a `Compensation` action and,
  when the run fails with no catching `try_catch` frame, the engine rolls back:
  it transitions the run to `RunStateCompensating`, then dispatches each
  already-completed compensable step's compensation action in **reverse order**
  before the run settles to `RunStateFailed`. A completed step with no
  `Compensation` is skipped.
