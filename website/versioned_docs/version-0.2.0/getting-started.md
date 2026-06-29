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

## 3. Error handling and compensation

_(Worked examples covering retries, `try/catch`, and compensation steps land here — see
[Verbs](verbs) for the full step catalog and [Embedding](embedding) for production wiring.)_
