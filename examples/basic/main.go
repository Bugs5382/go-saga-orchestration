// Command basic is a minimal, runnable example of embedding the saga
// orchestration engine as a library — no Postgres or RabbitMQ required.
//
// Run it with:
//
//	go run ./examples/basic
//
// It registers a custom verb, defines a small workflow that uses the custom
// verb, the built-in `switch` and `set_var` verbs, starts two runs, and prints
// where each one ended up.
package main

/*
MIT License

Copyright (c) 2026 Bugs5382

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"context"
	"fmt"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
)

func main() {
	ctx := context.Background()

	// 1. Spin up an embedded engine: in-memory store + in-process advance.
	sc := saga.InMemory()

	// 2. Register your own verb as a plain closure. This one classifies an
	//    order into a tier based on the run's "amount" input.
	sc.RegisterVerb("classify", "common",
		verbs.HandlerFunc(func(_ context.Context, run domain.SagaRun, _ domain.Step) (map[string]any, error) {
			amount, _ := run.Inputs["amount"].(int)
			tier := "standard"
			if amount >= 1000 {
				tier = "priority"
			}
			return map[string]any{"tier": tier}, nil
		}))

	// 3. Define + publish a workflow:
	//    classify (custom) -> switch on tier -> set a message -> end.
	if err := sc.Register(domain.WorkflowDefinition{
		ID: "order", Version: 1, Name: "Order routing", Start: "classify", Published: true,
		Steps: []domain.Step{
			{ID: "classify", Type: domain.StepType("classify"), Next: "route"},
			{ID: "route", Type: domain.StepTypeSwitch, Inputs: map[string]any{"expr": "tier"},
				Branches: map[string]domain.Branch{
					"priority": {Next: "vip"},
					"standard": {Next: "normal"},
				}},
			{ID: "vip", Type: domain.StepTypeSetVar,
				Inputs: map[string]any{"out_var": "message", "value": "VIP handling"}, Next: "done"},
			{ID: "normal", Type: domain.StepTypeSetVar,
				Inputs: map[string]any{"out_var": "message", "value": "standard handling"}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		panic(err)
	}

	// 4. Start runs with different inputs and print the outcome.
	for _, amount := range []int{50, 5000} {
		runID, err := sc.Start(ctx, "order", map[string]any{"amount": amount})
		if err != nil {
			panic(err)
		}
		run, err := sc.Get(ctx, runID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("amount=%-5d -> state=%s tier=%v message=%q\n",
			amount, run.State, run.Variables["tier"], run.Variables["message"])
	}
}
