package worker_test

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

	"github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
)

// Example shows the basic shape of using the worker SDK: define an action
// handler, declare it as part of a BootstrapConfig, and run it. The handler
// is invoked here directly instead of through Bootstrap so the example is
// self-contained (Bootstrap would dial RabbitMQ and go-saga-orchestration's gRPC
// engine). In production you would call worker.Bootstrap(ctx, cfg).
func Example() {
	// 1. Define an action handler. It receives the dispatch payload and
	// returns a Result that the engine merges into the saga's variables.
	setState := worker.HandlerFunc(func(ctx context.Context, p worker.ActionPayload) (worker.Result, error) {
		// p.Inputs carries the action arguments from the saga step.
		to, _ := p.Inputs["to"].(string)
		return worker.Result{"new_state": to}, nil
	})

	// 2. Register it. In a real service this config is passed to
	// worker.Bootstrap, which registers the actions with go-saga-orchestration,
	// declares the RabbitMQ queue, and runs the consumer loop.
	cfg := worker.BootstrapConfig{
		Service:        "example",
		ServiceVersion: "1.0.0",
		RegistryURL:    "http://go-saga-orchestration-api:8080",
		RmqURL:         "amqp://guest:guest@rabbitmq:5672/",
		GrpcURL:        "go-saga-orchestration-engine:9090",
		Actions: []worker.Action{
			{
				Name:        "set_state",
				Description: "Transition a record to a new state",
				Category:    "record_lifecycle",
				Compensable: true,
				Handler:     setState,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		fmt.Println("invalid config:", err)
		return
	}

	// 3. Show the handler's result shape for a sample dispatch.
	result, _ := cfg.Actions[0].Handler.Execute(context.Background(), worker.ActionPayload{
		RunID:  "run-1",
		StepID: "step-1",
		Action: "example.set_state",
		Inputs: map[string]any{"to": "approved"},
	})
	fmt.Println(result["new_state"])
	// Output: approved
}

// ExampleErrorf shows returning a CodedError so the engine records a stable
// error code and knows whether to retry the step.
func ExampleErrorf() {
	charge := worker.HandlerFunc(func(ctx context.Context, p worker.ActionPayload) (worker.Result, error) {
		// A business rule failure that should not be retried.
		return nil, worker.Errorf("insufficient_funds", false, "account %s lacks funds", "acct-7")
	})

	_, err := charge.Execute(context.Background(), worker.ActionPayload{Action: "example.charge"})
	var coded worker.CodedError
	if e, ok := err.(worker.CodedError); ok {
		coded = e
	}
	fmt.Printf("code=%s retryable=%t msg=%q\n", coded.Code(), coded.Retryable(), coded.Error())
	// Output: code=insufficient_funds retryable=false msg="account acct-7 lacks funds"
}

// ExampleWrap shows idempotency dedupe: wrapping a handler so a repeated
// dispatch carrying the same IdempotencyKey returns the cached result
// instead of running the handler's side effect twice.
func ExampleWrap() {
	calls := 0
	inner := worker.HandlerFunc(func(ctx context.Context, p worker.ActionPayload) (worker.Result, error) {
		calls++
		return worker.Result{"charged": true}, nil
	})

	store := worker.NewMemoryIdempotencyStore()
	deduped := worker.Wrap(store, inner)

	payload := worker.ActionPayload{Action: "example.charge", IdempotencyKey: "charge-42"}
	ctx := context.Background()
	_, _ = deduped.Execute(ctx, payload)
	_, _ = deduped.Execute(ctx, payload) // redelivery with same key

	fmt.Printf("handler ran %d time(s)\n", calls)
	// Output: handler ran 1 time(s)
}
