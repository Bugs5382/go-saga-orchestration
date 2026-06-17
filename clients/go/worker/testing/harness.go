// Package workertest provides an in-process harness so per-service
// worker tests can verify their handlers in isolation without
// RabbitMQ + gRPC + the real go-saga-orchestration stack.
//
// Usage:
//
//	h := workertest.New(t, []worker.Action{...})
//	out, err := h.Dispatch(ctx, worker.ActionPayload{Action: "example.set_state", ...})
//	// assert out + err per handler contract
package workertest

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
	"strings"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
)

// Harness runs handlers directly without RabbitMQ/gRPC, using only the
// handler-resolution logic that processDelivery uses in production.
type Harness struct {
	t        *testing.T
	handlers map[string]worker.Handler
}

// New builds a harness from a slice of Action declarations.
func New(t *testing.T, actions []worker.Action) *Harness {
	t.Helper()
	handlers := map[string]worker.Handler{}
	for _, a := range actions {
		if a.Handler == nil {
			t.Fatalf("workertest: action %q has nil Handler", a.Name)
		}
		handlers[a.Name] = a.Handler
	}
	return &Harness{t: t, handlers: handlers}
}

// Dispatch routes payload.Action (format "service.name") to the matching
// registered handler. The service prefix is stripped before lookup —
// same logic as processDelivery in production.
func (h *Harness) Dispatch(ctx context.Context, payload worker.ActionPayload) (worker.Result, error) {
	name := payload.Action
	if dot := strings.Index(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	handler, ok := h.handlers[name]
	if !ok {
		return nil, fmt.Errorf("workertest: no handler registered for %q", payload.Action)
	}
	return handler.Execute(ctx, payload)
}
