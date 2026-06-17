// Package worker is the shared library every saga-action worker imports
// to run as a worker. It wraps the gRPC ExecuteStep stream + RabbitMQ
// consumer + idempotency dedupe so each service only writes its action
// handler functions.
//
// Boot one in cmd/worker/main.go:
//
//	deps := worker.BootstrapConfig{
//	  Service:        "example",
//	  ServiceVersion: "1.0.0",
//	  RegistryURL:    cfg.WorkflowRegistryURL,
//	  RmqURL:         cfg.RmqURL,
//	  GrpcURL:        cfg.WorkflowGrpcURL,
//	  Actions: []worker.Action{
//	    {Name: "set_state", Handler: handlers.SetState, Compensable: true, Category: "record_lifecycle"},
//	    ...
//	  },
//	}
//	if err := worker.Bootstrap(ctx, deps); err != nil { log.Fatal(err) }
//
// Bootstrap registers the actions with go-saga-orchestration, declares the
// service's RabbitMQ queue, and runs the consumer until ctx is
// cancelled.
package worker

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

import "context"

// ActionPayload is the body of a saga.advance → action dispatch message
// that engine publishes and the worker consumes. Workers deserialize
// this to drive their handler.
//
// Mirrored from engine/verbs/action.ActionPayload — these
// MUST stay in sync. The duplication is intentional: the internal
// type is private to go-saga-orchestration; the public type is what worker
// SDKs import.
type ActionPayload struct {
	RunID          string         `json:"run_id"`
	StepID         string         `json:"step_id"`
	Attempt        int            `json:"attempt"`
	IdempotencyKey string         `json:"idempotency_key"`
	Action         string         `json:"action"` // "<service>.<action_name>"
	Inputs         map[string]any `json:"inputs"`
	DryRun         bool           `json:"dry_run,omitempty"`
}

// Result is what a successful Handler.Execute returns; merged into
// saga.Variables by the engine on Complete.
type Result map[string]any

// WouldChange is what a DryRunable.Preview returns when dispatch
// included dry_run=true. Surfaced in the run inspector to show "what
// would change" without applying the side effect.
//
// Per-action shape; the registry's output_schema documents it. Examples:
//
//	example.set_state:  {record_id, field, from, to}
//	example.create_asset: {would_create: {...}}
type WouldChange map[string]any

// Handler runs one action against the worker's host service. Returns
// the result map (becomes the saga's Variables update) or an error.
// Errors should be domain errors with a stable code — wrap them via
// worker.Errorf if needed to provide a code + retryable flag.
type Handler interface {
	Execute(ctx context.Context, payload ActionPayload) (Result, error)
}

// DryRunable handlers also support preview. The engine routes dispatch
// to Preview instead of Execute when payload.DryRun is true. Handlers
// that mutate state should implement this; pure-read handlers don't
// need to.
type DryRunable interface {
	Handler
	Preview(ctx context.Context, payload ActionPayload) (WouldChange, error)
}

// HandlerFunc adapts a plain func to the Handler interface for
// services that prefer function-style registration over types.
type HandlerFunc func(ctx context.Context, payload ActionPayload) (Result, error)

// Execute calls f, satisfying the Handler interface.
func (f HandlerFunc) Execute(ctx context.Context, payload ActionPayload) (Result, error) {
	return f(ctx, payload)
}
