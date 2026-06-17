package saga_test

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
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
)

// parallelDef builds a workflow whose single parallel step fans out the given
// short-form branch specs and joins with strategy "all".
func parallelDef(id string, branches []any) domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID: id, Version: 1, Name: id, Start: "fan", Published: true,
		Steps: []domain.Step{
			{ID: "fan", Type: domain.StepTypeParallel, Next: "end", Inputs: map[string]any{
				"join_strategy": "all",
				"branches":      branches,
			}},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
}

func TestSaga_Shutdown_Idle(t *testing.T) {
	sc := saga.InMemory()
	if err := sc.Shutdown(context.Background()); err != nil {
		t.Fatalf("idle Shutdown = %v, want nil", err)
	}
}

func TestSaga_Shutdown_DrainsBackgroundAdvances(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()
	def := parallelDef("par", []any{
		map[string]any{"type": "set_var", "inputs": map[string]any{"out_var": "a", "value": 1}},
		map[string]any{"type": "set_var", "inputs": map[string]any{"out_var": "b", "value": 2}},
	})
	if err := sc.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := sc.Start(ctx, "par", map[string]any{}); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Background child advances are tracked on the WaitGroup; Shutdown drains them.
	shutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := sc.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown = %v, want nil (should drain)", err)
	}
}

func TestSaga_Shutdown_RespectsDeadline(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	// A verb that ignores its context and blocks until released — simulates work
	// that does not finish, so the drain must hit the Shutdown deadline.
	sc.RegisterVerb("block", "common",
		verbs.HandlerFunc(func(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return map[string]any{}, nil
		}))

	def := parallelDef("blockwf", []any{
		map[string]any{"type": "block", "inputs": map[string]any{}},
	})
	if err := sc.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := sc.Start(ctx, "blockwf", map[string]any{}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait until the blocking verb is actually in flight.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking verb never started")
	}

	shutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := sc.Shutdown(shutCtx)
	close(release) // let the goroutine finish so it doesn't leak
	if err == nil {
		t.Fatal("Shutdown = nil, want a deadline error while a branch is blocked")
	}
}
