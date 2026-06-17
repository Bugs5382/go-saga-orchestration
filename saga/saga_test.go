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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
)

// TestSaga_StartAt_NamedEntrypoint verifies that StartAt begins at the named
// entrypoint, skipping the default Start step entirely.
func TestSaga_StartAt_NamedEntrypoint(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()

	if err := sc.Register(domain.WorkflowDefinition{
		ID: "wf_entry", Version: 1, Name: "EntrypointTest", Start: "s1", Published: true,
		Entrypoints: map[string]string{"alt": "s2"},
		Steps: []domain.Step{
			// s1: default path — sets entered=false, then ends
			{ID: "s1", Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "entered", "value": false}, Next: "done"},
			// s2: alt path — sets entered=true, then ends
			{ID: "s2", Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "entered", "value": true}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	runID, err := sc.StartAt(ctx, "wf_entry", "alt", map[string]any{})
	if err != nil {
		t.Fatalf("StartAt: %v", err)
	}
	run, err := sc.Get(ctx, runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state = %q, want succeeded; vars=%v", run.State, run.Variables)
	}
	if run.Variables["entered"] != true {
		t.Errorf("entered = %v, want true (proves run began at s2, not s1)", run.Variables["entered"])
	}
	// Regression guard: saga.started must fire exactly once even though the run's
	// CurrentStep was preset to the entry step (the emission keys on run state).
	events, err := sc.Store().ListEventsByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	started := 0
	for _, e := range events {
		if e.EventType == domain.EventSagaStarted {
			started++
		}
	}
	if started != 1 {
		t.Errorf("saga.started emitted %d times, want exactly 1", started)
	}
}

// Demonstrates the embedding API: in-memory engine + a custom verb (closure) +
// a built-in verb, started and run to completion in-process.
func TestSaga_InMemory_CustomVerb_RunsToSuccess(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()

	sc.RegisterVerb("double", "common",
		verbs.HandlerFunc(func(_ context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
			n, _ := step.Inputs["n"].(int)
			return map[string]any{"doubled": n * 2}, nil
		}))

	if err := sc.Register(domain.WorkflowDefinition{
		ID: "demo", Version: 1, Name: "Demo", Start: "dbl", Published: true,
		Steps: []domain.Step{
			{ID: "dbl", Type: domain.StepType("double"), Inputs: map[string]any{"n": 21}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	runID, err := sc.Start(ctx, "demo", map[string]any{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	run, err := sc.Get(ctx, runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state = %q, want succeeded; vars=%v", run.State, run.Variables)
	}
	if run.Variables["doubled"] != 42 {
		t.Errorf("doubled = %v, want 42", run.Variables["doubled"])
	}
}
