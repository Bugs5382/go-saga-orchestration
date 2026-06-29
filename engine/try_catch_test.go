package engine

/*
MIT License

Copyright (c) 2026 Shane

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
	"errors"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// errVerb is a test verb that always returns an error.
type errVerb struct{ msg string }

func (e errVerb) Execute(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
	return nil, errors.New(e.msg)
}

// TestTryCatchCatchesVerb verifies that when a verb errors and a TryCatchFrame
// is on the stack, the coordinator:
//   - does NOT transition the run to failed
//   - redirects CurrentStep to the catch step
//   - stores _error in Variables
//   - appends a step.failed event (actor "engine-caught")
func TestTryCatchCatchesVerb(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Workflow: risky_step --[error]--> catch_step --next--> end
	def := domain.WorkflowDefinition{
		ID: "wf_try", Version: 1, Name: "TryCatch",
		Published: true,
		Start:     "risky_step",
		Steps: []domain.Step{
			{ID: "risky_step", Type: domain.StepTypeNoop, Next: "end"}, // overridden by fake verb
			{ID: "catch_step", Type: domain.StepTypeEnd},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	run := domain.NewSagaRun("wf_try", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	// Push a try_catch frame pointing at catch_step.
	_ = s.PushTryCatch(ctx, run.ID, domain.TryCatchFrame{
		StepID:    "try_catch_1",
		CatchStep: "catch_step",
	})

	c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	// Replace the noop verb with one that always errors.
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: errVerb{msg: "simulated failure"}, LicenseGroup: "common"}

	// Advance: risky_step runs, errors, catch fires, jumps to catch_step (end), saga succeeds.
	if err := c.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("run state = %s, want succeeded", got.State)
	}

	// _error must be in Variables.
	errVal, ok := got.Variables["_error"]
	if !ok {
		t.Fatal("Variables[\"_error\"] not set after catch")
	}
	errMap, ok := errVal.(map[string]any)
	if !ok {
		t.Fatalf("Variables[\"_error\"] has wrong type %T", errVal)
	}
	if msg, _ := errMap["message"].(string); msg != "simulated failure" {
		t.Errorf("_error.message = %q, want %q", msg, "simulated failure")
	}
	if sid, _ := errMap["step_id"].(string); sid != "risky_step" {
		t.Errorf("_error.step_id = %q, want %q", sid, "risky_step")
	}

	// Events: must include a step.failed event with actor "engine-caught".
	events, _ := s.ListEventsByRun(ctx, run.ID)
	found := false
	for _, ev := range events {
		if ev.EventType == domain.EventStepFailed && ev.Actor == "engine-caught" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no step.failed/engine-caught event found; events: %+v", events)
	}
}

// TestTryCatchEmptyStackFails verifies that when there is no TryCatchFrame
// and a verb errors, the run transitions to failed as normal.
func TestTryCatchEmptyStackFails(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_no_try", Version: 1, Name: "NoTry",
		Published: true,
		Start:     "risky_step",
		Steps: []domain.Step{
			{ID: "risky_step", Type: domain.StepTypeNoop, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	run := domain.NewSagaRun("wf_no_try", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: errVerb{msg: "boom"}, LicenseGroup: "common"}

	err := c.Advance(ctx, run.ID.String())
	if err == nil {
		t.Fatal("expected Advance to return an error, got nil")
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("run state = %s, want failed", got.State)
	}

	// Ensure normal step.failed event (actor "engine", not "engine-caught").
	events, _ := s.ListEventsByRun(ctx, run.ID)
	found := false
	for _, ev := range events {
		if ev.EventType == domain.EventStepFailed && ev.Actor == "engine" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no step.failed/engine event found; events: %+v", events)
	}

	// _error must NOT be in Variables (no try_catch frame was present).
	if _, ok := got.Variables["_error"]; ok {
		t.Error("Variables[\"_error\"] set but no try_catch frame was present")
	}
}

// noopVerb is a local alias so tests in this file can use a succeeding verb
// without importing verbs.NoopVerb (which would be a cycle-safe import but is
// still cleaner kept local).
type succeedVerb struct{}

func (succeedVerb) Execute(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
	return map[string]any{}, nil
}

// Ensure errVerb and succeedVerb satisfy the Handler interface at compile time.
var _ verbs.Handler = errVerb{}
var _ verbs.Handler = succeedVerb{}
