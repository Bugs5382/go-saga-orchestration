package e2e

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
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// TestCancelVerb_SelfCancel_E2E verifies that a workflow containing a cancel
// step (with no run_id) terminates with state=cancelled rather than succeeded,
// and that steps after the cancel step do not execute.
func TestCancelVerb_SelfCancel_E2E(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Workflow: init(set_var) → stop(cancel) → end
	// The cancel step should halt the run as cancelled; end must not be reached.
	def := domain.WorkflowDefinition{
		ID:        "wf_cancel_e2e",
		Version:   1,
		Name:      "cancel self e2e",
		Start:     "init",
		Published: true,
		Steps: []domain.Step{
			{
				ID:     "init",
				Type:   domain.StepTypeSetVar,
				Next:   "stop",
				Inputs: map[string]any{"out_var": "was_here", "value": "yes"},
			},
			{
				ID:     "stop",
				Type:   domain.StepTypeCancel,
				Next:   "end",
				Inputs: map[string]any{},
			},
			{
				ID:   "end",
				Type: domain.StepTypeEnd,
			},
		},
	}
	defID, err := s.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		t.Fatalf("upsert def: %v", err)
	}

	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	// Advance should return nil (not an error) — self-cancel is a clean stop.
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Errorf("Advance returned error: %v (expected nil for self-cancel)", err)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateCancelled {
		t.Errorf("run state = %s, want cancelled", got.State)
	}
	if got.State == domain.RunStateSucceeded {
		t.Errorf("run must not have succeeded — cancel should have stopped it")
	}
}

// TestCancelVerb_TargetCancel_E2E verifies that a workflow can cancel another
// run and then continue to completion itself.
func TestCancelVerb_TargetCancel_E2E(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Target workflow: trivial, just sits at pending.
	targetDef := domain.WorkflowDefinition{
		ID:        "wf_cancel_target",
		Version:   1,
		Name:      "target to be cancelled",
		Start:     "end",
		Published: true,
		Steps: []domain.Step{
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	targetDefID, _ := s.UpsertWorkflowDefinition(ctx, targetDef)
	target := domain.NewSagaRun(targetDef.ID, targetDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, target)

	// Canceller workflow: cancel(target.ID) → end
	cancellerDef := domain.WorkflowDefinition{
		ID:        "wf_cancel_other_e2e",
		Version:   1,
		Name:      "cancel other e2e",
		Start:     "stop",
		Published: true,
		Steps: []domain.Step{
			{
				ID:   "stop",
				Type: domain.StepTypeCancel,
				Next: "end",
				Inputs: map[string]any{
					"run_id": target.ID.String(),
					"reason": "e2e-test",
				},
			},
			{
				ID:   "end",
				Type: domain.StepTypeEnd,
			},
		},
	}
	cancellerDefID, _ := s.UpsertWorkflowDefinition(ctx, cancellerDef)
	canceller := domain.NewSagaRun(cancellerDef.ID, cancellerDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, canceller)

	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)

	if err := coord.Advance(ctx, canceller.ID.String()); err != nil {
		t.Fatalf("Advance canceller: %v", err)
	}

	// Canceller should have succeeded.
	gotCanceller, _ := s.GetRun(ctx, canceller.ID)
	if gotCanceller.State != domain.RunStateSucceeded {
		t.Errorf("canceller state = %s, want succeeded", gotCanceller.State)
	}

	// Target should have been cancelled.
	gotTarget, _ := s.GetRun(ctx, target.ID)
	if gotTarget.State != domain.RunStateCancelled {
		t.Errorf("target state = %s, want cancelled", gotTarget.State)
	}

}
