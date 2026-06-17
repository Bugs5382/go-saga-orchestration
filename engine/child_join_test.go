package engine

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

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// TestChildJoinAllSiblingsTerminalWakesParent verifies that when ALL siblings
// of a child run become terminal, WakeFromExternal is called on the parent
// (and, without a real publisher, no panic occurs from nil publisher).
//
// Setup:
//   - A parent run in paused state (simulating it is waiting for child joins).
//   - Two child runs spawned against the same parentStepID.
//   - The first child completes: no wake (second sibling still running).
//   - The second child completes: wake fires — parent transitions to woken state.
func TestChildJoinAllSiblingsTerminalWakesParent(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Parent workflow: one step that we'll manually pause to simulate waiting.
	parentDef := domain.WorkflowDefinition{
		ID: "wf_parent", Version: 1, Name: "Parent",
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	parentDefID, _ := s.UpsertWorkflowDefinition(ctx, parentDef)

	parentRun := domain.NewSagaRun("wf_parent", parentDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, parentRun)
	// Manually pause the parent — simulates it waiting for children to finish.
	// Set CurrentStep to the spawning step ID first so checkParentJoin's guard
	// (parent.CurrentStep == ParentStepID) matches when all children terminate.
	_ = s.UpdateRunState(ctx, parentRun.ID, domain.RunStateRunning, "parallel_step")
	_ = s.SetPausedAwaitingSignal(ctx, parentRun.ID, "__child_join__", nil)

	// Spawn two children against the same parent step ("parallel_step").
	childDef := domain.WorkflowDefinition{
		ID: "wf_child", Version: 1, Name: "Child",
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	child1ID, _ := s.SpawnChildRun(ctx, parentRun.ID, "parallel_step", "branch_a", childDef, map[string]any{})
	child2ID, _ := s.SpawnChildRun(ctx, parentRun.ID, "parallel_step", "branch_b", childDef, map[string]any{})

	// Advance child 1 to succeeded.
	c := NewCoordinator(s, nil /*no publisher*/, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	if err := c.Advance(ctx, child1ID.String()); err != nil {
		t.Fatalf("advance child1: %v", err)
	}

	// After child 1 finishes, child 2 is still running — parent should NOT be woken yet.
	// We detect "not woken" by checking the parent is still in paused state with the
	// awaited_signal still set (WakeFromExternal clears it).
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken after only child1 finished — expected to wait for child2")
	}

	// Advance child 2 to succeeded. Now all siblings are terminal.
	if err := c.Advance(ctx, child2ID.String()); err != nil {
		t.Fatalf("advance child2: %v", err)
	}

	// Parent should now be woken: WakeFromExternal clears AwaitedSignal.
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent not woken after all children finished")
	}
	// State should still be paused (WakeFromExternal doesn't change state;
	// the subsequent Advance call on the parent — which the publisher would
	// trigger — is what clears it). Without publisher the parent stays paused.
	if parent.State != domain.RunStatePaused {
		t.Errorf("parent state = %s, want paused (publisher nil so advance not re-queued)", parent.State)
	}
}

// TestChildJoinNoParentIsNoop verifies that a top-level run (no ParentRunID)
// completing successfully does NOT cause any error — checkParentJoin is a no-op.
func TestChildJoinNoParentIsNoop(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_solo", Version: 1, Name: "Solo",
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	run := domain.NewSagaRun("wf_solo", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	if err := c.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("state = %s, want succeeded", got.State)
	}
}

// TestChildJoinOnlyOneChildWakesImmediately verifies the degenerate case:
// a single child (sub_saga pattern) — as soon as it finishes, parent is woken.
func TestChildJoinOnlyOneChildWakesImmediately(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentDef := domain.WorkflowDefinition{
		ID: "wf_parent_single", Version: 1, Name: "ParentSingle",
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	parentDefID, _ := s.UpsertWorkflowDefinition(ctx, parentDef)

	parentRun := domain.NewSagaRun("wf_parent_single", parentDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, parentRun)
	// Set CurrentStep to "sub_step" so checkParentJoin's step-match guard passes.
	_ = s.UpdateRunState(ctx, parentRun.ID, domain.RunStateRunning, "sub_step")
	_ = s.SetPausedAwaitingSignal(ctx, parentRun.ID, "__child_join__", nil)

	childDef := domain.WorkflowDefinition{
		ID: "wf_child_single", Version: 1, Name: "ChildSingle",
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	childID, _ := s.SpawnChildRun(ctx, parentRun.ID, "sub_step", "only", childDef, map[string]any{})

	c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	if err := c.Advance(ctx, childID.String()); err != nil {
		t.Fatalf("advance child: %v", err)
	}

	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent not woken after single child finished")
	}
}
