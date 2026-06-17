package e2e

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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// noopActionDispatchPub captures dispatched action payloads.
type noopActionDispatchPub struct {
	routingKeys []string
}

func (p *noopActionDispatchPub) PublishActionDispatch(_ context.Context, routingKey string, _ []byte) error {
	p.routingKeys = append(p.routingKeys, routingKey)
	return nil
}

// wf_action_dispatch workflow:
//
//	start â†’ action(example.set_state) â†’ end
func buildActionDispatchDef() domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID:        "wf_action_dispatch",
		Version:   1,
		Name:      "Action Dispatch Test",
		Start:     "dispatch",
		Published: true,
		Steps: []domain.Step{
			{
				ID:     "dispatch",
				Type:   domain.StepTypeAction,
				Action: "example.set_state",
				Inputs: map[string]any{"target_state": "resolved"},
				Next:   "done",
			},
			{
				ID:   "done",
				Type: domain.StepTypeEnd,
			},
		},
	}
}

// TestActionDispatch_PauseThenComplete verifies the full action dispatch cycle:
//  1. Advance reaches the action step â†’ saga pauses, dispatch published.
//  2. Simulated worker calls CompleteAction (sets wakeup_at=now).
//  3. Timer loop or direct Advance call resumes saga â†’ reaches end â†’ succeeded.
func TestActionDispatch_PauseThenComplete(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def := buildActionDispatchDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	actionPub := &noopActionDispatchPub{}
	pub := &actionPub2{} // saga.advance publisher
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, actionPub, nil)
	pub.coord = coord

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// First advance: should pause at action step.
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused {
		t.Fatalf("expected paused after action step, got %s", got.State)
	}
	if got.AwaitedActionDispatch == nil || *got.AwaitedActionDispatch != "example.set_state" {
		t.Fatalf("awaited_action_dispatch = %v, want example.set_state", got.AwaitedActionDispatch)
	}
	if got.CurrentAttempt != 1 {
		t.Fatalf("current_attempt = %d, want 1", got.CurrentAttempt)
	}

	// Verify action dispatch was published to RabbitMQ.
	if len(actionPub.routingKeys) != 1 || actionPub.routingKeys[0] != "example.set_state" {
		t.Fatalf("expected dispatch to example.set_state, got %v", actionPub.routingKeys)
	}

	// Simulated worker: calls CompleteAction with result.
	result := map[string]any{"ticket_number": "INC-999"}
	if err := s.CompleteAction(ctx, run.ID, 1, result); err != nil {
		t.Fatalf("CompleteAction: %v", err)
	}

	// Direct advance to simulate timer wakeup (wakeup_at is now in the past).
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance after complete: %v", err)
	}

	// Wait for async advances to settle.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ = s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ = s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("saga state = %s, want succeeded", got.State)
	}
	if got.Variables["ticket_number"] != "INC-999" {
		t.Errorf("ticket_number = %v, want INC-999", got.Variables["ticket_number"])
	}
}

// TestActionDispatch_FailAction_TransitionsFailed verifies that FailAction
// transitions the saga to failed state and does not resume it.
func TestActionDispatch_FailAction_TransitionsFailed(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def := buildActionDispatchDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	actionPub := &noopActionDispatchPub{}
	pub2 := &actionPub2{}
	coord := engine.NewCoordinator(s, pub2, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, actionPub, nil)
	pub2.coord = coord

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused {
		t.Fatalf("expected paused, got %s", got.State)
	}

	// Simulated worker reports failure.
	if err := s.FailAction(ctx, run.ID, 1, "ERR_WORKER_CRASH", "worker panicked", false); err != nil {
		t.Fatalf("FailAction: %v", err)
	}
	got, _ = s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", got.State)
	}
	events, _ := s.ListEventsByRun(ctx, run.ID)
	foundFailed := false
	for _, e := range events {
		if e.EventType == domain.EventStepFailed {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Errorf("expected step.failed audit event")
	}
}

// TestActionDispatch_LateComplete_NoOp verifies that a late CompleteAction
// (stale attempt) does not wake the saga.
func TestActionDispatch_LateComplete_NoOp(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	_ = s.MarkAwaitingAction(ctx, run.ID, "svc.act", 2)

	// Stale attempt 1 should be a no-op.
	if err := s.CompleteAction(ctx, run.ID, 1, map[string]any{"x": "y"}); err != nil {
		t.Fatalf("late complete: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.WakeupAt != nil {
		t.Errorf("late CompleteAction should not set wakeup_at")
	}
	if got.Variables["x"] != nil {
		t.Errorf("late CompleteAction should not merge result variables")
	}
}

// actionPub2 is a saga.advance publisher for the action e2e tests.
type actionPub2 struct {
	coord *engine.Coordinator
}

func (p *actionPub2) PublishSagaAdvance(ctx context.Context, runID string) error {
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}
