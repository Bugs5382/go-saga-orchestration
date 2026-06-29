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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/saga"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// fuPub is a loopback publisher: it advances the run in a background goroutine.
type fuPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *fuPub) PublishSagaAdvance(_ context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func setVarStep(id, outVar, val, next string) domain.Step {
	return domain.Step{ID: id, Type: domain.StepTypeSetVar,
		Inputs: map[string]any{"out_var": outVar, "value": val}, Next: next}
}

// Follow-up #3: wait_for_event honours timeout_s and routes to the "timeout"
// branch when the deadline elapses with no matching event.
func TestWaitForEvent_TimeoutBranch(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())

	def := domain.WorkflowDefinition{
		ID: "evt_timeout", Version: 1, Start: "wait", Published: true,
		Steps: []domain.Step{
			{ID: "wait", Type: domain.StepTypeWaitForEvent,
				Inputs:   map[string]any{"topic": "x.evt", "timeout_s": 5.0},
				Next:     "normal",
				Branches: map[string]domain.Branch{"timeout": {Next: "escalate"}}},
			setVarStep("normal", "outcome", "normal", "end"),
			setVarStep("escalate", "outcome", "escalate", "end"),
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &fuPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	timer := &engine.Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}
	timerCtx, cancelTimer := context.WithCancel(ctx)
	defer cancelTimer()
	go func() { _ = timer.Run(timerCtx) }()

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused || got.AwaitedEventTopic == nil {
		t.Fatalf("expected paused awaiting event, got state=%s topic=%v", got.State, got.AwaitedEventTopic)
	}
	if got.WakeupAt == nil {
		t.Fatalf("expected wakeup_at (timeout deadline) to be set")
	}

	time.Sleep(20 * time.Millisecond)
	fc.Advance(10 * time.Second) // past the 5s deadline; no event ever arrives

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ = s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			if got.Variables["outcome"] != "escalate" {
				t.Errorf("outcome = %v, want escalate (timeout branch)", got.Variables["outcome"])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("saga did not reach succeeded; state=%s", got.State)
}

// Follow-up #2: cancelling a TARGET child run re-evaluates its parent's join, so
// a parent paused on a join over that child is woken.
func TestTargetCancel_WakesParentJoin(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	pub := &fuPub{}
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	parentDef := domain.WorkflowDefinition{
		ID: "par", Version: 1, Start: "join", Published: true,
		Steps: []domain.Step{
			{ID: "join", Type: domain.StepTypeParallel, Next: "done", Inputs: map[string]any{}},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}
	parentDefID, _ := s.UpsertWorkflowDefinition(ctx, parentDef)
	parent := domain.NewSagaRun(parentDef.ID, parentDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	childDef := domain.WorkflowDefinition{
		ID: "child", Version: 1, Start: "c1", Published: true,
		Steps: []domain.Step{{ID: "c1", Type: domain.StepTypeNoop, Next: "cend"}, {ID: "cend", Type: domain.StepTypeEnd}},
	}
	childID, err := s.SpawnChildRunAt(ctx, parent.ID, "join", "b0", childDef, map[string]any{}, "")
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	// Pause the parent on the join step (as the parallel verb would).
	_ = s.UpdateRunState(ctx, parent.ID, domain.RunStatePaused, "join")

	// Cancel the child via the cancel verb wired with the coordinator as the
	// join checker (exactly how NewCoordinator registers it).
	cv := verbs.CancelVerb{S: s, JoinChecker: coord}
	if _, err := cv.Execute(ctx, domain.SagaRun{ID: uuid.New()},
		domain.Step{ID: "cancel", Inputs: map[string]any{"run_id": childID.String()}}); err != nil {
		t.Fatalf("cancel target: %v", err)
	}

	// The parent's join is now satisfied (its only child is terminal); it should
	// be woken and run through to succeeded.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, _ := s.GetRun(ctx, parent.ID)
		if p.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	p, _ := s.GetRun(ctx, parent.ID)
	t.Fatalf("parent not woken after target-cancel; state=%s publishCalls=%d", p.State, pub.calls.Load())
}

// Follow-up #1: an in-process emit_event fires matching triggers, starting new runs.
func TestEmitEvent_FiresTrigger(t *testing.T) {
	ctx := context.Background()
	sc := saga.InMemory()

	// Target workflow the trigger starts.
	if err := sc.Register(domain.WorkflowDefinition{
		ID: "ticket_flow", Version: 1, Start: "t1", Published: true,
		Steps: []domain.Step{setVarStep("t1", "handled", "yes", "end"), {ID: "end", Type: domain.StepTypeEnd}},
	}); err != nil {
		t.Fatalf("register target: %v", err)
	}
	// Trigger: a ticket open->closed transition starts ticket_flow.
	if _, err := sc.Store().UpsertTrigger(ctx, domain.SagaTrigger{
		ID: uuid.New(), TriggerType: domain.TriggerRecordTransition, WorkflowID: "ticket_flow", Version: 1,
		Config:  map[string]any{"record_type": "ticket", "from_state": "open", "to_state": "closed"},
		Enabled: true,
	}); err != nil {
		t.Fatalf("upsert trigger: %v", err)
	}
	// Emitter workflow: emits the matching record.transitioned event.
	if err := sc.Register(domain.WorkflowDefinition{
		ID: "emitter_flow", Version: 1, Start: "e1", Published: true,
		Steps: []domain.Step{
			{ID: "e1", Type: domain.StepTypeEmitEvent, Inputs: map[string]any{
				"topic": "svc.record.transitioned.ticket",
				"payload": map[string]any{
					"record_type": "ticket", "from_state": "open", "to_state": "closed",
					"record_id": "r1", "actor": "u1",
				},
			}, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register emitter: %v", err)
	}

	if _, err := sc.Start(ctx, "emitter_flow", map[string]any{}); err != nil {
		t.Fatalf("start emitter: %v", err)
	}

	// A ticket_flow run should have been started by the trigger and run to success.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runs, _ := sc.Store().ListRuns(ctx, store.RunFilter{WorkflowID: "ticket_flow"})
		for _, r := range runs {
			if r.State == domain.RunStateSucceeded && r.Variables["handled"] == "yes" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	runs, _ := sc.Store().ListRuns(ctx, store.RunFilter{WorkflowID: "ticket_flow"})
	t.Fatalf("emit_event did not start+complete a ticket_flow run via the trigger; found %d runs", len(runs))
}
