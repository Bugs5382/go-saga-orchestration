package memory

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

func TestRunCRUD(t *testing.T) {
	s := New()
	ctx := context.Background()

	defID := uuid.New()
	run := domain.NewSagaRun("wf_trivial", defID, nil, nil)

	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.WorkflowID != "wf_trivial" || got.State != domain.RunStatePending {
		t.Errorf("got %+v", got)
	}

	if err := s.UpdateRunState(ctx, run.ID, domain.RunStateRunning, "end"); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateRunning || got.CurrentStep != "end" {
		t.Errorf("after update: %+v", got)
	}

	if _, err := s.GetRun(ctx, uuid.New()); err == nil {
		t.Error("expected ErrNotFound for missing run")
	} else if _, ok := err.(store.ErrNotFound); !ok {
		t.Errorf("expected ErrNotFound, got %T: %v", err, err)
	}
}

func TestEventAppend(t *testing.T) {
	s := New()
	ctx := context.Background()
	runID := uuid.New()
	e := domain.NewEvent(runID, "end", 0, domain.EventSagaStarted, "engine")
	if err := s.AppendEvent(ctx, e); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err := s.ListEventsByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != 1 || events[0].EventType != domain.EventSagaStarted {
		t.Errorf("events: %+v", events)
	}
}

func TestMemoryStore_RuleDefinition_RoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()
	def := domain.NewRuleDefinition(
		"triage", 1, "Triage", domain.RuleTypeDecisionTable,
		domain.RuleSpec{
			HitPolicy: domain.HitPolicyFirst,
			Rows:      []domain.DecisionTableRow{{When: "true", Then: map[string]any{"branch": "high"}}},
		},
		"test",
	)
	if _, err := s.UpsertRuleDefinition(ctx, def); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := s.GetPublishedRuleByID(ctx, "triage", nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.RuleID != "triage" || len(got.Spec.Rows) != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestMemoryStore_UpdateRunVariables_DottedKey(t *testing.T) {
	s := New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.UpdateRunVariables(ctx, run.ID, map[string]any{"x": 42, "ctx.user_id": "u-1"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Variables["x"] != 42 {
		t.Errorf("x = %v, want 42", got.Variables["x"])
	}
	scope, _ := got.Variables["ctx"].(map[string]any)
	if scope["user_id"] != "u-1" {
		t.Errorf("ctx.user_id = %v, want u-1", scope["user_id"])
	}
}

func TestMemoryStore_PausedWakeup_RoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	wakeup := time.Now().UTC().Add(time.Second)
	if err := s.SetPausedWithWakeup(ctx, run.ID, wakeup); err != nil {
		t.Fatalf("set: %v", err)
	}
	ids, err := s.FindRunsByDueWakeup(ctx, wakeup.Add(time.Second), 100)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == run.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected run in due-wakeup list, got %v", ids)
	}
	if err := s.ClearPause(ctx, run.ID); err != nil {
		t.Fatalf("clear: %v", err)
	}
}

func TestMemoryStore_AwaitedSignal_ConsumeAndAdvance(t *testing.T) {
	s := New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := s.SetPausedAwaitingSignal(ctx, run.ID, "go", nil); err != nil {
		t.Fatal(err)
	}
	// wrong name → no match
	ok, _ := s.TryConsumeAwaitedSignal(ctx, run.ID, "other")
	if ok {
		t.Errorf("expected mismatch")
	}
	// right name → match
	ok, _ = s.TryConsumeAwaitedSignal(ctx, run.ID, "go")
	if !ok {
		t.Errorf("expected match")
	}
}

func TestMemoryStore_AwaitedEvent_FilterByTopic(t *testing.T) {
	s := New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := s.SetPausedAwaitingEvent(ctx, run.ID, "foo.bar", map[string]string{"x": "1"}); err != nil {
		t.Fatal(err)
	}
	runs, _ := s.FindRunsByAwaitedEvent(ctx, "foo.bar")
	if len(runs) != 1 {
		t.Errorf("expected 1 run, got %d", len(runs))
	}
	other, _ := s.FindRunsByAwaitedEvent(ctx, "no.match")
	if len(other) != 0 {
		t.Errorf("expected 0, got %d", len(other))
	}
}

func TestMemoryStore_SpawnChildRun_LinkedToParent(t *testing.T) {
	s := New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-parent", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)
	childDef := domain.WorkflowDefinition{ID: "wf-child", Version: 1, Name: "Child", Start: "end", Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}}, Published: true}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)
	childID, err := s.SpawnChildRun(ctx, parent.ID, "step_a", "b0", childDef, map[string]any{})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	children, err := s.ListChildrenByParent(ctx, parent.ID, "step_a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(children) != 1 || children[0].ID != childID {
		t.Errorf("expected one child with ID %s, got %v", childID, children)
	}
}

func TestMemoryStore_TryCatchStack_PushPop(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	if err := s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t1", CatchStep: "c1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t2", CatchStep: "c2"}); err != nil {
		t.Fatal(err)
	}
	f, ok, _ := s.PopTryCatch(ctx, r.ID)
	if !ok || f.StepID != "t2" {
		t.Errorf("expected pop t2, got %v ok=%v", f, ok)
	}
	f, ok, _ = s.PopTryCatch(ctx, r.ID)
	if !ok || f.StepID != "t1" {
		t.Errorf("expected pop t1, got %v ok=%v", f, ok)
	}
	_, ok, _ = s.PopTryCatch(ctx, r.ID)
	if ok {
		t.Errorf("expected empty stack")
	}
}

func TestMemoryStore_TryCatchStack_MaxDepth3(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t1", CatchStep: "c1"})
	_ = s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t2", CatchStep: "c2"})
	_ = s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t3", CatchStep: "c3"})
	if err := s.PushTryCatch(ctx, r.ID, domain.TryCatchFrame{StepID: "t4", CatchStep: "c4"}); err == nil {
		t.Errorf("expected error on 4th push (max depth 3)")
	}
}

func TestMemoryStore_ActionRegistration_RoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()
	reg := domain.ActionRegistration{
		Service:      "example",
		ActionName:   "set_state",
		Version:      1,
		Description:  "Transition record state",
		Category:     "record_lifecycle",
		Compensable:  true,
		LicenseGroup: "wf.worker_actions_basic",
		Transport:    domain.TransportHTTP,
		Address:      "https://worker.local/callback",
	}
	if err := s.UpsertActionRegistration(ctx, reg); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := s.GetAction(ctx, "example", "set_state", 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Description != "Transition record state" {
		t.Errorf("description = %q", got.Description)
	}
	// Dispatch descriptor must round-trip. (issue #59)
	if got.Transport != domain.TransportHTTP || got.Address != "https://worker.local/callback" {
		t.Errorf("dispatch descriptor = %q/%q, want http/https://worker.local/callback", got.Transport, got.Address)
	}
	actions, _ := s.ListActions(ctx, store.ActionFilter{Service: "example"})
	if len(actions) != 1 {
		t.Errorf("list len = %d, want 1", len(actions))
	}
	// Filter mismatch returns empty.
	actions, _ = s.ListActions(ctx, store.ActionFilter{Service: "no-such"})
	if len(actions) != 0 {
		t.Errorf("expected 0 actions for service filter no-such, got %d", len(actions))
	}
}

func TestMemoryStore_UserTask_CreateGetSubmit(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	task := domain.UserTask{ID: uuid.New(), RunID: r.ID, StepID: "m", Assignee: "user-1"}
	if err := s.CreateUserTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetUserTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Assignee != "user-1" {
		t.Errorf("assignee = %q, want user-1", got.Assignee)
	}
	if err := s.SubmitUserTask(ctx, task.ID, "actor-2", map[string]any{"vote": "approve"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetUserTask(ctx, task.ID)
	if got.SubmittedBy != "actor-2" || got.Result["vote"] != "approve" {
		t.Errorf("submission did not persist: %+v", got)
	}
}

func TestMemoryStore_MarkAwaitingAction_Idempotent(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	if err := s.MarkAwaitingAction(ctx, r.ID, "example.set_state", 1); err != nil {
		t.Fatalf("first mark: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.AwaitedActionDispatch == nil || *got.AwaitedActionDispatch != "example.set_state" {
		t.Errorf("awaited_action_dispatch = %v, want example.set_state", got.AwaitedActionDispatch)
	}
	if got.CurrentAttempt != 1 {
		t.Errorf("current_attempt = %d, want 1", got.CurrentAttempt)
	}

	// Idempotent: same args → no-op (would not bump to attempt 2).
	if err := s.MarkAwaitingAction(ctx, r.ID, "example.set_state", 1); err != nil {
		t.Fatalf("idempotent mark: %v", err)
	}
	got, _ = s.GetRun(ctx, r.ID)
	if got.CurrentAttempt != 1 {
		t.Errorf("current_attempt after idempotent call = %d, want 1", got.CurrentAttempt)
	}
}

func TestMemoryStore_CompleteAction_MergesVariables(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, nil)
	r.Variables = map[string]any{"existing": "value"} // set variables directly
	_ = s.CreateRun(ctx, r)
	_ = s.MarkAwaitingAction(ctx, r.ID, "svc.act", 1)

	result := map[string]any{"ticket_id": "INC-42"}
	if err := s.CompleteAction(ctx, r.ID, 1, result); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.AwaitedActionDispatch != nil {
		t.Errorf("awaited_action_dispatch should be nil after complete, got %v", *got.AwaitedActionDispatch)
	}
	if got.WakeupAt == nil {
		t.Errorf("wakeup_at should be set after complete")
	}
	if got.Variables["ticket_id"] != "INC-42" {
		t.Errorf("ticket_id = %v, want INC-42", got.Variables["ticket_id"])
	}
	if got.Variables["existing"] != "value" {
		t.Errorf("existing variable not preserved: %v", got.Variables["existing"])
	}
}

func TestMemoryStore_CompleteAction_LateDelivery_NoOp(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.MarkAwaitingAction(ctx, r.ID, "svc.act", 2)

	// attempt 1 is stale — should be no-op
	if err := s.CompleteAction(ctx, r.ID, 1, map[string]any{"x": "y"}); err != nil {
		t.Fatalf("late complete should not error: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.AwaitedActionDispatch == nil {
		t.Errorf("late delivery should not clear awaited_action_dispatch")
	}
	if got.Variables["x"] != nil {
		t.Errorf("late delivery should not merge variables")
	}
}

func TestMemoryStore_FailAction_TransitionsToFailed(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.MarkAwaitingAction(ctx, r.ID, "svc.act", 1)

	if err := s.FailAction(ctx, r.ID, 1, "ERR_TIMEOUT", "worker timed out", true); err != nil {
		t.Fatalf("fail: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", got.State)
	}
	if got.AwaitedActionDispatch != nil {
		t.Errorf("awaited_action_dispatch should be nil after fail")
	}
	// Audit event should have been appended.
	events, _ := s.ListEventsByRun(ctx, r.ID)
	found := false
	for _, e := range events {
		if e.EventType == domain.EventStepFailed {
			found = true
			if e.Metadata["code"] != "ERR_TIMEOUT" {
				t.Errorf("event code = %v, want ERR_TIMEOUT", e.Metadata["code"])
			}
		}
	}
	if !found {
		t.Errorf("expected step.failed audit event")
	}
}

func TestMemoryStore_FailAction_LateDelivery_NoOp(t *testing.T) {
	s := New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.MarkAwaitingAction(ctx, r.ID, "svc.act", 3)

	// attempt 1 is stale
	if err := s.FailAction(ctx, r.ID, 1, "ERR", "stale", false); err != nil {
		t.Fatalf("late fail should not error: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State == domain.RunStateFailed {
		t.Errorf("late fail should not transition run to failed")
	}
}

// TestMemoryStore_GetEventByID verifies round-trip and not-found paths.
func TestMemoryStore_GetEventByID(t *testing.T) {
	s := New()
	ctx := context.Background()
	runID := uuid.New()

	// Round-trip: append then fetch by ID.
	evt := domain.NewEvent(runID, "step1", 0, domain.EventSagaStarted, "engine")
	if err := s.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := s.GetEventByID(ctx, evt.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != evt.ID || got.EventType != domain.EventSagaStarted {
		t.Errorf("got %+v, want id=%s type=%s", got, evt.ID, domain.EventSagaStarted)
	}

	// Not-found: unknown UUID returns ErrNotFound.
	_, err = s.GetEventByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected ErrNotFound for unknown id, got nil")
	}
	if _, ok := err.(store.ErrNotFound); !ok {
		t.Errorf("expected store.ErrNotFound, got %T: %v", err, err)
	}
}
