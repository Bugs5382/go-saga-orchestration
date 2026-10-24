package engine_test

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
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// dispatchLog records action and compensation dispatches in order.
type dispatchLog struct {
	mu      sync.Mutex
	entries []string
}

func (d *dispatchLog) PublishActionDispatch(_ context.Context, key string, payload []byte) error {
	var p struct {
		StepID       string `json:"step_id"`
		Attempt      int    `json:"attempt"`
		Compensation bool   `json:"compensation"`
	}
	_ = json.Unmarshal(payload, &p)
	kind := "action"
	if p.Compensation {
		kind = "compensation"
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append(d.entries, kind+" "+key+" "+p.StepID)
	return nil
}

func (d *dispatchLog) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.entries...)
}

type noAdvance struct{}

func (noAdvance) PublishSagaAdvance(context.Context, string) error { return nil }

func threeStepDef() domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID: "wf_book", Version: 1, Name: "book", Start: "reserve", Published: true,
		Steps: []domain.Step{
			{ID: "reserve", Type: domain.StepTypeAction, Action: "svc.reserve", Next: "hold",
				Compensation: &domain.Compensation{Action: "svc.release"}},
			{ID: "hold", Type: domain.StepTypeAction, Action: "svc.hold", Next: "charge",
				Compensation: &domain.Compensation{Action: "svc.unhold"}},
			{ID: "charge", Type: domain.StepTypeAction, Action: "svc.charge", Next: "done",
				Compensation: &domain.Compensation{Action: "svc.refund"}},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}
}

// TestCompensateFailedStep_WorkerFailure fails the third action step the way
// a worker's Error report does, then compensates from outside Advance: the two
// completed steps are rolled back newest first and the run stays failed with
// the worker's error.
func TestCompensateFailedStep_WorkerFailure(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def := threeStepDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	log := &dispatchLog{}
	coord := engine.NewCoordinator(s, noAdvance{}, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, log, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err := coord.Advance(ctx, run.ID.String()); err != nil {
			t.Fatalf("advance %d: %v", attempt, err)
		}
		if attempt < 3 {
			if err := s.CompleteAction(ctx, run.ID, attempt, nil); err != nil {
				t.Fatalf("complete %d: %v", attempt, err)
			}
		}
	}
	if err := s.FailAction(ctx, run.ID, 3, "declined", "card declined", false); err != nil {
		t.Fatalf("fail: %v", err)
	}

	if err := coord.CompensateFailedStep(ctx, run.ID, "charge"); err != nil {
		t.Fatalf("CompensateFailedStep: %v", err)
	}

	want := []string{
		"action svc.reserve reserve", "action svc.hold hold", "action svc.charge charge",
		"compensation svc.unhold hold", "compensation svc.release reserve",
	}
	got := log.snapshot()
	if len(got) != len(want) {
		t.Fatalf("dispatches = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dispatches = %v, want %v", got, want)
		}
	}
	after, _ := s.GetRun(ctx, run.ID)
	if after.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", after.State)
	}
	if after.LastError == nil || *after.LastError != "card declined" {
		t.Errorf("LastError = %v, want the worker's message", after.LastError)
	}
	events, _ := s.ListEventsByRun(ctx, run.ID)
	var started bool
	for _, e := range events {
		if e.EventType == domain.EventCompensationStarted {
			started = true
		}
	}
	if !started {
		t.Error("no compensation.started event")
	}
}

// TestCompensateFailedStep_RunningRunIsFailed marks a run that isn't terminal
// yet as failed after compensating it.
func TestCompensateFailedStep_RunningRunIsFailed(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def := threeStepDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	log := &dispatchLog{}
	coord := engine.NewCoordinator(s, noAdvance{}, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, log, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	if err := coord.CompensateFailedStep(ctx, run.ID, "reserve"); err != nil {
		t.Fatalf("CompensateFailedStep: %v", err)
	}
	after, _ := s.GetRun(ctx, run.ID)
	if after.State != domain.RunStateFailed || after.CurrentStep != "reserve" {
		t.Errorf("run = %s on %q, want failed on reserve", after.State, after.CurrentStep)
	}
	if after.LastError == nil || *after.LastError == "" {
		t.Error("LastError empty, want a reason")
	}
	if got := log.snapshot(); len(got) != 1 {
		t.Errorf("dispatches = %v, want only the first action (nothing completed to roll back)", got)
	}
}

func TestCompensateFailedStep_Refusals(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	coord := engine.NewCoordinator(s, noAdvance{}, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, &dispatchLog{}, nil)
	if err := coord.CompensateFailedStep(ctx, uuid.New(), "x"); err == nil {
		t.Error("unknown run: want an error")
	}
	def := threeStepDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	_ = s.UpdateRunState(ctx, run.ID, domain.RunStateSucceeded, "done")
	if err := coord.CompensateFailedStep(ctx, run.ID, "charge"); err == nil {
		t.Error("succeeded run: want an error")
	}
}
