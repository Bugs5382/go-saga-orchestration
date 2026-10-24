package postgres

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
	"sync"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

// recordingActionPub records the action dispatches a run makes.
type recordingActionPub struct {
	mu   sync.Mutex
	keys []string
}

func (p *recordingActionPub) PublishActionDispatch(_ context.Context, routingKey string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, routingKey)
	return nil
}

// manualAdvance leaves advances to the test, which calls Advance itself.
type manualAdvance struct{}

func (manualAdvance) PublishSagaAdvance(context.Context, string) error { return nil }

// TestCoordinator_TwoActionSteps runs a workflow with two action steps on the
// PostgreSQL store and reports each one the way a worker does. The second
// step must be awaited at attempt 2 and its result must reach the run.
func TestCoordinator_TwoActionSteps(t *testing.T) {
	s := newTestDB(t)
	ctx := context.Background()
	def := domain.WorkflowDefinition{
		ID: "wf_two_actions", Version: 1, Name: "two actions", Start: "first", Published: true,
		Steps: []domain.Step{
			{ID: "first", Type: domain.StepTypeAction, Action: "svc.first", Next: "second"},
			{ID: "second", Type: domain.StepTypeAction, Action: "svc.second", Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}
	defID, err := s.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	pub := &recordingActionPub{}
	coord := engine.NewCoordinator(s, manualAdvance{}, clock.SystemClock{}, secrets.NewMemory(nil),
		licensing.StubAllowAll{}, pub, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	awaitStep := func(step string, attempt int) {
		t.Helper()
		got, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if got.State != domain.RunStatePaused || got.CurrentStep != step || got.CurrentAttempt != attempt ||
			got.AwaitedActionDispatch == nil {
			t.Fatalf("run = state %s step %s attempt %d dispatch %v, want paused on %s attempt %d",
				got.State, got.CurrentStep, got.CurrentAttempt, got.AwaitedActionDispatch, step, attempt)
		}
	}

	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance 1: %v", err)
	}
	awaitStep("first", 1)
	if err := s.CompleteAction(ctx, run.ID, 1, map[string]any{"first": "ok"}); err != nil {
		t.Fatalf("complete first: %v", err)
	}

	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance 2: %v", err)
	}
	awaitStep("second", 2)
	if err := s.CompleteAction(ctx, run.ID, 2, map[string]any{"second": "ok"}); err != nil {
		t.Fatalf("complete second: %v", err)
	}

	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance 3: %v", err)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.State != domain.RunStateSucceeded {
		t.Fatalf("state = %s, want succeeded", got.State)
	}
	if got.Variables["first"] != "ok" || got.Variables["second"] != "ok" {
		t.Errorf("variables = %v, want both step results", got.Variables)
	}
	if len(pub.keys) != 2 || pub.keys[0] != "svc.first" || pub.keys[1] != "svc.second" {
		t.Errorf("dispatches = %v, want [svc.first svc.second]", pub.keys)
	}
}
