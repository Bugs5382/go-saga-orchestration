package verbs

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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestEmitSignal_SendsSignalAndPublishesAdvance(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	// Create target run B and pause it awaiting signal "go".
	runB := domain.NewSagaRun("wf-b", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, runB); err != nil {
		t.Fatalf("create run B: %v", err)
	}
	if err := s.SetPausedAwaitingSignal(ctx, runB.ID, "go", nil); err != nil {
		t.Fatalf("set paused awaiting signal: %v", err)
	}

	// Emitting run A (the one running the emit_signal step).
	runA := domain.NewSagaRun("wf-a", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, runA); err != nil {
		t.Fatalf("create run A: %v", err)
	}

	pub := &recPub{}
	v := EmitSignalVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "emit",
		Type: domain.StepTypeEmitSignal,
		Inputs: map[string]any{
			"run_id":  runB.ID.String(),
			"name":    "go",
			"payload": map[string]any{"k": "v"},
		},
	}

	out, err := v.Execute(ctx, runA, step)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output map, got %v", out)
	}

	// Publisher should have been called with run B's ID.
	if len(pub.runs) != 1 || pub.runs[0] != runB.ID.String() {
		t.Errorf("published advances = %v, want [%s]", pub.runs, runB.ID)
	}

	// TryConsumeAwaitedSignal should now return false (already consumed).
	ok, err := s.TryConsumeAwaitedSignal(ctx, runB.ID, "go")
	if err != nil {
		t.Fatalf("TryConsumeAwaitedSignal: %v", err)
	}
	if ok {
		t.Errorf("signal should have been consumed already; TryConsume returned ok=true")
	}
}

func TestEmitSignal_TargetNotAwaiting_AppendsSignalNoPublish(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	// Target run exists but is NOT paused awaiting any signal.
	runB := domain.NewSagaRun("wf-b", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, runB); err != nil {
		t.Fatalf("create run B: %v", err)
	}

	runA := domain.NewSagaRun("wf-a", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, runA); err != nil {
		t.Fatalf("create run A: %v", err)
	}

	pub := &recPub{}
	v := EmitSignalVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "emit",
		Type: domain.StepTypeEmitSignal,
		Inputs: map[string]any{
			"run_id": runB.ID.String(),
			"name":   "go",
		},
	}

	_, err := v.Execute(ctx, runA, step)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// No publish since target wasn't awaiting.
	if len(pub.runs) != 0 {
		t.Errorf("expected no advance published, got %v", pub.runs)
	}
}

func TestEmitSignal_MissingRunID_Errors(t *testing.T) {
	v := EmitSignalVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"name": "go"},
	})
	if err == nil {
		t.Errorf("expected error for missing run_id")
	}
}

func TestEmitSignal_MissingName_Errors(t *testing.T) {
	v := EmitSignalVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"run_id": uuid.New().String()},
	})
	if err == nil {
		t.Errorf("expected error for missing name")
	}
}

func TestEmitSignal_BadRunID_Errors(t *testing.T) {
	v := EmitSignalVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"run_id": "not-a-uuid", "name": "go"},
	})
	if err == nil {
		t.Errorf("expected error for invalid run_id")
	}
}
