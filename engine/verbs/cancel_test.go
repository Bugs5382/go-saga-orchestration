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
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestCancelVerb_SelfCancel_NoRunID(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf_cancel", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	v := CancelVerb{S: s}
	step := domain.Step{
		ID:     "stop",
		Type:   domain.StepTypeCancel,
		Inputs: map[string]any{},
	}
	result, err := v.Execute(ctx, run, step)
	if !errors.Is(err, ErrSagaCancelled) {
		t.Errorf("expected ErrSagaCancelled, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCancelVerb_SelfCancel_SameRunID(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf_cancel_self", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	v := CancelVerb{S: s}
	step := domain.Step{
		ID:   "stop",
		Type: domain.StepTypeCancel,
		Inputs: map[string]any{
			"run_id": run.ID.String(),
		},
	}
	result, err := v.Execute(ctx, run, step)
	if !errors.Is(err, ErrSagaCancelled) {
		t.Errorf("expected ErrSagaCancelled for same run_id, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCancelVerb_TargetCancel(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	// Current run
	run := domain.NewSagaRun("wf_canceller", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	// Target run
	target := domain.NewSagaRun("wf_target", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, target)

	v := CancelVerb{S: s}
	step := domain.Step{
		ID:   "stop",
		Type: domain.StepTypeCancel,
		Next: "end",
		Inputs: map[string]any{
			"run_id": target.ID.String(),
			"reason": "x",
		},
	}
	result, err := v.Execute(ctx, run, step)
	if err != nil {
		t.Errorf("expected no error for target-cancel, got %v", err)
	}
	if result == nil {
		t.Errorf("expected non-nil result map")
	}

	got, err := s.GetRun(ctx, target.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.State != domain.RunStateCancelled {
		t.Errorf("target state = %s, want cancelled", got.State)
	}
}

func TestCancelVerb_BadRunID_Error(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf_bad", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	v := CancelVerb{S: s}
	step := domain.Step{
		ID:   "stop",
		Type: domain.StepTypeCancel,
		Inputs: map[string]any{
			"run_id": "not-a-uuid",
		},
	}
	_, err := v.Execute(ctx, run, step)
	if err == nil {
		t.Errorf("expected error for bad run_id, got nil")
	}
}
