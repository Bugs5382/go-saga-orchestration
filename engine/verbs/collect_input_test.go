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

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestCollectInput_CreatesTaskAndPauses(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	v := CollectInputVerb{S: s, Clock: clock.SystemClock{}}
	_, err := v.Execute(ctx, r, domain.Step{
		ID: "c", Type: domain.StepTypeCollectInput,
		Inputs: map[string]any{
			"assignee":    "u1",
			"form_schema": map[string]any{"fields": []any{map[string]any{"name": "reason", "type": "text"}}},
		},
	})
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.AwaitedSignal == nil {
		t.Fatalf("awaited_signal not set")
	}
	// The signal name should start with user_task.
	if len(*got.AwaitedSignal) < len("user_task.") || (*got.AwaitedSignal)[:10] != "user_task." {
		t.Errorf("awaited_signal = %v, want user_task.* prefix", got.AwaitedSignal)
	}
}

func TestCollectInput_MissingAssignee_Errors(t *testing.T) {
	v := CollectInputVerb{S: memory.New(), Clock: clock.SystemClock{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{
			"form_schema": map[string]any{"fields": []any{}},
		},
	})
	if err == nil {
		t.Errorf("expected error for missing assignee")
	}
}

func TestCollectInput_MissingFormSchema_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	v := CollectInputVerb{S: s, Clock: clock.SystemClock{}}
	_, err := v.Execute(ctx, r, domain.Step{
		ID: "c", Type: domain.StepTypeCollectInput,
		Inputs: map[string]any{"assignee": "u1"},
	})
	if err == nil {
		t.Errorf("expected error for missing form_schema")
	}
}

func TestCollectInput_EmptyFormSchema_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	v := CollectInputVerb{S: s, Clock: clock.SystemClock{}}
	_, err := v.Execute(ctx, r, domain.Step{
		ID: "c", Type: domain.StepTypeCollectInput,
		Inputs: map[string]any{
			"assignee":    "u1",
			"form_schema": map[string]any{},
		},
	})
	if err == nil {
		t.Errorf("expected error for empty form_schema")
	}
}

func TestCollectInput_BadDueIn_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	v := CollectInputVerb{S: s, Clock: clock.SystemClock{}}
	_, err := v.Execute(ctx, r, domain.Step{
		ID: "c", Type: domain.StepTypeCollectInput,
		Inputs: map[string]any{
			"assignee":    "u1",
			"form_schema": map[string]any{"f": "v"},
			"due_in":      "not-a-duration",
		},
	})
	if err == nil {
		t.Errorf("expected error for bad due_in")
	}
}
