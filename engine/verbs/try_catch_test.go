package verbs

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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestTryCatch_PushesFrame(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	v := TryCatchVerb{S: s}
	_, err := v.Execute(ctx, r, domain.Step{
		ID: "t1", Type: domain.StepTypeTryCatch,
		Inputs: map[string]any{"try": []any{"s1"}, "catch": "h"},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if len(got.TryCatchStack) != 1 {
		t.Fatalf("stack len = %d, want 1", len(got.TryCatchStack))
	}
	if got.TryCatchStack[0].CatchStep != "h" {
		t.Errorf("catch_step = %q, want h", got.TryCatchStack[0].CatchStep)
	}
}

func TestTryCatch_MissingTry_Errors(t *testing.T) {
	v := TryCatchVerb{S: memory.New()}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{"catch": "h"}})
	if err == nil {
		t.Errorf("expected error for missing try")
	}
}

func TestTryCatch_MissingCatch_Errors(t *testing.T) {
	v := TryCatchVerb{S: memory.New()}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{"try": []any{"s1"}}})
	if err == nil {
		t.Errorf("expected error for missing catch")
	}
}
