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

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func TestWhile_ConditionTrue_BranchContinue(t *testing.T) {
	out, err := WhileVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{"counter": int64(2)}},
		domain.Step{ID: "w", Inputs: map[string]any{"condition": "counter < 5"}})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["branch"] != "continue" {
		t.Errorf("branch = %v, want continue", out["branch"])
	}
	if out["_while.w.iter"].(int64) != 1 {
		t.Errorf("first iter should be 1, got %v", out["_while.w.iter"])
	}
}

func TestWhile_ConditionFalse_BranchExit(t *testing.T) {
	out, err := WhileVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{"counter": int64(10)}},
		domain.Step{ID: "w", Inputs: map[string]any{"condition": "counter < 5"}})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["branch"] != "exit" {
		t.Errorf("branch = %v, want exit", out["branch"])
	}
}

func TestWhile_MaxIterationsReached(t *testing.T) {
	vars := map[string]any{"_while": map[string]any{"w": map[string]any{"iter": int64(100)}}}
	_, err := WhileVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: vars},
		domain.Step{ID: "w", Inputs: map[string]any{"condition": "true", "max_iterations": float64(100)}})
	if err == nil {
		t.Errorf("expected max_iterations error")
	}
}
