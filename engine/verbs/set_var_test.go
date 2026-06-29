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

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func TestSetVar_Literal(t *testing.T) {
	out, err := SetVarVerb{}.Execute(context.Background(), domain.SagaRun{Variables: map[string]any{}}, domain.Step{
		ID: "s", Type: domain.StepTypeSetVar,
		Inputs: map[string]any{"out_var": "x", "value": 42},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["x"] != 42 {
		t.Errorf("out = %+v, want x=42", out)
	}
}

func TestSetVar_Expr(t *testing.T) {
	out, err := SetVarVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{"x": int64(5)}},
		domain.Step{ID: "s", Type: domain.StepTypeSetVar,
			Inputs: map[string]any{"out_var": "y", "expr": "x * 2 + 1"}})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["y"].(int64) != 11 {
		t.Errorf("y = %v, want 11", out["y"])
	}
}

func TestSetVar_MissingOutVar(t *testing.T) {
	_, err := SetVarVerb{}.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"value": 1},
	})
	if err == nil {
		t.Errorf("expected error for missing out_var")
	}
}
