package cel

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
	"testing"
)

// TestCELParallelBranchesFilter verifies that the CEL expression used by the
// quorum_rule rule can compile and evaluate against the _parallel variable shape
// produced by aggregateChildResults. This is a sanity probe for B5.5-T6.
//
// The expression:
//
//	size(_parallel.approve.branches.filter(b, b._user_task.result.vote == 'approve')) >= 2
//
// Note: CEL variable names starting with '_' are valid identifiers in the
// cel-go parser. If this test fails with a "compile" error, B5.5-T6 will need
// to reshape how variables are passed (e.g. by flattening _parallel into a
// positional var or renaming to "parallel").
func TestCELParallelBranchesFilter(t *testing.T) {
	vars := map[string]any{
		"_parallel": map[string]any{
			"approve": map[string]any{
				"branches": []any{
					map[string]any{
						"key": "u1",
						"_user_task": map[string]any{
							"result": map[string]any{"vote": "approve"},
						},
					},
					map[string]any{
						"key": "u2",
						"_user_task": map[string]any{
							"result": map[string]any{"vote": "reject"},
						},
					},
					map[string]any{
						"key": "u3",
						"_user_task": map[string]any{
							"result": map[string]any{"vote": "approve"},
						},
					},
				},
			},
		},
	}

	env, err := NewEnv("_parallel")
	if err != nil {
		t.Fatalf("NewEnv with _parallel identifier: %v", err)
	}

	expr := `size(_parallel.approve.branches.filter(b, b._user_task.result.vote == 'approve')) >= 2`
	prg, err := env.Compile(expr)
	if err != nil {
		t.Fatalf("Compile: %v\n\nIf this fails with 'undeclared reference', _parallel starting with '_' may be rejected — report in B5.5-T6", err)
	}

	got, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}

	result, ok := got.(bool)
	if !ok {
		t.Fatalf("expected bool result, got %T: %v", got, got)
	}
	if !result {
		t.Errorf("expression evaluated to false, expected true (2 approve votes >= 2)")
	}
}

// TestCELParallelBranchesFilter_BelowThreshold checks the false case: only
// 1 approve vote, threshold is 2, so the expression should return false.
func TestCELParallelBranchesFilter_BelowThreshold(t *testing.T) {
	vars := map[string]any{
		"_parallel": map[string]any{
			"approve": map[string]any{
				"branches": []any{
					map[string]any{
						"key": "u1",
						"_user_task": map[string]any{
							"result": map[string]any{"vote": "approve"},
						},
					},
					map[string]any{
						"key": "u2",
						"_user_task": map[string]any{
							"result": map[string]any{"vote": "reject"},
						},
					},
				},
			},
		},
	}

	env, err := NewEnv("_parallel")
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}
	expr := `size(_parallel.approve.branches.filter(b, b._user_task.result.vote == 'approve')) >= 2`
	prg, err := env.Compile(expr)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if got.(bool) {
		t.Error("expected false (1 approve < threshold 2), got true")
	}
}
