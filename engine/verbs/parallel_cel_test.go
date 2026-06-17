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
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// TestParallelCEL_ShortFormBranches verifies that a CEL-string branches
// expression is evaluated against run.Variables and that the resulting
// short-form branch objects (type + inputs, no start/steps) are normalised
// and spawned as full child runs.
func TestParallelCEL_ShortFormBranches(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	members := []any{
		map[string]any{"user_id": "u1"},
		map[string]any{"user_id": "u2"},
		map[string]any{"user_id": "u3"},
	}
	vars := map[string]any{"members": members}
	parent := domain.NewSagaRun("wf-cel", uuid.New(), nil, nil)
	parent.Variables = vars
	_ = s.CreateRun(ctx, parent)

	pub := &recPub{}
	v := ParallelVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": "members.map(_, {'type':'manual_approval','inputs':{'assignee':_.user_id}})",
		},
	}

	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}

	children, _ := s.ListChildrenByParent(ctx, parent.ID, "p")
	if len(children) != 3 {
		t.Fatalf("spawned %d children, want 3", len(children))
	}
	if len(pub.runs) != 3 {
		t.Errorf("published advance %d times, want 3", len(pub.runs))
	}

	// Verify each child's first step carries the expected assignee.
	wantAssignees := map[string]bool{"u1": true, "u2": true, "u3": true}
	for _, child := range children {
		def, err := s.GetWorkflowDefinition(ctx, child.DefinitionID)
		if err != nil {
			t.Fatalf("get def for child %s: %v", child.ID, err)
		}
		// First step is the verb step (manual_approval); second is end.
		if len(def.Steps) < 1 {
			t.Fatalf("child def has no steps")
		}
		firstStep := def.Steps[0]
		assignee, _ := firstStep.Inputs["assignee"].(string)
		if !wantAssignees[assignee] {
			t.Errorf("unexpected assignee %q in child step", assignee)
		}
		delete(wantAssignees, assignee)
	}
	if len(wantAssignees) != 0 {
		t.Errorf("missing assignees: %v", wantAssignees)
	}
}

// TestParallelCEL_LiteralBranchesRegression verifies that the existing
// literal []any branches path still works unchanged after the CEL extension.
func TestParallelCEL_LiteralBranchesRegression(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-lit", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	pub := &recPub{}
	v := ParallelVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
				map[string]any{"start": "e2", "steps": []any{map[string]any{"id": "e2", "type": "end"}}},
			},
		},
	}

	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "p")
	if len(children) != 2 {
		t.Errorf("spawned %d children, want 2", len(children))
	}
}

// TestParallelCEL_BadExpression verifies that an invalid CEL expression
// (references an undefined variable) returns an error containing "CEL eval".
func TestParallelCEL_BadExpression(t *testing.T) {
	v := ParallelVerb{S: memory.New(), Publisher: &recPub{}}
	// Empty variables — this_is_not_a_var is undeclared, so compilation will fail.
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{
			"branches": "this_is_not_a_var.map(x, x)",
		},
	})
	if err == nil {
		t.Fatal("expected error for bad CEL expression, got nil")
	}
	if !strings.Contains(err.Error(), "CEL eval") {
		t.Errorf("error %q does not contain 'CEL eval'", err.Error())
	}
}

// TestParallelCEL_EmptyCELResult verifies that a CEL expression that
// evaluates to an empty list returns the "non-empty list" error.
func TestParallelCEL_EmptyCELResult(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-empty", uuid.New(), nil, nil)
	parent.Variables = map[string]any{"empty_list": []any{}}
	_ = s.CreateRun(ctx, parent)

	v := ParallelVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		Inputs: map[string]any{
			// Filter everything out — result is an empty list.
			"branches": "empty_list.filter(_, false)",
		},
	})
	if err == nil {
		t.Fatal("expected error for empty CEL result, got nil")
	}
	if !strings.Contains(err.Error(), "non-empty list") {
		t.Errorf("error %q does not contain 'non-empty list'", err.Error())
	}
}

// TestNormalizeBranch_ShortFormRoundTrip verifies that normalizeBranch
// converts a short-form {type, inputs} map into the long-form {key, start,
// steps} shape, and that a long-form input is returned unchanged.
func TestNormalizeBranch_ShortFormRoundTrip(t *testing.T) {
	short := map[string]any{
		"type":   "manual_approval",
		"inputs": map[string]any{"assignee": "u1"},
	}
	result := normalizeBranch(short, 0)

	key, _ := result["key"].(string)
	if key == "" {
		t.Errorf("key is empty, want synthesised key")
	}
	start, _ := result["start"].(string)
	if start == "" {
		t.Errorf("start is empty after normalization")
	}
	steps, _ := result["steps"].([]any)
	if len(steps) != 2 {
		t.Fatalf("want 2 steps (verb + end), got %d", len(steps))
	}
	// First step should be the verb step.
	step0, _ := steps[0].(map[string]any)
	if step0["type"] != "manual_approval" {
		t.Errorf("first step type = %v, want manual_approval", step0["type"])
	}
	stepInputs, _ := step0["inputs"].(map[string]any)
	if stepInputs["assignee"] != "u1" {
		t.Errorf("assignee = %v, want u1", stepInputs["assignee"])
	}
	// Second step should be the end step.
	step1, _ := steps[1].(map[string]any)
	if step1["type"] != "end" {
		t.Errorf("second step type = %v, want end", step1["type"])
	}

	// Long-form input should pass through unchanged.
	long := map[string]any{
		"start": "s1",
		"steps": []any{map[string]any{"id": "s1", "type": "end"}},
	}
	got := normalizeBranch(long, 0)
	if got["start"] != "s1" {
		t.Errorf("long-form start changed: %v", got["start"])
	}
}
