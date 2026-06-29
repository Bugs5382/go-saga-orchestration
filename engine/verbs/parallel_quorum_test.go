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

// ---------------------------------------------------------------------------
// Verb-side validation tests
// ---------------------------------------------------------------------------

func TestParallel_QuorumN_Zero_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-q", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := ParallelVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
				map[string]any{"start": "e2", "steps": []any{map[string]any{"id": "e2", "type": "end"}}},
			},
			"join_strategy": "quorum",
			"quorum_n":      0,
		},
	})
	if err == nil {
		t.Errorf("expected error for quorum_n=0")
	}
}

func TestParallel_QuorumN_ExceedsBranchCount_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-q", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := ParallelVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
				map[string]any{"start": "e2", "steps": []any{map[string]any{"id": "e2", "type": "end"}}},
			},
			"join_strategy": "quorum",
			"quorum_n":      5, // more than 2 branches
		},
	})
	if err == nil {
		t.Errorf("expected error when quorum_n > branch count")
	}
}

func TestParallel_QuorumN_Missing_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-q", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := ParallelVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
			},
			"join_strategy": "quorum",
			// quorum_n omitted
		},
	})
	if err == nil {
		t.Errorf("expected error when quorum_n is missing")
	}
}

func TestParallel_QuorumValid_SpawnsAndPauses(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-q", uuid.New(), nil, map[string]any{})
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
				map[string]any{"start": "e3", "steps": []any{map[string]any{"id": "e3", "type": "end"}}},
			},
			"join_strategy": "quorum",
			"quorum_n":      2,
		},
	}
	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "p")
	if len(children) != 3 {
		t.Errorf("spawned %d children, want 3", len(children))
	}
	if len(pub.runs) != 3 {
		t.Errorf("published advance %d times, want 3", len(pub.runs))
	}
	gotParent, _ := s.GetRun(ctx, parent.ID)
	if gotParent.State != domain.RunStatePaused {
		t.Errorf("parent state = %s, want paused", gotParent.State)
	}
}

// ---------------------------------------------------------------------------
// CEL-string quorum_n test
// ---------------------------------------------------------------------------

func TestParallel_QuorumN_CELString(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	// Variables carry _config.quorum_n = 2. NewSagaRun puts inputs in Inputs,
	// not Variables — use UpdateRunVariables to seed the quorum config before Execute.
	parent := domain.NewSagaRun("wf-q-cel", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)
	_ = s.UpdateRunVariables(ctx, parent.ID, map[string]any{
		"_config": map[string]any{"quorum_n": int64(2)},
	})
	// Re-read so parent.Variables is populated for EvalQuorumNCEL.
	parent, _ = s.GetRun(ctx, parent.ID)

	pub := &recPub{}
	v := ParallelVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
				map[string]any{"start": "e2", "steps": []any{map[string]any{"id": "e2", "type": "end"}}},
				map[string]any{"start": "e3", "steps": []any{map[string]any{"id": "e3", "type": "end"}}},
			},
			"join_strategy": "quorum",
			"quorum_n":      "_config.quorum_n", // CEL string
		},
	}
	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "p")
	if len(children) != 3 {
		t.Errorf("spawned %d children, want 3", len(children))
	}
	if len(pub.runs) != 3 {
		t.Errorf("published advance %d times, want 3", len(pub.runs))
	}
}

// ---------------------------------------------------------------------------
// ToInt helper tests
// ---------------------------------------------------------------------------

func TestToInt(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		want   int
		wantOK bool
	}{
		{"int", 3, 3, true},
		{"int64", int64(7), 7, true},
		{"float64", float64(2), 2, true},
		{"nil", nil, 0, false},
		{"string", "3", 0, false},
		{"bool", true, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ToInt(tc.in)
			if ok != tc.wantOK || got != tc.want {
				t.Errorf("ToInt(%v) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
