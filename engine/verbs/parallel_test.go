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
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type recPub struct{ runs []string }

func (r *recPub) PublishSagaAdvance(_ context.Context, runID string) error {
	r.runs = append(r.runs, runID)
	return nil
}

func TestParallel_Spawns3Children_PausesParent(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
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

func TestParallel_MissingBranches_Errors(t *testing.T) {
	v := ParallelVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{}})
	if err == nil {
		t.Errorf("expected error for missing branches")
	}
}

func TestParallel_UnsupportedJoinStrategy_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := ParallelVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches":      []any{map[string]any{"start": "e", "steps": []any{map[string]any{"id": "e", "type": "end"}}}},
			"join_strategy": "first_terminal",
		},
	})
	if err == nil {
		t.Errorf("expected error for unsupported strategy")
	}
}

func TestParallel_BranchWithKey_UsesKey(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	pub := &recPub{}
	v := ParallelVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "p",
		Type: domain.StepTypeParallel,
		Inputs: map[string]any{
			"branches": []any{
				map[string]any{"key": "alpha", "start": "e1", "steps": []any{map[string]any{"id": "e1", "type": "end"}}},
				map[string]any{"key": "beta", "start": "e2", "steps": []any{map[string]any{"id": "e2", "type": "end"}}},
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
