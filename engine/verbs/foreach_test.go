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

func TestForeach_3Items_Spawns3ChildrenPausesParent(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-fe", uuid.New(), nil, map[string]any{})
	parent.Variables = map[string]any{"xs": []any{int64(1), int64(2), int64(3)}}
	_ = s.CreateRun(ctx, parent)

	pub := &recPub{}
	v := ForeachVerb{S: s, Publisher: pub}
	step := domain.Step{
		ID:   "fe",
		Type: domain.StepTypeForeach,
		Inputs: map[string]any{
			"list":  "xs",
			"start": "iter_end",
			"body":  []any{map[string]any{"id": "iter_end", "type": "end"}},
		},
	}
	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "fe")
	if len(children) != 3 {
		t.Errorf("children = %d, want 3", len(children))
	}
	if len(pub.runs) != 3 {
		t.Errorf("published advance %d times, want 3", len(pub.runs))
	}
	gotParent, _ := s.GetRun(ctx, parent.ID)
	if gotParent.State != domain.RunStatePaused {
		t.Errorf("parent state = %s, want paused", gotParent.State)
	}
}

func TestForeach_EmptyList_NoChildrenNoPause(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-fe", uuid.New(), nil, map[string]any{})
	parent.Variables = map[string]any{"xs": []any{}}
	_ = s.CreateRun(ctx, parent)
	v := ForeachVerb{S: s, Publisher: &recPub{}}
	out, err := v.Execute(ctx, parent, domain.Step{
		ID:   "fe",
		Type: domain.StepTypeForeach,
		Inputs: map[string]any{
			"list":  "xs",
			"start": "i_end",
			"body":  []any{map[string]any{"id": "i_end", "type": "end"}},
		},
	})
	if err != nil {
		t.Errorf("unexpected err for empty list: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no result map for empty list, got %+v", out)
	}
}

func TestForeach_SequentialMode_Rejected(t *testing.T) {
	v := ForeachVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{Variables: map[string]any{}}, domain.Step{
		Inputs: map[string]any{
			"list":     "[]",
			"start":    "x",
			"body":     []any{},
			"parallel": false,
		},
	})
	if err == nil {
		t.Errorf("expected error for sequential mode")
	}
}
