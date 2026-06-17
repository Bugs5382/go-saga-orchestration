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

func TestSpawnSaga_SpawnsChild_ParentDoesNotPause(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	childDef := domain.WorkflowDefinition{
		ID:        "wf_spawn_child",
		Version:   1,
		Name:      "child",
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)

	parent := domain.NewSagaRun("wf_spawn_parent", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	pub := &recPub{}
	v := SpawnSagaVerb{S: s, Publisher: pub}

	step := domain.Step{
		ID:   "sp",
		Type: domain.StepTypeSpawnSaga,
		Next: "after",
		Inputs: map[string]any{
			"workflow_id": "wf_spawn_child",
		},
	}
	result, err := v.Execute(ctx, parent, step)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Parent must not be paused.
	gotParent, _ := s.GetRun(ctx, parent.ID)
	if gotParent.State == domain.RunStatePaused {
		t.Errorf("parent was paused, want running/pending (fire-and-forget)")
	}
	// Child should be spawned.
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "sp")
	if len(children) != 1 {
		t.Errorf("spawned %d children, want 1", len(children))
	}
	// Advance published for child.
	if len(pub.runs) != 1 {
		t.Errorf("published advance %d times, want 1", len(pub.runs))
	}
	// Result should be an empty map (not nil), so the parent advances normally.
	if result == nil {
		t.Errorf("result is nil, want empty map")
	}
}

func TestSpawnSaga_MissingWorkflowID_Errors(t *testing.T) {
	v := SpawnSagaVerb{S: memory.New(), Publisher: &recPub{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{}})
	if err == nil {
		t.Errorf("expected error for missing workflow_id")
	}
}

func TestSpawnSaga_UnknownWorkflowID_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf_p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := SpawnSagaVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:     "sp",
		Type:   domain.StepTypeSpawnSaga,
		Inputs: map[string]any{"workflow_id": "nonexistent_wf"},
	})
	if err == nil {
		t.Errorf("expected error for unknown workflow_id")
	}
}

func TestSpawnSaga_PassesInputsToChild(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	childDef := domain.WorkflowDefinition{
		ID:        "wf_spawn_inputs",
		Version:   1,
		Name:      "child",
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)

	parent := domain.NewSagaRun("wf_p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := SpawnSagaVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "sp",
		Type: domain.StepTypeSpawnSaga,
		Inputs: map[string]any{
			"workflow_id": "wf_spawn_inputs",
			"inputs":      map[string]any{"key": "value"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "sp")
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}
	if children[0].Inputs["key"] != "value" {
		t.Errorf("child inputs = %v, want key=value", children[0].Inputs)
	}
}

func TestSpawnSaga_EntrypointStartsChildAtNamedStep(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	childDef := domain.WorkflowDefinition{
		ID:      "wf_spawn_entry",
		Version: 1,
		Name:    "child",
		Start:   "s1",
		Steps: []domain.Step{
			{ID: "s1", Type: domain.StepTypeEnd},
			{ID: "s2", Type: domain.StepTypeEnd},
		},
		Entrypoints: map[string]string{"alt": "s2"},
		Published:   true,
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)

	parent := domain.NewSagaRun("wf_spawn_parent_entry", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := SpawnSagaVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "sp_entry",
		Type: domain.StepTypeSpawnSaga,
		Inputs: map[string]any{
			"workflow_id": "wf_spawn_entry",
			"entrypoint":  "alt",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "sp_entry")
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}
	if children[0].CurrentStep != "s2" {
		t.Errorf("child CurrentStep = %q, want %q", children[0].CurrentStep, "s2")
	}
}

func TestSpawnSaga_UnknownEntrypoint_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	childDef := domain.WorkflowDefinition{
		ID:          "wf_spawn_badentry",
		Version:     1,
		Name:        "child",
		Start:       "s1",
		Steps:       []domain.Step{{ID: "s1", Type: domain.StepTypeEnd}},
		Entrypoints: map[string]string{"alt": "s2"},
		Published:   true,
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)

	parent := domain.NewSagaRun("wf_spawn_parent_badentry", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	v := SpawnSagaVerb{S: s, Publisher: &recPub{}}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID:   "sp_bad",
		Type: domain.StepTypeSpawnSaga,
		Inputs: map[string]any{
			"workflow_id": "wf_spawn_badentry",
			"entrypoint":  "nope",
		},
	})
	if err == nil {
		t.Errorf("expected error for unknown entrypoint, got nil")
	}
}
