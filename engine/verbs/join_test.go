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

// spawnChild spawns a trivial child under (parentRun, stepID, key) and returns
// its ID. The child is left in whatever state SpawnChildRun sets (pending);
// tests drive it to a terminal state via the store directly.
func spawnJoinChild(t *testing.T, s *memory.Store, ctx context.Context, parentID uuid.UUID, stepID, key string) uuid.UUID {
	t.Helper()
	childDef := domain.WorkflowDefinition{
		ID: "wf_child_" + key, Version: 1, Name: "child",
		Published: true, Start: "end",
		Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	id, err := s.SpawnChildRun(ctx, parentID, stepID, key, childDef, map[string]any{})
	if err != nil {
		t.Fatalf("spawn child %s: %v", key, err)
	}
	return id
}

func markTerminal(t *testing.T, s *memory.Store, ctx context.Context, id uuid.UUID, state domain.RunState) {
	t.Helper()
	if err := s.UpdateRunState(ctx, id, state, ""); err != nil {
		t.Fatalf("mark %s: %v", state, err)
	}
}

// TestJoin_AllTerminal_ResolvesImmediately: when every watched child is
// already terminal, the join returns the aggregate and does not pause.
func TestJoin_AllTerminal_ResolvesImmediately(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	c1 := spawnJoinChild(t, s, ctx, parent.ID, "spawn_a", "a")
	c2 := spawnJoinChild(t, s, ctx, parent.ID, "spawn_b", "b")
	markTerminal(t, s, ctx, c1, domain.RunStateSucceeded)
	markTerminal(t, s, ctx, c2, domain.RunStateSucceeded)

	v := JoinVerb{S: s}
	step := domain.Step{
		ID:   "j",
		Type: domain.StepTypeJoin,
		Next: "after",
		Inputs: map[string]any{
			"streams": []any{"spawn_a", "spawn_b"},
		},
	}
	// Re-read parent (children were added under it).
	parent, _ = s.GetRun(ctx, parent.ID)
	result, err := v.Execute(ctx, parent, step)
	if err != nil {
		t.Fatalf("expected immediate resolve, got err: %v", err)
	}
	agg, ok := result["_join.j.branches"].([]any)
	if !ok {
		t.Fatalf("missing _join.j.branches aggregate, got %#v", result)
	}
	if len(agg) != 2 {
		t.Errorf("aggregate has %d branches, want 2", len(agg))
	}
	// Parent must NOT be paused (immediate resolve).
	got, _ := s.GetRun(ctx, parent.ID)
	if got.State == domain.RunStatePaused {
		t.Error("parent paused on immediate resolve, want not-paused")
	}
}

// TestJoin_PendingChild_Pauses: when a watched child is still running, the
// join pauses the run and returns ErrSagaPaused.
func TestJoin_PendingChild_Pauses(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	c1 := spawnJoinChild(t, s, ctx, parent.ID, "spawn_a", "a")
	_ = spawnJoinChild(t, s, ctx, parent.ID, "spawn_a", "a2") // still pending
	markTerminal(t, s, ctx, c1, domain.RunStateSucceeded)

	v := JoinVerb{S: s}
	step := domain.Step{
		ID:   "j",
		Type: domain.StepTypeJoin,
		Next: "after",
		Inputs: map[string]any{
			"streams": []any{"spawn_a"},
		},
	}
	parent, _ = s.GetRun(ctx, parent.ID)
	_, err := v.Execute(ctx, parent, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, parent.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("parent state = %s, want paused", got.State)
	}
	if got.CurrentStep != "j" {
		t.Errorf("parent CurrentStep = %s, want j", got.CurrentStep)
	}
}

// TestJoin_Quorum_ResolvesImmediately: with join_strategy=quorum and 2 of 3
// children succeeded, the join resolves immediately.
func TestJoin_Quorum_ResolvesImmediately(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	c1 := spawnJoinChild(t, s, ctx, parent.ID, "spawn", "a")
	c2 := spawnJoinChild(t, s, ctx, parent.ID, "spawn", "b")
	_ = spawnJoinChild(t, s, ctx, parent.ID, "spawn", "c") // pending
	markTerminal(t, s, ctx, c1, domain.RunStateSucceeded)
	markTerminal(t, s, ctx, c2, domain.RunStateSucceeded)

	v := JoinVerb{S: s}
	step := domain.Step{
		ID:   "j",
		Type: domain.StepTypeJoin,
		Next: "after",
		Inputs: map[string]any{
			"streams":       []any{"spawn"},
			"join_strategy": "quorum",
			"quorum_n":      2,
		},
	}
	parent, _ = s.GetRun(ctx, parent.ID)
	result, err := v.Execute(ctx, parent, step)
	if err != nil {
		t.Fatalf("expected quorum immediate resolve, got err: %v", err)
	}
	if _, ok := result["_join.j.branches"]; !ok {
		t.Fatalf("missing aggregate, got %#v", result)
	}
}

// TestJoin_MissingStreams_Errors: streams is required.
func TestJoin_MissingStreams_Errors(t *testing.T) {
	v := JoinVerb{S: memory.New()}
	_, err := v.Execute(context.Background(), domain.SagaRun{ID: uuid.New()}, domain.Step{
		ID: "j", Type: domain.StepTypeJoin, Inputs: map[string]any{},
	})
	if err == nil {
		t.Error("expected error for missing streams")
	}
}

// TestJoin_NoMatchingChildren_Errors: a stream naming a step that spawned no
// children is a configuration error (would otherwise pause forever).
func TestJoin_NoMatchingChildren_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)
	v := JoinVerb{S: s}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID: "j", Type: domain.StepTypeJoin, Next: "after",
		Inputs: map[string]any{"streams": []any{"never_spawned"}},
	})
	if err == nil {
		t.Error("expected error when streams match no children")
	}
}

// TestJoin_QuorumExceedsChildren_Errors: quorum_n greater than the watched
// child count fails fast rather than pausing forever.
func TestJoin_QuorumExceedsChildren_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)
	_ = spawnJoinChild(t, s, ctx, parent.ID, "spawn", "a")
	parent, _ = s.GetRun(ctx, parent.ID)
	v := JoinVerb{S: s}
	_, err := v.Execute(ctx, parent, domain.Step{
		ID: "j", Type: domain.StepTypeJoin, Next: "after",
		Inputs: map[string]any{
			"streams":       []any{"spawn"},
			"join_strategy": "quorum",
			"quorum_n":      5,
		},
	})
	if err == nil {
		t.Error("expected error when quorum_n exceeds watched-child count")
	}
}

// TestJoin_RegisteredInDefault verifies the join verb is wired into the
// default registry under the parallel_control license group.
func TestJoin_RegisteredInDefault(t *testing.T) {
	reg := Default(memory.New(), nil, nil, nil, nil, nil)
	entry, ok := reg[domain.StepTypeJoin]
	if !ok {
		t.Fatal("join verb not registered in Default")
	}
	if entry.LicenseGroup != "parallel_control" {
		t.Errorf("join license group = %q, want parallel_control", entry.LicenseGroup)
	}
	if _, ok := entry.Handler.(JoinVerb); !ok {
		t.Errorf("join handler = %T, want JoinVerb", entry.Handler)
	}
}
