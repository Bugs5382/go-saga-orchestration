package engine

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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// makeJoinParent builds a parent run whose definition has a join step
// ("join_step") watching the given streams with the given strategy, and leaves
// the parent paused ON the join step (as the join verb would leave it).
func makeJoinParent(t *testing.T, s *memory.Store, ctx context.Context, streams []any, strategy string, quorumN int) domain.SagaRun {
	t.Helper()
	inputs := map[string]any{"streams": streams}
	if strategy != "" {
		inputs["join_strategy"] = strategy
		inputs["quorum_n"] = quorumN
	}
	def := domain.WorkflowDefinition{
		ID: "wf_join_parent", Version: 1, Name: "JoinParent", Published: true,
		Start: "join_step",
		Steps: []domain.Step{
			{ID: "join_step", Type: domain.StepTypeJoin, Inputs: inputs, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, err := s.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		t.Fatalf("upsert parent def: %v", err)
	}
	run := domain.NewSagaRun("wf_join_parent", defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	// Parent paused on the join step (mirrors JoinVerb.Execute pausing).
	if err := s.UpdateRunState(ctx, run.ID, domain.RunStateRunning, "join_step"); err != nil {
		t.Fatalf("set current step: %v", err)
	}
	if err := s.SetPausedAwaitingSignal(ctx, run.ID, "__join_barrier__", nil); err != nil {
		t.Fatalf("pause parent: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	return got
}

func spawnBarrierChild(t *testing.T, s *memory.Store, ctx context.Context, parentID uuid.UUID, stepID, key string) string {
	t.Helper()
	childDef := domain.WorkflowDefinition{
		ID: "wf_bchild_" + key, Version: 1, Name: "BChild", Published: true,
		Start: "end", Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	id, err := s.SpawnChildRun(ctx, parentID, stepID, key, childDef, map[string]any{})
	if err != nil {
		t.Fatalf("spawn barrier child %s: %v", key, err)
	}
	return id.String()
}

// hasJoinAggregate reports whether vars contains the nested
// _join.<stepID>.branches aggregate list (UpdateRunVariables expands the
// dotted result key "_join.<stepID>.branches" into nested maps).
func hasJoinAggregate(vars map[string]any, stepID string) bool {
	joinMap, ok := vars["_join"].(map[string]any)
	if !ok {
		return false
	}
	stepMap, ok := joinMap[stepID].(map[string]any)
	if !ok {
		return false
	}
	branches, ok := stepMap["branches"].([]any)
	return ok && len(branches) > 0
}

func joinCoordinator(s *memory.Store) *Coordinator {
	return NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
}

// TestJoinBarrier_All_WakesWhenLastStreamTerminates: parent paused on a join
// step watching two spawn streams wakes only after BOTH children terminate.
func TestJoinBarrier_All_WakesWhenLastStreamTerminates(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parent := makeJoinParent(t, s, ctx, []any{"spawn_a", "spawn_b"}, "", 0)
	c1 := spawnBarrierChild(t, s, ctx, parent.ID, "spawn_a", "a")
	c2 := spawnBarrierChild(t, s, ctx, parent.ID, "spawn_b", "b")

	coord := joinCoordinator(s)

	// Child 1 finishes - barrier not yet met (child 2 still running).
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance child1: %v", err)
	}
	got, _ := s.GetRun(ctx, parent.ID)
	if got.AwaitedSignal == nil {
		t.Error("parent woken after only child1 - expected to wait for both streams")
	}

	// Child 2 finishes - all streams terminal, parent should wake.
	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance child2: %v", err)
	}
	got, _ = s.GetRun(ctx, parent.ID)
	if got.AwaitedSignal != nil {
		t.Error("parent NOT woken after both streams terminal")
	}
	// Aggregate must be present in parent variables, nested under
	// _join.join_step.branches (UpdateRunVariables expands the dotted key).
	if !hasJoinAggregate(got.Variables, "join_step") {
		t.Errorf("missing _join.join_step.branches aggregate in parent vars: %#v", got.Variables)
	}
}

// TestJoinBarrier_Quorum_WakesAtQuorum: join_strategy=quorum, quorum_n=2 over 3
// children in one stream; parent wakes after the 2nd succeeds.
func TestJoinBarrier_Quorum_WakesAtQuorum(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parent := makeJoinParent(t, s, ctx, []any{"spawn"}, "quorum", 2)
	c1 := spawnBarrierChild(t, s, ctx, parent.ID, "spawn", "a")
	c2 := spawnBarrierChild(t, s, ctx, parent.ID, "spawn", "b")
	_ = spawnBarrierChild(t, s, ctx, parent.ID, "spawn", "c") // stays running

	coord := joinCoordinator(s)

	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance child1: %v", err)
	}
	got, _ := s.GetRun(ctx, parent.ID)
	if got.AwaitedSignal == nil {
		t.Error("parent woken after 1 of 3 - expected to wait for quorum=2")
	}

	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance child2: %v", err)
	}
	got, _ = s.GetRun(ctx, parent.ID)
	if got.AwaitedSignal != nil {
		t.Error("parent NOT woken after quorum=2 reached")
	}
}

// TestJoinBarrier_UnwatchedStream_NoWake: a child terminating on a stream the
// join does NOT watch must not wake the parent.
func TestJoinBarrier_UnwatchedStream_NoWake(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parent := makeJoinParent(t, s, ctx, []any{"spawn_a"}, "", 0)
	// Watched stream child stays running.
	_ = spawnBarrierChild(t, s, ctx, parent.ID, "spawn_a", "a")
	// Unwatched stream child.
	other := spawnBarrierChild(t, s, ctx, parent.ID, "spawn_other", "x")

	coord := joinCoordinator(s)
	if err := coord.Advance(ctx, other); err != nil {
		t.Fatalf("advance other: %v", err)
	}
	got, _ := s.GetRun(ctx, parent.ID)
	if got.AwaitedSignal == nil {
		t.Error("parent woken by an unwatched stream - expected no wake")
	}
}
