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

// makeParentWithQuorum creates a parent run whose workflow definition contains
// a parallel step ("parallel_step") configured with the given join_strategy and
// quorum_n. The parent is left in the paused/awaiting-signal state so that
// checkParentJoin's step-match guard passes.
func makeParentWithQuorum(t *testing.T, s *memory.Store, ctx context.Context, joinStrategy string, quorumN int) domain.SagaRun {
	t.Helper()

	stepInputs := map[string]any{
		"join_strategy": joinStrategy,
		"quorum_n":      quorumN,
		"branches":      []any{}, // not needed for engine-side tests; parallel verb already ran
	}
	parentDef := domain.WorkflowDefinition{
		ID:        "wf_parent_quorum",
		Version:   1,
		Name:      "ParentQuorum",
		Published: true,
		Start:     "parallel_step",
		Steps: []domain.Step{
			{
				ID:     "parallel_step",
				Type:   domain.StepTypeParallel,
				Inputs: stepInputs,
				Next:   "end",
			},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	parentDefID, err := s.UpsertWorkflowDefinition(ctx, parentDef)
	if err != nil {
		t.Fatalf("upsert parent def: %v", err)
	}

	parentRun := domain.NewSagaRun("wf_parent_quorum", parentDefID, nil, map[string]any{})
	if err := s.CreateRun(ctx, parentRun); err != nil {
		t.Fatalf("create parent run: %v", err)
	}
	// Place parent into running-on-parallel_step, then paused+awaiting-signal.
	if err := s.UpdateRunState(ctx, parentRun.ID, domain.RunStateRunning, "parallel_step"); err != nil {
		t.Fatalf("set current step: %v", err)
	}
	if err := s.SetPausedAwaitingSignal(ctx, parentRun.ID, "__child_join__", nil); err != nil {
		t.Fatalf("pause parent: %v", err)
	}
	// Re-read to get the fully-constructed run (DefinitionID needed by checkParentJoin).
	run, err := s.GetRun(ctx, parentRun.ID)
	if err != nil {
		t.Fatalf("get parent run: %v", err)
	}
	return run
}

// spawnTrivialChild creates a child run linked to the given parent/step with a
// trivial "end-only" definition. Callers call coord.Advance to drive it to succeeded.
func spawnTrivialChild(t *testing.T, s *memory.Store, ctx context.Context, parentRun domain.SagaRun, key string) string {
	t.Helper()
	childDef := domain.WorkflowDefinition{
		ID:        "wf_child_" + key,
		Version:   1,
		Name:      "ChildQuorum_" + key,
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	childID, err := s.SpawnChildRun(ctx, parentRun.ID, "parallel_step", key, childDef, map[string]any{})
	if err != nil {
		t.Fatalf("spawn child %s: %v", key, err)
	}
	return childID.String()
}

func newQuorumCoordinator(s *memory.Store) *Coordinator {
	return NewCoordinator(s, nil /*no publisher*/, clock.SystemClock{},
		secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
}

// ---------------------------------------------------------------------------
// Test 1: 2-of-3 quorum — parent wakes after 2nd succeeded, not 1st.
// ---------------------------------------------------------------------------

func TestChildJoinQuorum_2of3_WakesAfterSecond(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentWithQuorum(t, s, ctx, "quorum", 2)
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	c2 := spawnTrivialChild(t, s, ctx, parentRun, "b1")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b2") // 3rd — left running

	coord := newQuorumCoordinator(s)

	// Child 1 → succeeded (1 of 3). Quorum (2) not yet met.
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance child1: %v", err)
	}
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken after only 1 of 3 succeeded — expected to wait for quorum=2")
	}

	// Child 2 → succeeded (2 of 3). Quorum met — parent should be woken.
	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance child2: %v", err)
	}
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent NOT woken after 2 of 3 succeeded — expected quorum wake")
	}
	if parent.State != domain.RunStatePaused {
		t.Errorf("parent state = %s, want paused (publisher nil so advance not re-queued)", parent.State)
	}
}

// ---------------------------------------------------------------------------
// Test 2: only 1 of 3 succeeded — parent NOT woken.
// ---------------------------------------------------------------------------

func TestChildJoinQuorum_1of3_NotWoken(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentWithQuorum(t, s, ctx, "quorum", 2)
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b1")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b2")

	coord := newQuorumCoordinator(s)

	// Only child 1 finishes. Quorum (2) not met.
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance child1: %v", err)
	}
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken with only 1 of 3 succeeded — quorum=2 not yet met")
	}
}

// ---------------------------------------------------------------------------
// Test 3: quorum fires on exactly the Nth success, not before.
// ---------------------------------------------------------------------------

func TestChildJoinQuorum_NthSuccessFiresWake(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentWithQuorum(t, s, ctx, "quorum", 3)
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	c2 := spawnTrivialChild(t, s, ctx, parentRun, "b1")
	c3 := spawnTrivialChild(t, s, ctx, parentRun, "b2")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b3") // 4th — never advances

	coord := newQuorumCoordinator(s)

	// 1st success — not woken (need 3).
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance c1: %v", err)
	}
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken too early (after 1 of 4)")
	}

	// 2nd success — still not woken (need 3).
	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance c2: %v", err)
	}
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken too early (after 2 of 4)")
	}

	// 3rd success — quorum met, parent woken.
	if err := coord.Advance(ctx, c3); err != nil {
		t.Fatalf("advance c3: %v", err)
	}
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent NOT woken after 3rd success — quorum=3 should have fired")
	}
}

// ---------------------------------------------------------------------------
// Test 4: "all" regression — explicit "all" waits for all terminal.
// ---------------------------------------------------------------------------

func TestChildJoinAll_ExplicitAll_WaitsForAll(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentWithQuorum(t, s, ctx, "all", 0 /*ignored for all*/)
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	c2 := spawnTrivialChild(t, s, ctx, parentRun, "b1")

	coord := newQuorumCoordinator(s)

	// Child 1 done — NOT woken (child 2 still running).
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance c1: %v", err)
	}
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken after child1 only — 'all' strategy should wait for child2")
	}

	// Child 2 done — all terminal, parent woken.
	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance c2: %v", err)
	}
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent NOT woken after all children finished — 'all' strategy failed")
	}
}

// ---------------------------------------------------------------------------
// Test 6: quorum_n stored as CEL string in Variables — parent wakes correctly.
// ---------------------------------------------------------------------------

func TestChildJoin_QuorumN_FromVariables(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Build parent definition with quorum_n as a CEL string.
	stepInputs := map[string]any{
		"join_strategy": "quorum",
		"quorum_n":      "_config.quorum_n", // CEL string
		"branches":      []any{},
	}
	parentDef := domain.WorkflowDefinition{
		ID:        "wf_parent_cel_quorum",
		Version:   1,
		Name:      "ParentCELQuorum",
		Published: true,
		Start:     "parallel_step",
		Steps: []domain.Step{
			{
				ID:     "parallel_step",
				Type:   domain.StepTypeParallel,
				Inputs: stepInputs,
				Next:   "end",
			},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	parentDefID, err := s.UpsertWorkflowDefinition(ctx, parentDef)
	if err != nil {
		t.Fatalf("upsert parent def: %v", err)
	}

	// Parent Variables contain _config.quorum_n = 2.
	parentRun := domain.NewSagaRun("wf_parent_cel_quorum", parentDefID, nil, map[string]any{})
	if err := s.CreateRun(ctx, parentRun); err != nil {
		t.Fatalf("create parent run: %v", err)
	}
	// Inject _config into Variables (quorum config for CEL resolution).
	if err := s.UpdateRunVariables(ctx, parentRun.ID, map[string]any{
		"_config": map[string]any{"quorum_n": int64(2)},
	}); err != nil {
		t.Fatalf("set _config variable: %v", err)
	}
	if err := s.UpdateRunState(ctx, parentRun.ID, domain.RunStateRunning, "parallel_step"); err != nil {
		t.Fatalf("set current step: %v", err)
	}
	if err := s.SetPausedAwaitingSignal(ctx, parentRun.ID, "__child_join__", nil); err != nil {
		t.Fatalf("pause parent: %v", err)
	}
	parentRun, err = s.GetRun(ctx, parentRun.ID)
	if err != nil {
		t.Fatalf("get parent run: %v", err)
	}

	// Spawn 3 children; advance 2 → quorum_n=2 should fire.
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	c2 := spawnTrivialChild(t, s, ctx, parentRun, "b1")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b2") // 3rd left running

	coord := newQuorumCoordinator(s)

	// First success — quorum not yet met.
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance c1: %v", err)
	}
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken after 1 of 3 — quorum=2 not yet met")
	}

	// Second success — quorum=2 met (resolved from _config.quorum_n via CEL).
	if err := coord.Advance(ctx, c2); err != nil {
		t.Fatalf("advance c2: %v", err)
	}
	parent, _ = s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Error("parent NOT woken after 2nd success — CEL quorum_n=2 should have fired")
	}
}

// ---------------------------------------------------------------------------
// Test 5: failed children do NOT count toward quorum.
// ---------------------------------------------------------------------------

func TestChildJoinQuorum_FailedChildNotCounted(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentWithQuorum(t, s, ctx, "quorum", 2)
	c1 := spawnTrivialChild(t, s, ctx, parentRun, "b0")
	c2 := spawnTrivialChild(t, s, ctx, parentRun, "b1")
	_ = spawnTrivialChild(t, s, ctx, parentRun, "b2")

	coord := newQuorumCoordinator(s)

	// Child 1 → succeeded (1 success so far).
	if err := coord.Advance(ctx, c1); err != nil {
		t.Fatalf("advance c1: %v", err)
	}

	// Child 2 → manually set to failed and trigger checkParentJoin.
	c2ID, err := uuid.Parse(c2)
	if err != nil {
		t.Fatalf("parse c2 id: %v", err)
	}
	if err := s.UpdateRunState(ctx, c2ID, domain.RunStateFailed, "end"); err != nil {
		t.Fatalf("set child2 failed: %v", err)
	}
	failedRun, _ := s.GetRun(ctx, c2ID)
	coord.checkParentJoin(ctx, failedRun)

	// Only 1 succeeded (failed doesn't count toward quorum=2) — parent NOT woken.
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal == nil {
		t.Error("parent woken prematurely — a failed child should not count toward quorum")
	}
}
