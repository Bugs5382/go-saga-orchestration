package engine

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// makeParentForAggregate creates a parent run with a parallel step that uses
// the given join strategy. Returns the parent SagaRun and its definition ID.
func makeParentForAggregate(t *testing.T, s *memory.Store, ctx context.Context, joinStrategy string, quorumN int) domain.SagaRun {
	t.Helper()
	stepInputs := map[string]any{
		"join_strategy": joinStrategy,
		"quorum_n":      quorumN,
		"branches":      []any{},
	}
	parentDef := domain.WorkflowDefinition{
		ID:        "wf_agg_parent",
		Version:   1,
		Name:      "AggParent",
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
	parentRun := domain.NewSagaRun("wf_agg_parent", parentDefID, nil, map[string]any{})
	if err := s.CreateRun(ctx, parentRun); err != nil {
		t.Fatalf("create parent run: %v", err)
	}
	if err := s.UpdateRunState(ctx, parentRun.ID, domain.RunStateRunning, "parallel_step"); err != nil {
		t.Fatalf("set current step: %v", err)
	}
	if err := s.SetPausedAwaitingSignal(ctx, parentRun.ID, "__child_join__", nil); err != nil {
		t.Fatalf("pause parent: %v", err)
	}
	run, err := s.GetRun(ctx, parentRun.ID)
	if err != nil {
		t.Fatalf("get parent run: %v", err)
	}
	return run
}

// spawnChildForAggregate creates a child run with a manual_approval step so
// tests can submit a user task against it. The child workflow has:
//
//	start → manual_approval_step → end
//
// but we advance via direct store manipulation in tests that need a submitted
// user_task, because the manual_approval verb itself would pause the run.
// For trivial (no user_task) children we use the end-only form.
func spawnChildWithKey(t *testing.T, s *memory.Store, ctx context.Context, parentRun domain.SagaRun, branchKey string) uuid.UUID {
	t.Helper()
	childDef := domain.WorkflowDefinition{
		ID:        "wf_child_agg_" + branchKey,
		Version:   1,
		Name:      "AggChild_" + branchKey,
		Published: true,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	childID, err := s.SpawnChildRun(ctx, parentRun.ID, "parallel_step", branchKey, childDef, map[string]any{})
	if err != nil {
		t.Fatalf("spawn child %s: %v", branchKey, err)
	}
	return childID
}

// submitTaskForChild creates and submits a user_task on the given child run.
func submitTaskForChild(t *testing.T, s *memory.Store, ctx context.Context, childRunID uuid.UUID, vote string) {
	t.Helper()
	task := domain.UserTask{
		ID:       uuid.New(),
		RunID:    childRunID,
		StepID:   "approval",
		Assignee: "reviewer",
	}
	if err := s.CreateUserTask(ctx, task); err != nil {
		t.Fatalf("create user_task: %v", err)
	}
	if err := s.SubmitUserTask(ctx, task.ID, "u_"+vote, map[string]any{"vote": vote}); err != nil {
		t.Fatalf("submit user_task: %v", err)
	}
}

// newAggCoordinator builds a minimal coordinator for aggregation tests.
func newAggCoordinator(s *memory.Store) *Coordinator {
	return NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
}

// ---------------------------------------------------------------------------
// Test 1: all-join aggregation — 3 children + 3 submitted user_tasks.
// Parent.Variables._parallel.parallel_step.branches has 3 entries each with
// the corresponding _user_task.result.vote.
// ---------------------------------------------------------------------------

func TestAggregateChildResults_AllJoin_ThreeBranches(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentForAggregate(t, s, ctx, "all", 0)

	votes := []string{"approve", "reject", "approve"}
	childIDs := make([]uuid.UUID, len(votes))
	for i, vote := range votes {
		key := "u" + string(rune('1'+i))
		cid := spawnChildWithKey(t, s, ctx, parentRun, key)
		childIDs[i] = cid
		submitTaskForChild(t, s, ctx, cid, vote)
	}

	coord := newAggCoordinator(s)
	// Advance all 3 children to succeeded.
	for i, cid := range childIDs {
		if err := coord.Advance(ctx, cid.String()); err != nil {
			t.Fatalf("advance child%d: %v", i, err)
		}
	}

	// Parent should be woken now.
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Fatal("parent not woken after all children finished")
	}

	// Check Variables._parallel.parallel_step.branches.
	parallel, ok := parent.Variables["_parallel"].(map[string]any)
	if !ok {
		t.Fatalf("Variables._parallel not a map, got %T: %v", parent.Variables["_parallel"], parent.Variables)
	}
	stepData, ok := parallel["parallel_step"].(map[string]any)
	if !ok {
		t.Fatalf("Variables._parallel.parallel_step not a map, got %T", parallel["parallel_step"])
	}
	branches, ok := stepData["branches"].([]any)
	if !ok {
		t.Fatalf("Variables._parallel.parallel_step.branches not a []any, got %T", stepData["branches"])
	}
	if len(branches) != 3 {
		t.Fatalf("expected 3 branches, got %d", len(branches))
	}

	// Build a map of key -> vote from branches for stable assertion.
	gotVotes := map[string]string{}
	for _, b := range branches {
		entry := b.(map[string]any)
		key := entry["key"].(string)
		ut, hasUT := entry["_user_task"]
		if !hasUT {
			t.Errorf("branch %q missing _user_task", key)
			continue
		}
		utMap := ut.(map[string]any)
		result := utMap["result"].(map[string]any)
		gotVotes[key] = result["vote"].(string)
	}

	wantVotes := map[string]string{"u1": "approve", "u2": "reject", "u3": "approve"}
	for k, want := range wantVotes {
		got, ok := gotVotes[k]
		if !ok {
			t.Errorf("branch %q not found in aggregated results", k)
			continue
		}
		if got != want {
			t.Errorf("branch %q vote = %q, want %q", k, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 2: quorum-join aggregation — 5 children, quorum_n=3, 3 succeed.
// Parent wakes with 3 entries (3 succeeded); 2 still-running children
// exist but the parent was already woken by the quorum.
// ---------------------------------------------------------------------------

func TestAggregateChildResults_QuorumJoin_ThreeOfFive(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentForAggregate(t, s, ctx, "quorum", 3)

	// Spawn 5 children; submit tasks on the first 3.
	childIDs := make([]uuid.UUID, 5)
	for i := range childIDs {
		key := "u" + string(rune('1'+i))
		cid := spawnChildWithKey(t, s, ctx, parentRun, key)
		childIDs[i] = cid
		if i < 3 {
			submitTaskForChild(t, s, ctx, cid, "approve")
		}
	}

	coord := newAggCoordinator(s)

	// Advance only the first 3 (quorum) children.
	for i := 0; i < 3; i++ {
		if err := coord.Advance(ctx, childIDs[i].String()); err != nil {
			t.Fatalf("advance child%d: %v", i, err)
		}
	}

	// Parent should be woken after 3 succeeds (quorum met).
	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Fatal("parent not woken after quorum (3 of 5) reached")
	}

	// Verify branches were written. We don't know exactly which 3 out of 5
	// were aggregated (depends on siblings list ordering), but we expect
	// at least 3 entries and each succeeded entry should carry the user_task.
	parallel, ok := parent.Variables["_parallel"].(map[string]any)
	if !ok {
		t.Fatalf("Variables._parallel not a map: %T", parent.Variables["_parallel"])
	}
	stepData := parallel["parallel_step"].(map[string]any)
	branches := stepData["branches"].([]any)

	// All 5 siblings are listed, but only 3 have succeeded (and have user_tasks).
	// The aggregate covers all siblings at the time of wake.
	succeededCount := 0
	for _, b := range branches {
		entry := b.(map[string]any)
		if entry["state"] == "succeeded" {
			succeededCount++
			if _, hasUT := entry["_user_task"]; !hasUT {
				t.Errorf("succeeded branch %q missing _user_task", entry["key"])
			}
		}
	}
	if succeededCount != 3 {
		t.Errorf("expected 3 succeeded branches, got %d", succeededCount)
	}
}

// ---------------------------------------------------------------------------
// Test 3: child without a user_task — entry has key + variables + state but
// no _user_task key.
// ---------------------------------------------------------------------------

func TestAggregateChildResults_NoUserTask(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentForAggregate(t, s, ctx, "all", 0)
	cid := spawnChildWithKey(t, s, ctx, parentRun, "branch_noop")
	// No user_task created.

	coord := newAggCoordinator(s)
	if err := coord.Advance(ctx, cid.String()); err != nil {
		t.Fatalf("advance child: %v", err)
	}

	parent, _ := s.GetRun(ctx, parentRun.ID)
	if parent.AwaitedSignal != nil {
		t.Fatal("parent not woken after single child finished")
	}

	parallel := parent.Variables["_parallel"].(map[string]any)
	stepData := parallel["parallel_step"].(map[string]any)
	branches := stepData["branches"].([]any)
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch, got %d", len(branches))
	}

	entry := branches[0].(map[string]any)
	if _, hasUT := entry["_user_task"]; hasUT {
		t.Error("branch with no user_task should not have _user_task key")
	}
	if entry["key"] != "branch_noop" {
		t.Errorf("branch key = %q, want branch_noop", entry["key"])
	}
	if entry["state"] != "succeeded" {
		t.Errorf("branch state = %q, want succeeded", entry["state"])
	}
}

// ---------------------------------------------------------------------------
// Test 4: multiple user_tasks on one child — first by ID order wins.
// (domain.UserTask has no CreatedAt, so ID-order is used as proxy.)
// ---------------------------------------------------------------------------

func TestAggregateChildResults_MultipleUserTasks_FirstWins(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	parentRun := makeParentForAggregate(t, s, ctx, "all", 0)
	cid := spawnChildWithKey(t, s, ctx, parentRun, "branch_multi")

	// Create two tasks with a small sleep to ensure different IDs (v4 UUIDs are random,
	// so ID ordering is random — we just pin which task ID is "first" by using
	// known UUIDs where the first is lexicographically smaller).
	firstID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	secondID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")

	taskA := domain.UserTask{ID: firstID, RunID: cid, StepID: "step1", Assignee: "a"}
	taskB := domain.UserTask{ID: secondID, RunID: cid, StepID: "step2", Assignee: "b"}

	_ = s.CreateUserTask(ctx, taskA)
	_ = s.CreateUserTask(ctx, taskB)

	now := time.Now().UTC()
	// Submit both (second submitted first in time, first by ID submitted second — doesn't matter).
	taskA.SubmittedAt = &now
	taskA.SubmittedBy = "actor_a"
	taskA.Result = map[string]any{"vote": "first_wins"}
	_ = s.SubmitUserTask(ctx, firstID, "actor_a", map[string]any{"vote": "first_wins"})
	_ = s.SubmitUserTask(ctx, secondID, "actor_b", map[string]any{"vote": "second_ignored"})

	coord := newAggCoordinator(s)
	if err := coord.Advance(ctx, cid.String()); err != nil {
		t.Fatalf("advance child: %v", err)
	}

	parent, _ := s.GetRun(ctx, parentRun.ID)
	parallel := parent.Variables["_parallel"].(map[string]any)
	stepData := parallel["parallel_step"].(map[string]any)
	branches := stepData["branches"].([]any)
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch, got %d", len(branches))
	}

	entry := branches[0].(map[string]any)
	ut, hasUT := entry["_user_task"]
	if !hasUT {
		t.Fatal("expected _user_task, got none")
	}
	result := ut.(map[string]any)["result"].(map[string]any)
	if result["vote"] != "first_wins" {
		t.Errorf("_user_task.result.vote = %q, want first_wins", result["vote"])
	}
}

// ---------------------------------------------------------------------------
// Test 5: ListUserTasksByRun returns empty list for unknown runID.
// ---------------------------------------------------------------------------

func TestListUserTasksByRun_UnknownRunID_Empty(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	tasks, err := s.ListUserTasksByRun(ctx, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected empty list, got %d tasks", len(tasks))
	}
}
