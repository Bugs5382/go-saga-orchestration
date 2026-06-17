package memory

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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// makeRun builds a minimal SagaRun with provided fields.
func makeRun(workflowID string, state domain.RunState, startedAt time.Time) domain.SagaRun {
	return domain.SagaRun{
		ID:           uuid.New(),
		WorkflowID:   workflowID,
		DefinitionID: uuid.New(),
		State:        state,
		Inputs:       map[string]any{},
		Variables:    map[string]any{},
		StartedAt:    startedAt,
		LastEventAt:  startedAt,
	}
}

// TestListRuns_Unfiltered verifies that all runs are returned with default
// limit=50 applied.
func TestListRuns_Unfiltered(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	for i := 0; i < 3; i++ {
		r := makeRun("wf_a", domain.RunStateSucceeded, now.Add(-time.Duration(i)*time.Minute))
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	runs, err := s.ListRuns(ctx, store.RunFilter{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Errorf("len(runs) = %d, want 3", len(runs))
	}
	// Verify DESC order (most recent first).
	for i := 1; i < len(runs); i++ {
		if runs[i].StartedAt.After(runs[i-1].StartedAt) {
			t.Errorf("runs not sorted DESC at index %d", i)
		}
	}
}

// TestListRuns_FilterWorkflowID verifies WorkflowID filter.
func TestListRuns_FilterWorkflowID(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-1*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateRunning, now.Add(-2*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_b", domain.RunStateFailed, now.Add(-3*time.Minute)))

	runs, err := s.ListRuns(ctx, store.RunFilter{WorkflowID: "wf_b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Errorf("len = %d, want 1", len(runs))
	}
	if runs[0].WorkflowID != "wf_b" {
		t.Errorf("workflow_id = %q", runs[0].WorkflowID)
	}
}

// TestListRuns_FilterState verifies State filter.
func TestListRuns_FilterState(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-1*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateFailed, now.Add(-2*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateFailed, now.Add(-3*time.Minute)))

	runs, err := s.ListRuns(ctx, store.RunFilter{State: "failed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Errorf("len = %d, want 2 failed", len(runs))
	}
}

// TestListRuns_FilterSince verifies Since filter.
func TestListRuns_FilterSince(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-30*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-5*time.Minute)))

	threshold := now.Add(-10 * time.Minute)
	runs, err := s.ListRuns(ctx, store.RunFilter{Since: &threshold})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Errorf("len = %d, want 1 recent run", len(runs))
	}
}

// TestListRuns_HasError verifies HasError filter (maps to state==failed in v1).
func TestListRuns_HasError(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-1*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_a", domain.RunStateFailed, now.Add(-2*time.Minute)))

	hasErr := true
	runs, err := s.ListRuns(ctx, store.RunFilter{HasError: &hasErr})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Errorf("len = %d, want 1 failed run", len(runs))
	}
}

// TestListRuns_Pagination verifies Limit + Offset.
func TestListRuns_Pagination(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		s.CreateRun(ctx, makeRun("wf_a", domain.RunStateSucceeded, now.Add(-time.Duration(i)*time.Minute)))
	}

	// Page 1: first 2.
	runs, err := s.ListRuns(ctx, store.RunFilter{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Errorf("page1 len = %d, want 2", len(runs))
	}

	// Page 2: offset=2 limit=2.
	runs2, err := s.ListRuns(ctx, store.RunFilter{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs2) != 2 {
		t.Errorf("page2 len = %d, want 2", len(runs2))
	}

	// Pages should not overlap.
	ids1 := map[uuid.UUID]bool{runs[0].ID: true, runs[1].ID: true}
	for _, r := range runs2 {
		if ids1[r.ID] {
			t.Errorf("overlap: id %s in both pages", r.ID)
		}
	}
}

// TestCountRuns verifies that CountRuns ignores Limit/Offset.
func TestCountRuns(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		s.CreateRun(ctx, makeRun("wf_count", domain.RunStateSucceeded, now.Add(-time.Duration(i)*time.Minute)))
	}

	count, err := s.CountRuns(ctx, store.RunFilter{WorkflowID: "wf_count", Limit: 2, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5 (Limit/Offset ignored)", count)
	}
}

// TestStatsForWorkflow_NoRuns returns null rates and zero in_flight.
func TestStatsForWorkflow_NoRuns(t *testing.T) {
	s := New()
	stats, err := s.StatsForWorkflow(context.Background(), "no_runs_wf")
	if err != nil {
		t.Fatal(err)
	}
	if stats.SuccessRate24h != nil {
		t.Errorf("SuccessRate24h = %v, want nil", stats.SuccessRate24h)
	}
	if stats.LastRunAt != nil {
		t.Errorf("LastRunAt = %v, want nil", stats.LastRunAt)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight = %d, want 0", stats.InFlight)
	}
}

// TestStatsForWorkflow_Mixed tests mixed run states.
func TestStatsForWorkflow_Mixed(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	// 5 succeeded in window.
	for i := 0; i < 5; i++ {
		s.CreateRun(ctx, makeRun("wf_stat", domain.RunStateSucceeded, now.Add(-time.Duration(i+1)*time.Hour)))
	}
	// 1 failed in window.
	s.CreateRun(ctx, makeRun("wf_stat", domain.RunStateFailed, now.Add(-6*time.Hour)))
	// 2 in-flight.
	s.CreateRun(ctx, makeRun("wf_stat", domain.RunStateRunning, now.Add(-10*time.Minute)))
	s.CreateRun(ctx, makeRun("wf_stat", domain.RunStatePaused, now.Add(-20*time.Minute)))

	stats, err := s.StatsForWorkflow(ctx, "wf_stat")
	if err != nil {
		t.Fatal(err)
	}

	if stats.SuccessRate24h == nil {
		t.Fatal("SuccessRate24h is nil")
	}
	want := 5.0 / 6.0
	if diff := *stats.SuccessRate24h - want; diff > 0.001 || diff < -0.001 {
		t.Errorf("SuccessRate24h = %f, want ~%f", *stats.SuccessRate24h, want)
	}
	if stats.InFlight != 2 {
		t.Errorf("InFlight = %d, want 2", stats.InFlight)
	}
	if stats.LastRunAt == nil {
		t.Error("LastRunAt is nil")
	}
}

// TestStatsForWorkflow_OldRunsExcluded verifies old runs affect last_run_at
// but not success_rate_24h.
func TestStatsForWorkflow_OldRunsExcluded(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Now().UTC()

	// Old runs (outside 24h window).
	s.CreateRun(ctx, makeRun("wf_old", domain.RunStateSucceeded, now.Add(-30*time.Hour)))
	s.CreateRun(ctx, makeRun("wf_old", domain.RunStateFailed, now.Add(-25*time.Hour)))

	// One recent run.
	s.CreateRun(ctx, makeRun("wf_old", domain.RunStateSucceeded, now.Add(-1*time.Hour)))

	stats, err := s.StatsForWorkflow(ctx, "wf_old")
	if err != nil {
		t.Fatal(err)
	}

	// Only recent succeeded in window → rate = 1.0.
	if stats.SuccessRate24h == nil || *stats.SuccessRate24h != 1.0 {
		t.Errorf("SuccessRate24h = %v, want 1.0", stats.SuccessRate24h)
	}
	// last_run_at must be set (old runs count).
	if stats.LastRunAt == nil {
		t.Error("LastRunAt is nil, want a timestamp")
	}
}
