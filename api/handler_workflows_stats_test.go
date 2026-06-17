package api

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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// buildWorkflowStatsRouter wires the workflow stats handler onto a chi router.
func buildWorkflowStatsRouter(s *memory.Store) *chi.Mux {
	h := NewWorkflowHandler(s)
	r := chi.NewRouter()
	r.Get("/api/v1/workflows/{wf_id}/stats", h.Stats)
	return r
}

// seedRunAt is a convenience wrapper for creating runs with explicit StartedAt.
func seedRunAt(t *testing.T, s *memory.Store, workflowID string, state domain.RunState, startedAt time.Time) {
	t.Helper()
	run := domain.SagaRun{
		ID:           uuid.New(),
		WorkflowID:   workflowID,
		DefinitionID: uuid.New(),
		State:        state,
		Inputs:       map[string]any{},
		Variables:    map[string]any{},
		StartedAt:    startedAt,
		LastEventAt:  startedAt,
	}
	if err := s.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("seedRunAt: %v", err)
	}
}

// TestWorkflowStats_NoRuns — workflow with no runs: SuccessRate24h=null, LastRunAt=null, InFlight=0.
func TestWorkflowStats_NoRuns(t *testing.T) {
	s := memory.New()
	r := buildWorkflowStatsRouter(s)

	req := httptest.NewRequest("GET", "/api/v1/workflows/never_run_wf/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp struct {
		WorkflowID     string   `json:"workflow_id"`
		SuccessRate24h *float64 `json:"success_rate_24h"`
		LastRunAt      *string  `json:"last_run_at"`
		InFlight       int      `json:"in_flight"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.WorkflowID != "never_run_wf" {
		t.Errorf("workflow_id = %q, want never_run_wf", resp.WorkflowID)
	}
	if resp.SuccessRate24h != nil {
		t.Errorf("success_rate_24h = %v, want null", resp.SuccessRate24h)
	}
	if resp.LastRunAt != nil {
		t.Errorf("last_run_at = %v, want null", resp.LastRunAt)
	}
	if resp.InFlight != 0 {
		t.Errorf("in_flight = %d, want 0", resp.InFlight)
	}
}

// TestWorkflowStats_MixedRuns — 5 succeeded + 1 failed in last 24h, 2 running.
// Expect success_rate_24h = 5/6 ≈ 0.833; in_flight = 2.
func TestWorkflowStats_MixedRuns(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()

	// In-window runs.
	seedRunAt(t, s, "wf_mixed", domain.RunStateSucceeded, now.Add(-1*time.Hour))
	seedRunAt(t, s, "wf_mixed", domain.RunStateSucceeded, now.Add(-2*time.Hour))
	seedRunAt(t, s, "wf_mixed", domain.RunStateSucceeded, now.Add(-3*time.Hour))
	seedRunAt(t, s, "wf_mixed", domain.RunStateSucceeded, now.Add(-4*time.Hour))
	seedRunAt(t, s, "wf_mixed", domain.RunStateSucceeded, now.Add(-5*time.Hour))
	seedRunAt(t, s, "wf_mixed", domain.RunStateFailed, now.Add(-6*time.Hour))

	// In-flight (running, non-terminal).
	seedRunAt(t, s, "wf_mixed", domain.RunStateRunning, now.Add(-10*time.Minute))
	seedRunAt(t, s, "wf_mixed", domain.RunStatePaused, now.Add(-20*time.Minute))

	r := buildWorkflowStatsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows/wf_mixed/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp struct {
		WorkflowID     string   `json:"workflow_id"`
		SuccessRate24h *float64 `json:"success_rate_24h"`
		LastRunAt      *string  `json:"last_run_at"`
		InFlight       int      `json:"in_flight"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.SuccessRate24h == nil {
		t.Fatal("success_rate_24h is null, want ~0.833")
	}
	// 5/6 ≈ 0.8333…  Allow ±0.001 tolerance.
	want := 5.0 / 6.0
	if diff := *resp.SuccessRate24h - want; diff > 0.001 || diff < -0.001 {
		t.Errorf("success_rate_24h = %f, want ~%f", *resp.SuccessRate24h, want)
	}

	if resp.InFlight != 2 {
		t.Errorf("in_flight = %d, want 2", resp.InFlight)
	}

	if resp.LastRunAt == nil {
		t.Error("last_run_at is null, want a timestamp")
	}
}

// TestWorkflowStats_OldRunsExcludedFrom24h — runs older than 24h count toward
// last_run_at but NOT toward success_rate_24h.
func TestWorkflowStats_OldRunsExcludedFrom24h(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()

	// Old runs (outside 24h window): these should be excluded from rate.
	seedRunAt(t, s, "wf_old", domain.RunStateSucceeded, now.Add(-25*time.Hour)) // oldest
	seedRunAt(t, s, "wf_old", domain.RunStateFailed, now.Add(-26*time.Hour))

	// One recent run (inside window).
	seedRunAt(t, s, "wf_old", domain.RunStateSucceeded, now.Add(-1*time.Hour)) // most recent

	r := buildWorkflowStatsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows/wf_old/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp struct {
		SuccessRate24h *float64 `json:"success_rate_24h"`
		LastRunAt      *string  `json:"last_run_at"`
		InFlight       int      `json:"in_flight"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Only 1 succeeded in window → rate = 1.0.
	if resp.SuccessRate24h == nil {
		t.Fatal("success_rate_24h is null, want 1.0 (only recent succeeded run in window)")
	}
	if *resp.SuccessRate24h != 1.0 {
		t.Errorf("success_rate_24h = %f, want 1.0 (old runs excluded)", *resp.SuccessRate24h)
	}

	// last_run_at must be present (the old runs still count toward it).
	if resp.LastRunAt == nil {
		t.Error("last_run_at is null, want a timestamp")
	}
}
