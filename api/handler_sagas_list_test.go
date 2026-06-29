package api

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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// seedRun creates and stores a SagaRun with the given parameters. startedAt is
// set explicitly so tests can reason about ordering and Since filters.
func seedRun(t *testing.T, s *memory.Store, workflowID string, state domain.RunState, startedAt time.Time) domain.SagaRun {
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
		t.Fatalf("seedRun: %v", err)
	}
	return run
}

// buildSagaListRouter wires the saga list handler onto a chi router.
func buildSagaListRouter(s *memory.Store) *chi.Mux {
	h := NewSagaHandler(s, &fakePublisher{})
	r := chi.NewRouter()
	r.Get("/api/v1/sagas", h.List)
	return r
}

// TestSagaList_Unfiltered — seed 3 runs across 2 workflows; expect 3 back.
func TestSagaList_Unfiltered(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	seedRun(t, s, "wf_a", domain.RunStateSucceeded, now.Add(-3*time.Minute))
	seedRun(t, s, "wf_a", domain.RunStateRunning, now.Add(-2*time.Minute))
	seedRun(t, s, "wf_b", domain.RunStateFailed, now.Add(-1*time.Minute))

	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp struct {
		Sagas  []domain.SagaRun `json:"sagas"`
		Total  int              `json:"total"`
		Limit  int              `json:"limit"`
		Offset int              `json:"offset"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Sagas) != 3 {
		t.Errorf("sagas len = %d, want 3", len(resp.Sagas))
	}
	if resp.Total != 3 {
		t.Errorf("total = %d, want 3", resp.Total)
	}
	if resp.Limit != 50 {
		t.Errorf("limit = %d, want 50 (default)", resp.Limit)
	}
	if resp.Offset != 0 {
		t.Errorf("offset = %d, want 0 (default)", resp.Offset)
	}
}

// TestSagaList_FilterByWorkflowID — only runs for wf_a returned.
func TestSagaList_FilterByWorkflowID(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	seedRun(t, s, "wf_a", domain.RunStateSucceeded, now.Add(-3*time.Minute))
	seedRun(t, s, "wf_a", domain.RunStateRunning, now.Add(-2*time.Minute))
	seedRun(t, s, "wf_b", domain.RunStateFailed, now.Add(-1*time.Minute))

	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?workflow_id=wf_a", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp struct {
		Sagas []domain.SagaRun `json:"sagas"`
		Total int              `json:"total"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Total)
	}
	for _, run := range resp.Sagas {
		if run.WorkflowID != "wf_a" {
			t.Errorf("got workflow_id=%q, want wf_a", run.WorkflowID)
		}
	}
}

// TestSagaList_FilterByState — only succeeded runs returned.
func TestSagaList_FilterByState(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	seedRun(t, s, "wf_a", domain.RunStateSucceeded, now.Add(-3*time.Minute))
	seedRun(t, s, "wf_a", domain.RunStateRunning, now.Add(-2*time.Minute))
	seedRun(t, s, "wf_b", domain.RunStateFailed, now.Add(-1*time.Minute))

	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?state=succeeded", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp struct {
		Sagas []domain.SagaRun `json:"sagas"`
		Total int              `json:"total"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 1 {
		t.Errorf("total = %d, want 1", resp.Total)
	}
	if len(resp.Sagas) != 1 || resp.Sagas[0].State != domain.RunStateSucceeded {
		t.Errorf("expected 1 succeeded run, got %+v", resp.Sagas)
	}
}

// TestSagaList_FilterBySince — only runs started on/after the threshold.
func TestSagaList_FilterBySince(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	old := now.Add(-10 * time.Minute)
	recent := now.Add(-1 * time.Minute)

	seedRun(t, s, "wf_a", domain.RunStateSucceeded, old)
	seedRun(t, s, "wf_a", domain.RunStateRunning, recent)

	threshold := now.Add(-5 * time.Minute)
	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", fmt.Sprintf("/api/v1/sagas?since=%s", threshold.Format(time.RFC3339)), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp struct {
		Sagas []domain.SagaRun `json:"sagas"`
		Total int              `json:"total"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 1 {
		t.Errorf("total = %d, want 1 (only recent run)", resp.Total)
	}
}

// TestSagaList_HasError — has_error=true returns only failed state runs.
func TestSagaList_HasError(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	seedRun(t, s, "wf_a", domain.RunStateSucceeded, now.Add(-3*time.Minute))
	seedRun(t, s, "wf_a", domain.RunStateFailed, now.Add(-2*time.Minute))
	seedRun(t, s, "wf_b", domain.RunStateFailed, now.Add(-1*time.Minute))

	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?has_error=true", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Sagas []domain.SagaRun `json:"sagas"`
		Total int              `json:"total"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 2 {
		t.Errorf("total = %d, want 2 failed runs", resp.Total)
	}
	for _, run := range resp.Sagas {
		if run.State != domain.RunStateFailed {
			t.Errorf("expected failed state, got %q", run.State)
		}
	}
}

// TestSagaList_Pagination — limit=2, offset=1 returns rows 2-3 (of 3 total).
func TestSagaList_Pagination(t *testing.T) {
	s := memory.New()
	// Seed 3 runs with predictable ordering (oldest first → newest first after DESC sort).
	now := time.Now().UTC()
	seedRun(t, s, "wf_a", domain.RunStateSucceeded, now.Add(-3*time.Minute)) // oldest → index 2 after DESC
	seedRun(t, s, "wf_a", domain.RunStateRunning, now.Add(-2*time.Minute))   // middle → index 1 after DESC
	seedRun(t, s, "wf_a", domain.RunStateFailed, now.Add(-1*time.Minute))    // newest → index 0 after DESC

	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?limit=2&offset=1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Sagas  []domain.SagaRun `json:"sagas"`
		Total  int              `json:"total"`
		Limit  int              `json:"limit"`
		Offset int              `json:"offset"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 3 {
		t.Errorf("total = %d, want 3 (count ignores limit/offset)", resp.Total)
	}
	if len(resp.Sagas) != 2 {
		t.Errorf("sagas len = %d, want 2 (offset=1 skips first, limit=2)", len(resp.Sagas))
	}
	if resp.Limit != 2 {
		t.Errorf("limit = %d, want 2", resp.Limit)
	}
	if resp.Offset != 1 {
		t.Errorf("offset = %d, want 1", resp.Offset)
	}
}

// TestSagaList_BadLimit — limit=501 → 400.
func TestSagaList_BadLimit(t *testing.T) {
	s := memory.New()
	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?limit=501", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for limit=501", w.Code)
	}
}

// TestSagaList_BadSince — malformed timestamp → 400.
func TestSagaList_BadSince(t *testing.T) {
	s := memory.New()
	r := buildSagaListRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/sagas?since=not-a-timestamp", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for bad since", w.Code)
	}
}
