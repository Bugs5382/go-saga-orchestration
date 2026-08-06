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
	"bytes"
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

// buildWorkflowDefsRouter wires the workflow definition CRUD handlers.
func buildWorkflowDefsRouter(s *memory.Store) *chi.Mux {
	h := NewWorkflowHandler(s)
	r := chi.NewRouter()
	r.Get("/api/v1/workflows", h.List)
	r.Get("/api/v1/workflows/{id}", h.Get)
	r.Post("/api/v1/workflows", h.Save)
	return r
}

// defFixture builds a minimal single-step workflow definition.
func defFixture(id string, version int, published bool) domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID:        id,
		Version:   version,
		Name:      id,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: published,
	}
}

// TestWorkflowList_Unfiltered — seed 3 defs; expect 3 back newest-first.
func TestWorkflowList_Unfiltered(t *testing.T) {
	s := memory.New()
	now := time.Now().UTC()
	a := defFixture("wf_a", 1, true)
	a.CreatedAt = now.Add(-2 * time.Minute)
	b := defFixture("wf_b", 1, false)
	b.CreatedAt = now.Add(-1 * time.Minute)
	c := defFixture("wf_c", 1, true)
	c.CreatedAt = now
	for _, d := range []domain.WorkflowDefinition{a, b, c} {
		if _, err := s.UpsertWorkflowDefinition(context.Background(), d); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	r := buildWorkflowDefsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Workflows []domain.WorkflowDefinition `json:"workflows"`
		Total     int                         `json:"total"`
		Limit     int                         `json:"limit"`
		Offset    int                         `json:"offset"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Workflows) != 3 || resp.Total != 3 {
		t.Fatalf("workflows = %d total = %d, want 3/3", len(resp.Workflows), resp.Total)
	}
	if resp.Workflows[0].ID != "wf_c" {
		t.Errorf("newest = %q, want wf_c", resp.Workflows[0].ID)
	}
	if resp.Limit != 50 || resp.Offset != 0 {
		t.Errorf("limit/offset = %d/%d, want 50/0", resp.Limit, resp.Offset)
	}
}

// TestWorkflowList_PublishedFilter — published=true returns only published defs.
func TestWorkflowList_PublishedFilter(t *testing.T) {
	s := memory.New()
	for _, d := range []domain.WorkflowDefinition{
		defFixture("pub_1", 1, true),
		defFixture("draft_1", 1, false),
		defFixture("pub_2", 1, true),
	} {
		if _, err := s.UpsertWorkflowDefinition(context.Background(), d); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	r := buildWorkflowDefsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows?published=true", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Workflows []domain.WorkflowDefinition `json:"workflows"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Workflows) != 2 {
		t.Fatalf("workflows = %d, want 2 published", len(resp.Workflows))
	}
	for _, d := range resp.Workflows {
		if !d.Published {
			t.Errorf("returned draft %q under published=true", d.ID)
		}
	}
}

// TestWorkflowList_BadPublished — a non-bool published param is a 400.
func TestWorkflowList_BadPublished(t *testing.T) {
	s := memory.New()
	r := buildWorkflowDefsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows?published=maybe", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// TestWorkflowSave_PersistsAndRoundTrips — POST a valid def, then GET it by the
// returned definition_id.
func TestWorkflowSave_PersistsAndRoundTrips(t *testing.T) {
	s := memory.New()
	r := buildWorkflowDefsRouter(s)

	def := defFixture("wf_save", 3, true)
	body, _ := json.Marshal(def)
	req := httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("save status = %d, body = %s", w.Code, w.Body.String())
	}
	var saved struct {
		DefinitionID uuid.UUID `json:"definition_id"`
		ID           string    `json:"id"`
		Version      int       `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &saved); err != nil {
		t.Fatalf("decode save: %v", err)
	}
	if saved.DefinitionID == uuid.Nil {
		t.Fatal("definition_id = nil, want a storage id")
	}
	if saved.ID != "wf_save" || saved.Version != 3 {
		t.Errorf("saved def = %s v%d, want wf_save v3", saved.ID, saved.Version)
	}

	// Round-trip via Get.
	getReq := httptest.NewRequest("GET", "/api/v1/workflows/"+saved.DefinitionID.String(), nil)
	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getW.Code, getW.Body.String())
	}
	var got domain.WorkflowDefinition
	if err := json.Unmarshal(getW.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if got.ID != "wf_save" || got.Version != 3 {
		t.Errorf("round-trip def = %s v%d, want wf_save v3", got.ID, got.Version)
	}
}

// TestWorkflowSave_ValidationFails — an invalid definition (entrypoint pointing
// at a missing step) is rejected with 400.
func TestWorkflowSave_ValidationFails(t *testing.T) {
	s := memory.New()
	r := buildWorkflowDefsRouter(s)

	def := defFixture("wf_bad", 1, true)
	def.Entrypoints = map[string]string{"alt": "no_such_step"}
	body, _ := json.Marshal(def)
	req := httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for invalid definition; body = %s", w.Code, w.Body.String())
	}
}

// TestWorkflowGet_NotFound — a random id yields 404.
func TestWorkflowGet_NotFound(t *testing.T) {
	s := memory.New()
	r := buildWorkflowDefsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// TestWorkflowGet_BadID — a non-uuid id yields 400.
func TestWorkflowGet_BadID(t *testing.T) {
	s := memory.New()
	r := buildWorkflowDefsRouter(s)
	req := httptest.NewRequest("GET", "/api/v1/workflows/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
