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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// validRecordTransitionBody returns a minimal valid body for
// POST /api/v1/triggers with trigger_type=record_transition.
func validRecordTransitionBody() map[string]any {
	return map[string]any{
		"trigger_type": "record_transition",
		"workflow_id":  "approval_workflow_v1",
		"version":      1,
		"config": map[string]any{
			"record_type":   "change",
			"from_state":    "scheduled",
			"to_state":      "pending_approval",
			"input_mapping": map[string]any{"change_id": "$.record_id"},
		},
		"enabled":    true,
		"created_by": "admin",
	}
}

func newTriggerRouter(h *TriggerHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/api/v1/triggers", h.Create)
	r.Get("/api/v1/triggers", h.List)
	r.Get("/api/v1/triggers/{id}", h.Get)
	r.Delete("/api/v1/triggers/{id}", h.Delete)
	return r
}

// newTestHandler builds a TriggerHandler wired with the in-memory store,
// StubAllowAll licensing, and a FakeClock pinned to a fixed instant.
func newTestHandler() (*TriggerHandler, *chi.Mux) {
	s := memory.New()
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	h := NewTriggerHandler(s, licensing.StubAllowAll{}, clk)
	return h, newTriggerRouter(h)
}

// ---- Create ---------------------------------------------------------------

func TestTriggerHandler_Create_Valid(t *testing.T) {
	_, r := newTestHandler()

	body, _ := json.Marshal(validRecordTransitionBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp domain.SagaTrigger
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Errorf("expected non-nil ID in response, got zero UUID")
	}
	if resp.WorkflowID != "approval_workflow_v1" {
		t.Errorf("workflow_id = %q, want approval_workflow_v1", resp.WorkflowID)
	}
	if resp.TriggerType != domain.TriggerRecordTransition {
		t.Errorf("trigger_type = %q, want record_transition", resp.TriggerType)
	}
}

func TestTriggerHandler_Create_MissingTriggerType(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	delete(b, "trigger_type")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestTriggerHandler_Create_MissingWorkflowID(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	delete(b, "workflow_id")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestTriggerHandler_Create_VersionZero(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	b["version"] = 0
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestTriggerHandler_Create_NilConfig(t *testing.T) {
	_, r := newTestHandler()

	// Omit config entirely — json.Unmarshal leaves it nil.
	raw := `{"trigger_type":"record_transition","workflow_id":"w","version":1,"enabled":true,"created_by":"admin"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader([]byte(raw)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestTriggerHandler_Create_RecordTransition_MissingRecordType(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	cfg := b["config"].(map[string]any)
	delete(cfg, "record_type")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestTriggerHandler_Create_RecordTransition_MissingFromState(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	cfg := b["config"].(map[string]any)
	delete(cfg, "from_state")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestTriggerHandler_Create_RecordTransition_MissingToState(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	cfg := b["config"].(map[string]any)
	delete(cfg, "to_state")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}

func TestTriggerHandler_Create_MissingCreatedBy(t *testing.T) {
	_, r := newTestHandler()

	b := validRecordTransitionBody()
	delete(b, "created_by")
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestTriggerHandler_Create_IgnoresBodyID(t *testing.T) {
	_, r := newTestHandler()

	// Even if caller sends an id field, the server ignores it and assigns its own.
	b := validRecordTransitionBody()
	b["id"] = "00000000-dead-beef-0000-000000000000"
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp domain.SagaTrigger
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.ID.String() == "00000000-dead-beef-0000-000000000000" {
		t.Errorf("server should assign a new ID, not echo the body ID")
	}
}

// ---- Get ------------------------------------------------------------------

func TestTriggerHandler_Get_Existing(t *testing.T) {
	_, r := newTestHandler()

	// Create one.
	body, _ := json.Marshal(validRecordTransitionBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup create failed: %d %s", w.Code, w.Body.String())
	}
	var created domain.SagaTrigger
	json.Unmarshal(w.Body.Bytes(), &created)

	// Get it.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers/"+created.ID.String(), nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", w.Code, w.Body.String())
	}
	var got domain.SagaTrigger
	json.Unmarshal(w.Body.Bytes(), &got)
	if got.ID != created.ID {
		t.Errorf("id = %v, want %v", got.ID, created.ID)
	}
	if got.WorkflowID != "approval_workflow_v1" {
		t.Errorf("workflow_id = %q, want approval_workflow_v1", got.WorkflowID)
	}
}

func TestTriggerHandler_Get_Unknown(t *testing.T) {
	_, r := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/triggers/00000000-0000-0000-0000-000000000099", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// ---- List -----------------------------------------------------------------

func TestTriggerHandler_List_Unfiltered(t *testing.T) {
	_, r := newTestHandler()

	// Insert two triggers.
	for i := 0; i < 2; i++ {
		body, _ := json.Marshal(validRecordTransitionBody())
		req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("setup create %d failed: %d", i, w.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/triggers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Triggers []domain.SagaTrigger `json:"triggers"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Triggers) != 2 {
		t.Errorf("triggers count = %d, want 2", len(resp.Triggers))
	}
}

func TestTriggerHandler_List_FilterByType(t *testing.T) {
	_, r := newTestHandler()

	// Insert one record_transition trigger.
	body, _ := json.Marshal(validRecordTransitionBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup create failed: %d", w.Code)
	}

	// List filtered by type.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers?type=record_transition", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Triggers []domain.SagaTrigger `json:"triggers"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Triggers) != 1 {
		t.Errorf("triggers count = %d, want 1", len(resp.Triggers))
	}
	if resp.Triggers[0].TriggerType != domain.TriggerRecordTransition {
		t.Errorf("trigger_type = %q, want record_transition", resp.Triggers[0].TriggerType)
	}

	// Non-matching type should return empty list.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers?type=cron", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp2 struct {
		Triggers []domain.SagaTrigger `json:"triggers"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp2)
	if len(resp2.Triggers) != 0 {
		t.Errorf("expected 0 cron triggers, got %d", len(resp2.Triggers))
	}
}

func TestTriggerHandler_List_FilterByEnabled(t *testing.T) {
	_, r := newTestHandler()

	// Insert one enabled trigger.
	enabledBody := validRecordTransitionBody()
	enabledBody["enabled"] = true
	body, _ := json.Marshal(enabledBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup enabled create failed: %d", w.Code)
	}

	// Insert one disabled trigger.
	disabledBody := validRecordTransitionBody()
	disabledBody["enabled"] = false
	body, _ = json.Marshal(disabledBody)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup disabled create failed: %d", w.Code)
	}

	// Filter enabled=true → should get 1.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers?enabled=true", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d", w.Code)
	}
	var resp struct {
		Triggers []domain.SagaTrigger `json:"triggers"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Triggers) != 1 {
		t.Errorf("enabled count = %d, want 1", len(resp.Triggers))
	}
	if !resp.Triggers[0].Enabled {
		t.Errorf("expected enabled=true trigger, got false")
	}

	// Filter enabled=false → should get 1.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers?enabled=false", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp2 struct {
		Triggers []domain.SagaTrigger `json:"triggers"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp2)
	if len(resp2.Triggers) != 1 {
		t.Errorf("disabled count = %d, want 1", len(resp2.Triggers))
	}
	if resp2.Triggers[0].Enabled {
		t.Errorf("expected enabled=false trigger, got true")
	}
}

// ---- Delete ---------------------------------------------------------------

func TestTriggerHandler_Delete_ThenGet404(t *testing.T) {
	_, r := newTestHandler()

	// Create.
	body, _ := json.Marshal(validRecordTransitionBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup create failed: %d %s", w.Code, w.Body.String())
	}
	var created domain.SagaTrigger
	json.Unmarshal(w.Body.Bytes(), &created)

	// Delete → 204.
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/triggers/"+created.ID.String(), nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204; body=%s", w.Code, w.Body.String())
	}

	// Subsequent Get → 404.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/triggers/"+created.ID.String(), nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("after delete get status = %d, want 404", w.Code)
	}
}

func TestTriggerHandler_Delete_Unknown(t *testing.T) {
	_, r := newTestHandler()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/triggers/00000000-0000-0000-0000-000000000099", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("delete unknown status = %d, want 404", w.Code)
	}
}

// ---- Cron trigger create ---------------------------------------------------

// stubDenyAll is a licensing.Resolver that always denies every feature.
type stubDenyAll struct{}

func (stubDenyAll) IsFeatureEnabled(_ context.Context, _ *uuid.UUID, _ string, _ map[string]bool) (bool, error) {
	return false, nil
}

func validCronBody() map[string]any {
	return map[string]any{
		"trigger_type": "cron",
		"workflow_id":  "wf",
		"version":      1,
		"config":       map[string]any{"schedule": "* * * * *"},
		"enabled":      true,
		"created_by":   "admin",
	}
}

func TestTriggerHandler_Create_Cron_InvalidSchedule(t *testing.T) {
	_, r := newTestHandler()

	b := validCronBody()
	b["config"] = map[string]any{"schedule": "nope"}
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestTriggerHandler_Create_Cron_Unlicensed(t *testing.T) {
	s := memory.New()
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	h := NewTriggerHandler(s, stubDenyAll{}, clk)
	r := newTriggerRouter(h)

	body, _ := json.Marshal(validCronBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d: %s", w.Code, w.Body.String())
	}
}

// ---- Cron trigger with interval -------------------------------------------

func validCronIntervalBody() map[string]any {
	return map[string]any{
		"trigger_type": "cron",
		"workflow_id":  "wf",
		"version":      1,
		"config":       map[string]any{"interval": "30s"},
		"enabled":      true,
		"created_by":   "admin",
	}
}

func TestTriggerHandler_Create_Cron_Interval_Valid(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := memory.New()
	clk := clock.NewFakeClock(fixedNow)
	h := NewTriggerHandler(s, licensing.StubAllowAll{}, clk)
	r := newTriggerRouter(h)

	body, _ := json.Marshal(validCronIntervalBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp domain.SagaTrigger
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.NextFireAt == nil {
		t.Fatal("next_fire_at should be set for interval trigger, got nil")
	}
	// next_fire_at should be now + 30s.
	want := fixedNow.Add(30 * time.Second)
	if !resp.NextFireAt.Equal(want) {
		t.Errorf("next_fire_at = %v, want %v", resp.NextFireAt, want)
	}
}

func TestTriggerHandler_Create_Cron_Interval_Invalid(t *testing.T) {
	_, r := newTestHandler()

	b := validCronIntervalBody()
	b["config"] = map[string]any{"interval": "not-a-duration"}
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestTriggerHandler_Create_Cron_BothScheduleAndInterval(t *testing.T) {
	_, r := newTestHandler()

	b := validCronIntervalBody()
	b["config"] = map[string]any{
		"schedule": "* * * * *",
		"interval": "30s",
	}
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 when both schedule and interval present, got %d: %s", w.Code, w.Body.String())
	}
}

func TestTriggerHandler_Create_Cron_NeitherScheduleNorInterval(t *testing.T) {
	_, r := newTestHandler()

	b := validCronIntervalBody()
	b["config"] = map[string]any{}
	body, _ := json.Marshal(b)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 when neither schedule nor interval present, got %d: %s", w.Code, w.Body.String())
	}
}

func TestTriggerHandler_Create_Cron_InitializesNextFireAt(t *testing.T) {
	fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := memory.New()
	clk := clock.NewFakeClock(fixedNow)
	h := NewTriggerHandler(s, licensing.StubAllowAll{}, clk)
	r := newTriggerRouter(h)

	body, _ := json.Marshal(validCronBody())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/triggers", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp domain.SagaTrigger
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.NextFireAt == nil {
		t.Fatal("next_fire_at should be set for cron trigger, got nil")
	}
	// "* * * * *" fires every minute; next after 12:00:00 is 12:01:00.
	want := time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC)
	if !resp.NextFireAt.Equal(want) {
		t.Errorf("next_fire_at = %v, want %v", resp.NextFireAt, want)
	}
}
