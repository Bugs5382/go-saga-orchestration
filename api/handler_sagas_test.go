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
	"reflect"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// fakePublisher captures publishes for assertion without RabbitMQ.
type fakePublisher struct{ runs []string }

func (f *fakePublisher) PublishSagaAdvance(_ context.Context, runID string) error {
	f.runs = append(f.runs, runID)
	return nil
}

func TestStartSaga(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	def := domain.WorkflowDefinition{
		ID: "wf_trivial", Version: 1, Name: "Trivial",
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, def)

	pub := &fakePublisher{}
	h := NewSagaHandler(s, pub)

	body := mustJSON(t, map[string]any{"workflow_id": "wf_trivial", "inputs": map[string]any{}})
	req := httptest.NewRequest("POST", "/api/v1/sagas/start", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.Start(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["saga_run_id"] == "" {
		t.Errorf("missing saga_run_id in response: %s", w.Body.String())
	}
	if len(pub.runs) != 1 || pub.runs[0] != resp["saga_run_id"] {
		t.Errorf("publisher saw %v, expected one publish of %s", pub.runs, resp["saga_run_id"])
	}
}

func TestGetSagaNotFound(t *testing.T) {
	h := NewSagaHandler(memory.New(), &fakePublisher{})
	r := chi.NewRouter()
	r.Get("/api/v1/sagas/{id}", h.Get)
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/sagas/"+uuid.New().String(), nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestSagaHandler_Start_PersistsFeatureOverrides(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	// Seed a published workflow.
	def := domain.WorkflowDefinition{ID: "wf", Version: 1, Start: "end", Published: true,
		Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}}}
	_, _ = s.UpsertWorkflowDefinition(ctx, def)
	pub := &fakePublisher{}
	h := NewSagaHandler(s, pub)

	body := mustJSON(t, map[string]any{"workflow_id": "wf", "inputs": map[string]any{}})
	req := httptest.NewRequest("POST", "/api/v1/sagas/start", bytes.NewReader(body))
	req.Header.Set("X-Feature-Override", "wf.parallel=on,wf.timers=off")
	w := httptest.NewRecorder()
	h.Start(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	runID, _ := uuid.Parse(resp["saga_run_id"])
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if run.FeatureOverrides["wf.parallel"] != true {
		t.Errorf("wf.parallel = %v, want true", run.FeatureOverrides["wf.parallel"])
	}
	if run.FeatureOverrides["wf.timers"] != false {
		t.Errorf("wf.timers = %v, want false", run.FeatureOverrides["wf.timers"])
	}
}

func TestParseFeatureOverrideHeader(t *testing.T) {
	cases := []struct {
		name, header string
		want         map[string]bool
	}{
		{"empty", "", nil},
		{"single on", "wf.parallel=on", map[string]bool{"wf.parallel": true}},
		{"mixed", "wf.parallel=on,wf.timers=off", map[string]bool{"wf.parallel": true, "wf.timers": false}},
		{"whitespace", "  wf.parallel = TRUE  ,  wf.timers=0  ", map[string]bool{"wf.parallel": true, "wf.timers": false}},
		{"unknown value skipped", "wf.parallel=maybe", nil},
		{"missing equals", "wf.parallel", nil},
	}
	for _, tc := range cases {
		got := parseFeatureOverrideHeader(tc.header)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
