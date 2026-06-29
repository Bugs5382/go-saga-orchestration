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
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func seedTriageRule(t *testing.T, s *memory.Store) {
	t.Helper()
	rule := domain.NewRuleDefinition(
		"triage", 1, "Triage", domain.RuleTypeDecisionTable,
		domain.RuleSpec{
			HitPolicy: domain.HitPolicyFirst,
			Rows: []domain.DecisionTableRow{
				{When: "priority == 'p1'", Then: map[string]any{"branch": "high"}},
				{When: "priority == 'p3'", Then: map[string]any{"branch": "low"}},
			},
			DefaultOutput: map[string]any{"branch": "low"},
		},
		"test",
	)
	if _, err := s.UpsertRuleDefinition(context.Background(), rule); err != nil {
		t.Fatalf("seed rule: %v", err)
	}
}

func TestRulesHandler_Evaluate_HappyPath(t *testing.T) {
	s := memory.New()
	seedTriageRule(t, s)
	h := NewRulesHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/rules/{rule_id}/evaluate", h.Evaluate)

	req := httptest.NewRequest("POST", "/api/v1/rules/triage/evaluate",
		bytes.NewReader([]byte(`{"inputs":{"priority":"p1"}}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp rulesEvalResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Output["branch"] != "high" {
		t.Errorf("branch = %v, want high", resp.Output["branch"])
	}
	if len(resp.Audit) != 1 {
		t.Errorf("audit len = %d, want 1", len(resp.Audit))
	}
}

func TestRulesHandler_Evaluate_DefaultOutput(t *testing.T) {
	s := memory.New()
	seedTriageRule(t, s)
	h := NewRulesHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/rules/{rule_id}/evaluate", h.Evaluate)

	req := httptest.NewRequest("POST", "/api/v1/rules/triage/evaluate",
		bytes.NewReader([]byte(`{"inputs":{"priority":"p7"}}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	var resp rulesEvalResp
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Output["branch"] != "low" {
		t.Errorf("default branch = %v, want low", resp.Output["branch"])
	}
}

func TestRulesHandler_Evaluate_RuleNotFound(t *testing.T) {
	h := NewRulesHandler(memory.New())
	r := chi.NewRouter()
	r.Post("/api/v1/rules/{rule_id}/evaluate", h.Evaluate)

	req := httptest.NewRequest("POST", "/api/v1/rules/nope/evaluate",
		bytes.NewReader([]byte(`{"inputs":{}}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestRulesHandler_Evaluate_BadBody(t *testing.T) {
	h := NewRulesHandler(memory.New())
	r := chi.NewRouter()
	r.Post("/api/v1/rules/{rule_id}/evaluate", h.Evaluate)

	req := httptest.NewRequest("POST", "/api/v1/rules/x/evaluate",
		strings.NewReader(`not-json`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRulesHandler_Evaluate_BadRuleExpr_Returns422(t *testing.T) {
	s := memory.New()
	bad := domain.NewRuleDefinition(
		"bad", 1, "Bad", domain.RuleTypeDecisionTable,
		domain.RuleSpec{
			HitPolicy: domain.HitPolicyFirst,
			Rows:      []domain.DecisionTableRow{{When: "this is not valid cel", Then: map[string]any{"x": 1}}},
		},
		"test",
	)
	_, _ = s.UpsertRuleDefinition(context.Background(), bad)
	h := NewRulesHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/rules/{rule_id}/evaluate", h.Evaluate)

	req := httptest.NewRequest("POST", "/api/v1/rules/bad/evaluate",
		bytes.NewReader([]byte(`{"inputs":{}}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", w.Code)
	}
}
