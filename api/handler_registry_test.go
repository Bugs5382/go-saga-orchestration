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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestRegistryHandler_RegisterThenList(t *testing.T) {
	s := memory.New()
	h := NewRegistryHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/registry/register", h.Register)
	r.Get("/api/v1/registry/actions", h.List)

	regBody := registerReq{
		Service:        "example",
		ServiceVersion: "0.18.2",
		Actions: []domain.ActionRegistration{
			{ActionName: "set_state", Version: 1, Category: "record_lifecycle", Compensable: true},
			{ActionName: "create_record", Version: 1, Category: "record_lifecycle", Compensable: false},
		},
	}
	body, _ := json.Marshal(regBody)
	req := httptest.NewRequest("POST", "/api/v1/registry/register", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("register status = %d, body=%s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/api/v1/registry/actions?service=example", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d", w.Code)
	}
	var resp struct {
		Actions []domain.ActionRegistration `json:"actions"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Actions) != 2 {
		t.Errorf("actions = %d, want 2", len(resp.Actions))
	}
	// Confirm Service + ServiceVersion stamped from envelope.
	for _, a := range resp.Actions {
		if a.Service != "example" {
			t.Errorf("service = %q, want example", a.Service)
		}
		if a.ServiceVersion != "0.18.2" {
			t.Errorf("service_version = %q, want 0.18.2", a.ServiceVersion)
		}
	}
}

func TestRegistryHandler_Register_RejectsEmpty(t *testing.T) {
	s := memory.New()
	h := NewRegistryHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/registry/register", h.Register)

	req := httptest.NewRequest("POST", "/api/v1/registry/register", bytes.NewReader([]byte(`{"service":"x","actions":[]}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRegistryHandler_Register_RejectsMissingService(t *testing.T) {
	s := memory.New()
	h := NewRegistryHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/registry/register", h.Register)

	req := httptest.NewRequest("POST", "/api/v1/registry/register",
		bytes.NewReader([]byte(`{"actions":[{"action_name":"x","version":1}]}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRegistryHandler_Register_RejectsBadVersion(t *testing.T) {
	s := memory.New()
	h := NewRegistryHandler(s)
	r := chi.NewRouter()
	r.Post("/api/v1/registry/register", h.Register)

	req := httptest.NewRequest("POST", "/api/v1/registry/register",
		bytes.NewReader([]byte(`{"service":"x","actions":[{"action_name":"a","version":0}]}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}
