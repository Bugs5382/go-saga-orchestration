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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// newStreamHandler builds a SagaStreamHandler with a nil pool (sufficient for
// all unit tests that exercise paths before LISTEN is reached).
func newStreamHandler(s *memory.Store) *SagaStreamHandler {
	return NewSagaStreamHandler(s, nil)
}

func routedStreamServer(h *SagaStreamHandler) *httptest.Server {
	r := chi.NewRouter()
	r.Get("/api/v1/sagas/{run_id}/stream", h.Stream)
	return httptest.NewServer(r)
}

// TestStreamHandler_BadUUID verifies the handler returns 400 for a non-UUID run_id.
func TestStreamHandler_BadUUID(t *testing.T) {
	h := newStreamHandler(memory.New())
	srv := routedStreamServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/sagas/not-a-uuid/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// TestStreamHandler_NotFound verifies 404 is returned (before upgrade) when
// the run_id is valid UUID but no such run exists.
func TestStreamHandler_NotFound(t *testing.T) {
	h := newStreamHandler(memory.New())
	srv := routedStreamServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/sagas/" + uuid.New().String() + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestStreamHandler_NilPool_501 verifies that Stream returns 501 (and does not
// panic) when the handler is constructed with a nil pool, which happens when
// STORE_TYPE is redis or memory and no postgres pool is available.
func TestStreamHandler_NilPool_501(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	// Seed a run so the handler passes the 404 guard and reaches the pool check.
	def := domain.WorkflowDefinition{
		ID: "wf_nilpool", Version: 1, Name: "NilPoolTest",
		Start: "end", Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_nilpool", defID, nil, nil)
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	h := newStreamHandler(s) // nil pool
	srv := routedStreamServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/sagas/" + run.ID.String() + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", resp.StatusCode)
	}
}
