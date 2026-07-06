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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// fakeCanceller records the last Cancel call and returns a configured error.
type fakeCanceller struct {
	calls     int
	gotID     uuid.UUID
	gotReason string
	err       error
}

func (f *fakeCanceller) Cancel(_ context.Context, runID uuid.UUID, reason string) error {
	f.calls++
	f.gotID = runID
	f.gotReason = reason
	return f.err
}

// newCancelRouter mounts just the cancel route on a chi router so {id} is
// parsed exactly as it is in production.
func newCancelRouter(h *SagaHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{id}/cancel", h.Cancel)
	return r
}

func TestCancelSaga_AcceptedWithReason(t *testing.T) {
	fc := &fakeCanceller{}
	h := NewSagaHandler(memory.New(), &fakePublisher{}).WithCanceller(fc)
	r := newCancelRouter(h)

	id := uuid.New()
	body := mustJSON(t, map[string]any{"reason": "superseded"})
	req := httptest.NewRequest("POST", "/api/v1/sagas/"+id.String()+"/cancel", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body.String())
	}
	if fc.calls != 1 {
		t.Fatalf("Cancel called %d times, want 1", fc.calls)
	}
	if fc.gotID != id {
		t.Errorf("Cancel id = %s, want %s", fc.gotID, id)
	}
	if fc.gotReason != "superseded" {
		t.Errorf("Cancel reason = %q, want %q", fc.gotReason, "superseded")
	}
}

func TestCancelSaga_AcceptedNoBody(t *testing.T) {
	fc := &fakeCanceller{}
	h := NewSagaHandler(memory.New(), &fakePublisher{}).WithCanceller(fc)
	r := newCancelRouter(h)

	id := uuid.New()
	req := httptest.NewRequest("POST", "/api/v1/sagas/"+id.String()+"/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body.String())
	}
	if fc.calls != 1 || fc.gotReason != "" {
		t.Errorf("calls = %d reason = %q, want 1 call with empty reason", fc.calls, fc.gotReason)
	}
}

func TestCancelSaga_BadID(t *testing.T) {
	fc := &fakeCanceller{}
	h := NewSagaHandler(memory.New(), &fakePublisher{}).WithCanceller(fc)
	r := newCancelRouter(h)

	req := httptest.NewRequest("POST", "/api/v1/sagas/not-a-uuid/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if fc.calls != 0 {
		t.Errorf("Cancel called %d times, want 0 on bad id", fc.calls)
	}
}

func TestCancelSaga_NilCanceller(t *testing.T) {
	h := NewSagaHandler(memory.New(), &fakePublisher{})
	r := newCancelRouter(h)

	req := httptest.NewRequest("POST", "/api/v1/sagas/"+uuid.New().String()+"/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestCancelSaga_NotFound(t *testing.T) {
	id := uuid.New()
	fc := &fakeCanceller{err: store.ErrNotFound{Entity: "saga_run", ID: id.String()}}
	h := NewSagaHandler(memory.New(), &fakePublisher{}).WithCanceller(fc)
	r := newCancelRouter(h)

	req := httptest.NewRequest("POST", "/api/v1/sagas/"+id.String()+"/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}
