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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type capturingPub struct{ runs []string }

func (c *capturingPub) PublishSagaAdvance(_ context.Context, runID string) error {
	c.runs = append(c.runs, runID)
	return nil
}

func TestSignalHandler_PausedAwaiting_Returns202AndPublishes(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	_ = s.SetPausedAwaitingSignal(ctx, run.ID, "go", nil)

	pub := &capturingPub{}
	h := NewSignalHandler(s, pub)

	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", h.Post)
	req := httptest.NewRequest("POST", "/api/v1/sagas/"+run.ID.String()+"/signal/go", bytes.NewReader([]byte(`{"payload":{"k":"v"}}`)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if len(pub.runs) != 1 || pub.runs[0] != run.ID.String() {
		t.Errorf("publisher saw %v, want one publish of %s", pub.runs, run.ID)
	}
}

func TestSignalHandler_NotPaused_Returns409(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	// no SetPausedAwaitingSignal — the run is not awaiting anything.

	pub := &capturingPub{}
	h := NewSignalHandler(s, pub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", h.Post)
	req := httptest.NewRequest("POST", "/api/v1/sagas/"+run.ID.String()+"/signal/go", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
	if len(pub.runs) != 0 {
		t.Errorf("publisher should not have been called")
	}
}

func TestSignalHandler_BadRunID_Returns400(t *testing.T) {
	h := NewSignalHandler(memory.New(), &capturingPub{})
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", h.Post)
	req := httptest.NewRequest("POST", "/api/v1/sagas/not-a-uuid/signal/go", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}
