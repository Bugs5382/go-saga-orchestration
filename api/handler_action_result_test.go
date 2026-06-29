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

// pausedAwaitingRun creates a run paused-awaiting an action at attempt 1.
func pausedAwaitingRun(t *testing.T, s *memory.Store) domain.SagaRun {
	t.Helper()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.MarkAwaitingAction(ctx, run.ID, "svc.act", 1); err != nil {
		t.Fatalf("mark awaiting: %v", err)
	}
	return run
}

func serveResult(t *testing.T, h *ActionResultHandler, runID, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/actions/{step_id}/result", h.Post)
	req := httptest.NewRequest("POST", "/api/v1/sagas/"+runID+"/actions/act/result", bytes.NewReader([]byte(body)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestActionResult_Success_CompletesAndAdvances(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := pausedAwaitingRun(t, s)
	pub := &capturingPub{}
	h := NewActionResultHandler(s, pub)

	w := serveResult(t, h, run.ID.String(), `{"result":{"ticket_number":"INC-7"}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Variables["ticket_number"] != "INC-7" {
		t.Errorf("ticket_number = %v, want INC-7", got.Variables["ticket_number"])
	}
	if len(pub.runs) != 1 || pub.runs[0] != run.ID.String() {
		t.Errorf("publisher saw %v, want one advance of %s", pub.runs, run.ID)
	}
}

func TestActionResult_Error_FailsAction(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := pausedAwaitingRun(t, s)
	pub := &capturingPub{}
	h := NewActionResultHandler(s, pub)

	w := serveResult(t, h, run.ID.String(),
		`{"error":{"code":"ERR_X","message":"boom","retryable":false}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", got.State)
	}
	// Failure path must not publish an advance.
	if len(pub.runs) != 0 {
		t.Errorf("error path should not advance, saw %v", pub.runs)
	}
}

func TestActionResult_ExplicitAttempt_StaleIsNoOp(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	run := pausedAwaitingRun(t, s) // awaiting attempt 1
	h := NewActionResultHandler(s, &capturingPub{})

	// Stale attempt 0 -> CompleteAction is a no-op (idempotency in the store).
	w := serveResult(t, h, run.ID.String(), `{"attempt":0,"result":{"x":"y"}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Variables["x"] != nil {
		t.Errorf("stale attempt should not merge result, got %v", got.Variables["x"])
	}
}

func TestActionResult_BadRunID_Returns400(t *testing.T) {
	h := NewActionResultHandler(memory.New(), &capturingPub{})
	w := serveResult(t, h, "not-a-uuid", `{"result":{}}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestActionResult_BothResultAndError_Returns400(t *testing.T) {
	s := memory.New()
	run := pausedAwaitingRun(t, s)
	h := NewActionResultHandler(s, &capturingPub{})
	w := serveResult(t, h, run.ID.String(), `{"result":{},"error":{"code":"x"}}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

func TestActionResult_NeitherResultNorError_Returns400(t *testing.T) {
	s := memory.New()
	run := pausedAwaitingRun(t, s)
	h := NewActionResultHandler(s, &capturingPub{})
	w := serveResult(t, h, run.ID.String(), `{}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}
