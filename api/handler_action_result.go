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
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// ActionResultHandler accepts a transport-agnostic, asynchronous result
// callback for an action step. gRPC workers reply over the ExecuteStep stream;
// http and rmq workers have no return stream, so they report their result here.
// Both paths converge on the same CompleteAction / FailAction store hooks the
// gRPC server uses, preserving attempt handling and idempotency. (issue #59)
type ActionResultHandler struct {
	S         store.Store
	Publisher AdvancePublisher
}

// NewActionResultHandler returns an ActionResultHandler backed by the given
// store and advance publisher.
func NewActionResultHandler(s store.Store, p AdvancePublisher) *ActionResultHandler {
	return &ActionResultHandler{S: s, Publisher: p}
}

// actionErrorBody mirrors the gRPC Error message: a worker-reported failure.
type actionErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// actionResultReq is the body of the result-callback endpoint. Exactly one of
// Result / Error is expected: Result -> CompleteAction (success), Error ->
// FailAction (failure). Attempt defaults to the run's current attempt when
// omitted, matching the only in-flight dispatch.
type actionResultReq struct {
	Attempt *int             `json:"attempt,omitempty"`
	Result  map[string]any   `json:"result,omitempty"`
	Error   *actionErrorBody `json:"error,omitempty"`
}

// Post handles POST /api/v1/sagas/{run_id}/actions/{step_id}/result.
//
// Body (exactly one of):
//
//	{ "result": { ... } }                                  -> CompleteAction
//	{ "error": { "code", "message", "retryable" } }        -> FailAction
//
// An optional "attempt" pins the report to a specific dispatch attempt;
// omitted, it uses the run's current attempt. Mirrors the gRPC
// Complete/Error semantics (attempt handling + idempotency are enforced by
// the store hooks: a stale attempt is a no-op).
//
// Responses:
//
//	202 — result recorded (success completed, or failure transitioned).
//	400 — bad run_id or malformed body.
//	500 — internal error.
func (h *ActionResultHandler) Post(w http.ResponseWriter, r *http.Request) {
	runIDStr := chi.URLParam(r, "run_id")
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad run id")
		return
	}

	var body actionResultReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if (body.Result == nil) == (body.Error == nil) {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "exactly one of result or error required")
		return
	}

	attempt, err := h.resolveAttempt(r, runID, body.Attempt)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "run not found")
		return
	}

	if body.Error != nil {
		// Failure path — mirrors gRPC handleError -> FailAction.
		if err := h.S.FailAction(r.Context(), runID, attempt, body.Error.Code, body.Error.Message, body.Error.Retryable); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).Msg("action result: fail action")
			WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Success path — mirrors gRPC handleComplete -> CompleteAction + advance.
	if err := h.S.CompleteAction(r.Context(), runID, attempt, body.Result); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("action result: complete action")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	if h.Publisher != nil {
		if err := h.Publisher.PublishSagaAdvance(r.Context(), runID.String()); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).Msg("action result: publish advance")
			WriteError(w, http.StatusInternalServerError, CodePublishFailed, genericInternalMessage)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

// resolveAttempt returns the explicit attempt when supplied, else the run's
// current attempt (the only in-flight dispatch). A missing run is an error.
func (h *ActionResultHandler) resolveAttempt(r *http.Request, runID uuid.UUID, explicit *int) (int, error) {
	if explicit != nil {
		return *explicit, nil
	}
	run, err := h.S.GetRun(r.Context(), runID)
	if err != nil {
		return 0, err
	}
	return run.CurrentAttempt, nil
}
