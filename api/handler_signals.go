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
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// SignalHandler accepts external signals delivered to a saga.
type SignalHandler struct {
	S         store.Store
	Publisher AdvancePublisher
}

// NewSignalHandler returns a SignalHandler backed by the given store and
// advance publisher.
func NewSignalHandler(s store.Store, p AdvancePublisher) *SignalHandler {
	return &SignalHandler{S: s, Publisher: p}
}

type signalReq struct {
	Payload map[string]any `json:"payload,omitempty"`
}

// Post handles POST /api/v1/sagas/{run_id}/signal/{name}.
// Responses:
//
//	202 — signal recorded AND matched a paused saga awaiting it (advance published).
//	409 — signal recorded but the run wasn't paused-and-awaiting this name.
//	400 — bad run_id.
//	404 — run not found (only when AppendSignal returns a not-found error).
//	500 — internal error.
func (h *SignalHandler) Post(w http.ResponseWriter, r *http.Request) {
	runIDStr := chi.URLParam(r, "run_id")
	name := chi.URLParam(r, "name")
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad run id")
		return
	}

	var body signalReq
	_ = json.NewDecoder(r.Body).Decode(&body) // tolerate empty body

	sig := domain.SagaSignal{
		ID:         uuid.New(),
		RunID:      runID,
		SignalName: name,
		Payload:    body.Payload,
		ReceivedAt: time.Now().UTC(),
	}
	if err := h.S.AppendSignal(r.Context(), sig); err != nil {
		if _, isNF := err.(store.ErrNotFound); isNF {
			WriteError(w, http.StatusNotFound, CodeNotFound, "run not found")
			return
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("append signal failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	ok, err := h.S.TryConsumeAwaitedSignal(r.Context(), runID, name)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("consume awaited signal failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	if ok && h.Publisher != nil {
		if err := h.Publisher.PublishSagaAdvance(r.Context(), runID.String()); err != nil {
			log.Error().Err(err).Str("run_id", runID.String()).Msg("publish advance failed")
			WriteError(w, http.StatusInternalServerError, CodePublishFailed, genericInternalMessage)
			return
		}
	}
	if ok {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	WriteError(w, http.StatusConflict, CodeConflict, "signal recorded but run was not awaiting it")
}
