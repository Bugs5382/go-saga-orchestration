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
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UserTaskHandler accepts task submissions from assignees. On submit:
//  1. Persist the task's submitted_at / submitted_by / result.
//  2. Append a saga_signal of name `user_task.{task_id}.submitted` to
//     the run, carrying the result as the signal payload.
//  3. Try to consume the awaited signal; if it matches, publish saga.advance.
type UserTaskHandler struct {
	S         store.Store
	Publisher AdvancePublisher
}

// NewUserTaskHandler constructs the handler.
func NewUserTaskHandler(s store.Store, p AdvancePublisher) *UserTaskHandler {
	return &UserTaskHandler{S: s, Publisher: p}
}

type userTaskSubmitReq struct {
	SubmittedBy string         `json:"submitted_by"`
	Result      map[string]any `json:"result"`
}

// Submit handles POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit.
func (h *UserTaskHandler) Submit(w http.ResponseWriter, r *http.Request) {
	taskIDStr := chi.URLParam(r, "task_id")
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad task id")
		return
	}

	var body userTaskSubmitReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if body.SubmittedBy == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "submitted_by required")
		return
	}

	task, err := h.S.GetUserTask(r.Context(), taskID)
	if err != nil {
		WriteError(w, http.StatusNotFound, CodeNotFound, "task not found")
		return
	}
	if err := h.S.SubmitUserTask(r.Context(), taskID, body.SubmittedBy, body.Result); err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("submit user task failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	signalName := "user_task." + taskID.String() + ".submitted"
	sig := domain.SagaSignal{
		ID:         uuid.New(),
		RunID:      task.RunID,
		SignalName: signalName,
		Payload:    body.Result,
		ReceivedAt: time.Now().UTC(),
	}
	if err := h.S.AppendSignal(r.Context(), sig); err != nil {
		log.Error().Err(err).Str("run_id", task.RunID.String()).Msg("append signal failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	ok, err := h.S.TryConsumeAwaitedSignal(r.Context(), task.RunID, signalName)
	if err != nil {
		log.Error().Err(err).Str("run_id", task.RunID.String()).Msg("consume awaited signal failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	if ok && h.Publisher != nil {
		if err := h.Publisher.PublishSagaAdvance(r.Context(), task.RunID.String()); err != nil {
			log.Error().Err(err).Str("run_id", task.RunID.String()).Msg("publish advance failed")
			WriteError(w, http.StatusInternalServerError, CodePublishFailed, genericInternalMessage)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}
