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
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// WorkflowHandler owns workflow-level aggregate routes.
type WorkflowHandler struct {
	S store.Store
}

// NewWorkflowHandler constructs a WorkflowHandler.
func NewWorkflowHandler(s store.Store) *WorkflowHandler {
	return &WorkflowHandler{S: s}
}

// Stats handles GET /api/v1/workflows/{wf_id}/stats.
// Returns aggregate metrics: success_rate_24h, last_run_at, in_flight.
func (h *WorkflowHandler) Stats(w http.ResponseWriter, r *http.Request) {
	wfID := chi.URLParam(r, "wf_id")
	if wfID == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "wf_id required")
		return
	}
	stats, err := h.S.StatsForWorkflow(r.Context(), wfID)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", wfID).Msg("stats for workflow failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	WriteJSON(w, http.StatusOK, stats)
}
