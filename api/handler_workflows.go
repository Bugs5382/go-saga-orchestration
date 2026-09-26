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
	"errors"
	"net/http"
	"strconv"

	golog "github.com/Bugs5382/go-log"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
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
		sagalog.For(r.Context(), nil).Error(err, "stats for workflow failed", golog.F("workflow_id", wfID))
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	WriteJSON(w, http.StatusOK, stats)
}

// workflowListResponse is the body of GET /api/v1/workflows.
type workflowListResponse struct {
	Workflows []domain.WorkflowDefinition `json:"workflows"`
	Total     int                         `json:"total"`
	Limit     int                         `json:"limit"`
	Offset    int                         `json:"offset"`
}

// List handles GET /api/v1/workflows. Parses optional limit/offset/published/
// search query params into a store.DefinitionFilter, lists definitions
// newest-first, and returns them under a "workflows" envelope.
func (h *WorkflowHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Parse limit (1–500; default 50).
	limit := 50
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "limit must be an integer >= 1")
			return
		}
		if v > 500 {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "limit must be <= 500")
			return
		}
		limit = v
	}

	// Parse offset (>= 0; default 0).
	offset := 0
	if raw := q.Get("offset"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "offset must be an integer >= 0")
			return
		}
		offset = v
	}

	filter := store.DefinitionFilter{
		Search: q.Get("search"),
		Limit:  limit,
		Offset: offset,
	}

	// Parse published (true/false).
	if raw := q.Get("published"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "published must be true or false")
			return
		}
		filter.Published = &v
	}

	defs, err := h.S.ListWorkflowDefinitions(r.Context(), filter)
	if err != nil {
		sagalog.For(r.Context(), nil).Error(err, "list workflow definitions failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	// Ensure workflows is never null in JSON output.
	if defs == nil {
		defs = []domain.WorkflowDefinition{}
	}

	WriteJSON(w, http.StatusOK, workflowListResponse{
		Workflows: defs,
		Total:     len(defs),
		Limit:     limit,
		Offset:    offset,
	})
}

// Get handles GET /api/v1/workflows/{id}. The id is the definition's storage
// UUID (as returned by Save). 404 when no definition has that id.
func (h *WorkflowHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")

	// A definition is addressable either by its storage UUID or by its business
	// workflow id (the human-meaningful `id` field, e.g. "order_fulfillment").
	// A UUID resolves the exact stored row; a non-UUID resolves the newest
	// version carrying that workflow id.
	if id, err := uuid.Parse(idStr); err == nil {
		def, err := h.S.GetWorkflowDefinition(r.Context(), id)
		if err != nil {
			var nf store.ErrNotFound
			if errors.As(err, &nf) {
				WriteError(w, http.StatusNotFound, "workflow_not_found", idStr)
				return
			}
			sagalog.For(r.Context(), nil).Error(err, "get workflow definition failed", golog.F("definition_id", idStr))
			WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
			return
		}
		WriteJSON(w, http.StatusOK, def)
		return
	}

	// Non-UUID: resolve by business workflow id via the newest-first list.
	defs, err := h.S.ListWorkflowDefinitions(r.Context(), store.DefinitionFilter{Search: idStr, Limit: 500})
	if err != nil {
		sagalog.For(r.Context(), nil).Error(err, "resolve workflow definition by id failed", golog.F("workflow_id", idStr))
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	for _, def := range defs {
		if def.ID == idStr {
			WriteJSON(w, http.StatusOK, def)
			return
		}
	}
	WriteError(w, http.StatusNotFound, "workflow_not_found", idStr)
}

// Save handles POST /api/v1/workflows. It decodes a WorkflowDefinition,
// validates it structurally, upserts it, and returns the stored definition
// carrying its storage id under "definition_id".
func (h *WorkflowHandler) Save(w http.ResponseWriter, r *http.Request) {
	var def domain.WorkflowDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if err := engine.ValidateDefinition(def); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, err.Error())
		return
	}
	id, err := h.S.UpsertWorkflowDefinition(r.Context(), def)
	if err != nil {
		sagalog.For(r.Context(), nil).Error(err, "upsert workflow definition failed", golog.F("workflow_id", def.ID))
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	WriteJSON(w, http.StatusOK, workflowSaveResponse{
		DefinitionID:       id,
		WorkflowDefinition: def,
	})
}

// workflowSaveResponse is the body of POST /api/v1/workflows: the saved
// definition plus its storage id.
type workflowSaveResponse struct {
	DefinitionID uuid.UUID `json:"definition_id"`
	domain.WorkflowDefinition
}
