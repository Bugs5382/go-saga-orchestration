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
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// AdvancePublisher abstracts the RabbitMQ publisher so handlers can be
// unit-tested without a broker.
type AdvancePublisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// SagaHandler owns the /api/v1/sagas/* routes.
type SagaHandler struct {
	store     store.Store
	pub       AdvancePublisher
	providers []engine.StartupVariableProvider
}

// NewSagaHandler constructs the handler. Optional StartupVariableProviders are
// invoked at saga start to inject per-tenant "magic" variables.
func NewSagaHandler(s store.Store, p AdvancePublisher, providers ...engine.StartupVariableProvider) *SagaHandler {
	return &SagaHandler{store: s, pub: p, providers: providers}
}

// startRequest is the body of POST /sagas/start.
type startRequest struct {
	WorkflowID string         `json:"workflow_id"`
	Version    string         `json:"version,omitempty"` // "latest" or specific
	Inputs     map[string]any `json:"inputs"`
	TenantID   *uuid.UUID     `json:"tenant_id,omitempty"`
	DryRun     bool           `json:"dry_run,omitempty"`
}

// Start handles POST /api/v1/sagas/start. Resolves the published
// definition, inserts a saga_runs row, publishes saga.advance, returns
// 202 with the run ID.
func (h *SagaHandler) Start(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if req.WorkflowID == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "workflow_id is required")
		return
	}

	def, err := h.store.GetPublishedWorkflowByID(r.Context(), req.WorkflowID, req.TenantID)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, "workflow_not_found", req.WorkflowID)
			return
		}
		log.Error().Err(err).Str("workflow_id", req.WorkflowID).Msg("get published workflow failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	// Upsert the definition to get a row id we can reference from the run.
	// (In the memory store this generates a new UUID per call; that's fine
	// for v1 since runs only need *some* definition_id pointer. The
	// postgres store's UPSERT keeps a stable id per (workflow_id, version).)
	defRowID, err := h.store.UpsertWorkflowDefinition(r.Context(), def)
	if err != nil {
		log.Error().Err(err).Str("workflow_id", req.WorkflowID).Msg("upsert workflow definition failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	run := domain.NewSagaRun(def.ID, defRowID, req.TenantID, req.Inputs)
	run.DryRun = req.DryRun
	run.FeatureOverrides = parseFeatureOverrideHeader(r.Header.Get("X-Feature-Override"))
	if err := h.store.CreateRun(r.Context(), run); err != nil {
		log.Error().Err(err).Msg("create run failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	engine.InjectStartupVariables(r.Context(), h.store, run.ID, req.TenantID, log.Logger, h.providers...)
	if err := h.pub.PublishSagaAdvance(r.Context(), run.ID.String()); err != nil {
		log.Error().Err(err).Str("run_id", run.ID.String()).Msg("publish saga advance failed")
		WriteError(w, http.StatusInternalServerError, CodePublishFailed, genericInternalMessage)
		return
	}

	WriteJSON(w, http.StatusAccepted, map[string]string{"saga_run_id": run.ID.String()})
}

// Get handles GET /api/v1/sagas/{id}.
func (h *SagaHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "invalid id")
		return
	}
	run, err := h.store.GetRun(r.Context(), id)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, "saga_not_found", idStr)
			return
		}
		log.Error().Err(err).Str("run_id", idStr).Msg("get run failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	WriteJSON(w, http.StatusOK, run)
}

// listResponse is the body of GET /api/v1/sagas.
type listResponse struct {
	Sagas  []domain.SagaRun `json:"sagas"`
	Total  int              `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}

// List handles GET /api/v1/sagas. Parses optional filter query params,
// calls store.ListRuns + store.CountRuns, returns paginated JSON.
func (h *SagaHandler) List(w http.ResponseWriter, r *http.Request) {
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

	filter := store.RunFilter{
		WorkflowID:  q.Get("workflow_id"),
		State:       q.Get("state"),
		TriggerType: q.Get("trigger_type"),
		Limit:       limit,
		Offset:      offset,
	}

	// Parse since (RFC3339).
	if raw := q.Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "since must be an RFC3339 timestamp")
			return
		}
		filter.Since = &t
	}

	// Parse has_error (true/false).
	if raw := q.Get("has_error"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "has_error must be true or false")
			return
		}
		filter.HasError = &v
	}

	// Parse requires_review (true/false).
	if raw := q.Get("requires_review"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "requires_review must be true or false")
			return
		}
		filter.RequiresReview = &v
	}

	runs, err := h.store.ListRuns(r.Context(), filter)
	if err != nil {
		log.Error().Err(err).Msg("list runs failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	total, err := h.store.CountRuns(r.Context(), filter)
	if err != nil {
		log.Error().Err(err).Msg("count runs failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	// Ensure sagas is never null in JSON output.
	if runs == nil {
		runs = []domain.SagaRun{}
	}

	WriteJSON(w, http.StatusOK, listResponse{
		Sagas:  runs,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	})
}

// parseFeatureOverrideHeader parses the "X-Feature-Override" header
// value into a feature→bool map. Header format:
//
//	"wf.parallel=on,wf.timers=off,wf.user_tasks=true"
//
// Values "on", "true", "1" → true. "off", "false", "0" → false. Other
// values cause the entry to be skipped (lenient parsing — a typo shouldn't
// silently grant or revoke an unintended override).
func parseFeatureOverrideHeader(h string) map[string]bool {
	if h == "" {
		return nil
	}
	out := map[string]bool{}
	for _, pair := range strings.Split(h, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eq := strings.IndexByte(pair, '=')
		if eq < 1 || eq == len(pair)-1 {
			continue
		}
		feature := strings.TrimSpace(pair[:eq])
		valStr := strings.ToLower(strings.TrimSpace(pair[eq+1:]))
		var val bool
		switch valStr {
		case "on", "true", "1":
			val = true
		case "off", "false", "0":
			val = false
		default:
			continue
		}
		out[feature] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
