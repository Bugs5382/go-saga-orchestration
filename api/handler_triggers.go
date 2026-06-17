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
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// TriggerHandler serves the saga-trigger REST surface.
//
//	POST   /api/v1/triggers       — create
//	GET    /api/v1/triggers       — list (optional ?type= ?enabled=)
//	GET    /api/v1/triggers/{id}  — get one
//	DELETE /api/v1/triggers/{id}  — remove
type TriggerHandler struct {
	S store.Store
}

// NewTriggerHandler constructs the handler.
func NewTriggerHandler(s store.Store) *TriggerHandler {
	return &TriggerHandler{S: s}
}

// triggerCreateReq is the body of POST /api/v1/triggers.
type triggerCreateReq struct {
	TriggerType domain.TriggerType `json:"trigger_type"`
	WorkflowID  string             `json:"workflow_id"`
	Version     int                `json:"version"`
	Config      map[string]any     `json:"config"`
	Enabled     bool               `json:"enabled"`
	TenantID    *string            `json:"tenant_id,omitempty"`
	CreatedBy   string             `json:"created_by"`
}

// Create handles POST /api/v1/triggers.
func (h *TriggerHandler) Create(w http.ResponseWriter, r *http.Request) {
	var body triggerCreateReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad body: "+err.Error())
		return
	}

	// Basic field validation.
	if body.TriggerType == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "trigger_type required")
		return
	}
	if body.WorkflowID == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "workflow_id required")
		return
	}
	if body.Version < 1 {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "version must be >= 1")
		return
	}
	if body.Config == nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "config field must be present (use {} for empty)")
		return
	}
	if body.CreatedBy == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "created_by required")
		return
	}

	// Type-specific validation.
	if body.TriggerType == domain.TriggerRecordTransition {
		if err := validateRecordTransitionConfig(body.Config); err != nil {
			WriteError(w, http.StatusUnprocessableEntity, CodeInvalidConfig, err.Error())
			return
		}
	}

	// Build domain object — ignore any ID from body (server assigns).
	trigger := domain.SagaTrigger{
		TriggerType: body.TriggerType,
		WorkflowID:  body.WorkflowID,
		Version:     body.Version,
		Config:      body.Config,
		Enabled:     body.Enabled,
		CreatedBy:   body.CreatedBy,
	}
	if body.TenantID != nil && *body.TenantID != "" {
		if u, err := uuid.Parse(*body.TenantID); err == nil {
			trigger.TenantID = &u
		}
	}

	id, err := h.S.UpsertTrigger(r.Context(), trigger)
	if err != nil {
		log.Error().Err(err).Msg("upsert trigger failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	// Fetch the persisted row so we return the server-assigned fields.
	created, err := h.S.GetTrigger(r.Context(), id)
	if err != nil {
		log.Error().Err(err).Str("trigger_id", id.String()).Msg("get trigger after upsert failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	WriteJSON(w, http.StatusCreated, created)
}

// Get handles GET /api/v1/triggers/{id}.
func (h *TriggerHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "invalid id")
		return
	}

	trigger, err := h.S.GetTrigger(r.Context(), id)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, "trigger_not_found", idStr)
			return
		}
		log.Error().Err(err).Str("trigger_id", idStr).Msg("get trigger failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	WriteJSON(w, http.StatusOK, trigger)
}

// List handles GET /api/v1/triggers.
// Optional query params: ?type= ?enabled=true|false
func (h *TriggerHandler) List(w http.ResponseWriter, r *http.Request) {
	filter := store.TriggerFilter{}

	if t := r.URL.Query().Get("type"); t != "" {
		filter.Type = domain.TriggerType(t)
	}

	if e := r.URL.Query().Get("enabled"); e != "" {
		switch e {
		case "true", "1":
			v := true
			filter.Enabled = &v
		case "false", "0":
			v := false
			filter.Enabled = &v
		}
	}

	triggers, err := h.S.ListTriggers(r.Context(), filter)
	if err != nil {
		log.Error().Err(err).Msg("list triggers failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"triggers": triggers})
}

// Delete handles DELETE /api/v1/triggers/{id}.
// Returns 204 on success, 404 if no row exists (mirrors the non-idempotent
// pattern — store.DeleteTrigger returns ErrNotFound for missing rows).
func (h *TriggerHandler) Delete(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "invalid id")
		return
	}

	if err := h.S.DeleteTrigger(r.Context(), id); err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, "trigger_not_found", idStr)
			return
		}
		log.Error().Err(err).Str("trigger_id", idStr).Msg("delete trigger failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// validateRecordTransitionConfig checks that config has the three required
// string fields for TriggerRecordTransition.
func validateRecordTransitionConfig(cfg map[string]any) error {
	required := []string{"record_type", "from_state", "to_state"}
	for _, key := range required {
		v, ok := cfg[key]
		if !ok {
			return fmt.Errorf("config.%s is required for trigger_type record_transition", key)
		}
		s, ok := v.(string)
		if !ok || s == "" {
			return fmt.Errorf("config.%s must be a non-empty string", key)
		}
	}
	return nil
}
