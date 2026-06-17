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
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// RegistryHandler serves the action-registry REST surface.
//
//	POST /api/v1/registry/register — services call this on startup to
//	  register or refresh their action declarations.
//	GET  /api/v1/registry/actions   — read-only browser feed for admin/builder UIs.
type RegistryHandler struct {
	S store.Store
}

// NewRegistryHandler returns a RegistryHandler backed by the given store.
func NewRegistryHandler(s store.Store) *RegistryHandler {
	return &RegistryHandler{S: s}
}

// registerReq is the body of POST /api/v1/registry/register.
type registerReq struct {
	Service        string                      `json:"service"`
	ServiceVersion string                      `json:"service_version"`
	Actions        []domain.ActionRegistration `json:"actions"`
}

// Register accepts a service's startup payload. Upserts each action by
// (service, action_name, version). Idempotent — services may resend on
// every restart.
func (h *RegistryHandler) Register(w http.ResponseWriter, r *http.Request) {
	var body registerReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if body.Service == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "service required")
		return
	}
	if len(body.Actions) == 0 {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "actions required (non-empty)")
		return
	}
	for i, a := range body.Actions {
		if a.ActionName == "" {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "actions["+strconv.Itoa(i)+"].name required")
			return
		}
		if a.Version < 1 {
			WriteError(w, http.StatusBadRequest, CodeBadRequest, "actions["+strconv.Itoa(i)+"].version must be >= 1")
			return
		}
		// Stamp service + service_version from outer envelope (don't trust action's own values).
		a.Service = body.Service
		a.ServiceVersion = body.ServiceVersion
		if err := h.S.UpsertActionRegistration(r.Context(), a); err != nil {
			log.Error().Err(err).Str("action", a.ActionName).Msg("upsert action registration failed")
			WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service":         body.Service,
		"service_version": body.ServiceVersion,
		"registered":      len(body.Actions),
	})
}

// List returns the registered actions filtered by service/category/search.
//
//	GET /api/v1/registry/actions?service=&category=&search=
func (h *RegistryHandler) List(w http.ResponseWriter, r *http.Request) {
	filter := store.ActionFilter{
		Service:  r.URL.Query().Get("service"),
		Search:   r.URL.Query().Get("search"),
		Category: r.URL.Query().Get("category"),
	}
	actions, err := h.S.ListActions(r.Context(), filter)
	if err != nil {
		log.Error().Err(err).Msg("list actions failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"actions": actions})
}
