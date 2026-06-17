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
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/internal/rules"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// RulesHandler exposes the rules-package evaluator as a sync REST API.
type RulesHandler struct {
	S store.Store
}

// NewRulesHandler returns a RulesHandler backed by the given store.
func NewRulesHandler(s store.Store) *RulesHandler {
	return &RulesHandler{S: s}
}

type rulesEvalReq struct {
	Inputs   map[string]any `json:"inputs"`
	TenantID *string        `json:"tenant_id,omitempty"`
}

type rulesEvalResp struct {
	Output map[string]any       `json:"output"`
	Audit  []rules.EvaluatedRow `json:"audit"`
}

// Evaluate handles POST /api/v1/rules/{rule_id}/evaluate.
// Responses:
//
//	200 — rule found + evaluated; body {output, audit}.
//	404 — rule_id not found.
//	400 — bad request body.
//	422 — rule evaluation failed (CEL compile/eval error, no_decision_row_matched).
//	500 — internal error.
func (h *RulesHandler) Evaluate(w http.ResponseWriter, r *http.Request) {
	ruleID := chi.URLParam(r, "rule_id")
	if ruleID == "" {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "rule_id required")
		return
	}
	var body rulesEvalReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad request body")
		return
	}
	if body.Inputs == nil {
		body.Inputs = map[string]any{}
	}

	var tenant *uuid.UUID
	if body.TenantID != nil && *body.TenantID != "" {
		if u, err := uuid.Parse(*body.TenantID); err == nil {
			tenant = &u
		}
	}

	def, err := h.S.GetPublishedRuleByID(r.Context(), ruleID, tenant)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, CodeNotFound, "rule not found: "+ruleID)
			return
		}
		log.Error().Err(err).Str("rule_id", ruleID).Msg("get rule failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	output, audit, err := rules.Evaluate(r.Context(), def, body.Inputs)
	if err != nil {
		// CEL compile/eval failures and no_decision_row_matched are caller-fault.
		WriteError(w, http.StatusUnprocessableEntity, CodeUnprocessable, "rule evaluation failed: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rulesEvalResp{Output: output, Audit: audit})
}
