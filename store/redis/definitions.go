// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertWorkflowDefinition stores def under a fresh storage ID and appends
// that ID to the def:byname:{workflowID} list (oldest→newest order).
func (s *Store) UpsertWorkflowDefinition(ctx context.Context, def domain.WorkflowDefinition) (uuid.UUID, error) {
	id := uuid.New()
	b, err := json.Marshal(def)
	if err != nil {
		return uuid.Nil, err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("def", id.String()), b, 0)
	pipe.RPush(ctx, s.key("def", "byname", def.ID), id.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// GetWorkflowDefinition returns the definition stored at the given storage ID,
// or ErrNotFound.
func (s *Store) GetWorkflowDefinition(ctx context.Context, id uuid.UUID) (domain.WorkflowDefinition, error) {
	def, ok, err := getJSON[domain.WorkflowDefinition](ctx, s.rdb, s.key("def", id.String()))
	if err != nil {
		return domain.WorkflowDefinition{}, err
	}
	if !ok {
		return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: id.String()}
	}
	return def, nil
}

// GetPublishedWorkflowByID returns the newest published version of workflowID,
// falling back to the most recent version when none is published. Returns
// ErrNotFound when the workflow ID has never been upserted.
func (s *Store) GetPublishedWorkflowByID(ctx context.Context, workflowID string, _ *uuid.UUID) (domain.WorkflowDefinition, error) {
	ids, err := s.rdb.LRange(ctx, s.key("def", "byname", workflowID), 0, -1).Result()
	if err != nil {
		return domain.WorkflowDefinition{}, err
	}
	if len(ids) == 0 {
		return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: workflowID}
	}

	// Walk newest-first (tail to head); return the first Published==true.
	var fallback *domain.WorkflowDefinition
	for i := len(ids) - 1; i >= 0; i-- {
		def, ok, err := getJSON[domain.WorkflowDefinition](ctx, s.rdb, s.key("def", ids[i]))
		if err != nil {
			return domain.WorkflowDefinition{}, err
		}
		if !ok {
			continue
		}
		if fallback == nil {
			cp := def
			fallback = &cp
		}
		if def.Published {
			return def, nil
		}
	}
	if fallback != nil {
		return *fallback, nil
	}
	return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: workflowID}
}
