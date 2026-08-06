// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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
	"context"
	"encoding/json"
	"sort"
	"strings"

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
	pipe.SAdd(ctx, s.key("idx", "defs"), id.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// ListWorkflowDefinitions returns stored definitions matching filter,
// newest-first (CreatedAt DESC, then Version DESC). All versions of a
// workflow_id are returned (no dedupe).
func (s *Store) ListWorkflowDefinitions(ctx context.Context, filter store.DefinitionFilter) ([]domain.WorkflowDefinition, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "defs")).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.WorkflowDefinition{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("def", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	search := strings.ToLower(filter.Search)
	matched := make([]domain.WorkflowDefinition, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var def domain.WorkflowDefinition
		if err := unmarshalJSON([]byte(v.(string)), &def); err != nil {
			return nil, err
		}
		if filter.Published != nil && def.Published != *filter.Published {
			continue
		}
		if search != "" &&
			!strings.Contains(strings.ToLower(def.ID), search) &&
			!strings.Contains(strings.ToLower(def.Name), search) {
			continue
		}
		matched = append(matched, def)
	}

	// Newest-first: CreatedAt DESC, then Version DESC as a tiebreaker.
	sort.SliceStable(matched, func(i, j int) bool {
		if !matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].CreatedAt.After(matched[j].CreatedAt)
		}
		return matched[i].Version > matched[j].Version
	})

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(matched) {
		return []domain.WorkflowDefinition{}, nil
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], nil
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
