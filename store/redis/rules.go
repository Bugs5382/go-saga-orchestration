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

// UpsertRuleDefinition stores def. If def.ID is the zero UUID a new one is
// generated. When re-upserting an existing storage ID the old version-list
// entry is removed first to prevent duplicates.
func (s *Store) UpsertRuleDefinition(ctx context.Context, def domain.RuleDefinition) (uuid.UUID, error) {
	// Preserve caller-supplied ID; generate one only when the zero UUID is passed.
	id := def.ID
	if id == uuid.Nil {
		id = uuid.New()
		def.ID = id
	}

	// If this storage ID already exists, remove the old version-list entry so
	// we do not accumulate duplicates.
	existing, ok, err := getJSON[domain.RuleDefinition](ctx, s.rdb, s.key("rule", id.String()))
	if err != nil {
		return uuid.Nil, err
	}
	if ok {
		// Remove the old entry from the version list before re-appending.
		if err := s.rdb.LRem(ctx, s.key("rule", "byid", existing.RuleID), 0, id.String()).Err(); err != nil {
			return uuid.Nil, err
		}
	}

	b, err := json.Marshal(def)
	if err != nil {
		return uuid.Nil, err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("rule", id.String()), b, 0)
	pipe.RPush(ctx, s.key("rule", "byid", def.RuleID), id.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// GetPublishedRuleByID returns the newest published version of ruleID, falling
// back to the most recent version when none is published. Returns ErrNotFound
// when ruleID has never been upserted.
func (s *Store) GetPublishedRuleByID(ctx context.Context, ruleID string, _ *uuid.UUID) (domain.RuleDefinition, error) {
	ids, err := s.rdb.LRange(ctx, s.key("rule", "byid", ruleID), 0, -1).Result()
	if err != nil {
		return domain.RuleDefinition{}, err
	}
	if len(ids) == 0 {
		return domain.RuleDefinition{}, store.ErrNotFound{Entity: "rule_definition", ID: ruleID}
	}

	// Walk newest-first; return the first Published==true.
	var fallback *domain.RuleDefinition
	for i := len(ids) - 1; i >= 0; i-- {
		rule, ok, err := getJSON[domain.RuleDefinition](ctx, s.rdb, s.key("rule", ids[i]))
		if err != nil {
			return domain.RuleDefinition{}, err
		}
		if !ok {
			continue
		}
		if fallback == nil {
			cp := rule
			fallback = &cp
		}
		if rule.Published {
			return rule, nil
		}
	}
	if fallback != nil {
		return *fallback, nil
	}
	return domain.RuleDefinition{}, store.ErrNotFound{Entity: "rule_definition", ID: ruleID}
}
