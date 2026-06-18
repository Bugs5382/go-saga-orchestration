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
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertRuleDefinition stores def atomically using a WATCH/MULTI optimistic
// transaction retry loop. If def.ID is the zero UUID a new one is generated.
// When re-upserting an existing storage ID the old version-list entry is
// removed inside the same transaction to prevent duplicates under concurrency.
func (s *Store) UpsertRuleDefinition(ctx context.Context, def domain.RuleDefinition) (uuid.UUID, error) {
	// Preserve caller-supplied ID; generate one only when the zero UUID is passed.
	id := def.ID
	if id == uuid.Nil {
		id = uuid.New()
		def.ID = id
	}

	rk := s.key("rule", id.String())

	b, err := json.Marshal(def)
	if err != nil {
		return uuid.Nil, err
	}

	for i := 0; i < txMaxRetries; i++ {
		err = s.rdb.Watch(ctx, func(tx *goredis.Tx) error {
			// Read existing blob inside the WATCH so any concurrent write to rk
			// invalidates the transaction.
			existing, ok, err := getJSON[domain.RuleDefinition](ctx, tx, rk)
			if err != nil {
				return err
			}

			_, txErr := tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				if ok {
					// Remove the stale version-list entry before re-appending.
					p.LRem(ctx, s.key("rule", "byid", existing.RuleID), 0, id.String())
				}
				p.Set(ctx, rk, b, 0)
				p.RPush(ctx, s.key("rule", "byid", def.RuleID), id.String())
				return nil
			})
			return txErr
		}, rk)

		if errors.Is(err, goredis.TxFailedErr) {
			continue // optimistic conflict; retry
		}
		if err != nil {
			return uuid.Nil, err
		}
		return id, nil
	}
	return uuid.Nil, errors.New("redis: tx retry budget exhausted for rule " + id.String())
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
