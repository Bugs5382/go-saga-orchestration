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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertTrigger inserts or replaces a SagaTrigger. Generates a new ID when
// trigger.ID == uuid.Nil. Defaults CreatedAt to now when zero.
func (s *Store) UpsertTrigger(ctx context.Context, trigger domain.SagaTrigger) (uuid.UUID, error) {
	id := trigger.ID
	if id == uuid.Nil {
		id = uuid.New()
		trigger.ID = id
	}
	if trigger.CreatedAt.IsZero() {
		trigger.CreatedAt = time.Now().UTC()
	}
	b, err := json.Marshal(trigger)
	if err != nil {
		return uuid.Nil, err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("trigger", id.String()), b, 0)
	pipe.SAdd(ctx, s.key("idx", "triggers"), id.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// GetTrigger returns the SagaTrigger for id, or ErrNotFound.
func (s *Store) GetTrigger(ctx context.Context, id uuid.UUID) (domain.SagaTrigger, error) {
	t, ok, err := getJSON[domain.SagaTrigger](ctx, s.rdb, s.key("trigger", id.String()))
	if err != nil {
		return domain.SagaTrigger{}, err
	}
	if !ok {
		return domain.SagaTrigger{}, store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
	}
	return t, nil
}

// ListTriggers returns triggers matching the optional filter fields.
func (s *Store) ListTriggers(ctx context.Context, filter store.TriggerFilter) ([]domain.SagaTrigger, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "triggers")).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.SagaTrigger{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("trigger", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.SagaTrigger, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var t domain.SagaTrigger
		if err := unmarshalJSON([]byte(v.(string)), &t); err != nil {
			return nil, err
		}
		if filter.Type != "" && t.TriggerType != filter.Type {
			continue
		}
		if filter.Enabled != nil && t.Enabled != *filter.Enabled {
			continue
		}
		if filter.TenantID != nil {
			if t.TenantID == nil || *t.TenantID != *filter.TenantID {
				continue
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// DeleteTrigger removes the trigger. Returns ErrNotFound if absent.
func (s *Store) DeleteTrigger(ctx context.Context, id uuid.UUID) error {
	exists, err := s.rdb.Exists(ctx, s.key("trigger", id.String())).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, s.key("trigger", id.String()))
	pipe.SRem(ctx, s.key("idx", "triggers"), id.String())
	_, err = pipe.Exec(ctx)
	return err
}
