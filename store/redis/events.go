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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// AppendEvent appends evt to the per-run event list and stores it under its
// own key for direct lookup by ID.
func (s *Store) AppendEvent(ctx context.Context, evt domain.SagaRunEvent) error {
	b, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.RPush(ctx, s.key("events", evt.RunID.String()), b)
	pipe.Set(ctx, s.key("event", evt.ID.String()), b, 0)
	_, err = pipe.Exec(ctx)
	return err
}

// ListEventsByRun returns all events for runID in append order. An empty slice
// (not an error) is returned when no events have been appended.
func (s *Store) ListEventsByRun(ctx context.Context, runID uuid.UUID) ([]domain.SagaRunEvent, error) {
	items, err := s.rdb.LRange(ctx, s.key("events", runID.String()), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.SagaRunEvent, 0, len(items))
	for _, raw := range items {
		var evt domain.SagaRunEvent
		if err := json.Unmarshal([]byte(raw), &evt); err != nil {
			return nil, err
		}
		out = append(out, evt)
	}
	return out, nil
}

// GetEventByID returns the event with the given ID, or ErrNotFound.
func (s *Store) GetEventByID(ctx context.Context, id uuid.UUID) (domain.SagaRunEvent, error) {
	evt, ok, err := getJSON[domain.SagaRunEvent](ctx, s.rdb, s.key("event", id.String()))
	if err != nil {
		return domain.SagaRunEvent{}, err
	}
	if !ok {
		return domain.SagaRunEvent{}, store.ErrNotFound{Entity: "saga_run_event", ID: id.String()}
	}
	return evt, nil
}
