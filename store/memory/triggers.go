package memory

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
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertTrigger inserts or replaces a SagaTrigger. If trigger.ID == uuid.Nil a
// new ID is generated. If trigger.ID is set and a row already exists it is
// replaced in full.
func (s *Store) UpsertTrigger(_ context.Context, trigger domain.SagaTrigger) (uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggers == nil {
		s.triggers = map[uuid.UUID]domain.SagaTrigger{}
	}
	id := trigger.ID
	if id == uuid.Nil {
		id = uuid.New()
		trigger.ID = id
	}
	if trigger.CreatedAt.IsZero() {
		trigger.CreatedAt = time.Now().UTC()
	}
	s.triggers[id] = trigger
	return id, nil
}

// GetTrigger returns the SagaTrigger for id, or ErrNotFound.
func (s *Store) GetTrigger(_ context.Context, id uuid.UUID) (domain.SagaTrigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.triggers[id]
	if !ok {
		return domain.SagaTrigger{}, store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
	}
	return t, nil
}

// ListTriggers returns triggers matching the optional filter.
func (s *Store) ListTriggers(_ context.Context, filter store.TriggerFilter) ([]domain.SagaTrigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.SagaTrigger{}
	for _, t := range s.triggers {
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

// DeleteTrigger removes the trigger from the store. Returns ErrNotFound if it
// does not exist.
func (s *Store) DeleteTrigger(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.triggers[id]; !ok {
		return store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
	}
	delete(s.triggers, id)
	return nil
}

// ListDueCronTriggers returns enabled cron triggers whose next_fire_at is at
// or before now, sorted oldest-first, capped at limit.
func (s *Store) ListDueCronTriggers(_ context.Context, now time.Time, limit int) ([]domain.SagaTrigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []domain.SagaTrigger
	for _, t := range s.triggers {
		if t.TriggerType != domain.TriggerCron || !t.Enabled || t.NextFireAt == nil {
			continue
		}
		if !t.NextFireAt.After(now) { // next_fire_at <= now
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NextFireAt.Before(*out[j].NextFireAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ClaimCronFire atomically advances next_fire_at from expectedNextFire to
// newNextFire and stamps last_fired_at. Returns true iff this caller won
// (the CAS matched). Returns false without error when the expected value no
// longer matches — the caller lost the race.
func (s *Store) ClaimCronFire(_ context.Context, id uuid.UUID, expectedNextFire, newNextFire time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[id]
	if !ok || t.NextFireAt == nil || !t.NextFireAt.Equal(expectedNextFire) {
		return false, nil
	}
	now := newNextFire // memory has no wall clock; stamp last_fired_at off the claim
	t.NextFireAt = &newNextFire
	t.LastFiredAt = &now
	s.triggers[id] = t
	return true, nil
}
