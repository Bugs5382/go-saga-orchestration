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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// actionKey returns the canonical Redis key for a registration.
func actionKey(service, actionName string, version int) string {
	return fmt.Sprintf("%s.%s:%d", service, actionName, version)
}

// UpsertActionRegistration stores or replaces a registration and adds it to idx:actions.
func (s *Store) UpsertActionRegistration(ctx context.Context, reg domain.ActionRegistration) error {
	if reg.ID == uuid.Nil {
		reg.ID = uuid.New()
	}
	if reg.RegisteredAt.IsZero() {
		reg.RegisteredAt = time.Now().UTC()
	}
	ak := actionKey(reg.Service, reg.ActionName, reg.Version)
	b, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("action", ak), b, 0)
	pipe.SAdd(ctx, s.key("idx", "actions"), ak)
	_, err = pipe.Exec(ctx)
	return err
}

// GetAction returns the registration for service+name+version, or ErrNotFound.
func (s *Store) GetAction(ctx context.Context, service, name string, version int) (domain.ActionRegistration, error) {
	ak := actionKey(service, name, version)
	reg, ok, err := getJSON[domain.ActionRegistration](ctx, s.rdb, s.key("action", ak))
	if err != nil {
		return domain.ActionRegistration{}, err
	}
	if !ok {
		return domain.ActionRegistration{}, store.ErrNotFound{
			Entity: "action_registration",
			ID:     fmt.Sprintf("%s.%s:%d", service, name, version),
		}
	}
	return reg, nil
}

// ListActions returns all registrations matching the optional filter fields.
func (s *Store) ListActions(ctx context.Context, filter store.ActionFilter) ([]domain.ActionRegistration, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "actions")).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.ActionRegistration{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("action", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.ActionRegistration, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var reg domain.ActionRegistration
		if err := unmarshalJSON([]byte(v.(string)), &reg); err != nil {
			return nil, err
		}
		if filter.Service != "" && reg.Service != filter.Service {
			continue
		}
		if filter.Category != "" && reg.Category != filter.Category {
			continue
		}
		if filter.Search != "" && !strings.Contains(reg.ActionName, filter.Search) {
			continue
		}
		out = append(out, reg)
	}
	return out, nil
}

// MarkAwaitingAction sets state=paused and records the dispatch key + attempt.
// Idempotent on (attempt, dispatch): same pair returns without writing.
func (s *Store) MarkAwaitingAction(ctx context.Context, runID uuid.UUID, dispatch string, attempt int) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, _ goredis.Pipeliner) error {
		// Idempotent: same (attempt, dispatch) pair has no effect.
		if r.CurrentAttempt == attempt && r.AwaitedActionDispatch != nil && *r.AwaitedActionDispatch == dispatch {
			return errAbortNoWrite
		}
		r.State = domain.RunStatePaused
		d := dispatch
		r.AwaitedActionDispatch = &d
		r.CurrentAttempt = attempt
		return nil
	})
}

// CompleteAction clears the dispatch marker, sets WakeupAt=now, and merges result
// into Variables. No-op when attempt != CurrentAttempt (late delivery).
func (s *Store) CompleteAction(ctx context.Context, runID uuid.UUID, attempt int, result map[string]any) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.CurrentAttempt != attempt {
			return errAbortNoWrite
		}
		r.AwaitedActionDispatch = nil
		now := time.Now().UTC()
		r.WakeupAt = &now
		p.ZAdd(ctx, s.key("idx", "wakeup"), goredis.Z{
			Score:  float64(now.UnixNano()),
			Member: runID.String(),
		})
		if r.Variables == nil {
			r.Variables = map[string]any{}
		}
		for k, v := range result {
			r.Variables[k] = v
		}
		return nil
	})
}

// FailAction transitions the run to failed and appends an audit event AFTER the tx.
// No-op when attempt != CurrentAttempt (late delivery).
func (s *Store) FailAction(ctx context.Context, runID uuid.UUID, attempt int, code, message string, retryable bool) error {
	var dispatch string
	var currentStep string
	err := s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.CurrentAttempt != attempt {
			return errAbortNoWrite
		}
		if r.AwaitedActionDispatch != nil {
			dispatch = *r.AwaitedActionDispatch
		}
		currentStep = r.CurrentStep
		r.AwaitedActionDispatch = nil
		r.State = domain.RunStateFailed
		s.applyTerminalTTL(ctx, p, runID)
		return nil
	})
	if err != nil {
		return err
	}
	// Append audit event AFTER the tx (mirror memory store semantics).
	evt := domain.NewEvent(runID, currentStep, attempt, domain.EventStepFailed, "engine")
	evt.Metadata = map[string]any{
		"code":      code,
		"message":   message,
		"retryable": retryable,
		"action":    dispatch,
	}
	return s.AppendEvent(ctx, evt)
}
