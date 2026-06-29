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
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// CreateRun stores the run blob and adds it to the two index structures.
func (s *Store) CreateRun(ctx context.Context, run domain.SagaRun) error {
	b, err := json.Marshal(run)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("run", run.ID.String()), b, 0)
	pipe.ZAdd(ctx, s.key("idx", "runs"), goredis.Z{
		Score:  float64(run.StartedAt.UnixNano()),
		Member: run.ID.String(),
	})
	pipe.SAdd(ctx, s.key("idx", "runs", "byworkflow", run.WorkflowID), run.ID.String())
	_, err = pipe.Exec(ctx)
	return err
}

// GetRun returns the run with the given ID, or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, id uuid.UUID) (domain.SagaRun, error) {
	run, ok, err := getJSON[domain.SagaRun](ctx, s.rdb, s.key("run", id.String()))
	if err != nil {
		return domain.SagaRun{}, err
	}
	if !ok {
		return domain.SagaRun{}, store.ErrNotFound{Entity: "saga_run", ID: id.String()}
	}
	return run, nil
}

// UpdateRunState sets the run's state and current step via a WATCH/MULTI
// transaction. When the new state is terminal, the run is removed from the
// active idx:wakeup and idx:awaitevent indexes.
func (s *Store) UpdateRunState(ctx context.Context, id uuid.UUID, state domain.RunState, currentStep string) error {
	return s.txRun(ctx, id, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		r.State = state
		r.CurrentStep = currentStep
		if state.IsTerminal() {
			p.ZRem(ctx, s.key("idx", "wakeup"), id.String())
			if r.AwaitedEventTopic != nil {
				p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), id.String())
			}
			s.applyTerminalTTL(ctx, p, id)
		}
		return nil
	})
}

// Cancel terminates an in-flight run: terminal cancelled + terminal_at,
// reason in last_error, open user tasks closed, await markers / wakeup
// cleared (and the run dropped from the active indexes). Idempotent — a
// no-op (and no event) once the run is terminal. See issue #80.
func (s *Store) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	var (
		cancelled bool
		step      string
	)
	err := s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.State.IsTerminal() {
			return nil // idempotent
		}
		cancelled = true
		step = r.CurrentStep
		now := time.Now().UTC()
		r.State = domain.RunStateCancelled
		r.TerminalAt = &now
		r.LastEventAt = now
		if reason != "" {
			rc := reason
			r.LastError = &rc
		}
		p.ZRem(ctx, s.key("idx", "wakeup"), runID.String())
		if r.AwaitedEventTopic != nil {
			p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), runID.String())
		}
		r.WakeupAt = nil
		r.AwaitedSignal = nil
		r.AwaitedEventTopic = nil
		r.AwaitedEventHeaders = nil
		r.AwaitedActionDispatch = nil
		s.applyTerminalTTL(ctx, p, runID)
		return nil
	})
	if err != nil {
		return err
	}
	if !cancelled {
		return nil // already terminal
	}
	// Close the run's open user tasks so none linger pending.
	tasks, err := s.ListUserTasksByRun(ctx, runID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, t := range tasks {
		if t.SubmittedAt != nil {
			continue
		}
		t.SubmittedAt = &now
		t.SubmittedBy = "system:run-cancelled"
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if err := s.rdb.Set(ctx, s.key("usertask", t.ID.String()), b, 0).Err(); err != nil {
			return err
		}
	}
	evt := domain.NewEvent(runID, step, 0, domain.EventRunCancelled, "engine")
	if reason != "" {
		evt.Metadata = map[string]any{"reason": reason}
	}
	return s.AppendEvent(ctx, evt)
}

// MarkRunFailed transitions a run to terminal failed, stamps terminal_at,
// and persists lastError on the run. Idempotent on terminal runs. See
// issue #80.
func (s *Store) MarkRunFailed(ctx context.Context, runID uuid.UUID, currentStep, lastError string) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.State.IsTerminal() {
			return nil // idempotent
		}
		now := time.Now().UTC()
		r.State = domain.RunStateFailed
		r.CurrentStep = currentStep
		r.TerminalAt = &now
		r.LastEventAt = now
		if lastError != "" {
			ec := lastError
			r.LastError = &ec
		}
		p.ZRem(ctx, s.key("idx", "wakeup"), runID.String())
		if r.AwaitedEventTopic != nil {
			p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), runID.String())
		}
		s.applyTerminalTTL(ctx, p, runID)
		return nil
	})
}

// UpdateRunVariables merges entries of merge into the run's Variables using
// dotted-key path semantics, via a WATCH/MULTI transaction.
func (s *Store) UpdateRunVariables(ctx context.Context, id uuid.UUID, merge map[string]any) error {
	return s.txRun(ctx, id, func(r *domain.SagaRun, _ goredis.Pipeliner) error {
		if r.Variables == nil {
			r.Variables = map[string]any{}
		}
		for k, v := range merge {
			applyDottedKey(r.Variables, k, v)
		}
		return nil
	})
}

// applyDottedKey writes value at the dot-walked path within target.
// "scope.subkey" walks into a nested map. Top-level keys without a dot
// are a plain map assignment.
func applyDottedKey(target map[string]any, key string, value any) {
	parts := []string{}
	cur := ""
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			parts = append(parts, cur)
			cur = ""
			continue
		}
		cur += string(key[i])
	}
	parts = append(parts, cur)

	t := target
	for i, p := range parts {
		if i == len(parts)-1 {
			t[p] = value
			return
		}
		nested, ok := t[p].(map[string]any)
		if !ok {
			nested = map[string]any{}
			t[p] = nested
		}
		t = nested
	}
}
