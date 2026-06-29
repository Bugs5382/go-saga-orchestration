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
	"errors"
	"fmt"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// SpawnChildRunAt creates a child run linked to parentID/parentStepID/branchKey,
// beginning at startStep (empty string means the child definition's default start).
// The whole operation — def upsert and run/index writes — is performed inside a
// single WATCH/MULTI/EXEC optimistic-transaction retry loop so it is atomic.
func (s *Store) SpawnChildRunAt(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any, startStep string) (uuid.UUID, error) {
	byNameKey := s.key("def", "byname", def.ID)

	// Generate both IDs once before the retry loop so every attempt reuses the
	// same values (idempotent on Redis if the tx fires more than once).
	newDefID := uuid.New()
	childID := uuid.New()

	defB, err := json.Marshal(def)
	if err != nil {
		return uuid.Nil, err
	}

	// Build the child run struct once; the ID and timestamps are stable.
	child := domain.NewSagaRun(def.ID, uuid.Nil, nil, inputs) // DefinitionID resolved inside tx
	child.ID = childID
	if startStep != "" {
		child.CurrentStep = startStep
	}
	pid := parentID
	psid := parentStepID
	bid := branchKey
	child.ParentRunID = &pid
	child.ParentStepID = &psid
	child.ParentBranchID = &bid

	for i := 0; i < txMaxRetries; i++ {
		err = s.rdb.Watch(ctx, func(tx *goredis.Tx) error {
			// Read the def:byname list inside the WATCH so a concurrent upsert
			// to byNameKey invalidates this transaction.
			ids, err := tx.LRange(ctx, byNameKey, 0, -1).Result()
			if err != nil {
				return err
			}

			var defStoredID uuid.UUID
			needDef := false
			if len(ids) > 0 {
				defStoredID, err = uuid.Parse(ids[len(ids)-1])
				if err != nil {
					return err
				}
			} else {
				defStoredID = newDefID
				needDef = true
			}

			child.DefinitionID = defStoredID
			childB, err := json.Marshal(child)
			if err != nil {
				return err
			}

			_, txErr := tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				if needDef {
					p.Set(ctx, s.key("def", defStoredID.String()), defB, 0)
					p.RPush(ctx, byNameKey, defStoredID.String())
				}
				p.Set(ctx, s.key("run", child.ID.String()), childB, 0)
				p.ZAdd(ctx, s.key("idx", "runs"), goredis.Z{
					Score:  float64(child.StartedAt.UnixNano()),
					Member: child.ID.String(),
				})
				p.SAdd(ctx, s.key("idx", "runs", "byworkflow", def.ID), child.ID.String())
				p.SAdd(ctx, s.key("idx", "children", parentID.String(), parentStepID), child.ID.String())
				return nil
			})
			return txErr
		}, byNameKey)

		if errors.Is(err, goredis.TxFailedErr) {
			continue // optimistic conflict; retry
		}
		if err != nil {
			return uuid.Nil, err
		}
		return child.ID, nil
	}
	return uuid.Nil, errors.New("redis: tx retry budget exhausted for child spawn under parent " + parentID.String())
}

// SpawnChildRun creates a child run beginning at the child definition's default start step.
func (s *Store) SpawnChildRun(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any) (uuid.UUID, error) {
	return s.SpawnChildRunAt(ctx, parentID, parentStepID, branchKey, def, inputs, "")
}

// ListChildrenByParent returns all child runs for parentID/parentStepID.
func (s *Store) ListChildrenByParent(ctx context.Context, parentID uuid.UUID, parentStepID string) ([]domain.SagaRun, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "children", parentID.String(), parentStepID)).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.SagaRun{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("run", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.SagaRun, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var r domain.SagaRun
		if err := unmarshalJSON([]byte(v.(string)), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// PushTryCatch appends frame to the run's TryCatchStack. Returns an error if
// the stack is already at maximum depth (3).
func (s *Store) PushTryCatch(ctx context.Context, runID uuid.UUID, frame domain.TryCatchFrame) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, _ goredis.Pipeliner) error {
		const maxDepth = 3
		if len(r.TryCatchStack) >= maxDepth {
			return fmt.Errorf("try_catch max nesting depth %d exceeded for run %s", maxDepth, runID)
		}
		r.TryCatchStack = append(r.TryCatchStack, frame)
		return nil
	})
}

// PopTryCatch removes and returns the top TryCatchFrame. Returns (zero, false, nil)
// when the stack is empty. Uses a closure variable to surface the popped value.
func (s *Store) PopTryCatch(ctx context.Context, runID uuid.UUID) (domain.TryCatchFrame, bool, error) {
	var popped domain.TryCatchFrame
	var found bool
	err := s.txRun(ctx, runID, func(r *domain.SagaRun, _ goredis.Pipeliner) error {
		if len(r.TryCatchStack) == 0 {
			// Empty stack: abort without writing; return (zero, false, nil) at the method level.
			return errAbortNoWrite
		}
		top := r.TryCatchStack[len(r.TryCatchStack)-1]
		r.TryCatchStack = r.TryCatchStack[:len(r.TryCatchStack)-1]
		popped = top
		found = true
		return nil
	})
	if err != nil {
		return domain.TryCatchFrame{}, false, err
	}
	return popped, found, nil
}
