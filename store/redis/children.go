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

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// SpawnChildRunAt creates a child run linked to parentID/parentStepID/branchKey,
// beginning at startStep (empty string means the child definition's default start).
func (s *Store) SpawnChildRunAt(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any, startStep string) (uuid.UUID, error) {
	// Resolve (or upsert) the definition via def:byname:{workflowID}.
	byNameKey := s.key("def", "byname", def.ID)
	ids, err := s.rdb.LRange(ctx, byNameKey, 0, -1).Result()
	if err != nil {
		return uuid.Nil, err
	}

	var defStoredID uuid.UUID
	if len(ids) > 0 {
		// Use the most recently stored version.
		defStoredID, err = uuid.Parse(ids[len(ids)-1])
		if err != nil {
			return uuid.Nil, err
		}
	} else {
		// Upsert the definition.
		defStoredID = uuid.New()
		b, err := json.Marshal(def)
		if err != nil {
			return uuid.Nil, err
		}
		pipe := s.rdb.Pipeline()
		pipe.Set(ctx, s.key("def", defStoredID.String()), b, 0)
		pipe.RPush(ctx, byNameKey, defStoredID.String())
		if _, err := pipe.Exec(ctx); err != nil {
			return uuid.Nil, err
		}
	}

	// Build the child run.
	child := domain.NewSagaRun(def.ID, defStoredID, nil, inputs)
	if startStep != "" {
		child.CurrentStep = startStep
	}
	pid := parentID
	psid := parentStepID
	bid := branchKey
	child.ParentRunID = &pid
	child.ParentStepID = &psid
	child.ParentBranchID = &bid

	b, err := json.Marshal(child)
	if err != nil {
		return uuid.Nil, err
	}

	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("run", child.ID.String()), b, 0)
	pipe.ZAdd(ctx, s.key("idx", "runs"), goredis.Z{
		Score:  float64(child.StartedAt.UnixNano()),
		Member: child.ID.String(),
	})
	pipe.SAdd(ctx, s.key("idx", "runs", "byworkflow", def.ID), child.ID.String())
	pipe.SAdd(ctx, s.key("idx", "children", parentID.String(), parentStepID), child.ID.String())
	if _, err := pipe.Exec(ctx); err != nil {
		return uuid.Nil, err
	}
	return child.ID, nil
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
