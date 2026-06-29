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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// CreateUserTask stores a new UserTask and adds it to the per-run index.
func (s *Store) CreateUserTask(ctx context.Context, task domain.UserTask) error {
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key("usertask", task.ID.String()), b, 0)
	pipe.SAdd(ctx, s.key("idx", "usertasks", "byrun", task.RunID.String()), task.ID.String())
	_, err = pipe.Exec(ctx)
	return err
}

// GetUserTask returns the UserTask or ErrNotFound.
func (s *Store) GetUserTask(ctx context.Context, taskID uuid.UUID) (domain.UserTask, error) {
	t, ok, err := getJSON[domain.UserTask](ctx, s.rdb, s.key("usertask", taskID.String()))
	if err != nil {
		return domain.UserTask{}, err
	}
	if !ok {
		return domain.UserTask{}, store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	return t, nil
}

// SubmitUserTask marks the task submitted. Returns ErrNotFound if it does not exist.
func (s *Store) SubmitUserTask(ctx context.Context, taskID uuid.UUID, submittedBy string, result map[string]any) error {
	t, ok, err := getJSON[domain.UserTask](ctx, s.rdb, s.key("usertask", taskID.String()))
	if err != nil {
		return err
	}
	if !ok {
		return store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	now := time.Now().UTC()
	t.SubmittedAt = &now
	t.SubmittedBy = submittedBy
	t.Result = result
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.key("usertask", taskID.String()), b, 0).Err()
}

// ListUserTasksByRun returns all user tasks for runID sorted by ID bytes.
// This matches the memory store's uuidLess ordering.
func (s *Store) ListUserTasksByRun(ctx context.Context, runID uuid.UUID) ([]domain.UserTask, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "usertasks", "byrun", runID.String())).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.UserTask{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("usertask", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.UserTask, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var t domain.UserTask
		if err := unmarshalJSON([]byte(v.(string)), &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	// Sort by ID bytes (uuidLess) for stable, deterministic ordering.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && uuidLess(out[j].ID, out[j-1].ID); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

// uuidLess compares two UUIDs lexicographically (byte by byte).
func uuidLess(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
