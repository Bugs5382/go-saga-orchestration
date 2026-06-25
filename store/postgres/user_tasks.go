package postgres

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
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// CreateUserTask inserts a new user task row into runtime.saga_user_tasks.
func (s *Store) CreateUserTask(ctx context.Context, task domain.UserTask) error {
	formJSON, err := json.Marshal(task.FormSchema)
	if err != nil {
		return fmt.Errorf("create user task: marshal form_schema: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO runtime.saga_user_tasks
		  (id, run_id, step_id, assignee, due_at, form_schema)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		task.ID, task.RunID, task.StepID, task.Assignee, task.DueAt, formJSON,
	)
	if err != nil {
		return fmt.Errorf("create user task: %w", err)
	}
	return nil
}

// GetUserTask returns the user task by ID, or ErrNotFound.
func (s *Store) GetUserTask(ctx context.Context, taskID uuid.UUID) (domain.UserTask, error) {
	var (
		task       domain.UserTask
		formJSON   []byte
		resultJSON []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, run_id, step_id, assignee, due_at, form_schema,
		       submitted_at, COALESCE(submitted_by, '') AS submitted_by, result
		FROM runtime.saga_user_tasks WHERE id = $1`, taskID).Scan(
		&task.ID, &task.RunID, &task.StepID, &task.Assignee, &task.DueAt, &formJSON,
		&task.SubmittedAt, &task.SubmittedBy, &resultJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserTask{}, store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	if err != nil {
		return domain.UserTask{}, fmt.Errorf("get user task: %w", err)
	}
	if len(formJSON) > 0 {
		if err := json.Unmarshal(formJSON, &task.FormSchema); err != nil {
			return domain.UserTask{}, fmt.Errorf("get user task: unmarshal form_schema: %w", err)
		}
	}
	if len(resultJSON) > 0 {
		if err := json.Unmarshal(resultJSON, &task.Result); err != nil {
			return domain.UserTask{}, fmt.Errorf("get user task: unmarshal result: %w", err)
		}
	}
	return task, nil
}

// ListUserTasksByRun returns all user tasks for a given run, ordered by
// creation time (id order is used as a stable proxy since saga_user_tasks
// has no separate created_at column beyond the implicit id ordering).
func (s *Store) ListUserTasksByRun(ctx context.Context, runID uuid.UUID) ([]domain.UserTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, step_id, assignee, due_at, form_schema,
		       submitted_at, COALESCE(submitted_by, '') AS submitted_by, result
		FROM runtime.saga_user_tasks
		WHERE run_id = $1
		ORDER BY id`, runID)
	if err != nil {
		return nil, fmt.Errorf("list user tasks by run: %w", err)
	}
	defer rows.Close()

	var out []domain.UserTask
	for rows.Next() {
		var (
			task       domain.UserTask
			formJSON   []byte
			resultJSON []byte
		)
		if err := rows.Scan(
			&task.ID, &task.RunID, &task.StepID, &task.Assignee, &task.DueAt, &formJSON,
			&task.SubmittedAt, &task.SubmittedBy, &resultJSON,
		); err != nil {
			return nil, fmt.Errorf("list user tasks by run: scan: %w", err)
		}
		if len(formJSON) > 0 {
			if err := json.Unmarshal(formJSON, &task.FormSchema); err != nil {
				return nil, fmt.Errorf("list user tasks by run: unmarshal form_schema: %w", err)
			}
		}
		if len(resultJSON) > 0 {
			if err := json.Unmarshal(resultJSON, &task.Result); err != nil {
				return nil, fmt.Errorf("list user tasks by run: unmarshal result: %w", err)
			}
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list user tasks by run: rows: %w", err)
	}
	if out == nil {
		out = []domain.UserTask{}
	}
	return out, nil
}

// SubmitUserTask marks the task submitted with the given actor and result.
// Idempotent: re-submitting overwrites the previous submission fields.
// Returns ErrNotFound if the task does not exist.
func (s *Store) SubmitUserTask(ctx context.Context, taskID uuid.UUID, submittedBy string, result map[string]any) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("submit user task: marshal result: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE runtime.saga_user_tasks
		   SET submitted_at = now(),
		       submitted_by = $2,
		       result       = $3
		 WHERE id = $1`,
		taskID, submittedBy, resultJSON,
	)
	if err != nil {
		return fmt.Errorf("submit user task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	return nil
}
