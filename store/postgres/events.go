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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// AppendEvent inserts the audit event, ignoring duplicates on
// (run_id, step_id, attempt, event_type).
func (s *Store) AppendEvent(ctx context.Context, evt domain.SagaRunEvent) error {
	meta, err := json.Marshal(evt.Metadata)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO audit.saga_run_events
		  (id, run_id, step_id, attempt, event_type, from_state, to_state, actor, metadata, recorded_at)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, NULLIF($6, ''), NULLIF($7, ''), $8, $9, $10)
		ON CONFLICT (run_id, step_id, attempt, event_type) DO NOTHING`,
		evt.ID, evt.RunID, evt.StepID, evt.Attempt, evt.EventType,
		evt.FromState, evt.ToState, evt.Actor, meta, evt.RecordedAt,
	)
	return err
}

// ListEventsByRun returns the events recorded for runID ordered by recorded_at.
func (s *Store) ListEventsByRun(ctx context.Context, runID uuid.UUID) ([]domain.SagaRunEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, COALESCE(step_id,''), attempt, event_type,
		       COALESCE(from_state,''), COALESCE(to_state,''), actor, metadata, recorded_at
		  FROM audit.saga_run_events
		 WHERE run_id = $1
		 ORDER BY recorded_at ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SagaRunEvent
	for rows.Next() {
		var e domain.SagaRunEvent
		var meta []byte
		if err := rows.Scan(&e.ID, &e.RunID, &e.StepID, &e.Attempt, &e.EventType,
			&e.FromState, &e.ToState, &e.Actor, &meta, &e.RecordedAt); err != nil {
			return nil, err
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &e.Metadata)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetEventByID returns a single audit event by its UUID, or ErrNotFound.
func (s *Store) GetEventByID(ctx context.Context, id uuid.UUID) (domain.SagaRunEvent, error) {
	var e domain.SagaRunEvent
	var meta []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, run_id, COALESCE(step_id,''), attempt, event_type,
		       COALESCE(from_state,''), COALESCE(to_state,''), actor, metadata, recorded_at
		  FROM audit.saga_run_events
		 WHERE id = $1`, id).
		Scan(&e.ID, &e.RunID, &e.StepID, &e.Attempt, &e.EventType,
			&e.FromState, &e.ToState, &e.Actor, &meta, &e.RecordedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.SagaRunEvent{}, store.ErrNotFound{Entity: "saga_run_event", ID: id.String()}
		}
		return domain.SagaRunEvent{}, err
	}
	if len(meta) > 0 {
		_ = json.Unmarshal(meta, &e.Metadata)
	}
	return e, nil
}
