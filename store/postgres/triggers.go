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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertTrigger inserts or replaces a row in runtime.saga_triggers keyed by id.
// If trigger.ID == uuid.Nil a new ID is generated. If the ID is set and a row
// already exists, all mutable columns are replaced (upsert on conflict).
func (s *Store) UpsertTrigger(ctx context.Context, trigger domain.SagaTrigger) (uuid.UUID, error) {
	id := trigger.ID
	if id == uuid.Nil {
		id = uuid.New()
	}
	if trigger.CreatedAt.IsZero() {
		trigger.CreatedAt = time.Now().UTC()
	}
	configJSON, err := json.Marshal(trigger.Config)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal trigger config: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO runtime.saga_triggers
  (id, trigger_type, workflow_id, version, config, enabled, tenant_id, created_at, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO UPDATE SET
  trigger_type = EXCLUDED.trigger_type,
  workflow_id  = EXCLUDED.workflow_id,
  version      = EXCLUDED.version,
  config       = EXCLUDED.config,
  enabled      = EXCLUDED.enabled,
  tenant_id    = EXCLUDED.tenant_id
`,
		id,
		string(trigger.TriggerType),
		trigger.WorkflowID,
		trigger.Version,
		configJSON,
		trigger.Enabled,
		trigger.TenantID,
		trigger.CreatedAt,
		trigger.CreatedBy,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("upsert trigger: %w", err)
	}
	return id, nil
}

// GetTrigger returns the SagaTrigger for id, or ErrNotFound.
func (s *Store) GetTrigger(ctx context.Context, id uuid.UUID) (domain.SagaTrigger, error) {
	row := s.pool.QueryRow(ctx, `
SELECT id, trigger_type, workflow_id, version, config, enabled, tenant_id, created_at, created_by
FROM runtime.saga_triggers
WHERE id = $1`, id)
	t, err := scanTrigger(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.SagaTrigger{}, store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
		}
		return domain.SagaTrigger{}, fmt.Errorf("get trigger: %w", err)
	}
	return t, nil
}

// ListTriggers returns triggers matching the optional filter.
func (s *Store) ListTriggers(ctx context.Context, filter store.TriggerFilter) ([]domain.SagaTrigger, error) {
	where := []string{}
	args := []any{}
	if filter.Type != "" {
		args = append(args, string(filter.Type))
		where = append(where, "trigger_type = $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	if filter.TenantID != nil {
		args = append(args, *filter.TenantID)
		where = append(where, "tenant_id = $"+strconv.Itoa(len(args)))
	}
	q := `SELECT id, trigger_type, workflow_id, version, config, enabled, tenant_id, created_at, created_by
          FROM runtime.saga_triggers`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at"

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	defer rows.Close()
	out := []domain.SagaTrigger{}
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, fmt.Errorf("scan trigger: %w", err)
		}
		out = append(out, t)
	}
	return out, nil
}

// DeleteTrigger removes the trigger by id. Returns ErrNotFound if absent.
func (s *Store) DeleteTrigger(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM runtime.saga_triggers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete trigger: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound{Entity: "saga_trigger", ID: id.String()}
	}
	return nil
}

// scanTrigger scans a single row (from QueryRow or rows.Scan) into SagaTrigger.
type triggerScanner interface {
	Scan(dest ...any) error
}

func scanTrigger(sc triggerScanner) (domain.SagaTrigger, error) {
	var (
		t          domain.SagaTrigger
		trigType   string
		configJSON []byte
	)
	if err := sc.Scan(
		&t.ID,
		&trigType,
		&t.WorkflowID,
		&t.Version,
		&configJSON,
		&t.Enabled,
		&t.TenantID,
		&t.CreatedAt,
		&t.CreatedBy,
	); err != nil {
		return domain.SagaTrigger{}, err
	}
	t.TriggerType = domain.TriggerType(trigType)
	if err := json.Unmarshal(configJSON, &t.Config); err != nil {
		return domain.SagaTrigger{}, fmt.Errorf("unmarshal trigger config: %w", err)
	}
	return t, nil
}
