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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertWorkflowDefinition inserts def, or updates the existing row on a
// (workflow_id, version) conflict, returning the row's storage ID.
func (s *Store) UpsertWorkflowDefinition(ctx context.Context, def domain.WorkflowDefinition) (uuid.UUID, error) {
	id := uuid.New()
	spec, err := json.Marshal(def)
	if err != nil {
		return uuid.Nil, err
	}
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now().UTC()
	}
	if def.CreatedBy == "" {
		def.CreatedBy = "system"
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO definitions.workflow_definitions
		  (id, workflow_id, version, tenant_id, name, description, spec, published, created_at, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (workflow_id, version) DO UPDATE SET
		  name = EXCLUDED.name,
		  description = EXCLUDED.description,
		  spec = EXCLUDED.spec,
		  published = EXCLUDED.published
		RETURNING id`,
		id, def.ID, def.Version, def.TenantID, def.Name, def.Description, spec, def.Published, def.CreatedAt, def.CreatedBy,
	)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// GetWorkflowDefinition returns the definition with the given storage ID, or
// ErrNotFound.
func (s *Store) GetWorkflowDefinition(ctx context.Context, id uuid.UUID) (domain.WorkflowDefinition, error) {
	var spec []byte
	err := s.pool.QueryRow(ctx, `
		SELECT spec FROM definitions.workflow_definitions WHERE id = $1`, id).Scan(&spec)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: id.String()}
	}
	if err != nil {
		return domain.WorkflowDefinition{}, err
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(spec, &def); err != nil {
		return domain.WorkflowDefinition{}, err
	}
	return def, nil
}

// GetPublishedWorkflowByID returns the highest-version published definition for
// workflowID scoped to tenantID (nil = platform), or ErrNotFound.
func (s *Store) GetPublishedWorkflowByID(ctx context.Context, workflowID string, tenantID *uuid.UUID) (domain.WorkflowDefinition, error) {
	var spec []byte
	err := s.pool.QueryRow(ctx, `
		SELECT spec FROM definitions.workflow_definitions
		WHERE workflow_id = $1
		  AND ($2::uuid IS NULL AND tenant_id IS NULL OR tenant_id = $2)
		  AND published = TRUE
		ORDER BY version DESC LIMIT 1`, workflowID, tenantID).Scan(&spec)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: workflowID}
	}
	if err != nil {
		return domain.WorkflowDefinition{}, err
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(spec, &def); err != nil {
		return domain.WorkflowDefinition{}, err
	}
	return def, nil
}
