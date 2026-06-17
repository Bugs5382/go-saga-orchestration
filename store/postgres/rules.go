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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertRuleDefinition inserts (or upserts on (rule_id, version)) a rule.
func (s *Store) UpsertRuleDefinition(ctx context.Context, def domain.RuleDefinition) (uuid.UUID, error) {
	specJSON, err := json.Marshal(def.Spec)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal rule spec: %w", err)
	}
	id := def.ID
	if id == uuid.Nil {
		id = uuid.New()
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO definitions.rule_definitions (id, rule_id, version, tenant_id, name, rule_type, spec, published, created_at, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), $9)
ON CONFLICT (rule_id, version) DO UPDATE
SET spec = EXCLUDED.spec, name = EXCLUDED.name, published = EXCLUDED.published
`, id, def.RuleID, def.Version, def.TenantID, def.Name, string(def.RuleType), specJSON, def.Published, def.CreatedBy)
	if err != nil {
		return uuid.Nil, fmt.Errorf("upsert rule: %w", err)
	}
	return id, nil
}

// GetPublishedRuleByID returns the most recent published version of a rule.
func (s *Store) GetPublishedRuleByID(ctx context.Context, ruleID string, tenantID *uuid.UUID) (domain.RuleDefinition, error) {
	var (
		out      domain.RuleDefinition
		specJSON []byte
	)
	row := s.pool.QueryRow(ctx, `
SELECT id, rule_id, version, tenant_id, name, rule_type, spec, published, created_at, created_by
FROM definitions.rule_definitions
WHERE rule_id = $1 AND published = TRUE
  AND ($2::uuid IS NULL OR tenant_id IS NULL OR tenant_id = $2)
ORDER BY version DESC
LIMIT 1
`, ruleID, tenantID)
	var ruleType string
	var tenantUUID *uuid.UUID
	if err := row.Scan(&out.ID, &out.RuleID, &out.Version, &tenantUUID, &out.Name, &ruleType, &specJSON, &out.Published, &out.CreatedAt, &out.CreatedBy); err != nil {
		if err == pgx.ErrNoRows {
			return domain.RuleDefinition{}, store.ErrNotFound{Entity: "rule_definition", ID: ruleID}
		}
		return domain.RuleDefinition{}, fmt.Errorf("scan rule: %w", err)
	}
	out.RuleType = domain.RuleType(ruleType)
	if tenantUUID != nil {
		s := tenantUUID.String()
		out.TenantID = &s
	}
	if err := json.Unmarshal(specJSON, &out.Spec); err != nil {
		return domain.RuleDefinition{}, fmt.Errorf("unmarshal rule spec: %w", err)
	}
	return out, nil
}
