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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// UpsertActionRegistration inserts or updates an action_registry row keyed by
// (service, action_name, version).
func (s *Store) UpsertActionRegistration(ctx context.Context, reg domain.ActionRegistration) error {
	inSchema, _ := json.Marshal(reg.InputSchema)
	outSchema, _ := json.Marshal(reg.OutputSchema)
	retryJSON, _ := json.Marshal(reg.DefaultRetry)
	id := reg.ID
	if id == uuid.Nil {
		id = uuid.New()
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO definitions.action_registry
  (id, service, action_name, version, description, category, compensable,
   input_schema, output_schema, error_codes, default_retry, default_timeout_ms,
   deprecated, registered_at, service_version, dry_run_supported, license_group,
   transport, address)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),$14,$15,$16,$17,$18)
ON CONFLICT (service, action_name, version) DO UPDATE SET
  description        = EXCLUDED.description,
  category           = EXCLUDED.category,
  compensable        = EXCLUDED.compensable,
  input_schema       = EXCLUDED.input_schema,
  output_schema      = EXCLUDED.output_schema,
  error_codes        = EXCLUDED.error_codes,
  default_retry      = EXCLUDED.default_retry,
  default_timeout_ms = EXCLUDED.default_timeout_ms,
  deprecated         = EXCLUDED.deprecated,
  service_version    = EXCLUDED.service_version,
  dry_run_supported  = EXCLUDED.dry_run_supported,
  license_group      = EXCLUDED.license_group,
  transport          = EXCLUDED.transport,
  address            = EXCLUDED.address
`,
		id, reg.Service, reg.ActionName, reg.Version, reg.Description, reg.Category, reg.Compensable,
		inSchema, outSchema, reg.ErrorCodes, retryJSON, reg.DefaultTimeoutMS,
		reg.Deprecated, reg.ServiceVersion, reg.DryRunSupported, reg.LicenseGroup,
		reg.Transport, reg.Address,
	)
	if err != nil {
		return fmt.Errorf("upsert action: %w", err)
	}
	return nil
}

// ListActions returns all action registrations matching the optional filter.
func (s *Store) ListActions(ctx context.Context, filter store.ActionFilter) ([]domain.ActionRegistration, error) {
	where := []string{}
	args := []any{}
	if filter.Service != "" {
		args = append(args, filter.Service)
		where = append(where, "service = $"+strconv.Itoa(len(args)))
	}
	if filter.Category != "" {
		args = append(args, filter.Category)
		where = append(where, "category = $"+strconv.Itoa(len(args)))
	}
	if filter.Search != "" {
		args = append(args, "%"+filter.Search+"%")
		where = append(where, "action_name LIKE $"+strconv.Itoa(len(args)))
	}
	q := `SELECT id, service, action_name, version, description, category, compensable,
           input_schema, output_schema, error_codes, default_retry, default_timeout_ms,
           deprecated, registered_at, service_version, dry_run_supported, license_group,
           COALESCE(transport, ''), COALESCE(address, '')
          FROM definitions.action_registry`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY service, action_name, version"

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list actions: %w", err)
	}
	defer rows.Close()
	out := []domain.ActionRegistration{}
	for rows.Next() {
		var reg domain.ActionRegistration
		var inSchema, outSchema, retryJSON []byte
		if err := rows.Scan(
			&reg.ID, &reg.Service, &reg.ActionName, &reg.Version,
			&reg.Description, &reg.Category, &reg.Compensable,
			&inSchema, &outSchema, &reg.ErrorCodes, &retryJSON, &reg.DefaultTimeoutMS,
			&reg.Deprecated, &reg.RegisteredAt, &reg.ServiceVersion,
			&reg.DryRunSupported, &reg.LicenseGroup,
			&reg.Transport, &reg.Address,
		); err != nil {
			return nil, fmt.Errorf("scan action: %w", err)
		}
		_ = json.Unmarshal(inSchema, &reg.InputSchema)
		_ = json.Unmarshal(outSchema, &reg.OutputSchema)
		_ = json.Unmarshal(retryJSON, &reg.DefaultRetry)
		out = append(out, reg)
	}
	return out, nil
}

// GetAction returns the action registration for the given service/name/version.
func (s *Store) GetAction(ctx context.Context, service, name string, version int) (domain.ActionRegistration, error) {
	var reg domain.ActionRegistration
	var inSchema, outSchema, retryJSON []byte
	row := s.pool.QueryRow(ctx, `
SELECT id, service, action_name, version, description, category, compensable,
       input_schema, output_schema, error_codes, default_retry, default_timeout_ms,
       deprecated, registered_at, service_version, dry_run_supported, license_group,
       COALESCE(transport, ''), COALESCE(address, '')
FROM definitions.action_registry
WHERE service=$1 AND action_name=$2 AND version=$3`, service, name, version)
	if err := row.Scan(
		&reg.ID, &reg.Service, &reg.ActionName, &reg.Version,
		&reg.Description, &reg.Category, &reg.Compensable,
		&inSchema, &outSchema, &reg.ErrorCodes, &retryJSON, &reg.DefaultTimeoutMS,
		&reg.Deprecated, &reg.RegisteredAt, &reg.ServiceVersion,
		&reg.DryRunSupported, &reg.LicenseGroup,
		&reg.Transport, &reg.Address,
	); err != nil {
		if err == pgx.ErrNoRows {
			return domain.ActionRegistration{}, store.ErrNotFound{
				Entity: "action_registration",
				ID:     service + "." + name,
			}
		}
		return domain.ActionRegistration{}, fmt.Errorf("get action: %w", err)
	}
	_ = json.Unmarshal(inSchema, &reg.InputSchema)
	_ = json.Unmarshal(outSchema, &reg.OutputSchema)
	_ = json.Unmarshal(retryJSON, &reg.DefaultRetry)
	return reg, nil
}
