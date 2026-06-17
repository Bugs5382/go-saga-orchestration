package domain

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
	"time"

	"github.com/google/uuid"
)

// ActionRegistration describes one action a service exposes. Stored in
// definitions.action_registry.
type ActionRegistration struct {
	ID               uuid.UUID      `json:"id"`
	Service          string         `json:"service"`
	ActionName       string         `json:"action_name"`
	Version          int            `json:"version"`
	Description      string         `json:"description,omitempty"`
	Category         string         `json:"category,omitempty"`
	Compensable      bool           `json:"compensable"`
	InputSchema      map[string]any `json:"input_schema"`
	OutputSchema     map[string]any `json:"output_schema"`
	ErrorCodes       []string       `json:"error_codes,omitempty"`
	DefaultRetry     *RetryPolicy   `json:"default_retry,omitempty"`
	DefaultTimeoutMS int            `json:"default_timeout_ms,omitempty"`
	Deprecated       bool           `json:"deprecated"`
	RegisteredAt     time.Time      `json:"registered_at"`
	ServiceVersion   string         `json:"service_version,omitempty"`
	DryRunSupported  bool           `json:"dry_run_supported"`
	LicenseGroup     string         `json:"license_group,omitempty"`
}
