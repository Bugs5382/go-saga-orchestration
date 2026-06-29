package domain

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
	"time"

	"github.com/google/uuid"
)

// TriggerType is the dispatch key for matching incoming signals (events,
// cron ticks, etc.) to workflows.
type TriggerType string

// The TriggerType constants enumerate the supported trigger dispatch keys.
const (
	// TriggerRecordTransition fires when a record state-transition event
	// arrives matching the trigger's config.record_type, config.from_state,
	// config.to_state.
	TriggerRecordTransition TriggerType = "record_transition"
	// TriggerCron fires on a schedule defined by a cron expression in
	// config.schedule (standard 5-field cron syntax, UTC).
	TriggerCron TriggerType = "cron"
)

// SagaTrigger persists the binding between an external signal and a
// workflow definition. Created via REST or seeded via migration.
type SagaTrigger struct {
	ID          uuid.UUID
	TriggerType TriggerType
	WorkflowID  string
	Version     int
	// Config is shape-by-trigger-type. For TriggerRecordTransition:
	//   { "record_type": "order", "from_state": "created",
	//     "to_state": "pending_review", "input_mapping": {...} }
	Config    map[string]any
	Enabled   bool
	TenantID  *uuid.UUID
	CreatedAt time.Time
	CreatedBy string
	// Cron scheduling bookkeeping (cron triggers only).
	NextFireAt  *time.Time `json:"next_fire_at,omitempty"`
	LastFiredAt *time.Time `json:"last_fired_at,omitempty"`
}
