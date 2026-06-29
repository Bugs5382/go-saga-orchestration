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

// EventType identifies what changed.
type EventType string

// The EventType constants enumerate the audit events recorded for a run.
const (
	EventSagaStarted         EventType = "saga.started"
	EventStepDispatched      EventType = "step.dispatched"
	EventStepStarted         EventType = "step.started"
	EventStepSucceeded       EventType = "step.succeeded"
	EventStepFailed          EventType = "step.failed"
	EventStepSkipped         EventType = "step.skipped"
	EventStepPaused          EventType = "step.paused"
	EventRunSucceeded        EventType = "run.succeeded"
	EventRunFailed           EventType = "run.failed"
	EventRunCancelled        EventType = "run.cancelled"
	EventCompensationStarted EventType = "compensation.started"

	EventLog                 EventType = "log"
	EventMetric              EventType = "metric"
	EventRuleEvaluated       EventType = "rule.evaluated"
	EventLicenseGateRejected EventType = "license.gate.rejected"
)

// SagaRunEvent is one audit row. Stored in audit.saga_run_events.
type SagaRunEvent struct {
	ID         uuid.UUID      `json:"id"`
	RunID      uuid.UUID      `json:"run_id"`
	StepID     string         `json:"step_id,omitempty"`
	Attempt    int            `json:"attempt"`
	EventType  EventType      `json:"event_type"`
	FromState  string         `json:"from_state,omitempty"`
	ToState    string         `json:"to_state,omitempty"`
	Actor      string         `json:"actor"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	RecordedAt time.Time      `json:"recorded_at"`
}

// NewEvent constructs an audit event with ID + timestamp filled in.
func NewEvent(runID uuid.UUID, stepID string, attempt int, eventType EventType, actor string) SagaRunEvent {
	return SagaRunEvent{
		ID:         uuid.New(),
		RunID:      runID,
		StepID:     stepID,
		Attempt:    attempt,
		EventType:  eventType,
		Actor:      actor,
		RecordedAt: time.Now().UTC(),
	}
}
