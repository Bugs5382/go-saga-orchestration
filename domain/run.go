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

// RunState is the saga-level state.
type RunState string

// The RunState constants enumerate the saga-level lifecycle states.
const (
	RunStatePending      RunState = "pending"
	RunStateRunning      RunState = "running"
	RunStatePaused       RunState = "paused"
	RunStateCompensating RunState = "compensating"
	RunStateSucceeded    RunState = "succeeded"
	RunStateFailed       RunState = "failed"
	RunStateCancelled    RunState = "cancelled"
)

// IsTerminal reports whether the state is final (no further transitions).
func (s RunState) IsTerminal() bool {
	switch s {
	case RunStateSucceeded, RunStateFailed, RunStateCancelled:
		return true
	}
	return false
}

// SagaRun is one running instance of a workflow. Stored in
// runtime.saga_runs.
type SagaRun struct {
	ID                    uuid.UUID         `json:"id"`
	WorkflowID            string            `json:"workflow_id"`
	DefinitionID          uuid.UUID         `json:"definition_id"`
	TenantID              *uuid.UUID        `json:"tenant_id,omitempty"`
	State                 RunState          `json:"state"`
	CurrentStep           string            `json:"current_step,omitempty"`
	Inputs                map[string]any    `json:"inputs"`
	Variables             map[string]any    `json:"variables"`
	StartedAt             time.Time         `json:"started_at"`
	LastEventAt           time.Time         `json:"last_event_at"`
	TerminalAt            *time.Time        `json:"terminal_at,omitempty"`
	RequiresManualReview  bool              `json:"requires_manual_review"`
	TriggerID             *uuid.UUID        `json:"trigger_id,omitempty"`
	ParentRunID           *uuid.UUID        `json:"parent_run_id,omitempty"`
	ParentStepID          *string           `json:"parent_step_id,omitempty"`
	ParentBranchID        *string           `json:"parent_branch_id,omitempty"`
	TryCatchStack         []TryCatchFrame   `json:"try_catch_stack,omitempty"`
	DryRun                bool              `json:"dry_run,omitempty"`
	WakeupAt              *time.Time        `json:"wakeup_at,omitempty"`
	AwaitedSignal         *string           `json:"awaited_signal,omitempty"`
	AwaitedEventTopic     *string           `json:"awaited_event_topic,omitempty"`
	AwaitedEventHeaders   map[string]string `json:"awaited_event_headers,omitempty"`
	FeatureOverrides      map[string]bool   `json:"feature_overrides,omitempty"`
	AwaitedActionDispatch *string           `json:"awaited_action_dispatch,omitempty"`
	CurrentAttempt        int               `json:"current_attempt,omitempty"`
	// LastError records why a run reached a terminal failed/cancelled
	// state — the failing step's error message, or the cancel reason — so a
	// run is self-describing without diffing its event log. nil while the
	// run is non-terminal or terminated cleanly (succeeded). See issue #80.
	LastError *string `json:"last_error,omitempty"`
}

// TryCatchFrame is one entry in a saga's try_catch stack. When a step
// inside the try block errors, the coordinator pops the top frame and
// jumps to its CatchStep instead of failing the saga.
type TryCatchFrame struct {
	StepID    string `json:"step_id"`    // the try_catch step itself
	CatchStep string `json:"catch_step"` // step ID to jump to on error
}

// NewSagaRun constructs a fresh run row in pending state. The caller
// supplies the resolved definition ID + inputs; the engine fills in
// CurrentStep when it picks the run up.
func NewSagaRun(workflowID string, definitionID uuid.UUID, tenantID *uuid.UUID, inputs map[string]any) SagaRun {
	now := time.Now().UTC()
	return SagaRun{
		ID:           uuid.New(),
		WorkflowID:   workflowID,
		DefinitionID: definitionID,
		TenantID:     tenantID,
		State:        RunStatePending,
		Inputs:       inputs,
		Variables:    map[string]any{},
		StartedAt:    now,
		LastEventAt:  now,
	}
}
