// Package domain contains the in-process types go-saga-orchestration exchanges
// with its HTTP layer, store, and engine. Definitions and runs are the core
// concepts.
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
	"fmt"
	"time"
)

// StepType is the discriminant for the union of step shapes.
type StepType string

// The StepType constants enumerate every verb the engine can dispatch.
const (
	StepTypeEnd            StepType = "end"
	StepTypeAction         StepType = "action"
	StepTypeDecision       StepType = "decision"
	StepTypeError          StepType = "error"
	StepTypeNoop           StepType = "noop"
	StepTypeSetVar         StepType = "set_var"
	StepTypeTransform      StepType = "transform"
	StepTypeMerge          StepType = "merge"
	StepTypeFilter         StepType = "filter"
	StepTypeMap            StepType = "map"
	StepTypeLog            StepType = "log"
	StepTypeMetricEmit     StepType = "metric_emit"
	StepTypeAssert         StepType = "assert"
	StepTypeHTTPRequest    StepType = "http_request"
	StepTypeWebhookEmit    StepType = "webhook_emit"
	StepTypeWaitDuration   StepType = "wait_duration"
	StepTypeWaitUntil      StepType = "wait_until"
	StepTypeWaitForSignal  StepType = "wait_for_signal"
	StepTypeWaitForEvent   StepType = "wait_for_event"
	StepTypeParallel       StepType = "parallel"
	StepTypeForeach        StepType = "foreach"
	StepTypeWhile          StepType = "while"
	StepTypeTryCatch       StepType = "try_catch"
	StepTypeSubSaga        StepType = "sub_saga"
	StepTypeSpawnSaga      StepType = "spawn_saga"
	StepTypeManualApproval StepType = "manual_approval"
	StepTypeCollectInput   StepType = "collect_input"
	StepTypeSwitch         StepType = "switch"
	StepTypeEmitSignal     StepType = "emit_signal"
	StepTypeCancel         StepType = "cancel"
	StepTypeEmitEvent      StepType = "emit_event"
)

// WorkflowDefinition is one version of one workflow. Stored in
// definitions.workflow_definitions.
type WorkflowDefinition struct {
	ID          string            `json:"id"`
	Version     int               `json:"version"`
	TenantID    *string           `json:"tenant_id,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Start       string            `json:"start"`
	Entrypoints map[string]string `json:"entrypoints,omitempty"` // entry name -> step id; "" / "default" => Start
	Steps       []Step            `json:"steps"`
	Published   bool              `json:"published"`
	CreatedAt   time.Time         `json:"created_at,omitempty"`
	CreatedBy   string            `json:"created_by,omitempty"`
}

// Step is a single node in the workflow graph.
type Step struct {
	ID           string            `json:"id"`
	Type         StepType          `json:"type"`
	Next         string            `json:"next,omitempty"`
	Action       string            `json:"action,omitempty"`
	Inputs       map[string]any    `json:"inputs,omitempty"`
	Compensation *Compensation     `json:"compensation,omitempty"`
	Retry        *RetryPolicy      `json:"retry,omitempty"`
	Branches     map[string]Branch `json:"branches,omitempty"`
	Extra        map[string]any    `json:"-"`
}

// Branch is one outgoing edge of a decision/parallel step.
type Branch struct {
	Next string `json:"next"`
}

// Compensation describes how to undo a completed step. Nil means
// "non-compensable; log a warning when rolling back."
type Compensation struct {
	Action string         `json:"action"`
	Inputs map[string]any `json:"inputs,omitempty"`
}

// RetryPolicy bounds step-level retry. Defaults applied at engine time
// if a step omits the field.
type RetryPolicy struct {
	MaxAttempts      int     `json:"max_attempts"`
	InitialBackoffMS int     `json:"initial_backoff_ms"`
	MaxBackoffMS     int     `json:"max_backoff_ms,omitempty"`
	Multiplier       float64 `json:"multiplier,omitempty"`
	Jitter           bool    `json:"jitter,omitempty"`
}

// ResolveEntry returns the step id a run should begin at for the named entry
// point. "" and "default" resolve to Start. An unknown name returns an error.
func (d WorkflowDefinition) ResolveEntry(entrypoint string) (string, error) {
	if entrypoint == "" || entrypoint == "default" {
		if d.Start == "" {
			return "", fmt.Errorf("resolve entry: workflow %q has no Start", d.ID)
		}
		return d.Start, nil
	}
	stepID, ok := d.Entrypoints[entrypoint]
	if !ok {
		return "", fmt.Errorf("resolve entry: unknown entrypoint %q for workflow %q", entrypoint, d.ID)
	}
	return stepID, nil
}

// StepByID returns the step with the given ID and whether it was found.
func (d *WorkflowDefinition) StepByID(id string) (Step, bool) {
	for _, s := range d.Steps {
		if s.ID == id {
			return s, true
		}
	}
	return Step{}, false
}
