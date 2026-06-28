// Package store defines the persistence interface go-saga-orchestration uses for
// definitions, runs, events, and registry rows. Two implementations:
//   - memory/  — in-memory, test-only.
//   - postgres/ — production.
//
// Engine + API both depend on the interface, never on a concrete impl.
package store

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// RunFilter narrows ListRuns / CountRuns queries. All fields optional.
type RunFilter struct {
	WorkflowID     string     // empty = any
	State          string     // empty = any; values per domain.RunState
	TriggerType    string     // empty = any; checks saga_triggers.trigger_type via Run.TriggerID
	Since          *time.Time // started_at >= Since when set
	HasError       *bool      // nil = any; true = state==failed; v1: state==failed is sufficient (noted)
	RequiresReview *bool      // nil = any; checks SagaRun.RequiresManualReview
	Limit          int        // server caps at 500; 0 → 50 default
	Offset         int
}

// WorkflowStats holds aggregate metrics for a single workflow.
type WorkflowStats struct {
	WorkflowID     string     `json:"workflow_id"`
	SuccessRate24h *float64   `json:"success_rate_24h"` // null when no runs in last 24h
	LastRunAt      *time.Time `json:"last_run_at"`      // null when no runs at all
	InFlight       int        `json:"in_flight"`
}

// Store is the persistence surface.
type Store interface {
	// Definitions
	GetWorkflowDefinition(ctx context.Context, id uuid.UUID) (domain.WorkflowDefinition, error)
	GetPublishedWorkflowByID(ctx context.Context, workflowID string, tenantID *uuid.UUID) (domain.WorkflowDefinition, error)
	UpsertWorkflowDefinition(ctx context.Context, def domain.WorkflowDefinition) (uuid.UUID, error)

	// Runs
	CreateRun(ctx context.Context, run domain.SagaRun) error
	GetRun(ctx context.Context, id uuid.UUID) (domain.SagaRun, error)
	UpdateRunState(ctx context.Context, id uuid.UUID, state domain.RunState, currentStep string) error
	// ListRuns returns saga runs matching the optional filter. Sorted by
	// started_at DESC (newest first). Limit + Offset are for pagination;
	// caller must validate (zero Limit → 50 default; hard max 500 enforced
	// server-side before this call).
	ListRuns(ctx context.Context, filter RunFilter) ([]domain.SagaRun, error)
	// CountRuns returns the total count matching filter (ignoring Limit/Offset)
	// so callers can render total + page nav.
	CountRuns(ctx context.Context, filter RunFilter) (int, error)
	// StatsForWorkflow returns aggregate metrics for a workflow.
	StatsForWorkflow(ctx context.Context, workflowID string) (WorkflowStats, error)

	// Events
	AppendEvent(ctx context.Context, evt domain.SagaRunEvent) error
	ListEventsByRun(ctx context.Context, runID uuid.UUID) ([]domain.SagaRunEvent, error)
	// GetEventByID returns a single audit event by its UUID, or ErrNotFound.
	GetEventByID(ctx context.Context, id uuid.UUID) (domain.SagaRunEvent, error)

	// Rule definitions.
	UpsertRuleDefinition(ctx context.Context, def domain.RuleDefinition) (uuid.UUID, error)
	GetPublishedRuleByID(ctx context.Context, ruleID string, tenantID *uuid.UUID) (domain.RuleDefinition, error)

	// Run variable mutation (merge a verb result map into saga_runs.variables JSONB).
	UpdateRunVariables(ctx context.Context, runID uuid.UUID, merge map[string]any) error

	// Pause / resume helpers.
	SetPausedWithWakeup(ctx context.Context, runID uuid.UUID, wakeupAt time.Time) error
	SetPausedAwaitingSignal(ctx context.Context, runID uuid.UUID, signalName string, deadline *time.Time) error
	SetPausedAwaitingEvent(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string) error
	// SetPausedAwaitingEventWithDeadline is SetPausedAwaitingEvent but also sets a
	// wakeup deadline (nil = no deadline). On deadline elapse with the event still
	// unmatched, the engine routes to the step's "timeout" branch if defined.
	SetPausedAwaitingEventWithDeadline(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string, deadline *time.Time) error
	ClearPause(ctx context.Context, runID uuid.UUID) error
	FindRunsByDueWakeup(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)
	FindRunsByAwaitedEvent(ctx context.Context, topic string) ([]domain.SagaRun, error)
	TryConsumeAwaitedSignal(ctx context.Context, runID uuid.UUID, signalName string) (ok bool, err error)
	AppendSignal(ctx context.Context, sig domain.SagaSignal) error

	// WakeFromExternal clears all await-markers (awaited_signal,
	// awaited_event_topic, awaited_event_headers) and sets wakeup_at=now()
	// while leaving state=paused. The Advance loop sees paused+due-wakeup
	// and resumes the saga uniformly, regardless of whether a signal or
	// event triggered the wake.
	WakeFromExternal(ctx context.Context, runID uuid.UUID) error

	// Child runs / try_catch stack.
	SpawnChildRun(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any) (uuid.UUID, error)
	// SpawnChildRunAt is SpawnChildRun but the child begins at startStep
	// ("" => the child definition's Start). SpawnChildRun delegates with "".
	SpawnChildRunAt(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any, startStep string) (uuid.UUID, error)
	ListChildrenByParent(ctx context.Context, parentID uuid.UUID, parentStepID string) ([]domain.SagaRun, error)
	PushTryCatch(ctx context.Context, runID uuid.UUID, frame domain.TryCatchFrame) error
	PopTryCatch(ctx context.Context, runID uuid.UUID) (domain.TryCatchFrame, bool, error)

	// User tasks.
	CreateUserTask(ctx context.Context, task domain.UserTask) error
	GetUserTask(ctx context.Context, taskID uuid.UUID) (domain.UserTask, error)
	SubmitUserTask(ctx context.Context, taskID uuid.UUID, submittedBy string, result map[string]any) error
	// ListUserTasksByRun returns all user tasks created during runID's
	// execution (any step). Returned in creation order (by ID, since
	// domain.UserTask has no CreatedAt field). Empty list if none.
	ListUserTasksByRun(ctx context.Context, runID uuid.UUID) ([]domain.UserTask, error)

	// Action registry.
	UpsertActionRegistration(ctx context.Context, reg domain.ActionRegistration) error
	ListActions(ctx context.Context, filter ActionFilter) ([]domain.ActionRegistration, error)
	GetAction(ctx context.Context, service, name string, version int) (domain.ActionRegistration, error)

	// Saga triggers.
	UpsertTrigger(ctx context.Context, trigger domain.SagaTrigger) (uuid.UUID, error)
	GetTrigger(ctx context.Context, id uuid.UUID) (domain.SagaTrigger, error)
	ListTriggers(ctx context.Context, filter TriggerFilter) ([]domain.SagaTrigger, error)
	DeleteTrigger(ctx context.Context, id uuid.UUID) error
	// ListDueCronTriggers returns enabled cron triggers whose next_fire_at is at
	// or before now, oldest first, capped at limit.
	ListDueCronTriggers(ctx context.Context, now time.Time, limit int) ([]domain.SagaTrigger, error)
	// ClaimCronFire atomically advances a cron trigger's next_fire_at from
	// expectedNextFire to newNextFire (and stamps last_fired_at). Returns true
	// iff this caller won the row — the single-fire guarantee across pods.
	ClaimCronFire(ctx context.Context, id uuid.UUID, expectedNextFire, newNextFire time.Time) (bool, error)

	// Action dispatch tracking.
	// MarkAwaitingAction sets state=paused, records the dispatch key and attempt.
	// Idempotent on (runID, attempt) — same args = no-op.
	MarkAwaitingAction(ctx context.Context, runID uuid.UUID, dispatch string, attempt int) error
	// CompleteAction clears the await marker and merges result into variables.
	// If attempt does not match current_attempt the call is a no-op (late delivery).
	CompleteAction(ctx context.Context, runID uuid.UUID, attempt int, result map[string]any) error
	// FailAction transitions the run to failed and appends an audit event.
	// If attempt does not match current_attempt the call is a no-op (late delivery).
	FailAction(ctx context.Context, runID uuid.UUID, attempt int, code, message string, retryable bool) error
}

// ActionFilter narrows ListActions queries. All fields optional.
type ActionFilter struct {
	Service  string
	Search   string // substring match against action name (case-sensitive)
	Category string
}

// TriggerFilter narrows ListTriggers queries. All fields optional.
type TriggerFilter struct {
	Type     domain.TriggerType
	Enabled  *bool      // nil = any
	TenantID *uuid.UUID // nil = any
}

// ErrNotFound is returned by Get* methods when no row matches.
type ErrNotFound struct {
	Entity string
	ID     string
}

// Error reports the missing entity and ID.
func (e ErrNotFound) Error() string {
	return e.Entity + " not found: " + e.ID
}
