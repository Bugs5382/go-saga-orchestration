package postgres

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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// CreateRun inserts a new saga run row.
func (s *Store) CreateRun(ctx context.Context, run domain.SagaRun) error {
	inputs, err := json.Marshal(run.Inputs)
	if err != nil {
		return err
	}
	vars, err := json.Marshal(run.Variables)
	if err != nil {
		return err
	}
	stack, err := json.Marshal(run.TryCatchStack)
	if err != nil {
		return err
	}
	overrides, err := json.Marshal(run.FeatureOverrides)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO runtime.saga_runs
		  (id, workflow_id, definition_id, tenant_id, state, current_step, inputs, variables,
		   started_at, last_event_at, requires_manual_review, trigger_id,
		   parent_run_id, parent_step_id, parent_branch_id, try_catch_stack, dry_run,
		   feature_overrides)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		run.ID, run.WorkflowID, run.DefinitionID, run.TenantID, run.State, run.CurrentStep,
		inputs, vars, run.StartedAt, run.LastEventAt, run.RequiresManualReview,
		run.TriggerID, run.ParentRunID, run.ParentStepID, run.ParentBranchID, stack, run.DryRun,
		overrides,
	)
	return err
}

// GetRun loads the run with the given ID, or returns ErrNotFound.
func (s *Store) GetRun(ctx context.Context, id uuid.UUID) (domain.SagaRun, error) {
	var (
		run           domain.SagaRun
		inputs, vars  []byte
		overridesJSON []byte
		terminalAt    *time.Time
		lastErr       *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, workflow_id, definition_id, tenant_id, state, COALESCE(current_step, '') AS current_step,
		       inputs, variables, started_at, last_event_at, terminal_at,
		       requires_manual_review, trigger_id, parent_run_id, dry_run,
		       feature_overrides, last_error
		FROM runtime.saga_runs WHERE id = $1`, id).Scan(
		&run.ID, &run.WorkflowID, &run.DefinitionID, &run.TenantID, &run.State, &run.CurrentStep,
		&inputs, &vars, &run.StartedAt, &run.LastEventAt, &terminalAt,
		&run.RequiresManualReview, &run.TriggerID, &run.ParentRunID, &run.DryRun,
		&overridesJSON, &lastErr,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SagaRun{}, store.ErrNotFound{Entity: "saga_run", ID: id.String()}
	}
	if err != nil {
		return domain.SagaRun{}, err
	}
	run.TerminalAt = terminalAt
	run.LastError = lastErr
	if err := json.Unmarshal(inputs, &run.Inputs); err != nil {
		return domain.SagaRun{}, err
	}
	if err := json.Unmarshal(vars, &run.Variables); err != nil {
		return domain.SagaRun{}, err
	}
	if len(overridesJSON) > 0 {
		if err := json.Unmarshal(overridesJSON, &run.FeatureOverrides); err != nil {
			return domain.SagaRun{}, err
		}
	}
	return run, nil
}

// UpdateRunState sets the run's state and current step, stamping terminal_at
// when the new state is terminal.
func (s *Store) UpdateRunState(ctx context.Context, id uuid.UUID, state domain.RunState, currentStep string) error {
	terminal := state.IsTerminal()
	_, err := s.pool.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET state = $2,
		       current_step = NULLIF($3, ''),
		       last_event_at = now(),
		       terminal_at = CASE WHEN $4 THEN now() ELSE terminal_at END
		 WHERE id = $1`, id, state, currentStep, terminal)
	return err
}

// Cancel terminates an in-flight run: terminal cancelled + terminal_at,
// reason in last_error, open user tasks closed, await markers / wakeup
// cleared — all in one transaction. Idempotent: when the run is already
// terminal the guard updates no row, so user tasks are untouched and no
// event is emitted. See issue #80.
func (s *Store) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cancel: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var stepID *string
	ct, err := tx.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET state = 'cancelled',
		       terminal_at = now(),
		       last_event_at = now(),
		       last_error = COALESCE(NULLIF($2, ''), last_error),
		       wakeup_at = NULL,
		       awaited_signal = NULL,
		       awaited_event_topic = NULL,
		       awaited_event_headers = NULL,
		       awaited_action_dispatch = NULL
		 WHERE id = $1
		   AND state NOT IN ('succeeded', 'failed', 'cancelled')`, runID, reason)
	if err != nil {
		return fmt.Errorf("cancel: update run: %w", err)
	}
	if ct.RowsAffected() == 0 {
		// Either the run does not exist or it is already terminal. Disambiguate
		// so callers still get ErrNotFound for a genuinely missing run.
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime.saga_runs WHERE id=$1)`, runID).Scan(&exists); err != nil {
			return fmt.Errorf("cancel: existence check: %w", err)
		}
		if !exists {
			return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
		}
		return nil // already terminal — idempotent no-op
	}
	// Resolve the current step for the audit event.
	if err := tx.QueryRow(ctx, `SELECT current_step FROM runtime.saga_runs WHERE id=$1`, runID).Scan(&stepID); err != nil {
		return fmt.Errorf("cancel: read step: %w", err)
	}
	// Close the run's open user tasks so none linger pending.
	if _, err := tx.Exec(ctx, `
		UPDATE runtime.saga_user_tasks
		   SET submitted_at = now(),
		       submitted_by = 'system:run-cancelled'
		 WHERE run_id = $1 AND submitted_at IS NULL`, runID); err != nil {
		return fmt.Errorf("cancel: close user tasks: %w", err)
	}
	step := ""
	if stepID != nil {
		step = *stepID
	}
	metaJSON, err := json.Marshal(map[string]any{"reason": reason})
	if err != nil {
		return fmt.Errorf("cancel: marshal metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit.saga_run_events (id, run_id, step_id, attempt, event_type, actor, metadata, recorded_at)
		VALUES (gen_random_uuid(), $1, $2, 0, 'run.cancelled', 'engine', $3::jsonb, now())`,
		runID, step, metaJSON); err != nil {
		return fmt.Errorf("cancel: append event: %w", err)
	}
	return tx.Commit(ctx)
}

// MarkRunFailed transitions a run to terminal failed, stamps terminal_at,
// and persists lastError on the run. Idempotent: the terminal guard means
// an already-terminal run is left untouched. See issue #80.
func (s *Store) MarkRunFailed(ctx context.Context, runID uuid.UUID, currentStep, lastError string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET state = 'failed',
		       current_step = NULLIF($2, ''),
		       terminal_at = now(),
		       last_event_at = now(),
		       last_error = COALESCE(NULLIF($3, ''), last_error)
		 WHERE id = $1
		   AND state NOT IN ('succeeded', 'failed', 'cancelled')`, runID, currentStep, lastError)
	if err != nil {
		return fmt.Errorf("mark run failed: %w", err)
	}
	if ct.RowsAffected() == 0 {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime.saga_runs WHERE id=$1)`, runID).Scan(&exists); err != nil {
			return fmt.Errorf("mark run failed: existence check: %w", err)
		}
		if !exists {
			return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
		}
	}
	return nil
}

// SetPausedWithWakeup marks the run paused and records wakeup_at.
func (s *Store) SetPausedWithWakeup(ctx context.Context, runID uuid.UUID, wakeupAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE runtime.saga_runs SET state='paused', wakeup_at=$1, last_event_at=now() WHERE id=$2`,
		wakeupAt, runID)
	if err != nil {
		return fmt.Errorf("set paused with wakeup: %w", err)
	}
	return nil
}

// SetPausedAwaitingSignal marks the run paused awaiting signalName, with an
// optional wakeup deadline.
func (s *Store) SetPausedAwaitingSignal(ctx context.Context, runID uuid.UUID, signalName string, deadline *time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE runtime.saga_runs SET state='paused', awaited_signal=$1, wakeup_at=$2, last_event_at=now() WHERE id=$3`,
		signalName, deadline, runID)
	if err != nil {
		return fmt.Errorf("set paused awaiting signal: %w", err)
	}
	return nil
}

// SetPausedAwaitingEvent marks the run paused awaiting an event on topic with
// the given header filter.
func (s *Store) SetPausedAwaitingEvent(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string) error {
	return s.SetPausedAwaitingEventWithDeadline(ctx, runID, topic, headers, nil)
}

// SetPausedAwaitingEventWithDeadline is SetPausedAwaitingEvent plus an optional
// wakeup deadline (nil = wait indefinitely). The deadline is stored as wakeup_at
// so the timer dispatcher wakes the run if no matching event arrives in time.
func (s *Store) SetPausedAwaitingEventWithDeadline(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string, deadline *time.Time) error {
	hdrsJSON, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("marshal event headers: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		`UPDATE runtime.saga_runs SET state='paused', awaited_event_topic=$1, awaited_event_headers=$2, wakeup_at=$3, last_event_at=now() WHERE id=$4`,
		topic, hdrsJSON, deadline, runID)
	if err != nil {
		return fmt.Errorf("set paused awaiting event: %w", err)
	}
	return nil
}

// ClearPause returns the run to the running state and clears all wakeup and
// await markers.
func (s *Store) ClearPause(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE runtime.saga_runs SET state='running', wakeup_at=NULL, awaited_signal=NULL, awaited_event_topic=NULL, awaited_event_headers=NULL, last_event_at=now() WHERE id=$1`,
		runID)
	if err != nil {
		return fmt.Errorf("clear pause: %w", err)
	}
	return nil
}

// FindRunsByDueWakeup returns up to limit IDs of paused runs whose wakeup_at is
// at or before now, ordered by wakeup_at.
func (s *Store) FindRunsByDueWakeup(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM runtime.saga_runs WHERE state='paused' AND wakeup_at IS NOT NULL AND wakeup_at <= $1 ORDER BY wakeup_at ASC LIMIT $2`,
		now, limit)
	if err != nil {
		return nil, fmt.Errorf("find runs by due wakeup: %w", err)
	}
	defer rows.Close()
	out := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan wakeup row: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// FindRunsByAwaitedEvent returns paused runs awaiting an event on topic.
func (s *Store) FindRunsByAwaitedEvent(ctx context.Context, topic string) ([]domain.SagaRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_id, definition_id, tenant_id, state, COALESCE(current_step, '') AS current_step,
		       inputs, variables, started_at, last_event_at, terminal_at,
		       requires_manual_review, trigger_id, parent_run_id, dry_run,
		       wakeup_at, awaited_signal, awaited_event_topic, awaited_event_headers
		FROM runtime.saga_runs
		WHERE state='paused' AND awaited_event_topic=$1`, topic)
	if err != nil {
		return nil, fmt.Errorf("find runs by awaited event: %w", err)
	}
	defer rows.Close()
	out := []domain.SagaRun{}
	for rows.Next() {
		var (
			run          domain.SagaRun
			inputs, vars []byte
			hdrsJSON     []byte
		)
		if err := rows.Scan(
			&run.ID, &run.WorkflowID, &run.DefinitionID, &run.TenantID, &run.State, &run.CurrentStep,
			&inputs, &vars, &run.StartedAt, &run.LastEventAt, &run.TerminalAt,
			&run.RequiresManualReview, &run.TriggerID, &run.ParentRunID, &run.DryRun,
			&run.WakeupAt, &run.AwaitedSignal, &run.AwaitedEventTopic, &hdrsJSON,
		); err != nil {
			return nil, fmt.Errorf("scan awaited-event row: %w", err)
		}
		if err := json.Unmarshal(inputs, &run.Inputs); err != nil {
			return nil, fmt.Errorf("unmarshal inputs: %w", err)
		}
		if err := json.Unmarshal(vars, &run.Variables); err != nil {
			return nil, fmt.Errorf("unmarshal variables: %w", err)
		}
		if len(hdrsJSON) > 0 {
			if err := json.Unmarshal(hdrsJSON, &run.AwaitedEventHeaders); err != nil {
				return nil, fmt.Errorf("unmarshal event headers: %w", err)
			}
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// TryConsumeAwaitedSignal atomically clears the await markers and marks
// matching unconsumed signal rows consumed when the run is paused awaiting
// signalName, reporting whether it did so.
func (s *Store) TryConsumeAwaitedSignal(ctx context.Context, runID uuid.UUID, signalName string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var awaited *string
	err = tx.QueryRow(ctx,
		`SELECT awaited_signal FROM runtime.saga_runs WHERE id=$1 AND state='paused' FOR UPDATE`,
		runID).Scan(&awaited)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select awaited signal: %w", err)
	}
	if awaited == nil || *awaited != signalName {
		return false, nil
	}

	if _, err := tx.Exec(ctx,
		`UPDATE runtime.saga_runs SET awaited_signal=NULL, awaited_event_topic=NULL, awaited_event_headers=NULL, wakeup_at=NULL, last_event_at=now() WHERE id=$1`,
		runID); err != nil {
		return false, fmt.Errorf("clear awaited signal: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE runtime.saga_signals SET consumed_at=now() WHERE run_id=$1 AND signal_name=$2 AND consumed_at IS NULL`,
		runID, signalName); err != nil {
		return false, fmt.Errorf("consume signal rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// WakeFromExternal clears all await markers and wakeup_at while leaving the run
// paused, so the Advance loop resumes it.
func (s *Store) WakeFromExternal(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE runtime.saga_runs SET awaited_signal=NULL, awaited_event_topic=NULL, awaited_event_headers=NULL, wakeup_at=NULL, last_event_at=now() WHERE id=$1`,
		runID)
	if err != nil {
		return fmt.Errorf("wake from external: %w", err)
	}
	return nil
}

// AppendSignal inserts a received signal row for the run.
func (s *Store) AppendSignal(ctx context.Context, sig domain.SagaSignal) error {
	payload, err := json.Marshal(sig.Payload)
	if err != nil {
		return fmt.Errorf("marshal signal payload: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO runtime.saga_signals (id, run_id, signal_name, payload, received_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		sig.ID, sig.RunID, sig.SignalName, payload, sig.ReceivedAt)
	if err != nil {
		return fmt.Errorf("append signal: %w", err)
	}
	return nil
}

// SpawnChildRunAt looks up the definition_id for def, constructs a child
// SagaRun with the parent fields set, overrides CurrentStep with startStep
// when non-empty, and inserts it via CreateRun.
func (s *Store) SpawnChildRunAt(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any, startStep string) (uuid.UUID, error) {
	var defID uuid.UUID
	if err := s.pool.QueryRow(ctx,
		`SELECT id FROM definitions.workflow_definitions WHERE workflow_id=$1 AND version=$2`,
		def.ID, def.Version).Scan(&defID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, fmt.Errorf("spawn child: resolve definition %s v%d: %w", def.ID, def.Version, store.ErrNotFound{Entity: "workflow_definition", ID: def.ID})
		}
		return uuid.Nil, fmt.Errorf("spawn child: resolve definition: %w", err)
	}
	child := domain.NewSagaRun(def.ID, defID, nil, inputs)
	if startStep != "" {
		child.CurrentStep = startStep
	}
	pid := parentID
	psid := parentStepID
	bid := branchKey
	child.ParentRunID = &pid
	child.ParentStepID = &psid
	child.ParentBranchID = &bid
	if err := s.CreateRun(ctx, child); err != nil {
		return uuid.Nil, fmt.Errorf("spawn child: create: %w", err)
	}
	return child.ID, nil
}

// SpawnChildRun looks up the definition_id for def, constructs a child
// SagaRun with the parent fields set, and inserts it via CreateRun.
// The child begins at the definition's default Start step.
func (s *Store) SpawnChildRun(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any) (uuid.UUID, error) {
	return s.SpawnChildRunAt(ctx, parentID, parentStepID, branchKey, def, inputs, "")
}

// ListChildrenByParent returns all runs with parent_run_id=$1 AND parent_step_id=$2.
func (s *Store) ListChildrenByParent(ctx context.Context, parentID uuid.UUID, parentStepID string) ([]domain.SagaRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_id, definition_id, tenant_id, state, COALESCE(current_step, '') AS current_step,
		       inputs, variables, started_at, last_event_at, terminal_at,
		       requires_manual_review, trigger_id, parent_run_id, dry_run
		FROM runtime.saga_runs
		WHERE parent_run_id = $1 AND parent_step_id = $2`, parentID, parentStepID)
	if err != nil {
		return nil, fmt.Errorf("list children by parent: %w", err)
	}
	defer rows.Close()
	out := []domain.SagaRun{}
	for rows.Next() {
		var (
			run          domain.SagaRun
			inputs, vars []byte
			terminalAt   *time.Time
		)
		if err := rows.Scan(
			&run.ID, &run.WorkflowID, &run.DefinitionID, &run.TenantID, &run.State, &run.CurrentStep,
			&inputs, &vars, &run.StartedAt, &run.LastEventAt, &terminalAt,
			&run.RequiresManualReview, &run.TriggerID, &run.ParentRunID, &run.DryRun,
		); err != nil {
			return nil, fmt.Errorf("scan child row: %w", err)
		}
		run.TerminalAt = terminalAt
		if err := json.Unmarshal(inputs, &run.Inputs); err != nil {
			return nil, fmt.Errorf("unmarshal child inputs: %w", err)
		}
		if err := json.Unmarshal(vars, &run.Variables); err != nil {
			return nil, fmt.Errorf("unmarshal child variables: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// PushTryCatch reads the try_catch_stack JSONB, appends frame (enforcing max
// depth 3), and writes it back within a transaction.
func (s *Store) PushTryCatch(ctx context.Context, runID uuid.UUID, frame domain.TryCatchFrame) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("push try_catch: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var stackJSON []byte
	if err := tx.QueryRow(ctx,
		`SELECT try_catch_stack FROM runtime.saga_runs WHERE id=$1 FOR UPDATE`, runID).Scan(&stackJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
		}
		return fmt.Errorf("push try_catch: read stack: %w", err)
	}

	var stack []domain.TryCatchFrame
	if err := json.Unmarshal(stackJSON, &stack); err != nil {
		return fmt.Errorf("push try_catch: unmarshal stack: %w", err)
	}
	const maxDepth = 3
	if len(stack) >= maxDepth {
		return fmt.Errorf("try_catch max nesting depth %d exceeded for run %s", maxDepth, runID)
	}
	stack = append(stack, frame)

	newJSON, err := json.Marshal(stack)
	if err != nil {
		return fmt.Errorf("push try_catch: marshal stack: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE runtime.saga_runs SET try_catch_stack=$1, last_event_at=now() WHERE id=$2`,
		newJSON, runID); err != nil {
		return fmt.Errorf("push try_catch: update: %w", err)
	}
	return tx.Commit(ctx)
}

// PopTryCatch removes and returns the top TryCatchFrame atomically.
// Returns (zero, false, nil) if the stack is empty.
func (s *Store) PopTryCatch(ctx context.Context, runID uuid.UUID) (domain.TryCatchFrame, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var stackJSON []byte
	if err := tx.QueryRow(ctx,
		`SELECT try_catch_stack FROM runtime.saga_runs WHERE id=$1 FOR UPDATE`, runID).Scan(&stackJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.TryCatchFrame{}, false, store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
		}
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: read stack: %w", err)
	}

	var stack []domain.TryCatchFrame
	if err := json.Unmarshal(stackJSON, &stack); err != nil {
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: unmarshal stack: %w", err)
	}
	if len(stack) == 0 {
		return domain.TryCatchFrame{}, false, nil
	}

	top := stack[len(stack)-1]
	stack = stack[:len(stack)-1]
	newJSON, err := json.Marshal(stack)
	if err != nil {
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: marshal stack: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE runtime.saga_runs SET try_catch_stack=$1, last_event_at=now() WHERE id=$2`,
		newJSON, runID); err != nil {
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.TryCatchFrame{}, false, fmt.Errorf("pop try_catch: commit: %w", err)
	}
	return top, true, nil
}

// UpdateRunVariables merges merge into saga_runs.variables using
// jsonb_set per top-level key. Dotted-key writes are flattened into
// jsonb_set path expressions; nested merges go through the JSONB
// operator '||' for shallow object combine, then jsonb_set for the
// dotted writes.
func (s *Store) UpdateRunVariables(ctx context.Context, runID uuid.UUID, merge map[string]any) error {
	if len(merge) == 0 {
		return nil
	}
	flat := map[string]any{}
	type dottedWrite struct {
		parts []string
		value any
	}
	dotted := []dottedWrite{}
	for k, v := range merge {
		parts := []string{}
		cur := ""
		for i := 0; i < len(k); i++ {
			if k[i] == '.' {
				parts = append(parts, cur)
				cur = ""
				continue
			}
			cur += string(k[i])
		}
		parts = append(parts, cur)
		if len(parts) == 1 {
			flat[parts[0]] = v
		} else {
			dotted = append(dotted, dottedWrite{parts: parts, value: v})
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if len(flat) > 0 {
		flatJSON, err := json.Marshal(flat)
		if err != nil {
			return fmt.Errorf("marshal flat merge: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE runtime.saga_runs SET variables = variables || $1::jsonb, last_event_at = now() WHERE id = $2`,
			flatJSON, runID); err != nil {
			return fmt.Errorf("merge flat: %w", err)
		}
	}
	for _, d := range dotted {
		valJSON, err := json.Marshal(d.value)
		if err != nil {
			return fmt.Errorf("marshal dotted value: %w", err)
		}
		path := "{"
		for i, p := range d.parts {
			if i > 0 {
				path += ","
			}
			path += p
		}
		path += "}"
		if _, err := tx.Exec(ctx,
			`UPDATE runtime.saga_runs SET variables = jsonb_set(variables, $1::text[], $2::jsonb, true), last_event_at = now() WHERE id = $3`,
			path, valJSON, runID); err != nil {
			return fmt.Errorf("merge dotted %v: %w", d.parts, err)
		}
	}
	return tx.Commit(ctx)
}

// ListRuns returns saga runs matching filter, sorted by started_at DESC.
// For TriggerType filtering a LEFT JOIN on saga_triggers is added.
// Limit defaults to 50 when 0; hard max of 500 is enforced by the handler
// before this is called.
func (s *Store) ListRuns(ctx context.Context, filter store.RunFilter) ([]domain.SagaRun, error) {
	q, args := buildListRunsQuery(filter, false)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	out := []domain.SagaRun{}
	for rows.Next() {
		r, err := scanRunRow(rows)
		if err != nil {
			return nil, fmt.Errorf("list runs scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CountRuns returns the total count of runs matching filter (ignoring Limit/Offset).
func (s *Store) CountRuns(ctx context.Context, filter store.RunFilter) (int, error) {
	q, args := buildListRunsQuery(filter, true)
	var count int
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count runs: %w", err)
	}
	return count, nil
}

// buildListRunsQuery constructs the parameterized SQL for ListRuns / CountRuns.
// When countOnly=true it emits SELECT count(*) without ORDER BY / LIMIT / OFFSET.
func buildListRunsQuery(filter store.RunFilter, countOnly bool) (string, []any) {
	args := []any{}
	idx := 1

	needsTriggerJoin := filter.TriggerType != ""

	var q string
	if countOnly {
		q = "SELECT count(*) FROM runtime.saga_runs r"
	} else {
		q = `SELECT r.id, r.workflow_id, r.definition_id, r.tenant_id, r.state,
		            r.current_step, r.inputs, r.variables, r.started_at, r.last_event_at,
		            r.terminal_at, r.requires_manual_review, r.trigger_id, r.parent_run_id,
		            r.dry_run, r.feature_overrides
		     FROM runtime.saga_runs r`
	}

	if needsTriggerJoin {
		q += " LEFT JOIN runtime.saga_triggers t ON r.trigger_id = t.id"
	}

	wheres := []string{}

	if filter.WorkflowID != "" {
		args = append(args, filter.WorkflowID)
		wheres = append(wheres, fmt.Sprintf("r.workflow_id = $%d", idx))
		idx++
	}
	if filter.State != "" {
		args = append(args, filter.State)
		wheres = append(wheres, fmt.Sprintf("r.state = $%d", idx))
		idx++
	}
	if filter.Since != nil {
		args = append(args, *filter.Since)
		wheres = append(wheres, fmt.Sprintf("r.started_at >= $%d", idx))
		idx++
	}
	if filter.HasError != nil {
		// v1: HasError maps to state=='failed' (see RunFilter comment).
		if *filter.HasError {
			wheres = append(wheres, "r.state = 'failed'")
		} else {
			wheres = append(wheres, "r.state != 'failed'")
		}
	}
	if filter.RequiresReview != nil {
		args = append(args, *filter.RequiresReview)
		wheres = append(wheres, fmt.Sprintf("r.requires_manual_review = $%d", idx))
		idx++
	}
	if needsTriggerJoin {
		args = append(args, filter.TriggerType)
		wheres = append(wheres, fmt.Sprintf("t.trigger_type = $%d", idx))
		idx++
	}

	if len(wheres) > 0 {
		q += " WHERE "
		for i, w := range wheres {
			if i > 0 {
				q += " AND "
			}
			q += w
		}
	}

	if !countOnly {
		q += " ORDER BY r.started_at DESC"

		limit := filter.Limit
		if limit <= 0 {
			limit = 50
		}
		args = append(args, limit)
		q += fmt.Sprintf(" LIMIT $%d", idx)
		idx++

		if filter.Offset > 0 {
			args = append(args, filter.Offset)
			q += fmt.Sprintf(" OFFSET $%d", idx)
			idx++ //nolint:ineffassign
		}
	}

	return q, args
}

// scanRunRow scans a single saga_runs SELECT row (the column order from
// buildListRunsQuery's SELECT list).
func scanRunRow(rows interface {
	Scan(dest ...any) error
}) (domain.SagaRun, error) {
	var (
		run           domain.SagaRun
		inputs        []byte
		vars          []byte
		overridesJSON []byte
		terminalAt    *time.Time
	)
	if err := rows.Scan(
		&run.ID, &run.WorkflowID, &run.DefinitionID, &run.TenantID, &run.State, &run.CurrentStep,
		&inputs, &vars, &run.StartedAt, &run.LastEventAt, &terminalAt,
		&run.RequiresManualReview, &run.TriggerID, &run.ParentRunID, &run.DryRun,
		&overridesJSON,
	); err != nil {
		return domain.SagaRun{}, err
	}
	run.TerminalAt = terminalAt
	if err := json.Unmarshal(inputs, &run.Inputs); err != nil {
		return domain.SagaRun{}, fmt.Errorf("unmarshal inputs: %w", err)
	}
	if err := json.Unmarshal(vars, &run.Variables); err != nil {
		return domain.SagaRun{}, fmt.Errorf("unmarshal variables: %w", err)
	}
	if len(overridesJSON) > 0 {
		if err := json.Unmarshal(overridesJSON, &run.FeatureOverrides); err != nil {
			return domain.SagaRun{}, fmt.Errorf("unmarshal feature_overrides: %w", err)
		}
	}
	return run, nil
}

// StatsForWorkflow computes aggregate metrics for a single workflow using two
// aggregate queries: one for success_rate_24h + last_run_at, one for in_flight.
func (s *Store) StatsForWorkflow(ctx context.Context, workflowID string) (store.WorkflowStats, error) {
	stats := store.WorkflowStats{WorkflowID: workflowID}

	// Query 1: last_run_at + 24h window counts.
	row := s.pool.QueryRow(ctx, `
		SELECT
		    max(started_at),
		    count(*) FILTER (WHERE state = 'succeeded' AND started_at >= now() - interval '24 hours'),
		    count(*) FILTER (WHERE state = 'failed'    AND started_at >= now() - interval '24 hours')
		FROM runtime.saga_runs
		WHERE workflow_id = $1`, workflowID)

	var lastRunAt *time.Time
	var succeeded, failed int
	if err := row.Scan(&lastRunAt, &succeeded, &failed); err != nil {
		return stats, fmt.Errorf("stats query: %w", err)
	}
	stats.LastRunAt = lastRunAt

	total24h := succeeded + failed
	if total24h > 0 {
		rate := float64(succeeded) / float64(total24h)
		stats.SuccessRate24h = &rate
	}

	// Query 2: in_flight count.
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM runtime.saga_runs
		WHERE workflow_id = $1
		  AND state NOT IN ('succeeded', 'failed', 'cancelled')`, workflowID).Scan(&stats.InFlight); err != nil {
		return stats, fmt.Errorf("stats in_flight query: %w", err)
	}

	return stats, nil
}

// MarkAwaitingAction sets state=paused, awaited_action_dispatch, and
// current_attempt for the given run. Idempotent on (runID, attempt, dispatch).
func (s *Store) MarkAwaitingAction(ctx context.Context, runID uuid.UUID, dispatch string, attempt int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET state = 'paused',
		       awaited_action_dispatch = $2,
		       current_attempt = $3,
		       last_event_at = now()
		 WHERE id = $1
		   AND NOT (current_attempt = $3 AND awaited_action_dispatch = $2)`,
		runID, dispatch, attempt)
	if err != nil {
		return fmt.Errorf("mark awaiting action: %w", err)
	}
	return nil
}

// CompleteAction clears the await marker, merges result into variables, and
// sets wakeup_at=now() so the Advance paused-handling loop resumes the saga.
// If attempt does not match current_attempt the row is unchanged (late delivery).
func (s *Store) CompleteAction(ctx context.Context, runID uuid.UUID, attempt int, result map[string]any) error {
	if len(result) == 0 {
		_, err := s.pool.Exec(ctx, `
			UPDATE runtime.saga_runs
			   SET awaited_action_dispatch = NULL,
			       wakeup_at = now(),
			       last_event_at = now()
			 WHERE id = $1 AND current_attempt = $2`,
			runID, attempt)
		if err != nil {
			return fmt.Errorf("complete action (no result): %w", err)
		}
		return nil
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("complete action: marshal result: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET awaited_action_dispatch = NULL,
		       wakeup_at = now(),
		       variables = variables || $3::jsonb,
		       last_event_at = now()
		 WHERE id = $1 AND current_attempt = $2`,
		runID, attempt, resultJSON)
	if err != nil {
		return fmt.Errorf("complete action: %w", err)
	}
	return nil
}

// FailAction transitions the run to failed and appends an audit event.
// If attempt does not match current_attempt the call is a no-op (late delivery).
func (s *Store) FailAction(ctx context.Context, runID uuid.UUID, attempt int, code, message string, retryable bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("fail action: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var currentAttempt int
	var dispatchPtr *string
	var currentStep *string
	err = tx.QueryRow(ctx,
		`SELECT current_attempt, awaited_action_dispatch, current_step
		   FROM runtime.saga_runs WHERE id=$1 FOR UPDATE`,
		runID).Scan(&currentAttempt, &dispatchPtr, &currentStep)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	if err != nil {
		return fmt.Errorf("fail action: read row: %w", err)
	}
	if currentAttempt != attempt {
		return nil // late delivery; no-op
	}

	dispatch := ""
	if dispatchPtr != nil {
		dispatch = *dispatchPtr
	}
	stepID := ""
	if currentStep != nil {
		stepID = *currentStep
	}

	if _, err := tx.Exec(ctx, `
		UPDATE runtime.saga_runs
		   SET state = 'failed',
		       awaited_action_dispatch = NULL,
		       terminal_at = now(),
		       last_event_at = now(),
		       last_error = NULLIF($2, '')
		 WHERE id = $1`, runID, message); err != nil {
		return fmt.Errorf("fail action: update run: %w", err)
	}

	metaJSON, err := json.Marshal(map[string]any{
		"code":      code,
		"message":   message,
		"retryable": retryable,
		"action":    dispatch,
	})
	if err != nil {
		return fmt.Errorf("fail action: marshal metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit.saga_run_events (id, run_id, step_id, attempt, event_type, actor, metadata, recorded_at)
		VALUES (gen_random_uuid(), $1, $2, $3, 'step.failed', 'engine', $4::jsonb, now())`,
		runID, stepID, attempt, metaJSON); err != nil {
		return fmt.Errorf("fail action: append event: %w", err)
	}

	return tx.Commit(ctx)
}
