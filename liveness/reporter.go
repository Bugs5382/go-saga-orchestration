package liveness

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
	"errors"
	"fmt"
	"time"

	golog "github.com/Bugs5382/go-log"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// AdvancePublisher wakes a run once a worker completes its step. The engine's
// Publisher satisfies it.
type AdvancePublisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// Compensator rolls back the completed steps of a run whose step failed.
// *engine.Coordinator satisfies it.
type Compensator interface {
	CompensateFailedStep(ctx context.Context, runID uuid.UUID, failedStepID string) error
}

// StoreReporter is the default Reporter, backed by a store.Store.
//
// A report is accepted only while the run is paused on the reported step with
// that attempt and an awaited action, as ActionVerb leaves it. CompleteStep
// merges the result into the run's variables and publishes an advance;
// FailStep fails the run and, when Compensator is set, compensates the steps
// that finished before it.
type StoreReporter struct {
	Store store.Store
	// Advance wakes the run after a completion. nil leaves waking to the
	// timer, which finds the wakeup CompleteAction sets.
	Advance AdvancePublisher
	// Compensator rolls back a failed run. nil fails the run without
	// compensation.
	Compensator Compensator
	// Logger is optional; nil is silent unless the context carries a logger.
	Logger golog.Logger
}

// CheckStep returns nil when the run is waiting for stepID at attempt.
func (r *StoreReporter) CheckStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int) error {
	run, err := r.Store.GetRun(ctx, runID)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
		}
		return fmt.Errorf("liveness: read run %s: %w", runID, err)
	}
	if run.State != domain.RunStatePaused || run.AwaitedActionDispatch == nil ||
		run.CurrentStep != stepID || run.CurrentAttempt != attempt {
		sagalog.Trace(sagalog.For(ctx, r.Logger), "liveness: run is not waiting for this report",
			golog.F("run_id", runID.String()), golog.F("state", string(run.State)),
			golog.F("current_step", run.CurrentStep), golog.F("current_attempt", run.CurrentAttempt),
			golog.F("awaiting_action", run.AwaitedActionDispatch != nil))
		return fmt.Errorf("%w: run %s step %s attempt %d", ErrStepNotAwaited, runID, stepID, attempt)
	}
	return nil
}

// CompleteStep records a worker's result and wakes the run.
func (r *StoreReporter) CompleteStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int, result map[string]any) error {
	lg := sagalog.For(ctx, r.Logger).With(golog.F("run_id", runID.String()), golog.F("step_id", stepID), golog.F("attempt", attempt))
	if err := r.CheckStep(ctx, runID, stepID, attempt); err != nil {
		return err
	}
	if err := r.Store.CompleteAction(ctx, runID, attempt, result); err != nil {
		lg.Error(err, "liveness: complete action failed")
		return fmt.Errorf("liveness: complete %s/%s: %w", runID, stepID, err)
	}
	lg.Debug("liveness: step completed", golog.F("result_keys", len(result)))
	if r.Advance != nil {
		t0 := time.Now()
		if err := r.Advance.PublishSagaAdvance(ctx, runID.String()); err != nil {
			// The completion is stored with a wakeup, so the timer still resumes the run.
			lg.Error(err, "liveness: publish advance failed; the timer resumes the run",
				golog.F("duration", time.Since(t0).String()))
		}
	}
	return nil
}

// FailStep records a worker's failure, fails the run and compensates.
func (r *StoreReporter) FailStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int, code, message string, retryable bool) error {
	lg := sagalog.For(ctx, r.Logger).With(golog.F("run_id", runID.String()), golog.F("step_id", stepID), golog.F("attempt", attempt))
	if err := r.CheckStep(ctx, runID, stepID, attempt); err != nil {
		return err
	}
	if err := r.Store.FailAction(ctx, runID, attempt, code, message, retryable); err != nil {
		lg.Error(err, "liveness: fail action failed", golog.F("code", code))
		return fmt.Errorf("liveness: fail %s/%s: %w", runID, stepID, err)
	}
	lg.Info("liveness: step failed; run failed", golog.F("code", code), golog.F("retryable", retryable))
	if r.Compensator == nil {
		lg.Debug("liveness: no compensator; completed steps are not rolled back")
		return nil
	}
	if err := r.Compensator.CompensateFailedStep(ctx, runID, stepID); err != nil {
		// The failure itself is recorded; compensation is best-effort.
		lg.Error(err, "liveness: compensation failed")
	}
	return nil
}
