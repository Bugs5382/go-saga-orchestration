package engine

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
	"fmt"

	golog "github.com/Bugs5382/go-log"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

// CompensateFailedStep rolls back the steps a run completed before
// failedStepID failed, for a failure recorded outside Advance: a worker's
// Error report on the liveness stream, or an application that fails an
// awaited step itself (a timeout, a held-back dispatch that expired). It is
// the same rollback Advance runs when a step fails while it executes: each
// completed step's Compensation action is dispatched newest first, over the
// action's transport, best-effort.
//
// The run ends failed. A run that is already failed keeps its recorded error;
// a run that isn't terminal yet is marked failed on failedStepID. A succeeded
// or cancelled run is refused. A parent waiting on the run is re-evaluated, as
// it is when a step fails inside Advance.
//
// Compensation dispatches are not deduplicated: call it once per failure.
func (c *Coordinator) CompensateFailedStep(ctx context.Context, runID uuid.UUID, failedStepID string) error {
	lg := c.logger(ctx).With(golog.F("run_id", runID.String()), golog.F("failed_step", failedStepID))
	unlock := c.runLocks.lock(runID.String())
	defer unlock()
	run, err := c.store.GetRun(ctx, runID)
	if err != nil {
		lg.Error(err, "compensation: read run failed")
		return fmt.Errorf("compensate %s: %w", runID, err)
	}
	if run.State == domain.RunStateSucceeded || run.State == domain.RunStateCancelled {
		lg.Warn("compensation: refused for a run that did not fail", golog.F("state", string(run.State)))
		return fmt.Errorf("compensate %s: run is %s, not failed", runID, run.State)
	}
	def, err := c.store.GetWorkflowDefinition(ctx, run.DefinitionID)
	if err != nil {
		lg.Error(err, "compensation: read definition failed")
		return fmt.Errorf("compensate %s: definition: %w", runID, err)
	}
	step, ok := def.StepByID(failedStepID)
	if !ok {
		lg.Debug("compensation: failed step is not in the definition; rolling back every completed step")
		step = domain.Step{ID: failedStepID}
	}
	wasFailed := run.State == domain.RunStateFailed
	lg.Debug("compensation: requested from outside advance", golog.F("state", string(run.State)))
	c.compensate(ctx, run, def, step)

	if wasFailed {
		cur, err := c.store.GetRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("compensate %s: reread run: %w", runID, err)
		}
		if cur.State == domain.RunStateCompensating {
			if err := c.store.UpdateRunState(ctx, runID, domain.RunStateFailed, run.CurrentStep); err != nil {
				lg.Error(err, "compensation: settle failed state failed")
				return fmt.Errorf("compensate %s: settle: %w", runID, err)
			}
			lg.Debug("run state changed", golog.F("to_state", string(domain.RunStateFailed)))
		}
	} else {
		reason := fmt.Sprintf("step %q failed", failedStepID)
		if err := c.store.MarkRunFailed(ctx, runID, failedStepID, reason); err != nil {
			lg.Error(err, "compensation: mark run failed failed")
			return fmt.Errorf("compensate %s: mark failed: %w", runID, err)
		}
		lg.Debug("run state changed", golog.F("to_state", string(domain.RunStateFailed)))
	}
	c.checkParentJoin(ctx, run)
	return nil
}

// compensate rolls back the steps a run completed before it failed. It runs the
// Compensation action of each already-completed compensable step in reverse
// order: it transitions the run to RunStateCompensating, emits
// EventCompensationStarted, dispatches each step's Compensation.Action over the
// same transport an action step uses, then leaves the run for the caller to
// settle to RunStateFailed.
//
// A completed step with a nil Compensation is skipped (a warning is logged). A
// step whose Compensation has an empty Action, or whose dispatch fails, is
// logged and skipped — compensation is best-effort and must not block the run
// from reaching its terminal failed state.
//
// failedStep is the step whose error triggered the rollback; it is not
// compensated (it did not complete).
func (c *Coordinator) compensate(ctx context.Context, run domain.SagaRun, def domain.WorkflowDefinition, failedStep domain.Step) {
	lg := c.logger(ctx).With(golog.F("run_id", run.ID.String()), golog.F("failed_step", failedStep.ID))
	completed := c.completedCompensableSteps(ctx, run, def, failedStep.ID)
	if len(completed) == 0 {
		lg.Debug("compensation: no completed steps to roll back")
		return // nothing to roll back; caller settles to failed
	}

	if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateCompensating, run.CurrentStep); err != nil {
		lg.Error(err, "compensation: set compensating state failed")
		return
	}
	lg.Debug("run state changed", golog.F("to_state", string(domain.RunStateCompensating)))
	lg.Info("compensation started", golog.F("steps", len(completed)))
	_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, failedStep.ID, 0, domain.EventCompensationStarted, "engine"))

	// Resolve the action verb so compensation reuses the action dispatch path.
	var av verbs.ActionVerb
	if entry, ok := c.verbs[domain.StepTypeAction]; ok {
		if v, ok := entry.Handler.(verbs.ActionVerb); ok {
			av = v
		}
	}

	// Reverse order: most recently completed step is compensated first.
	compensated, skipped, failed := 0, 0, 0
	for i := len(completed) - 1; i >= 0; i-- {
		step := completed[i]
		slg := lg.With(golog.F("step_id", step.ID), golog.F("step_type", string(step.Type)))
		if step.Compensation == nil {
			slg.Warn("compensation: step has no compensation; skipping")
			skipped++
			continue
		}
		slg.Debug("compensating step", golog.F("action", step.Compensation.Action), golog.F("dry_run", run.DryRun))
		if err := av.DispatchCompensation(ctx, run.ID.String(), step.ID, step.Compensation.Action, step.Compensation.Inputs, run.DryRun); err != nil {
			slg.Error(err, "compensation: dispatch failed; continuing rollback", golog.F("action", step.Compensation.Action))
			failed++
			continue
		}
		slg.Debug("compensation dispatched", golog.F("action", step.Compensation.Action))
		compensated++
	}
	lg.Info("compensation finished", golog.F("compensated", compensated), golog.F("skipped", skipped), golog.F("failed", failed))
}

// completedCompensableSteps returns, in completion order, the steps that
// reached step.succeeded before the run failed. The failed step (excludeID) and
// non-graph steps (end) are omitted. Steps without a Compensation are retained
// so the caller can log the skip warning; only steps that actually completed are
// included. Completion order is derived from the run's audit events.
func (c *Coordinator) completedCompensableSteps(ctx context.Context, run domain.SagaRun, def domain.WorkflowDefinition, excludeID string) []domain.Step {
	events, err := c.store.ListEventsByRun(ctx, run.ID)
	if err != nil {
		c.logger(ctx).Error(err, "compensation: list events failed", golog.F("run_id", run.ID.String()))
		return nil
	}
	seen := map[string]bool{}
	out := make([]domain.Step, 0, len(events))
	for _, ev := range events {
		if ev.EventType != domain.EventStepSucceeded {
			continue
		}
		if ev.StepID == "" || ev.StepID == excludeID || seen[ev.StepID] {
			continue
		}
		step, ok := def.StepByID(ev.StepID)
		if !ok || step.Type == domain.StepTypeEnd {
			continue
		}
		seen[ev.StepID] = true
		out = append(out, step)
	}
	return out
}
