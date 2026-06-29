package engine

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

	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

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
	completed := c.completedCompensableSteps(ctx, run, def, failedStep.ID)
	if len(completed) == 0 {
		return // nothing to roll back; caller settles to failed
	}

	if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateCompensating, run.CurrentStep); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID.String()).Msg("compensation: set compensating state failed")
		return
	}
	_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, failedStep.ID, 0, domain.EventCompensationStarted, "engine"))

	// Resolve the action verb so compensation reuses the action dispatch path.
	var av verbs.ActionVerb
	if entry, ok := c.verbs[domain.StepTypeAction]; ok {
		if v, ok := entry.Handler.(verbs.ActionVerb); ok {
			av = v
		}
	}

	// Reverse order: most recently completed step is compensated first.
	for i := len(completed) - 1; i >= 0; i-- {
		step := completed[i]
		if step.Compensation == nil {
			log.Warn().Str("run_id", run.ID.String()).Str("step_id", step.ID).
				Msg("compensation: step has no compensation; skipping")
			continue
		}
		if err := av.DispatchCompensation(ctx, run.ID.String(), step.ID, step.Compensation.Action, step.Compensation.Inputs, run.DryRun); err != nil {
			log.Warn().Err(err).Str("run_id", run.ID.String()).Str("step_id", step.ID).
				Msg("compensation: dispatch failed; continuing rollback")
			continue
		}
	}
}

// completedCompensableSteps returns, in completion order, the steps that
// reached step.succeeded before the run failed. The failed step (excludeID) and
// non-graph steps (end) are omitted. Steps without a Compensation are retained
// so the caller can log the skip warning; only steps that actually completed are
// included. Completion order is derived from the run's audit events.
func (c *Coordinator) completedCompensableSteps(ctx context.Context, run domain.SagaRun, def domain.WorkflowDefinition, excludeID string) []domain.Step {
	events, err := c.store.ListEventsByRun(ctx, run.ID)
	if err != nil {
		log.Warn().Err(err).Str("run_id", run.ID.String()).Msg("compensation: list events failed")
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
