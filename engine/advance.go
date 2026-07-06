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

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

// Advance walks the saga forward until it either reaches a terminal state or
// a step that needs external I/O (future async verbs). Each loop iteration
// re-reads the run from the store so it sees any variable updates written by
// the previous step.
func (c *Coordinator) Advance(ctx context.Context, runIDStr string) error {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		return fmt.Errorf("parse run id: %w", err)
	}
	for {
		// Stop between steps if the context was cancelled (e.g. Saga.Shutdown).
		if err := ctx.Err(); err != nil {
			return err
		}
		run, err := c.store.GetRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("get run: %w", err)
		}
		if run.State.IsTerminal() {
			return nil
		}

		// Handle paused runs. Two wakeup conditions:
		//   1. Time-based: wakeup_at is non-nil and <= now (wait_duration, wait_until).
		//   2. External: awaited_signal and awaited_event_topic are both nil and
		//      wakeup_at is nil — the signal/event handler consumed the await markers
		//      and published saga.advance (wait_for_signal, wait_for_event).
		// In both cases: clear pause state, emit step.succeeded, advance to Next.
		// If neither condition holds the run is still legitimately paused; ACK.
		if run.State == domain.RunStatePaused {
			now := c.clock.Now()
			wakeupDue := run.WakeupAt != nil && !run.WakeupAt.After(now)
			noPendingAwaits := run.AwaitedSignal == nil && run.AwaitedEventTopic == nil
			externalWake := noPendingAwaits && run.WakeupAt == nil

			if wakeupDue || externalWake {
				// Detect timeout: timer fired while await markers are still set.
				// A real signal/event clears AwaitedSignal/AwaitedEventTopic before
				// waking (TryConsumeAwaitedSignal / WakeFromExternal), so timedOut
				// is false in that case → step.Next is used unchanged (backward-compat).
				timedOut := wakeupDue && (run.AwaitedSignal != nil || run.AwaitedEventTopic != nil)

				// Wakeup condition met: clear pause state.
				if err := c.store.ClearPause(ctx, run.ID); err != nil {
					return fmt.Errorf("clear pause: %w", err)
				}
				// Re-read to get cleared state with same CurrentStep.
				run, err = c.store.GetRun(ctx, runID)
				if err != nil {
					return fmt.Errorf("re-read after clear: %w", err)
				}
				// The wait verb already succeeded (it returned ErrSagaPaused after
				// persisting its pause marker). Emit step.succeeded now and advance to next.
				def, err := c.store.GetWorkflowDefinition(ctx, run.DefinitionID)
				if err != nil {
					return fmt.Errorf("get def after clear: %w", err)
				}
				step, ok := def.StepByID(run.CurrentStep)
				if !ok {
					return fmt.Errorf("def missing step %s", run.CurrentStep)
				}
				_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepSucceeded, "engine"))
				next := step.Next
				if timedOut {
					if br, ok := step.Branches["timeout"]; ok && br.Next != "" {
						next = br.Next
					}
				}
				if next == "" {
					return fmt.Errorf("paused step %s has no next", step.ID)
				}
				if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateRunning, next); err != nil {
					return fmt.Errorf("set next after clear: %w", err)
				}
				// Re-read so the loop's next iteration sees the new CurrentStep.
				run, err = c.store.GetRun(ctx, runID)
				if err != nil {
					return fmt.Errorf("re-read after next: %w", err)
				}
				// Continue loop with updated run (CurrentStep now points to step.Next).
				_ = run // used by next iteration via GetRun at top of loop
				continue
			}
			// Still legitimately paused (wakeup in the future, or pending await markers).
			// This advance call arrived prematurely — ACK without action.
			return nil
		}

		def, err := c.store.GetWorkflowDefinition(ctx, run.DefinitionID)
		if err != nil {
			return fmt.Errorf("get definition: %w", err)
		}

		stepID := run.CurrentStep
		if stepID == "" {
			stepID = def.Start
		}
		if run.State == domain.RunStatePending {
			_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, "", 0, domain.EventSagaStarted, "engine"))
		}
		step, ok := def.StepByID(stepID)
		if !ok {
			return fmt.Errorf("definition references missing step: %s", stepID)
		}

		_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepDispatched, "engine"))
		_ = c.store.UpdateRunState(ctx, run.ID, domain.RunStateRunning, step.ID)

		if step.Type == domain.StepTypeEnd {
			return c.completeRun(ctx, run, step)
		}

		entry, ok := c.verbs[step.Type]
		if !ok {
			return fmt.Errorf("no handler registered for step type: %s", step.Type)
		}
		// Runtime license gate — check before dispatch.
		group := verbs.LicenseGroupForStep(step, entry.LicenseGroup)
		if feature := verbs.GroupToFeature[group]; feature != "" {
			overrides := run.FeatureOverrides
			enabled, err := c.licensing.IsFeatureEnabled(ctx, run.TenantID, feature, overrides)
			if err != nil {
				_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventLicenseGateRejected, "engine"))
				_ = c.store.MarkRunFailed(ctx, run.ID, step.ID, fmt.Sprintf("license check for feature %q: %v", feature, err))
				return fmt.Errorf("license check for step %q (feature %q): %w", step.ID, feature, err)
			}
			if !enabled {
				evt := domain.NewEvent(run.ID, step.ID, 0, domain.EventLicenseGateRejected, "engine")
				evt.Metadata = map[string]any{"group": group, "feature": feature}
				_ = c.store.AppendEvent(ctx, evt)
				_ = c.store.MarkRunFailed(ctx, run.ID, step.ID, fmt.Sprintf("license_gate: feature %q not enabled for tenant", feature))
				return fmt.Errorf("license_gate: feature %q not enabled for tenant", feature)
			}
		}
		result, err := c.executeStep(ctx, run, step, entry.Handler)
		if errors.Is(err, verbs.ErrSagaPaused) {
			_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepPaused, "engine"))
			return nil // ACK queue msg; external/timer wakeup will republish saga.advance
		}
		if errors.Is(err, verbs.ErrSagaCancelled) {
			_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, "", 0, domain.EventRunCancelled, "engine"))
			if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateCancelled, step.ID); err != nil {
				return fmt.Errorf("cancel: set cancelled: %w", err)
			}
			c.checkParentJoin(ctx, run)
			return nil
		}
		if err != nil {
			// Check try_catch stack — if non-empty, jump to catch step instead of failing.
			frame, popped, popErr := c.store.PopTryCatch(ctx, run.ID)
			if popErr == nil && popped {
				errMap := map[string]any{
					"_error": map[string]any{
						"step_id": step.ID,
						"message": err.Error(),
						"verb":    string(step.Type),
					},
				}
				_ = c.store.UpdateRunVariables(ctx, run.ID, errMap)
				_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepFailed, "engine-caught"))
				_ = c.store.UpdateRunState(ctx, run.ID, domain.RunStateRunning, frame.CatchStep)
				continue // re-loop with the catch step
			}
			// No try_catch frame: record the failure, roll back completed steps, then fail.
			_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepFailed, "engine"))
			c.compensate(ctx, run, def, step)
			// Persist the failing step's error so the failed run is
			// self-describing (issue #80), not just state=failed.
			_ = c.store.MarkRunFailed(ctx, run.ID, step.ID, fmt.Sprintf("step %q (%s): %v", step.ID, step.Type, err))
			c.checkParentJoin(ctx, run)
			return fmt.Errorf("verb %s: %w", step.Type, err)
		}
		if len(result) > 0 {
			if err := c.store.UpdateRunVariables(ctx, run.ID, result); err != nil {
				return fmt.Errorf("update variables: %w", err)
			}
		}
		_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepSucceeded, "engine"))

		next := step.Next
		if branchKey, ok := result["branch"].(string); ok && branchKey != "" {
			if br, found := step.Branches[branchKey]; found {
				next = br.Next
			} else if step.Type == domain.StepTypeDecision || step.Type == domain.StepTypeWhile || step.Type == domain.StepTypeSwitch {
				// For verbs that always require branch resolution, a missing branch is an error.
				return fmt.Errorf("%s branch %q not found in step.Branches", step.Type, branchKey)
			}
		}
		if next == "" {
			return fmt.Errorf("step %q has no next and is not end", step.ID)
		}
		if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateRunning, next); err != nil {
			return fmt.Errorf("set next step: %w", err)
		}
		// loop: next iteration re-reads run with the new CurrentStep
	}
}

func (c *Coordinator) completeRun(ctx context.Context, run domain.SagaRun, step domain.Step) error {
	_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, 0, domain.EventStepSucceeded, "engine"))
	_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, "", 0, domain.EventRunSucceeded, "engine"))
	if err := c.store.UpdateRunState(ctx, run.ID, domain.RunStateSucceeded, ""); err != nil {
		return err
	}
	c.checkParentJoin(ctx, run)
	return nil
}

// checkParentJoin checks whether the join condition of the parent's parallel
// step is satisfied. It fires after any terminal state write (succeeded or
// failed). Errors are logged and swallowed — the child's own terminal write
// already committed.
//
// Join strategies:
//   - "all" (default): wake parent when every sibling is in a terminal state.
//   - "quorum": wake parent when quorum_n siblings have reached RunStateSucceeded.
//     Remaining branches keep running but no longer gate the parent.
//
// Idempotency: the existing guard (parent.State==paused &&
// parent.CurrentStep==parentStepID) blocks re-entry once the parent advances.
// Before the parent advances, repeated calls hit WakeFromExternal which is
// idempotent (clears already-nil markers — a no-op in the store).
func (c *Coordinator) checkParentJoin(ctx context.Context, run domain.SagaRun) {
	if run.ParentRunID == nil || run.ParentStepID == nil {
		return
	}

	// A join barrier (a later step in the same parent run) may also be
	// watching this stream. checkParentJoin wakes a parent paused on the
	// spawning step; checkJoinBarriers wakes a parent paused on a downstream
	// join step. They are independent and both guarded and idempotent, so run
	// the barrier check on every terminal write too.
	c.checkJoinBarriers(ctx, run)

	// Guard: only wake the parent if it is still paused on the same step that
	// spawned this child. For spawn_saga (fire-and-forget), the parent advances
	// immediately without pausing, so its CurrentStep differs from the child's
	// ParentStepID. Waking such a parent would be a no-op at best, or could
	// prematurely resume the parent if it is later paused on a different step.
	parent, err := c.store.GetRun(ctx, *run.ParentRunID)
	if err != nil {
		log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("child-join: get parent failed")
		return
	}
	if parent.State != domain.RunStatePaused || parent.CurrentStep != *run.ParentStepID {
		return // parent has moved on or is not paused; nothing to do
	}

	siblings, err := c.store.ListChildrenByParent(ctx, *run.ParentRunID, *run.ParentStepID)
	if err != nil {
		log.Warn().Err(err).Str("run_id", run.ID.String()).Msg("child-join: list siblings failed")
		return
	}

	// Read the parent's workflow definition to determine the join strategy.
	parentDef, err := c.store.GetWorkflowDefinition(ctx, parent.DefinitionID)
	if err != nil {
		log.Warn().Err(err).Msg("child-join: get parent def failed")
		return
	}
	stepInputs := lookupStepInputs(parentDef, *run.ParentStepID)
	if !verbs.JoinConditionMet(stepInputs, parent.Variables, siblings) {
		return
	}

	// Wake condition met. Aggregate child results into parent Variables first.
	aggregated := verbs.AggregateJoinResults(ctx, c.store, siblings)
	if aggregated != nil {
		merge := map[string]any{
			"_parallel." + *run.ParentStepID + ".branches": aggregated,
		}
		if err := c.store.UpdateRunVariables(ctx, *run.ParentRunID, merge); err != nil {
			log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("child-join: write _parallel aggregate failed")
			// Don't return — still wake the parent. The aggregate write is best-effort.
		}
	}

	if err := c.store.WakeFromExternal(ctx, *run.ParentRunID); err != nil {
		log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("child-join: WakeFromExternal failed")
		return
	}
	if c.publisher != nil {
		if err := c.publisher.PublishSagaAdvance(ctx, run.ParentRunID.String()); err != nil {
			log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("child-join: publish advance failed")
		}
	}
}

// lookupStepInputs finds the step by id in def.Steps and returns its Inputs.
// Returns nil if the step is not found.
func lookupStepInputs(def domain.WorkflowDefinition, stepID string) map[string]any {
	step, ok := def.StepByID(stepID)
	if !ok {
		return nil
	}
	return step.Inputs
}

// checkJoinBarriers re-evaluates any join steps in the parent run that are
// waiting on the terminated run's stream (its ParentStepID). It fires after
// any terminal state write, alongside checkParentJoin. Whereas checkParentJoin
// wakes a parent paused on the SPAWNING step (parallel/sub_saga), a join
// barrier is a LATER step in the same parent run: the parent's CurrentStep is
// the join step, not the step that spawned the terminated child.
//
// For the terminated child, the "parent run" is *run.ParentRunID and the
// stream it belongs to is *run.ParentStepID. This scans that parent's
// definition for join steps whose "streams" list includes that stream id, and
// for each such join step that the parent is currently paused on, aggregates
// the union of watched children and wakes the parent if the strategy is met.
//
// Errors are logged and swallowed; the child's own terminal write already
// committed.
func (c *Coordinator) checkJoinBarriers(ctx context.Context, run domain.SagaRun) {
	if run.ParentRunID == nil || run.ParentStepID == nil {
		return
	}
	parent, err := c.store.GetRun(ctx, *run.ParentRunID)
	if err != nil {
		log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("join-barrier: get parent failed")
		return
	}
	if parent.State != domain.RunStatePaused {
		return // parent not waiting; nothing to do
	}
	parentDef, err := c.store.GetWorkflowDefinition(ctx, parent.DefinitionID)
	if err != nil {
		log.Warn().Err(err).Msg("join-barrier: get parent def failed")
		return
	}
	joinStep, ok := parentDef.StepByID(parent.CurrentStep)
	if !ok || joinStep.Type != domain.StepTypeJoin {
		return // parent is not paused on a join step
	}

	streamIDs, err := verbs.ResolveJoinStreams(joinStep.Inputs["streams"], parent.Variables)
	if err != nil {
		log.Warn().Err(err).Str("join_step", joinStep.ID).Msg("join-barrier: resolve streams failed")
		return
	}
	// Only act if this terminated child belongs to a stream this join watches.
	watchesThis := false
	for _, sid := range streamIDs {
		if sid == *run.ParentStepID {
			watchesThis = true
			break
		}
	}
	if !watchesThis {
		return
	}

	watched := []domain.SagaRun{}
	seen := map[string]bool{}
	for _, sid := range streamIDs {
		if seen[sid] {
			continue
		}
		seen[sid] = true
		children, err := c.store.ListChildrenByParent(ctx, *run.ParentRunID, sid)
		if err != nil {
			log.Warn().Err(err).Str("stream", sid).Msg("join-barrier: list children failed")
			return
		}
		watched = append(watched, children...)
	}

	if !verbs.JoinConditionMet(joinStep.Inputs, parent.Variables, watched) {
		return // barrier not yet satisfied
	}

	// Barrier met: aggregate watched-child outputs into the parent, then wake.
	aggregated := verbs.AggregateJoinResults(ctx, c.store, watched)
	if aggregated != nil {
		merge := map[string]any{
			"_join." + joinStep.ID + ".branches": aggregated,
		}
		if err := c.store.UpdateRunVariables(ctx, *run.ParentRunID, merge); err != nil {
			log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("join-barrier: write _join aggregate failed")
			// Don't return; still wake the parent. The aggregate write is best-effort.
		}
	}
	if err := c.store.WakeFromExternal(ctx, *run.ParentRunID); err != nil {
		log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("join-barrier: WakeFromExternal failed")
		return
	}
	if c.publisher != nil {
		if err := c.publisher.PublishSagaAdvance(ctx, run.ParentRunID.String()); err != nil {
			log.Warn().Err(err).Str("parent_run_id", run.ParentRunID.String()).Msg("join-barrier: publish advance failed")
		}
	}
}
