package verbs

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
	"fmt"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// JoinVerb is a barrier that reconvenes independently-spawned upstream
// streams before the run continues. Unlike parallel (which spawns its own
// branches and immediately pauses), join watches children that earlier steps
// spawned in the same run - the natural producer is spawn_saga, whose
// fire-and-forget children the parent did not wait on. join lets a later step
// gather those streams back together.
//
// Inputs:
//   - "streams" (required, []any of strings, or a CEL string that evaluates
//     to a non-empty list of strings): the IDs of upstream steps in THIS run
//     whose spawned children the join waits on. Each named step must have
//     spawned at least one child (via spawn_saga, parallel, foreach, or
//     sub_saga); join collects the children of every named step by calling
//     ListChildrenByParent(run.ID, streamStepID) and treats the union as the
//     watched group.
//   - "join_strategy" (optional, string, default "all"): "all" waits for
//     every watched child to reach a terminal state; "quorum" resolves once
//     quorum_n watched children have succeeded.
//   - "quorum_n" (required when join_strategy=="quorum", int or CEL string):
//     positive integer, must be <= the number of watched children.
//
// Resolution:
//   - If the barrier is already satisfied when the verb runs (the watched
//     children finished before control reached the join), Execute aggregates
//     their outputs and returns them so the run advances to step.Next without
//     pausing.
//   - Otherwise Execute pauses the run (ErrSagaPaused). The coordinator's
//     checkJoinBarriers hook (engine/advance.go) re-evaluates every join step
//     whenever a watched child terminates and wakes the run once the strategy
//     is satisfied, aggregating the outputs at wake time.
//
// Aggregated outputs land in Variables under "_join.<step_id>.branches" as a
// list of {key, variables, state, _user_task?} entries, mirroring the
// "_parallel.<step_id>.branches" shape produced by the parallel verb.
type JoinVerb struct {
	S store.Store
}

// Execute resolves the watched streams, validates the join strategy, and
// either aggregates-and-continues (barrier already met) or pauses the run
// (ErrSagaPaused) until checkJoinBarriers wakes it.
func (v JoinVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	streamIDs, err := ResolveJoinStreams(step.Inputs["streams"], run.Variables)
	if err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}

	// Validate join_strategy / quorum_n up front (before pausing) so a
	// misconfigured barrier fails fast rather than pausing forever.
	watched, err := v.collectWatched(ctx, run.ID, streamIDs)
	if err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}
	if len(watched) == 0 {
		return nil, fmt.Errorf("join: streams %v matched no spawned children in this run", streamIDs)
	}
	if err := validateJoinStrategy(step.Inputs, run.Variables, len(watched)); err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}

	// Barrier already satisfied? Aggregate and continue without pausing.
	if JoinConditionMet(step.Inputs, run.Variables, watched) {
		aggregated := AggregateJoinResults(ctx, v.S, watched)
		return map[string]any{
			"_join." + step.ID + ".branches": aggregated,
		}, nil
	}

	// Not yet satisfied - pause. checkJoinBarriers wakes us when a watched
	// child terminates and the strategy is met. Use step.ID so the paused
	// record carries the join step as CurrentStep (the wakeup path advances
	// to step.Next).
	if err := v.S.UpdateRunState(ctx, run.ID, domain.RunStatePaused, step.ID); err != nil {
		return nil, fmt.Errorf("join: pause run: %w", err)
	}
	return nil, ErrSagaPaused
}

// collectWatched returns the union of all children spawned under this run by
// any of the named upstream steps. Duplicate step IDs are de-duplicated by
// step; children are unique per (run, step) so the union has no repeats.
func (v JoinVerb) collectWatched(ctx context.Context, runID uuid.UUID, streamIDs []string) ([]domain.SagaRun, error) {
	seen := map[string]bool{}
	out := []domain.SagaRun{}
	for _, sid := range streamIDs {
		if seen[sid] {
			continue
		}
		seen[sid] = true
		children, err := v.S.ListChildrenByParent(ctx, runID, sid)
		if err != nil {
			return nil, fmt.Errorf("list children of step %q: %w", sid, err)
		}
		out = append(out, children...)
	}
	return out, nil
}
