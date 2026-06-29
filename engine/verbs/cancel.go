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

// ParentJoinChecker re-evaluates a parent's parallel/sub_saga join after a child
// reaches a terminal state, waking the parent if the join is now satisfied. The
// Coordinator implements it; CancelVerb uses it so cancelling a target child
// does not leave a parent paused on a join.
type ParentJoinChecker interface {
	CheckParentJoin(ctx context.Context, run domain.SagaRun)
}

// CancelVerb cancels a run. With no run_id (or run_id == the current run) it
// self-cancels: returns ErrSagaCancelled and the engine sets state=cancelled.
// With a different run_id it cancels that target run and the current run
// continues to Next. Inputs: "run_id" (optional, string), "reason" (optional).
type CancelVerb struct {
	S store.Store
	// JoinChecker, when set, re-evaluates a cancelled target's parent join so a
	// parent paused on a parallel/sub_saga join is woken. Nil-safe.
	JoinChecker ParentJoinChecker
}

func (v CancelVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	runIDStr, _ := step.Inputs["run_id"].(string)
	if runIDStr == "" || runIDStr == run.ID.String() {
		return nil, ErrSagaCancelled
	}
	targetID, err := uuid.Parse(runIDStr)
	if err != nil {
		return nil, fmt.Errorf("cancel: bad run_id: %w", err)
	}
	if err := v.S.UpdateRunState(ctx, targetID, domain.RunStateCancelled, ""); err != nil {
		return nil, fmt.Errorf("cancel: update target: %w", err)
	}
	_ = v.S.AppendEvent(ctx, domain.NewEvent(targetID, step.ID, 0, domain.EventRunCancelled, "engine"))

	// The target is now terminal; if it was a child of a join-waiting parent,
	// re-evaluate that join so the parent isn't left paused forever.
	if v.JoinChecker != nil {
		if target, err := v.S.GetRun(ctx, targetID); err == nil {
			v.JoinChecker.CheckParentJoin(ctx, target)
		}
	}
	return map[string]any{}, nil
}
