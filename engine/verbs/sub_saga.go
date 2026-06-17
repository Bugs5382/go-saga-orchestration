package verbs

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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// SubSagaVerb starts a named workflow as a child saga and pauses the
// parent until the child reaches a terminal state. The coordinator's
// child-terminal hook (engine/advance.go:checkParentJoin) wakes the
// parent when all children of this step terminate.
//
// Inputs:
//   - "workflow_id" (required, string): the child workflow's stable ID.
//   - "inputs"      (optional, map[string]any): inputs passed to the child.
//
// Note: sub_saga reuses the same child-run + WakeFromExternal mechanism
// as `parallel` — by definition there's exactly one "branch" (the child).
type SubSagaVerb struct {
	S         store.Store
	Publisher Publisher
}

// Execute resolves and spawns the named workflow as a child saga, then pauses
// the parent awaiting the child's terminal state and returns ErrSagaPaused.
func (v SubSagaVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	workflowID, _ := step.Inputs["workflow_id"].(string)
	if workflowID == "" {
		return nil, fmt.Errorf("sub_saga: workflow_id required")
	}
	inputs, _ := step.Inputs["inputs"].(map[string]any)
	if inputs == nil {
		inputs = map[string]any{}
	}

	// SagaRun.TenantID is *uuid.UUID — pass directly to the store lookup.
	def, err := v.S.GetPublishedWorkflowByID(ctx, workflowID, run.TenantID)
	if err != nil {
		return nil, fmt.Errorf("sub_saga: resolve %q: %w", workflowID, err)
	}

	entrypoint, _ := step.Inputs["entrypoint"].(string)
	startStep, err := def.ResolveEntry(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("sub_saga: %w", err)
	}

	childID, err := v.S.SpawnChildRunAt(ctx, run.ID, step.ID, "sub", def, inputs, startStep)
	if err != nil {
		return nil, fmt.Errorf("sub_saga: spawn: %w", err)
	}
	if v.Publisher != nil {
		if err := v.Publisher.PublishSagaAdvance(ctx, childID.String()); err != nil {
			return nil, fmt.Errorf("sub_saga: publish: %w", err)
		}
	}

	// Pause the parent; it will be woken by checkParentJoin once the child
	// reaches a terminal state. We call UpdateRunState here (same as parallel)
	// so CurrentStep is already set to step.ID when the parent is paused.
	if err := v.S.UpdateRunState(ctx, run.ID, domain.RunStatePaused, step.ID); err != nil {
		return nil, fmt.Errorf("sub_saga: pause parent: %w", err)
	}
	return nil, ErrSagaPaused
}
