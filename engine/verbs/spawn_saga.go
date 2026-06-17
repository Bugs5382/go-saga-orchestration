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

// SpawnSagaVerb starts a named workflow as a fire-and-forget child.
// The parent continues immediately to step.Next without pausing.
// The child runs independently; its outcome doesn't block the parent.
//
// Implementation note: SpawnChildRun is reused (same as sub_saga) so the
// child carries a ParentRunID for audit purposes. The "fire-and-forget"
// property is achieved by NOT calling UpdateRunState(paused) and NOT
// returning ErrSagaPaused — the parent advances normally.
//
// The coordinator's checkParentJoin guard (engine/advance.go) checks that
// the parent is still paused on the spawning step before waking it, so a
// fire-and-forget child terminating never prematurely wakes a parent that
// is paused on a later step.
//
// Inputs:
//   - "workflow_id" (required, string): the child workflow's stable ID.
//   - "inputs"      (optional, map[string]any): inputs passed to the child.
type SpawnSagaVerb struct {
	S         store.Store
	Publisher Publisher
}

// Execute resolves and spawns the named workflow as a fire-and-forget child,
// then returns an empty result so the parent advances without pausing.
func (v SpawnSagaVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	workflowID, _ := step.Inputs["workflow_id"].(string)
	if workflowID == "" {
		return nil, fmt.Errorf("spawn_saga: workflow_id required")
	}
	inputs, _ := step.Inputs["inputs"].(map[string]any)
	if inputs == nil {
		inputs = map[string]any{}
	}

	// SagaRun.TenantID is *uuid.UUID — pass directly.
	def, err := v.S.GetPublishedWorkflowByID(ctx, workflowID, run.TenantID)
	if err != nil {
		return nil, fmt.Errorf("spawn_saga: resolve %q: %w", workflowID, err)
	}

	entrypoint, _ := step.Inputs["entrypoint"].(string)
	startStep, err := def.ResolveEntry(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("spawn_saga: %w", err)
	}

	childID, err := v.S.SpawnChildRunAt(ctx, run.ID, step.ID, "spawn", def, inputs, startStep)
	if err != nil {
		return nil, fmt.Errorf("spawn_saga: spawn: %w", err)
	}
	if v.Publisher != nil {
		// Best-effort: if publish fails, the child is orphaned in pending state
		// but the parent is not affected. Log is handled by callers.
		_ = v.Publisher.PublishSagaAdvance(ctx, childID.String())
	}

	// Return an empty result map so the parent advances to step.Next without
	// pausing. No ErrSagaPaused, no UpdateRunState(paused).
	return map[string]any{}, nil
}
