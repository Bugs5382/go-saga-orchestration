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
	"github.com/Bugs5382/go-saga-orchestration/internal/cel"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// ForeachVerb fans out one child run per element of a CEL-evaluated
// list. PARALLEL mode runs one branch per list element.
// Sequential mode is intentionally deferred — use `while` with an
// explicit counter for now.
//
// TODO(future-batch): add sequential mode (parallel: false) — state-machine
// back-edge loop where each iteration executes the body steps one at a time,
// advancing via a back-edge to re-enter the foreach step after the body
// completes. For now, use `while` with an index counter for sequential loops.
//
// Inputs:
//   - "list"     (required, string): CEL expression evaluating to a list.
//   - "body"     (required, []any): step objects forming the loop body.
//     Each child run gets the list element bound as Variables["_foreach_item"].
//   - "start"    (required, string): ID of the first step inside body.
//   - "parallel" (optional, bool, default true): the only supported mode
//     in v1 is parallel; passing false returns an error noting the
//     deferred sequential mode.
type ForeachVerb struct {
	S         store.Store
	Publisher Publisher
}

// Execute evaluates the list expression and spawns one child run per element
// (each with the element bound as _foreach_item), then pauses the parent
// awaiting the children and returns ErrSagaPaused. An empty list advances
// without spawning; sequential mode is rejected.
func (v ForeachVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	if par, ok := step.Inputs["parallel"].(bool); ok && !par {
		return nil, fmt.Errorf("foreach: sequential mode deferred; use `while` with an index counter")
	}

	listExpr, _ := step.Inputs["list"].(string)
	if listExpr == "" {
		return nil, fmt.Errorf("foreach: list required")
	}
	body, ok := step.Inputs["body"].([]any)
	if !ok || len(body) == 0 {
		return nil, fmt.Errorf("foreach: body required (non-empty []any of step objects)")
	}
	start, _ := step.Inputs["start"].(string)
	if start == "" {
		return nil, fmt.Errorf("foreach: start (first step ID of body) required")
	}

	prg, err := cel.CompiledProgram(keysOf(run.Variables), listExpr)
	if err != nil {
		return nil, fmt.Errorf("foreach: compile list: %w", err)
	}
	listVal, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("foreach: eval list: %w", err)
	}
	items, ok := listVal.([]any)
	if !ok {
		return nil, fmt.Errorf("foreach: list expr did not produce []any, got %T", listVal)
	}
	if len(items) == 0 {
		// Empty list — advance to step.Next without spawning anything.
		return map[string]any{}, nil
	}

	stepsParsed, err := parseSteps(body)
	if err != nil {
		return nil, fmt.Errorf("foreach: parse body: %w", err)
	}

	for i, item := range items {
		branchKey := fmt.Sprintf("i%d", i)
		branchDef := domain.WorkflowDefinition{
			ID:        run.WorkflowID + "@" + step.ID + "/" + branchKey,
			Version:   1,
			Name:      "synthetic-foreach-iter",
			Start:     start,
			Steps:     stepsParsed,
			Published: true,
		}
		if _, err := v.S.UpsertWorkflowDefinition(ctx, branchDef); err != nil {
			return nil, fmt.Errorf("foreach: upsert iter %d def: %w", i, err)
		}
		childInputs := map[string]any{}
		for k, val := range run.Variables {
			childInputs[k] = val
		}
		childInputs["_foreach_item"] = item
		childInputs["_foreach_index"] = int64(i)
		childID, err := v.S.SpawnChildRun(ctx, run.ID, step.ID, branchKey, branchDef, childInputs)
		if err != nil {
			return nil, fmt.Errorf("foreach: spawn iter %d: %w", i, err)
		}
		if v.Publisher != nil {
			if err := v.Publisher.PublishSagaAdvance(ctx, childID.String()); err != nil {
				return nil, fmt.Errorf("foreach: publish iter %d: %w", i, err)
			}
		}
	}

	// Mark parent as paused awaiting child iterations. Same pattern as ParallelVerb:
	// the child-terminal hook in coordinator/advance.go calls WakeFromExternal once
	// all siblings terminate, then PublishSagaAdvance(parentID) resumes the parent.
	if err := v.S.UpdateRunState(ctx, run.ID, domain.RunStatePaused, step.ID); err != nil {
		return nil, fmt.Errorf("foreach: pause parent: %w", err)
	}
	return nil, ErrSagaPaused
}
