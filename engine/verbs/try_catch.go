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

// TryCatchVerb pushes a try_catch frame onto the saga's stack. When a
// step inside the try body errors, the coordinator pops the top frame
// and advances to the catch step (with the error context written to
// Variables._error). On success, the body's last step's `next` should
// point to whatever comes after the try block — the frame stays on
// the stack until the saga terminates (acceptable for v1 since max
// nesting depth is 3 per the publish-time validator).
//
// Inputs:
//   - "try"   (required, []any of step IDs): metadata used by the
//     validator (engine.ValidateDefinition) to reject parallel-in-try.
//     Not consumed by the runtime — author wires step.Next to the
//     first try step.
//   - "catch" (required, string): step ID to jump to on error.
type TryCatchVerb struct {
	S store.Store
}

// Execute pushes a try_catch frame (recording the catch step) onto the run's
// stack and returns an empty result so the engine routes to the first try step.
func (v TryCatchVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	tryAny, _ := step.Inputs["try"].([]any)
	if len(tryAny) == 0 {
		return nil, fmt.Errorf("try_catch: try list required")
	}
	catch, _ := step.Inputs["catch"].(string)
	if catch == "" {
		return nil, fmt.Errorf("try_catch: catch step ID required")
	}
	frame := domain.TryCatchFrame{StepID: step.ID, CatchStep: catch}
	if err := v.S.PushTryCatch(ctx, run.ID, frame); err != nil {
		return nil, fmt.Errorf("try_catch: push frame: %w", err)
	}
	return map[string]any{}, nil // engine routes to step.Next (first try step)
}
