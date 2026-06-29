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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/internal/cel"
)

const (
	defaultMaxIterations = 100
	hardMaxIterations    = 10000
)

// WhileVerb evaluates a CEL condition and chooses a branch. Inputs:
//   - "condition"      (required, string): CEL expression evaluated against Variables.
//   - "max_iterations" (optional, number, default 100, hard cap 10000):
//     if the per-step iteration counter reaches this, the verb returns an error.
//     Cap prevents runaway loops.
//
// The verb returns a {"branch": "continue"|"exit"} output map. The
// workflow author wires step.Branches:
//   - "continue" → next: body's first step.
//   - "exit"     → next: step after the loop.
//
// The body's last step should point its next back at this while step so
// the loop closes. The iteration counter persists at
// Variables._while.{step.ID}.iter (int64).
type WhileVerb struct{}

// Execute evaluates the condition, increments the persisted iteration counter,
// and returns branch "continue" (condition true) or "exit" (false). It errors
// once the iteration count reaches max_iterations.
func (WhileVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	cond, _ := step.Inputs["condition"].(string)
	if cond == "" {
		return nil, fmt.Errorf("while: condition required")
	}
	maxIter := int64(defaultMaxIterations)
	if mi, ok := step.Inputs["max_iterations"].(float64); ok {
		maxIter = int64(mi)
	}
	if maxIter > hardMaxIterations {
		maxIter = hardMaxIterations
	}
	if maxIter < 1 {
		maxIter = defaultMaxIterations
	}

	// Read iteration counter from the nested map.
	iter := int64(0)
	if whileMap, ok := run.Variables["_while"].(map[string]any); ok {
		if stepMap, ok := whileMap[step.ID].(map[string]any); ok {
			switch v := stepMap["iter"].(type) {
			case int64:
				iter = v
			case float64:
				iter = int64(v)
			}
		}
	}
	if iter >= maxIter {
		return nil, fmt.Errorf("while: max_iterations %d reached", maxIter)
	}

	prg, err := cel.CompiledProgram(keysOf(run.Variables), cond)
	if err != nil {
		return nil, fmt.Errorf("while: compile: %w", err)
	}
	val, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("while: eval: %w", err)
	}
	condTrue, _ := val.(bool)

	branch := "exit"
	if condTrue {
		branch = "continue"
	}
	// Always emit the incremented iter counter. The dotted-key write in
	// UpdateRunVariables will create/walk the nested _while.{step.ID}.iter path.
	counterKey := "_while." + step.ID + ".iter"
	return map[string]any{
		"branch":   branch,
		counterKey: iter + 1,
	}, nil
}
