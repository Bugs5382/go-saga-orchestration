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

// FilterVerb keeps list elements where expr is truthy. Inputs:
//   - "list" (required, string): CEL expression that must evaluate to a list.
//   - "expr" (required, string): CEL predicate; element bound as `_`.
//   - "out_var" (required, string): variable to write the filtered list to.
type FilterVerb struct{}

// Execute evaluates the list expression, applies the predicate to each
// element (bound as `_`), and writes the retained elements to out_var.
func (FilterVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	listExpr, _ := step.Inputs["list"].(string)
	predExpr, _ := step.Inputs["expr"].(string)
	outVar, _ := step.Inputs["out_var"].(string)
	if listExpr == "" || predExpr == "" || outVar == "" {
		return nil, fmt.Errorf("filter: list, expr, out_var required")
	}
	listPrg, err := cel.CompiledProgram(keysOf(run.Variables), listExpr)
	if err != nil {
		return nil, fmt.Errorf("filter: compile list: %w", err)
	}
	listVal, err := listPrg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("filter: eval list: %w", err)
	}
	xs, ok := listVal.([]any)
	if !ok {
		return nil, fmt.Errorf("filter: list expr did not produce []any, got %T", listVal)
	}
	predPrg, err := cel.CompiledProgram(append(keysOf(run.Variables), "_"), predExpr)
	if err != nil {
		return nil, fmt.Errorf("filter: compile pred: %w", err)
	}
	// Reuse one activation across elements (see map.go for the rationale): Eval
	// does not retain the map, so overwriting "_" per element avoids cloning
	// run.Variables each time. A run variable named "_" shadows the element
	// binding, preserving the prior behavior.
	varMap := make(map[string]any, len(run.Variables)+1)
	for k, v := range run.Variables {
		varMap[k] = v
	}
	_, shadowed := run.Variables["_"]
	out := make([]any, 0, len(xs))
	for _, x := range xs {
		if !shadowed {
			varMap["_"] = x
		}
		v, err := predPrg.Eval(varMap)
		if err != nil {
			return nil, fmt.Errorf("filter: eval pred for element %v: %w", x, err)
		}
		if b, _ := v.(bool); b {
			out = append(out, x)
		}
	}
	return map[string]any{outVar: out}, nil
}
