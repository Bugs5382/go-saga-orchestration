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
)

// MapVerb transforms each element of a list via a CEL expression where
// the element is bound as `_`. Inputs: list, expr, out_var (all
// required strings).
type MapVerb struct{}

// Execute evaluates the list expression and applies the map expression to
// each element (bound as `_`), writing the resulting list to out_var.
func (MapVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	listExpr, _ := step.Inputs["list"].(string)
	mapExpr, _ := step.Inputs["expr"].(string)
	outVar, _ := step.Inputs["out_var"].(string)
	if listExpr == "" || mapExpr == "" || outVar == "" {
		return nil, fmt.Errorf("map: list, expr, out_var required")
	}
	listPrg, err := cel.CompiledProgram(keysOf(run.Variables), listExpr)
	if err != nil {
		return nil, fmt.Errorf("map: compile list: %w", err)
	}
	listVal, err := listPrg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("map: eval list: %w", err)
	}
	xs, ok := listVal.([]any)
	if !ok {
		return nil, fmt.Errorf("map: list expr did not produce []any, got %T", listVal)
	}
	mapPrg, err := cel.CompiledProgram(append(keysOf(run.Variables), "_"), mapExpr)
	if err != nil {
		return nil, fmt.Errorf("map: compile expr: %w", err)
	}
	// Build the activation once and reuse it across elements, overwriting only
	// the bound element "_" each iteration. Eval copies the map into its own
	// activation and does not retain it, so reusing one map is safe and avoids
	// cloning run.Variables per element. A run variable literally named "_"
	// shadows the element binding, preserving the prior behavior.
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
		v, err := mapPrg.Eval(varMap)
		if err != nil {
			return nil, fmt.Errorf("map: eval for element %v: %w", x, err)
		}
		out = append(out, v)
	}
	return map[string]any{outVar: out}, nil
}
