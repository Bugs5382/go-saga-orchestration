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

// SetVarVerb writes a value to a variable. Inputs:
//   - "out_var" (required, string): the destination variable name (dotted
//     keys allowed for nested scope writes).
//   - "value" (optional): a literal — written through unchanged.
//   - "expr" (optional): a CEL expression evaluated against the current
//     run.Variables; the result is written.
//
// Exactly one of "value" or "expr" must be set. If both are set, "expr"
// wins (so workflow authors can swap a literal for an expression
// without renaming the key).
type SetVarVerb struct{}

// Execute writes either the evaluated expr or the literal value to out_var,
// with expr taking precedence when both are present.
func (SetVarVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	outVar, _ := step.Inputs["out_var"].(string)
	if outVar == "" {
		return nil, fmt.Errorf("set_var: out_var required")
	}

	if exprAny, ok := step.Inputs["expr"]; ok {
		expr, _ := exprAny.(string)
		if expr == "" {
			return nil, fmt.Errorf("set_var: expr must be a non-empty string")
		}
		prg, err := cel.CompiledProgram(keysOf(run.Variables), expr)
		if err != nil {
			return nil, fmt.Errorf("set_var: compile: %w", err)
		}
		val, err := prg.Eval(run.Variables)
		if err != nil {
			return nil, fmt.Errorf("set_var: eval: %w", err)
		}
		return map[string]any{outVar: val}, nil
	}

	if v, ok := step.Inputs["value"]; ok {
		return map[string]any{outVar: v}, nil
	}
	return nil, fmt.Errorf("set_var: one of value or expr is required")
}

// keysOf returns the keys of a map[string]any in arbitrary order.
// Shared helper used by several verbs that build a CEL env from
// current variables.
func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
