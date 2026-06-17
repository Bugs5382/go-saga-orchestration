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

// TransformVerb evaluates a CEL expression against current Variables and
// writes the result to out_var. Inputs:
//   - "expr" (required, string): CEL expression.
//   - "out_var" (required, string): variable name to write to (dotted ok).
type TransformVerb struct{}

// Execute evaluates the CEL expr against run.Variables and writes the result
// to out_var.
func (TransformVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	expr, _ := step.Inputs["expr"].(string)
	if expr == "" {
		return nil, fmt.Errorf("transform: expr required")
	}
	outVar, _ := step.Inputs["out_var"].(string)
	if outVar == "" {
		return nil, fmt.Errorf("transform: out_var required")
	}
	env, err := cel.NewEnv(keysOf(run.Variables)...)
	if err != nil {
		return nil, fmt.Errorf("transform: env: %w", err)
	}
	prg, err := env.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("transform: compile: %w", err)
	}
	val, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("transform: eval: %w", err)
	}
	return map[string]any{outVar: val}, nil
}
