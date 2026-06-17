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

// AssertVerb evaluates a CEL expression; on false returns an error.
// Inputs:
//   - "expr" (required, string)
//   - "code" (optional, string; default "assertion_failed")
type AssertVerb struct{}

// Execute compiles and evaluates the CEL expr against run.Variables and
// returns an error tagged with code when the result is not true.
func (AssertVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	expr, _ := step.Inputs["expr"].(string)
	if expr == "" {
		return nil, fmt.Errorf("assert: expr required")
	}
	code, _ := step.Inputs["code"].(string)
	if code == "" {
		code = "assertion_failed"
	}
	prg, err := cel.CompiledProgram(keysOf(run.Variables), expr)
	if err != nil {
		return nil, fmt.Errorf("assert: compile: %w", err)
	}
	val, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("assert: eval: %w", err)
	}
	if b, _ := val.(bool); !b {
		return nil, fmt.Errorf("%s: %q is false", code, expr)
	}
	return map[string]any{}, nil
}
