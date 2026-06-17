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

// SwitchVerb evaluates a CEL expression over run.Variables to a string branch
// key and returns {"branch": key}; the engine routes that to
// step.Branches[key].Next. Inputs:
//   - "expr" (required, CEL string) must evaluate to a string.
type SwitchVerb struct{}

// Execute compiles and evaluates the CEL expr against run.Variables, asserts
// the result is a string, and returns {"branch": <result>}.
func (SwitchVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	expr, _ := step.Inputs["expr"].(string)
	if expr == "" {
		return nil, fmt.Errorf("switch: expr required")
	}
	prg, err := cel.CompiledProgram(keysOf(run.Variables), expr)
	if err != nil {
		return nil, fmt.Errorf("switch: compile: %w", err)
	}
	val, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("switch: CEL eval: %w", err)
	}
	key, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("switch: expr must evaluate to string, got %T", val)
	}
	return map[string]any{"branch": key}, nil
}
