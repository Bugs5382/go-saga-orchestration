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

// MergeVerb deep-merges a CEL-evaluated map into a target variable.
// Inputs:
//   - "from" (required, string): CEL expression that must evaluate to a map.
//   - "into" (required, string): target variable name (dotted ok). The
//     target's existing value is merged with the from value
//     (last-write-wins per key, recursively for nested maps).
type MergeVerb struct{}

// Execute evaluates the from expression to a map and returns dotted-key
// entries rooted at into, deep-merging nested maps.
func (MergeVerb) Execute(_ context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	expr, _ := step.Inputs["from"].(string)
	into, _ := step.Inputs["into"].(string)
	if expr == "" || into == "" {
		return nil, fmt.Errorf("merge: from and into required")
	}
	env, err := cel.NewEnv(keysOf(run.Variables)...)
	if err != nil {
		return nil, fmt.Errorf("merge: env: %w", err)
	}
	prg, err := env.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("merge: compile: %w", err)
	}
	val, err := prg.Eval(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("merge: eval: %w", err)
	}
	fromMap, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("merge: from did not evaluate to a string-keyed map, got %T", val)
	}
	out := map[string]any{}
	deepFlatten(into, fromMap, out)
	return out, nil
}

// deepFlatten walks a nested map and produces dotted-key entries
// rooted at prefix. {"meta":{"src":"demo"}} with prefix "ctx" becomes
// {"ctx.meta.src":"demo"}.
func deepFlatten(prefix string, in map[string]any, out map[string]any) {
	for k, v := range in {
		fullKey := prefix + "." + k
		if nested, ok := v.(map[string]any); ok {
			deepFlatten(fullKey, nested, out)
			continue
		}
		out[fullKey] = v
	}
}
