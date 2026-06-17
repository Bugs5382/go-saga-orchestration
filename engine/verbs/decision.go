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
	"github.com/Bugs5382/go-saga-orchestration/internal/rules"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// DecisionVerb evaluates a stored rule and returns its output map. The
// engine reads result["branch"] to pick step.Branches[...].Next.
// Inputs:
//   - "rule_id"    (required, string)
//   - "inputs_map" (optional, map[string]string): narrow the inputs
//     passed to the rule by mapping rule-input-name → variable-name. If
//     omitted, run.Variables is passed directly.
type DecisionVerb struct {
	S store.Store
}

// Execute loads the published rule, evaluates it against the (optionally
// remapped) inputs, records a rule-evaluated event, and returns the rule's
// output map.
func (v DecisionVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	ruleID, _ := step.Inputs["rule_id"].(string)
	if ruleID == "" {
		return nil, fmt.Errorf("decision: rule_id required")
	}
	def, err := v.S.GetPublishedRuleByID(ctx, ruleID, nil)
	if err != nil {
		return nil, fmt.Errorf("decision: rule_not_found: %w", err)
	}
	inputs := run.Variables
	if im, ok := step.Inputs["inputs_map"].(map[string]any); ok {
		inputs = map[string]any{}
		for ruleKey, runVarAny := range im {
			runVar, _ := runVarAny.(string)
			if runVar == "" {
				return nil, fmt.Errorf("decision: inputs_map[%s] must be a string variable name", ruleKey)
			}
			inputs[ruleKey] = run.Variables[runVar]
		}
	}
	out, audit, err := rules.Evaluate(ctx, def, inputs)
	if err != nil {
		return nil, fmt.Errorf("decision: %w", err)
	}
	evt := domain.NewEvent(run.ID, step.ID, 0, domain.EventRuleEvaluated, "workflow")
	evt.Metadata = map[string]any{"rule_id": ruleID, "audit": audit}
	_ = v.S.AppendEvent(ctx, evt)
	return out, nil
}
