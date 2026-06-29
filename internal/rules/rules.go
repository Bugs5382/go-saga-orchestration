// Package rules evaluates rule definitions. It supports the
// decision_table rule type (hit policy = first). Pure function of
// (RuleDefinition, inputs map) → (output map, audit trail, error).
package rules

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

// EvaluatedRow is one entry in the audit trail returned by Evaluate.
type EvaluatedRow struct {
	Index   int    `json:"index"`
	When    string `json:"when"`
	Matched bool   `json:"matched"`
}

// Evaluate runs def against inputs and returns the rule's output map
// plus the audit trail of which rows were evaluated and which matched.
func Evaluate(_ context.Context, def domain.RuleDefinition, inputs map[string]any) (map[string]any, []EvaluatedRow, error) {
	if def.RuleType != domain.RuleTypeDecisionTable {
		return nil, nil, fmt.Errorf("rules: unsupported rule type: %s", def.RuleType)
	}
	if def.Spec.HitPolicy != domain.HitPolicyFirst {
		return nil, nil, fmt.Errorf("rules: unsupported hit policy: %s", def.Spec.HitPolicy)
	}

	varNames := make([]string, 0, len(inputs))
	for k := range inputs {
		varNames = append(varNames, k)
	}
	audit := make([]EvaluatedRow, 0, len(def.Spec.Rows))
	for i, row := range def.Spec.Rows {
		prg, err := cel.CompiledProgram(varNames, row.When)
		if err != nil {
			return nil, audit, fmt.Errorf("rules: row %d compile: %w", i, err)
		}
		v, err := prg.Eval(inputs)
		if err != nil {
			return nil, audit, fmt.Errorf("rules: row %d eval: %w", i, err)
		}
		matched, _ := v.(bool)
		audit = append(audit, EvaluatedRow{Index: i, When: row.When, Matched: matched})
		if matched {
			return row.Then, audit, nil
		}
	}

	if def.Spec.DefaultOutput != nil {
		return def.Spec.DefaultOutput, audit, nil
	}
	return nil, audit, fmt.Errorf("rules: no_decision_row_matched")
}
