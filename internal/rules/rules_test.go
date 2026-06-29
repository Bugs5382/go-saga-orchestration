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
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func triageRule(t *testing.T) domain.RuleDefinition {
	t.Helper()
	return domain.NewRuleDefinition(
		"triage", 1, "Incident triage",
		domain.RuleTypeDecisionTable,
		domain.RuleSpec{
			HitPolicy: domain.HitPolicyFirst,
			Rows: []domain.DecisionTableRow{
				{When: "priority == 'p1'", Then: map[string]any{"branch": "high"}},
				{When: "priority == 'p2'", Then: map[string]any{"branch": "high"}},
				{When: "priority == 'p3'", Then: map[string]any{"branch": "low"}},
			},
			DefaultOutput: map[string]any{"branch": "low"},
		},
		"test",
	)
}

func TestEvaluate_HitPolicyFirst_Matches(t *testing.T) {
	out, audit, err := Evaluate(context.Background(), triageRule(t), map[string]any{"priority": "p2"})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if out["branch"] != "high" {
		t.Errorf("out branch = %v, want high", out["branch"])
	}
	if len(audit) != 2 {
		t.Errorf("audit len = %d, want 2 (rows 0+1 evaluated, 1 matched)", len(audit))
	}
	if !audit[1].Matched {
		t.Errorf("row 1 should be matched")
	}
}

func TestEvaluate_NoMatch_UsesDefaultOutput(t *testing.T) {
	out, audit, err := Evaluate(context.Background(), triageRule(t), map[string]any{"priority": "p4"})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if out["branch"] != "low" {
		t.Errorf("out branch = %v, want low (default)", out["branch"])
	}
	if len(audit) != 3 {
		t.Errorf("audit should record all 3 rows when none matched, got %d", len(audit))
	}
}

func TestEvaluate_NoMatch_NoDefault_Errors(t *testing.T) {
	def := triageRule(t)
	def.Spec.DefaultOutput = nil
	_, _, err := Evaluate(context.Background(), def, map[string]any{"priority": "p4"})
	if err == nil {
		t.Fatalf("expected no_decision_row_matched error, got nil")
	}
}

func TestEvaluate_UnsupportedRuleType_Errors(t *testing.T) {
	def := triageRule(t)
	def.RuleType = "script"
	_, _, err := Evaluate(context.Background(), def, map[string]any{"priority": "p1"})
	if err == nil {
		t.Errorf("expected unsupported rule type error, got nil")
	}
}

func TestEvaluate_BadExpression_ReturnsRowIndex(t *testing.T) {
	def := triageRule(t)
	def.Spec.Rows[1].When = "this is not valid cel"
	_, audit, err := Evaluate(context.Background(), def, map[string]any{"priority": "p2"})
	if err == nil {
		t.Fatalf("expected compile error, got nil")
	}
	if len(audit) < 1 || audit[0].Matched {
		t.Errorf("audit unexpected: %+v", audit)
	}
}
