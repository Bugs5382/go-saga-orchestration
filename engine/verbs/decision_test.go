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
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestDecision_PicksBranchFromRule(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	_, _ = s.UpsertRuleDefinition(ctx, domain.NewRuleDefinition(
		"triage", 1, "Triage", domain.RuleTypeDecisionTable,
		domain.RuleSpec{
			HitPolicy: domain.HitPolicyFirst,
			Rows: []domain.DecisionTableRow{
				{When: "priority == 'p1'", Then: map[string]any{"branch": "high"}},
				{When: "priority == 'p3'", Then: map[string]any{"branch": "low"}},
			},
			DefaultOutput: map[string]any{"branch": "low"},
		},
		"test",
	))
	run := domain.NewSagaRun("wf", uuid.New(), nil, nil)
	run.Variables = map[string]any{"priority": "p1"}
	_ = s.CreateRun(ctx, run)
	out, err := DecisionVerb{S: s}.Execute(ctx, run,
		domain.Step{ID: "d", Inputs: map[string]any{"rule_id": "triage"}})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["branch"] != "high" {
		t.Errorf("branch = %v, want high", out["branch"])
	}
	events, _ := s.ListEventsByRun(ctx, run.ID)
	var found bool
	for _, e := range events {
		if e.EventType == domain.EventRuleEvaluated {
			found = true
		}
	}
	if !found {
		t.Errorf("EventRuleEvaluated not appended")
	}
}

func TestDecision_RuleNotFound(t *testing.T) {
	s := memory.New()
	_, err := DecisionVerb{S: s}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{}},
		domain.Step{ID: "d", Inputs: map[string]any{"rule_id": "nonexistent"}})
	if err == nil {
		t.Errorf("expected rule_not_found error")
	}
}
