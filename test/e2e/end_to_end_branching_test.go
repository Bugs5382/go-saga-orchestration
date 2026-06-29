package e2e

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
	"encoding/json"
	"os"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestDemoBranching_HighPath(t *testing.T) {
	got := runDemo(t, []int64{1, 2, 3, 4}) // filter keeps [3,4] -> size 2 -> high
	if got.Variables["branch_taken"] != "high" {
		t.Errorf("branch_taken = %v, want high", got.Variables["branch_taken"])
	}
}

func TestDemoBranching_LowPath(t *testing.T) {
	got := runDemo(t, []int64{1, 2, 3}) // filter keeps [3] -> size 1 -> default low
	if got.Variables["branch_taken"] != "low" {
		t.Errorf("branch_taken = %v, want low", got.Variables["branch_taken"])
	}
}

func runDemo(t *testing.T, list []int64) domain.SagaRun {
	t.Helper()
	ctx := context.Background()
	s := memory.New()
	_, _ = s.UpsertRuleDefinition(ctx, loadRule(t, "../fixtures/rules/demo_branch.json"))

	raw, _ := os.ReadFile("../fixtures/wf_demo_branching.json")
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Patch the inline list to test both branches without duplicating the fixture.
	for i, st := range def.Steps {
		if st.ID == "s4" {
			elements := make([]any, 0, len(list))
			for _, v := range list {
				elements = append(elements, v)
			}
			litList := stringifyJSON(t, elements)
			def.Steps[i].Inputs["list"] = litList
		}
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("state = %s, want succeeded", got.State)
	}

	events, _ := s.ListEventsByRun(ctx, run.ID)
	hasLog := false
	hasRule := false
	for _, e := range events {
		switch e.EventType {
		case domain.EventLog:
			hasLog = true
		case domain.EventRuleEvaluated:
			hasRule = true
		}
	}
	if !hasLog || !hasRule {
		t.Errorf("expected EventLog AND EventRuleEvaluated; events = %+v", events)
	}
	return got
}

func stringifyJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
