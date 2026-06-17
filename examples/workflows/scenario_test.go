package examples_test

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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/saga"
)

// TestScenarioParallelSetVars_RunsInMemory runs the parallel fan-out scenario on
// an in-memory engine and confirms the four set_var branches aggregate into
// _parallel.fanout.branches so the downstream set_var can read them. This proves
// the data hand-off (one verb per step; dependencies flow through Variables).
func TestScenarioParallelSetVars_RunsInMemory(t *testing.T) {
	raw, err := os.ReadFile("scenario_parallel_setvars.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	ctx := context.Background()
	sc := saga.InMemory()
	if err := sc.Register(def); err != nil {
		t.Fatalf("register: %v", err)
	}
	runID, err := sc.Start(ctx, def.ID, map[string]any{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Parallel children advance via background goroutines; poll until terminal.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, _ := sc.Get(ctx, runID)
		switch run.State {
		case domain.RunStateSucceeded:
			if toInt(run.Variables["branch_count"]) != 4 {
				t.Fatalf("branch_count = %v (%T), want 4; vars=%v",
					run.Variables["branch_count"], run.Variables["branch_count"], run.Variables)
			}
			return
		case domain.RunStateFailed, domain.RunStateCancelled:
			t.Fatalf("run ended %s; vars=%v", run.State, run.Variables)
		}
		time.Sleep(10 * time.Millisecond)
	}
	run, _ := sc.Get(ctx, runID)
	t.Fatalf("did not reach succeeded in time; state=%s vars=%v", run.State, run.Variables)
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return -1
	}
}
