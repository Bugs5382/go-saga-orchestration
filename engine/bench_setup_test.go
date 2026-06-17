package engine

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

// Shared setup helpers for the coordinator hot-path benchmarks (issue #19).
// These mirror the unit-test wiring in advance_test.go: an in-memory store,
// a SystemClock, and a Coordinator with all built-in verbs registered. The
// benchmarks isolate engine CPU + allocation cost; service-mode latency is
// dominated by store/queue I/O and is out of scope here.

import (
	"context"
	"fmt"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// benchCoordinator builds a Coordinator over the given store with no
// publisher, a SystemClock, and stub secrets/licensing — enough to drive the
// synchronous (non-spawning, non-pausing) verbs the hot-path benchmarks use.
func benchCoordinator(s *memory.Store) *Coordinator {
	return NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
}

// seedRuns publishes def once and creates n pending runs against it,
// returning their string IDs. Each Advance call consumes one run (drives it
// to a terminal state), so benchmarks pre-seed b.N runs before resetting the
// timer to keep run creation out of the measured loop.
func seedRuns(tb testing.TB, s *memory.Store, def domain.WorkflowDefinition, n int) []string {
	tb.Helper()
	ctx := context.Background()
	defID, err := s.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		tb.Fatalf("upsert def: %v", err)
	}
	ids := make([]string, n)
	for i := range ids {
		run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
		if err := s.CreateRun(ctx, run); err != nil {
			tb.Fatalf("create run: %v", err)
		}
		ids[i] = run.ID.String()
	}
	return ids
}

// defTrivial is a single end step — the lower bound for Advance: start,
// terminate, emit run.succeeded.
func defTrivial() domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID: "bench_trivial", Version: 1, Name: "Trivial",
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
}

// defSingleVerb is one literal set_var followed by end — one verb dispatch
// plus the surrounding event/state writes.
func defSingleVerb() domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID: "bench_single", Version: 1, Name: "SingleVerb",
		Start: "s0",
		Steps: []domain.Step{
			{ID: "s0", Type: domain.StepTypeSetVar, Next: "end", Inputs: map[string]any{"out_var": "x", "value": 1}},
			{ID: "end", Type: domain.StepTypeEnd},
		},
		Published: true,
	}
}

// defMultiStep chains n literal set_var steps into end. A single Advance call
// drives all n steps (the loop re-reads the run between steps), so this
// measures the per-step overhead of the hot loop at scale.
func defMultiStep(n int) domain.WorkflowDefinition {
	steps := make([]domain.Step, 0, n+1)
	for i := 0; i < n; i++ {
		next := "end"
		if i+1 < n {
			next = fmt.Sprintf("s%d", i+1)
		}
		steps = append(steps, domain.Step{
			ID:     fmt.Sprintf("s%d", i),
			Type:   domain.StepTypeSetVar,
			Next:   next,
			Inputs: map[string]any{"out_var": fmt.Sprintf("v%d", i), "value": i},
		})
	}
	steps = append(steps, domain.Step{ID: "end", Type: domain.StepTypeEnd})
	return domain.WorkflowDefinition{
		ID: fmt.Sprintf("bench_multi_%d", n), Version: 1, Name: "MultiStep",
		Start:     "s0",
		Steps:     steps,
		Published: true,
	}
}
