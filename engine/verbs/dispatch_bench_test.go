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

// Benchmarks for verb dispatch (issue #19): the registry lookup and the
// Execute path of a representative set of verbs, in isolation from the
// coordinator loop. The CEL-heavy verbs (set_var/expr, transform, map,
// filter) are the prime allocation targets for the follow-up tuning PR.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// celVars is a small, fixed variable set used by the CEL-expression verbs so
// the env-construction cost is representative without being dominated by a
// huge name list.
func celVars() map[string]any {
	return map[string]any{"a": 1, "b": 2, "c": 3}
}

// listVars returns variables holding an n-element integer list under "xs",
// used by the map/filter per-element benchmarks.
func listVars(n int) map[string]any {
	xs := make([]any, n)
	for i := range xs {
		xs[i] = i
	}
	return map[string]any{"xs": xs}
}

// branchesOf builds n trivial single-step branches for the parallel verb.
func branchesOf(n int) []any {
	out := make([]any, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("e%d", i)
		out[i] = map[string]any{
			"start": id,
			"steps": []any{map[string]any{"id": id, "type": "end"}},
		}
	}
	return out
}

// runExecute times h.Execute, treating a nil error as success.
func runExecute(b *testing.B, h Handler, run domain.SagaRun, step domain.Step) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.Execute(ctx, run, step); err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

// BenchmarkRegistryLookup measures the cost of resolving a step type to its
// handler — the map lookup the coordinator does once per step.
func BenchmarkRegistryLookup(b *testing.B) {
	reg := Default(memory.New(), clock.SystemClock{}, secrets.NewMemory(map[string]string{}), nil, nil, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := reg[domain.StepTypeSetVar]; !ok {
			b.Fatal("set_var not registered")
		}
	}
}

// BenchmarkVerbExecute covers a representative spread of verbs in isolation.
func BenchmarkVerbExecute(b *testing.B) {
	b.Run("noop", func(b *testing.B) {
		runExecute(b, NoopVerb{}, domain.SagaRun{Variables: map[string]any{}},
			domain.Step{ID: "n", Type: domain.StepTypeNoop})
	})

	b.Run("set_var_literal", func(b *testing.B) {
		runExecute(b, SetVarVerb{}, domain.SagaRun{Variables: map[string]any{}},
			domain.Step{ID: "s", Type: domain.StepTypeSetVar,
				Inputs: map[string]any{"out_var": "x", "value": 1}})
	})

	b.Run("set_var_cel", func(b *testing.B) {
		runExecute(b, SetVarVerb{}, domain.SagaRun{Variables: celVars()},
			domain.Step{ID: "s", Type: domain.StepTypeSetVar,
				Inputs: map[string]any{"out_var": "x", "expr": "a + b + c"}})
	})

	b.Run("transform", func(b *testing.B) {
		runExecute(b, TransformVerb{}, domain.SagaRun{Variables: celVars()},
			domain.Step{ID: "t", Type: domain.StepTypeTransform,
				Inputs: map[string]any{"out_var": "x", "expr": "a + b + c"}})
	})

	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("map_%d", n), func(b *testing.B) {
			runExecute(b, MapVerb{}, domain.SagaRun{Variables: listVars(n)},
				domain.Step{ID: "m", Type: domain.StepTypeMap,
					Inputs: map[string]any{"list": "xs", "expr": "_ * 2", "out_var": "out"}})
		})
		b.Run(fmt.Sprintf("filter_%d", n), func(b *testing.B) {
			runExecute(b, FilterVerb{}, domain.SagaRun{Variables: listVars(n)},
				domain.Step{ID: "f", Type: domain.StepTypeFilter,
					Inputs: map[string]any{"list": "xs", "expr": "_ % 2 == 0", "out_var": "out"}})
		})
	}

	b.Run("decision", func(b *testing.B) {
		ctx := context.Background()
		s := memory.New()
		if _, err := s.UpsertRuleDefinition(ctx, domain.NewRuleDefinition(
			"triage", 1, "Triage", domain.RuleTypeDecisionTable,
			domain.RuleSpec{
				HitPolicy: domain.HitPolicyFirst,
				Rows: []domain.DecisionTableRow{
					{When: "priority == 'p1'", Then: map[string]any{"branch": "high"}},
					{When: "priority == 'p3'", Then: map[string]any{"branch": "low"}},
				},
				DefaultOutput: map[string]any{"branch": "low"},
			},
			"bench",
		)); err != nil {
			b.Fatalf("seed rule: %v", err)
		}
		run := domain.NewSagaRun("wf", uuid.New(), nil, nil)
		run.Variables = map[string]any{"priority": "p1"}
		if err := s.CreateRun(ctx, run); err != nil {
			b.Fatalf("create run: %v", err)
		}
		step := domain.Step{ID: "d", Type: domain.StepTypeDecision,
			Inputs: map[string]any{"rule_id": "triage"}}
		// DecisionVerb appends a rule.evaluated event per call, so the store
		// grows over the run; acceptable for the short benchmark window.
		runExecute(b, DecisionVerb{S: s}, run, step)
	})

	// Parallel spawns child runs and pauses the parent (returning
	// ErrSagaPaused), so it needs its own runner. The store accumulates child
	// runs across iterations; b.N stays modest because Execute is heavy.
	for _, n := range []int{2, 4} {
		b.Run(fmt.Sprintf("parallel_%d", n), func(b *testing.B) {
			ctx := context.Background()
			s := memory.New()
			parent := domain.NewSagaRun("wf-p", uuid.New(), nil, map[string]any{})
			if err := s.CreateRun(ctx, parent); err != nil {
				b.Fatalf("create parent: %v", err)
			}
			v := ParallelVerb{S: s} // nil publisher: no advance is published
			step := domain.Step{ID: "p", Type: domain.StepTypeParallel,
				Inputs: map[string]any{"branches": branchesOf(n)}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := v.Execute(ctx, parent, step); !errors.Is(err, ErrSagaPaused) {
					b.Fatalf("got %v, want ErrSagaPaused", err)
				}
			}
		})
	}
}

// BenchmarkVerbExecuteParallel measures CEL-bound verbs under concurrency.
// These verbs are pure (no store), so the contention is purely CPU/alloc on
// the repeated cel.NewEnv/Compile path — the dimension the follow-up cache
// targets.
func BenchmarkVerbExecuteParallel(b *testing.B) {
	cases := []struct {
		name string
		h    Handler
		run  domain.SagaRun
		step domain.Step
	}{
		{"set_var_cel", SetVarVerb{}, domain.SagaRun{Variables: celVars()},
			domain.Step{Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "x", "expr": "a + b + c"}}},
		{"transform", TransformVerb{}, domain.SagaRun{Variables: celVars()},
			domain.Step{Type: domain.StepTypeTransform, Inputs: map[string]any{"out_var": "x", "expr": "a + b + c"}}},
		{"map_100", MapVerb{}, domain.SagaRun{Variables: listVars(100)},
			domain.Step{Type: domain.StepTypeMap, Inputs: map[string]any{"list": "xs", "expr": "_ * 2", "out_var": "out"}}},
	}
	ctx := context.Background()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := tc.h.Execute(ctx, tc.run, tc.step); err != nil {
						b.Fatalf("execute: %v", err)
					}
				}
			})
		})
	}
}
