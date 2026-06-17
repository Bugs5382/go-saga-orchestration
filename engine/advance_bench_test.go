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

// Benchmarks for the coordinator hot path: Coordinator.Advance (issue #19).
// Each scenario seeds b.N runs up front and times only the Advance calls.

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// advanceScenarios are the workflow shapes exercised by both the serial and
// parallel Advance benchmarks.
func advanceScenarios() []struct {
	name string
	def  domain.WorkflowDefinition
} {
	return []struct {
		name string
		def  domain.WorkflowDefinition
	}{
		{"trivial", defTrivial()},
		{"single_verb", defSingleVerb()},
		{"multi_step_10", defMultiStep(10)},
		{"multi_step_100", defMultiStep(100)},
	}
}

// BenchmarkAdvance measures the serial cost of driving one saga run to a
// terminal state via Coordinator.Advance. A single call advances every
// synchronous step, so multi_step_N reports the amortised per-call cost of an
// N-step linear saga.
func BenchmarkAdvance(b *testing.B) {
	ctx := context.Background()
	for _, sc := range advanceScenarios() {
		b.Run(sc.name, func(b *testing.B) {
			s := memory.New()
			ids := seedRuns(b, s, sc.def, b.N)
			c := benchCoordinator(s)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := c.Advance(ctx, ids[i]); err != nil {
					b.Fatalf("advance: %v", err)
				}
			}
		})
	}
}

// BenchmarkAdvanceParallel measures throughput under concurrency: many
// goroutines advance distinct runs against one Coordinator. Because the
// in-memory store is RWMutex-guarded, this also surfaces store-lock
// contention, which is expected and is a store concern rather than an
// Advance/CEL one.
func BenchmarkAdvanceParallel(b *testing.B) {
	ctx := context.Background()
	for _, sc := range advanceScenarios() {
		b.Run(sc.name, func(b *testing.B) {
			s := memory.New()
			c := benchCoordinator(s)
			ids := seedRuns(b, s, sc.def, b.N)
			var idx int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := atomic.AddInt64(&idx, 1) - 1
					if err := c.Advance(ctx, ids[i]); err != nil {
						b.Fatalf("advance: %v", err)
					}
				}
			})
		})
	}
}
