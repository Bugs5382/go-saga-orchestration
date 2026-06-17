package e2e

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// e2ePublisher calls coord.Advance synchronously in a goroutine so the
// flow walks through the wait without blocking the test.
type e2ePublisher struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *e2ePublisher) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestWaitDurationVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	raw, _ := os.ReadFile("../fixtures/wf_wait_duration.json")
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	start := time.Unix(1000, 0).UTC()
	fc := clock.NewFakeClock(start)
	pub := &e2ePublisher{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	timer := &engine.Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}
	timerCtx, cancelTimer := context.WithCancel(ctx)
	defer cancelTimer()
	go func() { _ = timer.Run(timerCtx) }()

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}
	// At this point the saga should be paused with wakeup at start+100ms.
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected paused, got %s", got.State)
		}
	}

	// Give the timer goroutine time to call clock.After and block in select â€”
	// FakeClock only fires waiters that were registered before Advance is called.
	// This mirrors the pattern in engine/timer_test.go.
	time.Sleep(20 * time.Millisecond)

	// Advance the clock past the wakeup; timer dispatcher fires; publisher kicks coord.Advance.
	fc.Advance(150 * time.Millisecond)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; final state=%s, publisher.calls=%d", got.State, pub.calls.Load())
}
