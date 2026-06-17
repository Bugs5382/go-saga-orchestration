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
	"strings"
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

type waitUntilPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *waitUntilPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestWaitUntilVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	start := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(start)

	raw, _ := os.ReadFile("../fixtures/wf_wait_until.json")
	deadline := start.Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	patched := strings.ReplaceAll(string(raw), "__DEADLINE__", deadline)

	var def domain.WorkflowDefinition
	if err := json.Unmarshal([]byte(patched), &def); err != nil {
		t.Fatalf("parse: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &waitUntilPub{}
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
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected paused, got %s", got.State)
		}
	}
	time.Sleep(20 * time.Millisecond) // allow timer goroutine to register its After waiter
	fc.Advance(200 * time.Millisecond)

	deadlineTimeout := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadlineTimeout) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s publishCalls=%d", got.State, pub.calls.Load())
}
