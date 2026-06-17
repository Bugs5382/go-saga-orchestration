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

type eventPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *eventPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestWaitForEventVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())

	raw, _ := os.ReadFile("../fixtures/wf_wait_for_event.json")
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &eventPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

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
		if got.AwaitedEventTopic == nil || *got.AwaitedEventTopic != "test.signal" {
			t.Fatalf("awaited topic wrong: %v", got.AwaitedEventTopic)
		}
	}

	sub := &engine.EventSubscriber{S: s, Publisher: pub}
	if err := sub.Deliver(ctx, engine.EventDelivery{
		Topic:   "test.signal",
		Headers: map[string]string{"x": "1", "extra": "ignored"},
		Body:    []byte(`{}`),
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("did not reach succeeded; state=%s publishCalls=%d", got.State, pub.calls.Load())
}
