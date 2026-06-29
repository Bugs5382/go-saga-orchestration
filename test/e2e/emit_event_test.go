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
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// eventSubEmitter adapts engine.EventSubscriber to the verbs.EventEmitter
// interface so it can be used as the in-process emitter in tests.
type eventSubEmitter struct {
	sub *engine.EventSubscriber
}

func (e *eventSubEmitter) EmitEvent(ctx context.Context, topic string, headers map[string]string, _ map[string]any) error {
	return e.sub.Deliver(ctx, engine.EventDelivery{
		Topic:   topic,
		Headers: headers,
		Body:    []byte("{}"),
	})
}

// TestEmitEvent_WakesAwaitingRun verifies the end-to-end scenario:
//   - Saga B starts and pauses on wait_for_event (topic="test.emit.event", headers={env: test})
//   - Saga A starts and runs emit_event with matching topic + headers
//   - The in-process emitter wakes B (via EventSubscriber.Deliver)
//   - B advances to succeeded
func TestEmitEvent_WakesAwaitingRun(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())

	// Load waiter workflow.
	rawWaiter, _ := os.ReadFile("../fixtures/wf_emit_event_waiter.json")
	var waiterDef domain.WorkflowDefinition
	if err := json.Unmarshal(rawWaiter, &waiterDef); err != nil {
		t.Fatalf("parse waiter: %v", err)
	}
	waiterDefID, _ := s.UpsertWorkflowDefinition(ctx, waiterDef)

	// Load emitter workflow.
	rawEmitter, _ := os.ReadFile("../fixtures/wf_emit_event_emitter.json")
	var emitterDef domain.WorkflowDefinition
	if err := json.Unmarshal(rawEmitter, &emitterDef); err != nil {
		t.Fatalf("parse emitter: %v", err)
	}
	emitterDefID, _ := s.UpsertWorkflowDefinition(ctx, emitterDef)

	// Use the lgPub loopback pattern (defined in license_gate_test.go in this package).
	pub := &lgPub{}

	// Build the in-process emitter: EventSubscriber delegates to the same store + pub.
	sub := &engine.EventSubscriber{S: s, Publisher: pub}
	emitter := &eventSubEmitter{sub: sub}

	coord := engine.NewCoordinator(s, pub, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, emitter)
	pub.coord = coord

	// Start saga B (waiter) — should pause on wait_for_event.
	runB := domain.NewSagaRun(waiterDef.ID, waiterDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, runB)
	if err := coord.Advance(ctx, runB.ID.String()); err != nil {
		t.Fatalf("advance B: %v", err)
	}
	{
		got, _ := s.GetRun(ctx, runB.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected B paused, got %s", got.State)
		}
		if got.AwaitedEventTopic == nil || *got.AwaitedEventTopic != "test.emit.event" {
			t.Fatalf("B awaited topic wrong: %v", got.AwaitedEventTopic)
		}
	}

	// Start saga A (emitter) — emit_event should wake B.
	runA := domain.NewSagaRun(emitterDef.ID, emitterDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, runA)
	if err := coord.Advance(ctx, runA.ID.String()); err != nil {
		t.Fatalf("advance A: %v", err)
	}
	{
		got, _ := s.GetRun(ctx, runA.ID)
		if got.State != domain.RunStateSucceeded {
			t.Fatalf("expected A succeeded, got %s", got.State)
		}
	}

	// B should reach succeeded shortly (lgPub advances B in a background goroutine).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, runB.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, runB.ID)
	t.Fatalf("B did not reach succeeded; state=%s", got.State)
}
