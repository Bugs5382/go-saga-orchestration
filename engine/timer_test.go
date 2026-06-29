package engine

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type recordingPublisher struct{ runs []string }

func (r *recordingPublisher) PublishSagaAdvance(_ context.Context, runID string) error {
	r.runs = append(r.runs, runID)
	return nil
}

func TestTimer_PublishesWhenWakeupDue(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	start := time.Unix(1000, 0).UTC()
	fc := clock.NewFakeClock(start)
	wakeup := start.Add(5 * time.Second)
	_ = s.SetPausedWithWakeup(ctx, r.ID, wakeup)

	pub := &recordingPublisher{}
	timer := &Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- timer.Run(ctx) }()

	// Allow one tick (no advance) — fc.After receives nothing until Advance.
	// Brief sleep ensures the goroutine has called After and is blocking in select.
	time.Sleep(10 * time.Millisecond)
	// Advance the clock past the wakeup; that fires the timer's After channel.
	fc.Advance(5 * time.Second)
	// Wait for the publish to register (allow a brief moment for the goroutine to loop).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(pub.runs) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if len(pub.runs) < 1 {
		t.Fatalf("expected at least 1 publish, got %d", len(pub.runs))
	}
	if pub.runs[0] != r.ID.String() {
		t.Errorf("published runID = %s, want %s", pub.runs[0], r.ID)
	}
}

func TestTimer_NoPublishWhenNoDueWakeups(t *testing.T) {
	s := memory.New()
	pub := &recordingPublisher{}
	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	timer := &Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- timer.Run(ctx) }()
	// Advance the clock; no paused runs exist, so no publishes.
	fc.Advance(10 * time.Second)
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if len(pub.runs) != 0 {
		t.Errorf("expected 0 publishes, got %d", len(pub.runs))
	}
}
