package verbs

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
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestWaitUntil_FutureDeadline_PausesWithThatTime(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	start := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(start)
	deadline := start.Add(10 * time.Second)

	v := WaitUntilVerb{S: s, Clock: fc}
	_, err := v.Execute(ctx, r, domain.Step{Inputs: map[string]any{"timestamp": deadline.Format(time.RFC3339)}})
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.WakeupAt == nil || !got.WakeupAt.Equal(deadline) {
		t.Errorf("wakeup_at = %v, want %v", got.WakeupAt, deadline)
	}
}

func TestWaitUntil_PastDeadline_PausesAtNow(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	start := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(start)
	past := start.Add(-1 * time.Hour)

	v := WaitUntilVerb{S: s, Clock: fc}
	_, _ = v.Execute(ctx, r, domain.Step{Inputs: map[string]any{"timestamp": past.Format(time.RFC3339)}})
	got, _ := s.GetRun(ctx, r.ID)
	if got.WakeupAt == nil || !got.WakeupAt.Equal(start) {
		t.Errorf("wakeup_at = %v, want %v (now)", got.WakeupAt, start)
	}
}

func TestWaitUntil_MissingTimestamp_Errors(t *testing.T) {
	v := WaitUntilVerb{S: memory.New(), Clock: clock.SystemClock{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{}})
	if err == nil {
		t.Errorf("expected error for missing timestamp")
	}
}

func TestWaitUntil_BadTimestamp_Errors(t *testing.T) {
	v := WaitUntilVerb{S: memory.New(), Clock: clock.SystemClock{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{"timestamp": "not-rfc3339"}})
	if err == nil {
		t.Errorf("expected parse error")
	}
}
