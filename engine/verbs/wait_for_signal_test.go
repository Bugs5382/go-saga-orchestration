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

func TestWaitForSignal_PausesSagaAwaitingSignal(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())
	v := WaitForSignalVerb{S: s, Clock: fc}

	_, err := v.Execute(ctx, r, domain.Step{Inputs: map[string]any{"name": "go"}})
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.AwaitedSignal == nil || *got.AwaitedSignal != "go" {
		t.Errorf("awaited_signal = %v, want \"go\"", got.AwaitedSignal)
	}
	if got.WakeupAt != nil {
		t.Errorf("wakeup_at should be nil when no timeout given, got %v", got.WakeupAt)
	}
}

func TestWaitForSignal_WithTimeout_SetsWakeupAt(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)

	start := time.Unix(1000, 0).UTC()
	fc := clock.NewFakeClock(start)
	v := WaitForSignalVerb{S: s, Clock: fc}

	_, err := v.Execute(ctx, r, domain.Step{Inputs: map[string]any{"name": "go", "timeout_s": float64(30)}})
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("got err %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.WakeupAt == nil || !got.WakeupAt.Equal(start.Add(30*time.Second)) {
		t.Errorf("wakeup_at = %v, want %v", got.WakeupAt, start.Add(30*time.Second))
	}
}

func TestWaitForSignal_MissingName_Errors(t *testing.T) {
	v := WaitForSignalVerb{S: memory.New(), Clock: clock.SystemClock{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{Inputs: map[string]any{}})
	if err == nil {
		t.Errorf("expected error for missing name")
	}
}
