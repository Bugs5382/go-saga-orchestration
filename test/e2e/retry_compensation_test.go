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
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// flakyThenOK fails its first N Execute calls, then succeeds.
type flakyThenOK struct {
	mu   sync.Mutex
	fail int
	n    int
}

func (f *flakyThenOK) Execute(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	if f.n <= f.fail {
		return nil, errors.New("flaky")
	}
	return map[string]any{}, nil
}

// failOnStep errors only for the named step.
type failOnStep struct{ id string }

func (h failOnStep) Execute(_ context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
	if step.ID == h.id {
		return nil, errors.New("boom")
	}
	return map[string]any{}, nil
}

// compCapture records compensation dispatch routing keys in order.
type compCapture struct {
	mu   sync.Mutex
	keys []string
}

func (c *compCapture) PublishActionDispatch(_ context.Context, routingKey string, _ []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = append(c.keys, routingKey)
	return nil
}

// TestRetryAndCompensationEndToEnd drives a workflow whose first step retries
// to success and whose later step fails permanently, triggering reverse-order
// compensation of the completed compensable steps.
func TestRetryAndCompensationEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_retry_comp_e2e", Version: 1, Name: "RetryCompE2E",
		Published: true,
		Start:     "reserve",
		Steps: []domain.Step{
			{ID: "reserve", Type: domain.StepTypeNoop, Next: "charge",
				Retry:        &domain.RetryPolicy{MaxAttempts: 4, InitialBackoffMS: 5, MaxBackoffMS: 50, Multiplier: 2.0},
				Compensation: &domain.Compensation{Action: "inventory.release"}},
			{ID: "charge", Type: domain.StepTypeNoop, Next: "ship",
				Compensation: &domain.Compensation{Action: "billing.refund"}},
			{ID: "ship", Type: domain.StepType("fail"), Next: "end"}, // always fails
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	// Drive the fake clock so the retry backoff waits unblock deterministically.
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				fc.Advance(time.Hour)
			}
		}
	}()
	defer close(done)

	cap := &compCapture{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, cap, nil)
	// "reserve" + "charge" succeed (charge is a plain noop); reserve fails twice first.
	coord.RegisterVerb(domain.StepTypeNoop, &flakyThenOK{fail: 2}, "common")
	// A distinct step type that always fails, registered as the "fail" verb.
	coord.RegisterVerb("fail", failOnStep{id: "ship"}, "common")

	if err := coord.Advance(ctx, run.ID.String()); err == nil {
		t.Fatal("expected run to fail at ship")
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", got.State)
	}

	// Compensation must run completed compensable steps in reverse: charge then reserve.
	cap.mu.Lock()
	keys := append([]string{}, cap.keys...)
	cap.mu.Unlock()
	want := []string{"billing.refund", "inventory.release"}
	if len(keys) != len(want) {
		t.Fatalf("compensations = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("comp[%d] = %q, want %q (full %v)", i, keys[i], want[i], keys)
		}
	}

	// A compensation.started event must be recorded.
	events, _ := s.ListEventsByRun(ctx, run.ID)
	found := false
	for _, ev := range events {
		if ev.EventType == domain.EventCompensationStarted {
			found = true
		}
	}
	if !found {
		t.Error("no compensation.started event recorded")
	}
}

var _ verbs.Handler = (*flakyThenOK)(nil)
var _ verbs.Handler = failOnStep{}
