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
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// flakyVerb fails its first failCount Execute calls, then succeeds. It counts
// the total number of Execute invocations so a test can assert retry attempts.
type flakyVerb struct {
	mu        sync.Mutex
	failCount int // number of leading calls that should fail
	calls     int // total Execute invocations
}

func (f *flakyVerb) Execute(_ context.Context, _ domain.SagaRun, _ domain.Step) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failCount {
		return nil, errors.New("transient failure")
	}
	return map[string]any{}, nil
}

func (f *flakyVerb) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// driveClock advances a FakeClock in the background so inline retry waits
// (c.clock.After) unblock deterministically without real time.Sleep. It stops
// when the returned cancel function is called.
func driveClock(fc *clock.FakeClock) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fc.Advance(time.Hour)
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// TestRetryExhaustionFails verifies that a step whose verb always errors is
// retried up to MaxAttempts and then the run fails (no try_catch frame).
func TestRetryExhaustionFails(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_retry_exhaust", Version: 1, Name: "RetryExhaust",
		Published: true,
		Start:     "risky",
		Steps: []domain.Step{
			{ID: "risky", Type: domain.StepTypeNoop, Next: "end", Retry: &domain.RetryPolicy{
				MaxAttempts: 3, InitialBackoffMS: 10, MaxBackoffMS: 100, Multiplier: 2.0,
			}},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_retry_exhaust", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	stop := driveClock(fc)
	defer stop()

	c := NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	fv := &flakyVerb{failCount: 99} // always fails
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: fv, LicenseGroup: "common"}

	err := c.Advance(ctx, run.ID.String())
	if err == nil {
		t.Fatal("expected Advance to return an error after retry exhaustion")
	}

	if got := fv.count(); got != 3 {
		t.Errorf("verb executed %d times, want 3 (MaxAttempts)", got)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("run state = %s, want failed", got.State)
	}
}

// TestRetryThenSuccess verifies a step that fails twice then succeeds runs to
// completion, and the verb was invoked exactly 3 times.
func TestRetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_retry_ok", Version: 1, Name: "RetryOK",
		Published: true,
		Start:     "risky",
		Steps: []domain.Step{
			{ID: "risky", Type: domain.StepTypeNoop, Next: "end", Retry: &domain.RetryPolicy{
				MaxAttempts: 5, InitialBackoffMS: 10, MaxBackoffMS: 100, Multiplier: 2.0,
			}},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_retry_ok", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	stop := driveClock(fc)
	defer stop()

	c := NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	fv := &flakyVerb{failCount: 2} // fail twice, then succeed
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: fv, LicenseGroup: "common"}

	if err := c.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	if got := fv.count(); got != 3 {
		t.Errorf("verb executed %d times, want 3 (2 failures + 1 success)", got)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("run state = %s, want succeeded", got.State)
	}
}

// compDispatchCapture records compensation action dispatches in order.
type compDispatchCapture struct {
	mu      sync.Mutex
	actions []string
}

func (c *compDispatchCapture) PublishActionDispatch(_ context.Context, routingKey string, _ []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actions = append(c.actions, routingKey)
	return nil
}

func (c *compDispatchCapture) ordered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.actions))
	copy(out, c.actions)
	return out
}

// TestCompensationReverseOrder verifies that when a run fails with completed
// compensable steps and no catching frame, their compensations dispatch in
// reverse order and the run settles to failed via compensating.
func TestCompensationReverseOrder(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_comp", Version: 1, Name: "Comp",
		Published: true,
		Start:     "s1",
		Steps: []domain.Step{
			{ID: "s1", Type: domain.StepTypeNoop, Next: "s2",
				Compensation: &domain.Compensation{Action: "svc.undo1"}},
			{ID: "s2", Type: domain.StepTypeNoop, Next: "s3",
				Compensation: &domain.Compensation{Action: "svc.undo2"}},
			{ID: "s3", Type: domain.StepTypeNoop, Next: "end"}, // fails; no compensation of its own
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_comp", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	cap := &compDispatchCapture{}
	c := NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, cap, nil)

	// s1, s2 succeed (noop). s3 errors.
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: stepRouter{failOn: "s3"}, LicenseGroup: "common"}

	err := c.Advance(ctx, run.ID.String())
	if err == nil {
		t.Fatal("expected Advance to fail at s3")
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("run state = %s, want failed", got.State)
	}

	// Compensations must dispatch in reverse: s2 (undo2) then s1 (undo1).
	want := []string{"svc.undo2", "svc.undo1"}
	gotActions := cap.ordered()
	if len(gotActions) != len(want) {
		t.Fatalf("compensation dispatches = %v, want %v", gotActions, want)
	}
	for i := range want {
		if gotActions[i] != want[i] {
			t.Errorf("compensation[%d] = %q, want %q (full: %v)", i, gotActions[i], want[i], gotActions)
		}
	}

	// A compensation.started event must be recorded.
	events, _ := s.ListEventsByRun(ctx, run.ID)
	foundStarted := false
	for _, ev := range events {
		if ev.EventType == domain.EventCompensationStarted {
			foundStarted = true
		}
	}
	if !foundStarted {
		t.Error("no compensation.started event recorded")
	}
}

// TestCompensationSkipsNil verifies that a completed step with nil Compensation
// is skipped (no dispatch) while compensable siblings still run.
func TestCompensationSkipsNil(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	def := domain.WorkflowDefinition{
		ID: "wf_comp_nil", Version: 1, Name: "CompNil",
		Published: true,
		Start:     "s1",
		Steps: []domain.Step{
			{ID: "s1", Type: domain.StepTypeNoop, Next: "s2",
				Compensation: &domain.Compensation{Action: "svc.undo1"}},
			{ID: "s2", Type: domain.StepTypeNoop, Next: "s3"},  // nil compensation -> skipped
			{ID: "s3", Type: domain.StepTypeNoop, Next: "end"}, // fails
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_comp_nil", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)

	fc := clock.NewFakeClock(time.Unix(0, 0).UTC())
	cap := &compDispatchCapture{}
	c := NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, cap, nil)
	c.verbs[domain.StepTypeNoop] = verbs.RegistryEntry{Handler: stepRouter{failOn: "s3"}, LicenseGroup: "common"}

	if err := c.Advance(ctx, run.ID.String()); err == nil {
		t.Fatal("expected Advance to fail at s3")
	}

	// Only s1's compensation should have dispatched; s2 has nil compensation.
	want := []string{"svc.undo1"}
	gotActions := cap.ordered()
	if len(gotActions) != len(want) || gotActions[0] != want[0] {
		t.Errorf("compensation dispatches = %v, want %v (s2 nil-compensation must be skipped)", gotActions, want)
	}
}

// stepRouter is a test verb that succeeds for every step except failOn, which
// it errors. Lets a single registry entry stand in for several noop steps.
type stepRouter struct{ failOn string }

func (r stepRouter) Execute(_ context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
	if step.ID == r.failOn {
		return nil, errors.New("boom at " + step.ID)
	}
	return map[string]any{}, nil
}

var _ verbs.Handler = (*flakyVerb)(nil)
var _ verbs.Handler = stepRouter{}
