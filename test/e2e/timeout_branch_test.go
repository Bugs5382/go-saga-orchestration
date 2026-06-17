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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type timeoutBranchPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *timeoutBranchPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

// loadTimeoutBranchDef loads the timeout-branch fixture and upserts it into s,
// returning the definition and its storage ID.
func loadTimeoutBranchDef(t *testing.T, s *memory.Store) (domain.WorkflowDefinition, uuid.UUID) {
	t.Helper()
	raw, err := os.ReadFile("../fixtures/wf_wait_for_signal_timeout_branch.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	ctx := context.Background()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	return def, defID
}

// TestTimeoutBranch_TimesOut verifies that when a wait_for_signal step has a
// "timeout" branch and no signal arrives before the deadline, the run routes to
// the timeout branch's Next step (escalate) rather than step.Next (normal).
func TestTimeoutBranch_TimesOut(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def, defID := loadTimeoutBranchDef(t, s)

	// Start the fake clock 5 seconds before the timeout would fire.
	start := time.Unix(1000, 0).UTC()
	fc := clock.NewFakeClock(start)

	pub := &timeoutBranchPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	// Start the timer dispatcher so it fires when the clock advances.
	timer := &engine.Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}
	timerCtx, cancelTimer := context.WithCancel(ctx)
	defer cancelTimer()
	go func() { _ = timer.Run(timerCtx) }()

	// Create the run with a fresh definition ID UUID (not the fixture UUID).
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}

	// Run should be paused awaiting the "go" signal with a wakeup deadline.
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected paused after initial advance, got %s", got.State)
		}
		if got.AwaitedSignal == nil || *got.AwaitedSignal != "go" {
			t.Fatalf("expected awaited_signal=\"go\", got %v", got.AwaitedSignal)
		}
		if got.WakeupAt == nil {
			t.Fatalf("expected wakeup_at to be set (timeout deadline), got nil")
		}
	}

	// Give the timer goroutine time to register its clock.After waiter before
	// we advance the clock. This mirrors the pattern in wait_duration_test.go.
	time.Sleep(20 * time.Millisecond)

	// Advance the clock past the 5s timeout deadline — no signal was sent.
	fc.Advance(10 * time.Second)

	// Wait for the saga to complete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			// Verify it went to "escalate" (timeout branch), not "normal".
			outcome, _ := got.Variables["outcome"].(string)
			if outcome != "escalate" {
				t.Errorf("expected outcome=escalate (timeout branch), got %q", outcome)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s publishCalls=%d", got.State, pub.calls.Load())
}

// TestTimeoutBranch_SignalArrives verifies that when a signal arrives before the
// deadline, the run routes to step.Next (normal), not the timeout branch (escalate).
func TestTimeoutBranch_SignalArrives(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	def, defID := loadTimeoutBranchDef(t, s)

	start := time.Unix(1000, 0).UTC()
	fc := clock.NewFakeClock(start)

	pub := &timeoutBranchPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	// Wire up the signal HTTP handler.
	signalH := api.NewSignalHandler(s, pub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", signalH.Post)
	srv := httptest.NewServer(r)
	defer srv.Close()

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}

	// Confirm paused awaiting signal.
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected paused, got %s", got.State)
		}
		if got.AwaitedSignal == nil || *got.AwaitedSignal != "go" {
			t.Fatalf("expected awaited_signal=\"go\", got %v", got.AwaitedSignal)
		}
	}

	// Deliver the "go" signal before the 5s deadline (clock is still at t=1000s).
	resp, err := http.Post(
		srv.URL+"/api/v1/sagas/"+run.ID.String()+"/signal/go",
		"application/json",
		bytes.NewReader([]byte("{}")),
	)
	if err != nil {
		t.Fatalf("post signal: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("signal POST status = %d, want 202", resp.StatusCode)
	}

	// Wait for saga to complete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			// Verify it went to "normal" (step.Next), not "escalate" (timeout branch).
			outcome, _ := got.Variables["outcome"].(string)
			if outcome != "normal" {
				t.Errorf("expected outcome=normal (signal path), got %q", outcome)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s publishCalls=%d", got.State, pub.calls.Load())
}
