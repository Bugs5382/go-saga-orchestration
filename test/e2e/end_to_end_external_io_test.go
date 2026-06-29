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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type demoPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *demoPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestExternalIODemo_EndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())

	// httptest server for the http_request step.
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer httpSrv.Close()

	// Load fixture + patch URL placeholder.
	raw, _ := os.ReadFile("../fixtures/wf_external_io_demo.json")
	patched := strings.ReplaceAll(string(raw), "__SERVER_URL__", httpSrv.URL)
	var def domain.WorkflowDefinition
	if err := json.Unmarshal([]byte(patched), &def); err != nil {
		t.Fatalf("parse: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &demoPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	// Timer dispatcher for wait_duration wake-up.
	timer := &engine.Timer{S: s, Publisher: pub, Clock: fc, Tick: 1 * time.Millisecond, BatchSize: 10}
	timerCtx, cancelTimer := context.WithCancel(ctx)
	defer cancelTimer()
	go func() { _ = timer.Run(timerCtx) }()

	// Signal REST endpoint for wait_for_signal wake-up.
	signalH := api.NewSignalHandler(s, pub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", signalH.Post)
	apiSrv := httptest.NewServer(r)
	defer apiSrv.Close()

	// Start the saga.
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}

	// After initial advance, saga should be paused at wait_duration (s3).
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("after initial advance: state=%s, want paused at wait_duration", got.State)
		}
		if got.CurrentStep != "s3" {
			t.Fatalf("after initial advance: CurrentStep=%q, want s3", got.CurrentStep)
		}
		// http_request result should already be in Variables.
		if got.Variables["r_status"] == nil {
			t.Errorf("r_status not in Variables: %v", got.Variables)
		}
	}

	// Advance the clock past wait_duration's 100ms wakeup.
	time.Sleep(20 * time.Millisecond) // allow timer goroutine's After waiter to register
	fc.Advance(200 * time.Millisecond)

	// Wait for the saga to reach s4 (wait_for_signal paused state).
	signalDeadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(signalDeadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStatePaused && got.CurrentStep == "s4" {
			break
		}
		if got.State == domain.RunStateSucceeded || got.State == domain.RunStateFailed {
			t.Fatalf("unexpected early terminal state %s before signal step", got.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.CurrentStep != "s4" || got.AwaitedSignal == nil || *got.AwaitedSignal != "approved" {
			t.Fatalf("after timer wakeup: state=%s step=%q awaitedSignal=%v", got.State, got.CurrentStep, got.AwaitedSignal)
		}
	}

	// POST the signal.
	resp, err := http.Post(apiSrv.URL+"/api/v1/sagas/"+run.ID.String()+"/signal/approved", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("post signal: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("signal post status = %d, want 202", resp.StatusCode)
	}

	// Wait for terminal.
	termDeadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(termDeadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s step=%q", got.State, got.CurrentStep)
}
