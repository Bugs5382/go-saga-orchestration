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

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type signalPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *signalPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestWaitForSignalVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	fc := clock.NewFakeClock(time.Unix(1000, 0).UTC())

	raw, _ := os.ReadFile("../fixtures/wf_wait_for_signal.json")
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &signalPub{}
	coord := engine.NewCoordinator(s, nil, fc, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	// Wire up the signal HTTP handler with an in-process test server.
	signalH := api.NewSignalHandler(s, pub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/signal/{name}", signalH.Post)
	srv := httptest.NewServer(r)
	defer srv.Close()

	// Create and advance the run â€” it should pause awaiting the "go" signal.
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("initial advance: %v", err)
	}
	{
		got, _ := s.GetRun(ctx, run.ID)
		if got.State != domain.RunStatePaused {
			t.Fatalf("expected paused after initial advance, got %s", got.State)
		}
		if got.AwaitedSignal == nil || *got.AwaitedSignal != "go" {
			t.Fatalf("expected awaited_signal=\"go\", got %v", got.AwaitedSignal)
		}
	}

	// POST the "go" signal. The handler calls TryConsumeAwaitedSignal (clears
	// awaited markers + wakeup_at) then publishes saga.advance, which calls
	// coord.Advance in a goroutine. Advance sees paused+externalWake â†’ resumes.
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

	// Wait for the goroutine-driven Advance to complete the saga.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s publishCalls=%d", got.State, pub.calls.Load())
}
