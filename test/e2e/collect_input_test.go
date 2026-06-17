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

type ciPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *ciPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestCollectInputVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	raw, err := os.ReadFile("../fixtures/wf_collect_input.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	pub := &ciPub{}
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	// User-task REST handler.
	utH := api.NewUserTaskHandler(s, pub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/user_task/{task_id}/submit", utH.Submit)
	srv := httptest.NewServer(r)
	defer srv.Close()

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// Saga should be paused awaiting user_task.{id}.submitted.
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused || got.AwaitedSignal == nil {
		t.Fatalf("expected paused awaiting signal; got state=%s sig=%v", got.State, got.AwaitedSignal)
	}
	// Parse task ID from awaited_signal: "user_task.<uuid>.submitted"
	parts := strings.SplitN(*got.AwaitedSignal, ".", 3)
	if len(parts) < 3 {
		t.Fatalf("malformed awaited_signal: %s", *got.AwaitedSignal)
	}
	taskID := parts[1]

	// POST submit with structured result data.
	body := `{"submitted_by":"u1","result":{"reason":"security risk identified"}}`
	resp, err := http.Post(
		srv.URL+"/api/v1/sagas/"+run.ID.String()+"/user_task/"+taskID+"/submit",
		"application/json",
		bytes.NewReader([]byte(body)),
	)
	if err != nil {
		t.Fatalf("post submit: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ = s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s", got.State)
}
