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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/internal/dispatch"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// TestActionDispatch_HTTPTransport_FullLoop proves the dispatch-descriptor http
// path end to end: the engine routes the action over http to a worker, the
// worker reports its result via the result-callback REST endpoint, and the
// saga resumes to succeeded. (issue #59)
func TestActionDispatch_HTTPTransport_FullLoop(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// REST router exposing the result-callback endpoint, backed by the same store.
	advancePub := &actionPub2{}
	resultHandler := api.NewActionResultHandler(s, advancePub)
	r := chi.NewRouter()
	r.Post("/api/v1/sagas/{run_id}/actions/{step_id}/result", resultHandler.Post)
	restSrv := httptest.NewServer(r)
	defer restSrv.Close()

	// HTTP worker: receives the dispatch, then posts a success result back.
	var workerSrv *httptest.Server
	workerSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		payload, _ := io.ReadAll(req.Body)
		w.WriteHeader(http.StatusAccepted)
		var ap verbs.ActionPayload
		if err := json.Unmarshal(payload, &ap); err != nil {
			t.Errorf("worker: decode payload: %v", err)
			return
		}
		go func() {
			result := map[string]any{"result": map[string]any{"ticket_number": "INC-HTTP"}}
			b, _ := json.Marshal(result)
			url := restSrv.URL + "/api/v1/sagas/" + ap.RunID + "/actions/" + ap.StepID + "/result"
			resp, err := http.Post(url, "application/json", bytes.NewReader(b))
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
	}))
	defer workerSrv.Close()

	coord := engine.NewCoordinator(s, advancePub, clock.SystemClock{},
		secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil,
		verbs.WithHTTPDispatcher(dispatch.NewHTTPDispatcher()))
	advancePub.coord = coord

	// Register the action with an http dispatch descriptor pointing at the worker.
	reg := domain.ActionRegistration{
		Service: "example", ActionName: "set_state", Version: 1,
		InputSchema: map[string]any{}, OutputSchema: map[string]any{},
		Transport: domain.TransportHTTP, Address: workerSrv.URL,
	}
	if err := s.UpsertActionRegistration(ctx, reg); err != nil {
		t.Fatalf("register action: %v", err)
	}

	def := buildActionDispatchDef()
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// Wait for the worker callback + resume to settle.
	deadline := time.Now().Add(3 * time.Second)
	var got domain.SagaRun
	for time.Now().Before(deadline) {
		got, _ = s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got.State != domain.RunStateSucceeded {
		t.Fatalf("saga state = %s, want succeeded", got.State)
	}
	if got.Variables["ticket_number"] != "INC-HTTP" {
		t.Errorf("ticket_number = %v, want INC-HTTP", got.Variables["ticket_number"])
	}
}
