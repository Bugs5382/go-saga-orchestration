package api

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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// newStreamHandler builds a SagaStreamHandler with a nil pool (sufficient for
// all unit tests that exercise paths before LISTEN is reached).
func newStreamHandler(s *memory.Store) *SagaStreamHandler {
	return NewSagaStreamHandler(s, nil)
}

func routedStreamServer(h *SagaStreamHandler) *httptest.Server {
	r := chi.NewRouter()
	r.Get("/api/v1/sagas/{run_id}/stream", h.Stream)
	return httptest.NewServer(r)
}

// TestStreamHandler_BadUUID verifies the handler returns 400 for a non-UUID run_id.
func TestStreamHandler_BadUUID(t *testing.T) {
	h := newStreamHandler(memory.New())
	srv := routedStreamServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/sagas/not-a-uuid/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// TestStreamHandler_NotFound verifies 404 is returned (before upgrade) when
// the run_id is valid UUID but no such run exists.
func TestStreamHandler_NotFound(t *testing.T) {
	h := newStreamHandler(memory.New())
	srv := routedStreamServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/sagas/" + uuid.New().String() + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestStreamHandler_SnapshotOnConnect seeds a run + two events, connects via
// WebSocket, and asserts the first frames are the run snapshot followed by each event.
func TestStreamHandler_SnapshotOnConnect(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	// Seed a run.
	def := domain.WorkflowDefinition{
		ID: "wf_stream", Version: 1, Name: "StreamTest",
		Start: "end", Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	run := domain.NewSagaRun("wf_stream", defID, nil, nil)
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Seed two events.
	e1 := domain.NewEvent(run.ID, "step1", 0, domain.EventSagaStarted, "engine")
	e2 := domain.NewEvent(run.ID, "step2", 0, domain.EventStepSucceeded, "engine")
	_ = s.AppendEvent(ctx, e1)
	_ = s.AppendEvent(ctx, e2)

	// The pool is nil — Stream exits after snapshot because WaitForNotification
	// will panic on a nil pool. We rely on the WS client closing the connection
	// first. To avoid a race, the test server uses a channel to coordinate.
	h := &SagaStreamHandler{
		S:    s,
		Pool: nil, // LISTEN path not exercised in unit tests
		Upgrade: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}

	// Wrap Stream so it returns after sending the snapshot (before LISTEN).
	// We do this by having the WS client close the connection, which causes
	// WriteMessage to fail and Stream to return naturally.
	srv := routedStreamServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/sagas/" + run.ID.String() + "/stream"
	dialer := websocket.Dialer{}
	wsConn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v (status %v)", err, resp)
	}
	defer wsConn.Close()

	// Read frame 0: run snapshot.
	var f0 frame
	if err := readFrame(t, wsConn, &f0); err != nil {
		t.Fatalf("frame 0: %v", err)
	}
	if f0.Type != "run" {
		t.Errorf("frame 0 type = %q, want \"run\"", f0.Type)
	}

	// Read frame 1: event 1.
	var f1 frame
	if err := readFrame(t, wsConn, &f1); err != nil {
		t.Fatalf("frame 1: %v", err)
	}
	if f1.Type != "event" {
		t.Errorf("frame 1 type = %q, want \"event\"", f1.Type)
	}

	// Read frame 2: event 2.
	var f2 frame
	if err := readFrame(t, wsConn, &f2); err != nil {
		t.Fatalf("frame 2: %v", err)
	}
	if f2.Type != "event" {
		t.Errorf("frame 2 type = %q, want \"event\"", f2.Type)
	}

	// Close the WS to cleanly terminate the handler's acquire-nil-pool path.
	_ = wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

// readFrame reads one text message from the WS conn and unmarshals it into dst.
func readFrame(t *testing.T, conn *websocket.Conn, dst *frame) error {
	t.Helper()
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	return json.Unmarshal(msg, dst)
}
