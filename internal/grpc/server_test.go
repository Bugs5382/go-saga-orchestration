package grpc

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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type capPub struct{ runs []string }

func (c *capPub) PublishSagaAdvance(_ context.Context, runID string) error {
	c.runs = append(c.runs, runID)
	return nil
}

func newTestServer(t *testing.T, s *memory.Store, pub AdvancePublisher) (pb.WorkerLivenessClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	Register(srv, s, pub)
	go srv.Serve(lis) //nolint:errcheck
	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close() //nolint:errcheck
		srv.Stop()
	}
	return pb.NewWorkerLivenessClient(conn), cleanup
}

func TestExecuteStep_CompleteHappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := memory.New()

	// Seed a run + put it in awaiting-action state.
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	_ = s.MarkAwaitingAction(ctx, run.ID, "test.echo", 1)

	pub := &capPub{}
	client, done := newTestServer(t, s, pub)
	defer done()

	stream, err := client.ExecuteStep(ctx)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// Start
	if err := stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{
		RunId: run.ID.String(), StepId: "s1", Attempt: 1,
	}}}); err != nil {
		t.Fatalf("send start: %v", err)
	}
	// Expect Ack
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ack: %v", err)
	}
	if ev.GetAck() == nil {
		t.Fatalf("expected ack, got %+v", ev)
	}
	// Complete
	result, _ := json.Marshal(map[string]any{"ok": true, "id": "abc"})
	if err := stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{
		Complete: &pb.Complete{ResultJson: result},
	}}); err != nil {
		t.Fatalf("send complete: %v", err)
	}
	_ = stream.CloseSend()
	// Wait for server to handle.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(pub.runs) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(pub.runs) != 1 || pub.runs[0] != run.ID.String() {
		t.Errorf("publisher saw %v, want one publish of %s", pub.runs, run.ID)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.AwaitedActionDispatch != nil {
		t.Errorf("AwaitedActionDispatch should be cleared, got %v", *got.AwaitedActionDispatch)
	}
	if got.Variables["ok"] != true {
		t.Errorf("Variables.ok = %v, want true", got.Variables["ok"])
	}
}

func TestExecuteStep_ErrorPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := memory.New()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	_ = s.MarkAwaitingAction(ctx, run.ID, "test.echo", 1)

	pub := &capPub{}
	client, done := newTestServer(t, s, pub)
	defer done()

	stream, _ := client.ExecuteStep(ctx)
	stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{ //nolint:errcheck
		RunId: run.ID.String(), StepId: "s1", Attempt: 1,
	}}})
	stream.Recv()                                                              //nolint:errcheck // ack
	stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Error{Error: &pb.Error{ //nolint:errcheck
		Code: "validation", Message: "bad input", Retryable: false,
	}}})
	_ = stream.CloseSend()

	deadline := time.Now().Add(2 * time.Second)
	var failed atomic.Bool
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateFailed {
			failed.Store(true)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !failed.Load() {
		t.Errorf("expected run to be failed after worker error")
	}
}
