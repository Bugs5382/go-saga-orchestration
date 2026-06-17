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
	"context"
	"encoding/json"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	grpcsrv "github.com/Bugs5382/go-saga-orchestration/internal/grpc"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// e2ePub satisfies both engine.Publisher (saga.advance) and
// grpcsrv.AdvancePublisher — same method signature. Calls coord.Advance
// in a goroutine for the saga.advance case; the action-dispatch case is
// handled by a separate inProcActionPub.
type e2ePub struct {
	coord        *engine.Coordinator
	advanceCalls atomic.Int32
}

func (p *e2ePub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.advanceCalls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

// inProcActionPub bridges the engine's PublishActionDispatch call to an
// in-process worker driving the engine's gRPC server. This proves the
// engine → dispatch → worker → gRPC → CompleteAction → resume flow
// without RabbitMQ.
type inProcActionPub struct {
	grpcClient pb.WorkerLivenessClient
	handler    worker.Handler
}

func (p *inProcActionPub) PublishActionDispatch(_ context.Context, _ string, payload []byte) error {
	var ap worker.ActionPayload
	if err := json.Unmarshal(payload, &ap); err != nil {
		return err
	}
	go func() {
		stream, err := p.grpcClient.ExecuteStep(context.Background())
		if err != nil {
			return
		}
		defer stream.CloseSend() //nolint:errcheck
		_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{
			RunId: ap.RunID, StepId: ap.StepID, Attempt: int32(ap.Attempt),
		}}})
		_, _ = stream.Recv() // ack
		result, hErr := p.handler.Execute(context.Background(), ap)
		if hErr != nil {
			_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Error{Error: &pb.Error{
				Code: "stub_error", Message: hErr.Error(), Retryable: false,
			}}})
			return
		}
		rb, _ := json.Marshal(result)
		_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{Complete: &pb.Complete{ResultJson: rb}}})
	}()
	return nil
}

// TestActionDispatch_StubWorker_EndToEnd proves the full action-dispatch flow
// without RabbitMQ:
//  1. saga reaches action step "a" → engine publishes via inProcActionPub
//  2. inProcActionPub drives a stub worker against the engine's in-process gRPC server
//  3. stub handler echoes inputs as outputs
//  4. worker sends Complete → gRPC server calls CompleteAction + PublishSagaAdvance
//  5. e2ePub.PublishSagaAdvance calls coord.Advance → saga resumes → reaches end → succeeded
func TestActionDispatch_StubWorker_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := memory.New()

	// Set up the engine-side gRPC server on bufconn.
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	// e2ePub satisfies grpcsrv.AdvancePublisher; we backfill coord below.
	pub := &e2ePub{}
	grpcsrv.Register(srv, s, pub)
	go srv.Serve(lis) //nolint:errcheck
	defer srv.Stop()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	defer conn.Close() //nolint:errcheck
	client := pb.NewWorkerLivenessClient(conn)

	// Stub handler — echoes each input key back as "echo_<key>".
	stubHandler := worker.HandlerFunc(func(_ context.Context, p worker.ActionPayload) (worker.Result, error) {
		out := worker.Result{}
		for k, v := range p.Inputs {
			out["echo_"+k] = v
		}
		out["handled_by"] = "test.echo"
		return out, nil
	})

	actionPub := &inProcActionPub{grpcClient: client, handler: stubHandler}

	// Register the stub action so the engine knows about test.echo.
	_ = s.UpsertActionRegistration(ctx, domain.ActionRegistration{
		Service: "test", ActionName: "echo", Version: 1,
		Description:  "echo inputs back",
		Category:     "external_io",
		LicenseGroup: "common",
	})

	// Load the fixture workflow.
	raw, err := os.ReadFile("../fixtures/wf_action_dispatch.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, actionPub, nil)
	pub.coord = coord

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// Wait for the saga to reach succeeded.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			if got.Variables["echo_hello"] != "world" {
				t.Errorf("echo_hello = %v, want world; full vars=%v", got.Variables["echo_hello"], got.Variables)
			}
			if got.Variables["handled_by"] != "test.echo" {
				t.Errorf("handled_by = %v, want test.echo", got.Variables["handled_by"])
			}
			return
		}
		if got.State == domain.RunStateFailed {
			t.Fatalf("saga failed unexpectedly; state=%s vars=%v", got.State, got.Variables)
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("saga did not reach succeeded; state=%s step=%q advanceCalls=%d",
		got.State, got.CurrentStep, pub.advanceCalls.Load())
}

// Compile-time interface satisfaction checks.
var (
	_ engine.Publisher         = (*e2ePub)(nil)
	_ grpcsrv.AdvancePublisher = (*e2ePub)(nil)
)
