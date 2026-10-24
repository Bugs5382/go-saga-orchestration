package liveness_test

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
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/liveness"
	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// capPub records the runs the reporter asks to advance.
type capPub struct {
	mu   sync.Mutex
	runs []string
}

func (c *capPub) PublishSagaAdvance(_ context.Context, runID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs = append(c.runs, runID)
	return nil
}

func (c *capPub) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.runs...)
}

// The coordinator is the Compensator cmd/engine wires in.
var _ liveness.Compensator = (*engine.Coordinator)(nil)

// capComp records compensation requests.
type capComp struct {
	mu    sync.Mutex
	calls []string
}

func (c *capComp) CompensateFailedStep(_ context.Context, runID uuid.UUID, stepID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, runID.String()+"/"+stepID)
	return nil
}

func (c *capComp) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func serve(t *testing.T, r liveness.Reporter) pb.WorkerLivenessClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	liveness.Register(srv, liveness.NewServer(r, nil))
	go srv.Serve(lis) //nolint:errcheck
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return pb.NewWorkerLivenessClient(conn)
}

// awaitingRun seeds a run paused on step at attempt, the way ActionVerb
// leaves it.
func awaitingRun(t *testing.T, s *memory.Store, step string, attempt int) domain.SagaRun {
	t.Helper()
	ctx := context.Background()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.UpdateRunState(ctx, run.ID, domain.RunStateRunning, step); err != nil {
		t.Fatalf("update run: %v", err)
	}
	if err := s.MarkAwaitingAction(ctx, run.ID, "svc.act", attempt); err != nil {
		t.Fatalf("mark awaiting: %v", err)
	}
	return run
}

func start(t *testing.T, client pb.WorkerLivenessClient, runID, step string, attempt int32) (pb.WorkerLiveness_ExecuteStepClient, error) {
	t.Helper()
	stream, err := client.ExecuteStep(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{
		RunId: runID, StepId: step, Attempt: attempt,
	}}}); err != nil {
		t.Fatalf("send start: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		return stream, err
	}
	if ev.GetAck() == nil {
		t.Fatalf("expected ack, got %+v", ev)
	}
	return stream, nil
}

// finish sends the terminal event and waits for the server to end the stream.
func finish(t *testing.T, stream pb.WorkerLiveness_ExecuteStepClient, ev *pb.WorkerEvent) error {
	t.Helper()
	if err := stream.Send(ev); err != nil {
		t.Fatalf("send terminal: %v", err)
	}
	_ = stream.CloseSend()
	_, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func TestStoreReporter_CompleteMergesResultAndAdvances(t *testing.T) {
	s := memory.New()
	run := awaitingRun(t, s, "s1", 1)
	pub := &capPub{}
	client := serve(t, &liveness.StoreReporter{Store: s, Advance: pub})

	stream, err := start(t, client, run.ID.String(), "s1", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	result, _ := json.Marshal(map[string]any{"ok": true})
	if err := finish(t, stream, &pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{Complete: &pb.Complete{ResultJson: result}}}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, _ := s.GetRun(context.Background(), run.ID)
	if got.AwaitedActionDispatch != nil {
		t.Errorf("AwaitedActionDispatch = %v, want cleared", *got.AwaitedActionDispatch)
	}
	if got.Variables["ok"] != true {
		t.Errorf("Variables.ok = %v, want true", got.Variables["ok"])
	}
	if p := pub.snapshot(); len(p) != 1 || p[0] != run.ID.String() {
		t.Errorf("advances = %v, want [%s]", p, run.ID)
	}
}

func TestStoreReporter_RefusesStepNotAwaited(t *testing.T) {
	s := memory.New()
	run := awaitingRun(t, s, "s1", 2)
	client := serve(t, &liveness.StoreReporter{Store: s, Advance: &capPub{}})

	cases := []struct {
		name    string
		runID   string
		step    string
		attempt int32
		want    codes.Code
	}{
		{"wrong step", run.ID.String(), "s0", 2, codes.FailedPrecondition},
		{"stale attempt", run.ID.String(), "s1", 1, codes.FailedPrecondition},
		{"unknown run", uuid.NewString(), "s1", 2, codes.NotFound},
		{"bad run id", "not-a-uuid", "s1", 2, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := start(t, client, tc.runID, tc.step, tc.attempt)
			if status.Code(err) != tc.want {
				t.Fatalf("start error = %v, want code %s", err, tc.want)
			}
		})
	}
	got, _ := s.GetRun(context.Background(), run.ID)
	if got.AwaitedActionDispatch == nil || got.CurrentAttempt != 2 {
		t.Errorf("refused reports changed the run: dispatch %v attempt %d", got.AwaitedActionDispatch, got.CurrentAttempt)
	}
}

func TestStoreReporter_ErrorFailsRunAndCompensates(t *testing.T) {
	s := memory.New()
	run := awaitingRun(t, s, "charge", 1)
	comp := &capComp{}
	client := serve(t, &liveness.StoreReporter{Store: s, Advance: &capPub{}, Compensator: comp})

	stream, err := start(t, client, run.ID.String(), "charge", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := finish(t, stream, &pb.WorkerEvent{Event: &pb.WorkerEvent_Error{Error: &pb.Error{
		Code: "declined", Message: "card declined",
	}}}); err != nil {
		t.Fatalf("error report: %v", err)
	}
	got, _ := s.GetRun(context.Background(), run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed", got.State)
	}
	if c := comp.snapshot(); len(c) != 1 || c[0] != run.ID.String()+"/charge" {
		t.Errorf("compensations = %v, want [%s/charge]", c, run.ID)
	}

	// A second failure report for the same attempt is refused.
	if _, err := start(t, client, run.ID.String(), "charge", 1); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("repeat start error = %v, want FailedPrecondition", err)
	}
}

// recReporter is an embedder's own Reporter.
type recReporter struct {
	mu     sync.Mutex
	events []string
	check  error
}

func (r *recReporter) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recReporter) CheckStep(_ context.Context, _ uuid.UUID, step string, attempt int) error {
	r.add("check " + step)
	return r.check
}

func (r *recReporter) CompleteStep(_ context.Context, _ uuid.UUID, step string, _ int, result map[string]any) error {
	r.add("complete " + step + " " + result["k"].(string))
	return nil
}

func (r *recReporter) FailStep(_ context.Context, _ uuid.UUID, step string, _ int, code, _ string, _ bool) error {
	r.add("fail " + step + " " + code)
	return nil
}

func TestServer_DelegatesToCustomReporter(t *testing.T) {
	rep := &recReporter{}
	client := serve(t, rep)
	id := uuid.NewString()

	stream, err := start(t, client, id, "a", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	res, _ := json.Marshal(map[string]any{"k": "v"})
	if err := finish(t, stream, &pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{Complete: &pb.Complete{ResultJson: res}}}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	stream, err = start(t, client, id, "b", 1)
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	if err := finish(t, stream, &pb.WorkerEvent{Event: &pb.WorkerEvent_Error{Error: &pb.Error{Code: "x"}}}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	rep.mu.Lock()
	got := append([]string(nil), rep.events...)
	rep.mu.Unlock()
	want := []string{"check a", "complete a v", "check b", "fail b x"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}

	// A reporter error that wraps ErrStepNotAwaited maps to FailedPrecondition.
	rep.check = errors.Join(liveness.ErrStepNotAwaited, errors.New("held back"))
	if _, err := start(t, client, id, "c", 1); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("start error = %v, want FailedPrecondition", err)
	}
	rep.check = errors.New("database down")
	if _, err := start(t, client, id, "c", 1); status.Code(err) != codes.Internal {
		t.Errorf("start error = %v, want Internal", err)
	}
}

func TestServer_ProtocolViolations(t *testing.T) {
	client := serve(t, &recReporter{})
	stream, err := client.ExecuteStep(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{Complete: &pb.Complete{}}})
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("complete before start = %v, want InvalidArgument", err)
	}

	stream, err = start(t, client, uuid.NewString(), "a", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{RunId: uuid.NewString(), StepId: "a", Attempt: 1}}})
	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("second start = %v, want InvalidArgument", err)
	}
}
