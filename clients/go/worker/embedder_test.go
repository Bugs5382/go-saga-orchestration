package worker

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
	"net"
	"sync"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Bugs5382/go-saga-orchestration/liveness"
	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
)

func noopHandler() Handler {
	return HandlerFunc(func(context.Context, ActionPayload) (Result, error) { return nil, nil })
}

func TestBootstrapConfig_SkipRegistrationNeedsNoRegistryURL(t *testing.T) {
	cfg := BootstrapConfig{
		Service: "billing", RmqURL: "amqp://x", GrpcURL: "engine:9090", SkipRegistration: true,
		Actions: []Action{{Name: "charge", Handler: noopHandler()}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	cfg.SkipRegistration = false
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate without RegistryURL or SkipRegistration: want an error")
	}
}

func TestBootstrapConfig_RoutingKeys(t *testing.T) {
	cfg := BootstrapConfig{Service: "billing", Actions: []Action{{Name: "charge"}, {Name: "refund"}}}
	if got := cfg.RoutingKeys(); len(got) != 2 || got[0] != "billing.charge" || got[1] != "billing.refund" {
		t.Errorf("RoutingKeys = %v, want [billing.charge billing.refund]", got)
	}
	cfg.QualifiedActionNames = true
	cfg.Actions = []Action{{Name: "com.example.billing.charge"}}
	if got := cfg.RoutingKeys(); len(got) != 1 || got[0] != "com.example.billing.charge" {
		t.Errorf("qualified RoutingKeys = %v, want [com.example.billing.charge]", got)
	}
	if cfg.QueueName() != "billing.actions" {
		t.Errorf("QueueName = %q, want billing.actions", cfg.QueueName())
	}
	cfg.Queue = "billing-worker"
	if cfg.QueueName() != "billing-worker" {
		t.Errorf("QueueName = %q, want billing-worker", cfg.QueueName())
	}
}

func TestBootstrapConfig_ValidateRequiresDottedQualifiedName(t *testing.T) {
	cfg := BootstrapConfig{
		Service: "billing", RmqURL: "u", GrpcURL: "u", SkipRegistration: true, QualifiedActionNames: true,
		Actions: []Action{{Name: "billing", Handler: noopHandler()}},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("qualified name without a dot: want an error (the engine rejects it)")
	}
}

// TestProcessDelivery_CompensationRunsWithoutStream runs a compensation
// dispatch's handler directly: there is no awaited step to report on.
func TestProcessDelivery_CompensationRunsWithoutStream(t *testing.T) {
	var got ActionPayload
	h := HandlerFunc(func(_ context.Context, p ActionPayload) (Result, error) {
		got = p
		return nil, nil
	})
	body, _ := json.Marshal(map[string]any{
		"run_id": "r1", "step_id": "charge", "action": "billing.refund", "compensation": true,
		"inputs": map[string]any{"amount": 5},
	})
	ack := &fakeAck{}
	// A nil client panics if the worker tries to open a stream.
	processDelivery(context.Background(), amqp.Delivery{Body: body, Acknowledger: ack},
		map[string]Handler{"billing.refund": h}, nil)
	if !ack.acked || ack.nacked {
		t.Fatalf("want ack, got %+v", ack)
	}
	if !got.Compensation || got.StepID != "charge" {
		t.Errorf("handler saw %+v, want the compensation payload", got)
	}
}

func TestProcessDelivery_FailedCompensationGoesToDLQ(t *testing.T) {
	h := HandlerFunc(func(context.Context, ActionPayload) (Result, error) {
		return nil, Errorf("down", true, "refund service down")
	})
	body, _ := json.Marshal(map[string]any{"run_id": "r1", "step_id": "charge", "action": "billing.refund", "compensation": true})
	ack := &fakeAck{}
	processDelivery(context.Background(), amqp.Delivery{Body: body, Acknowledger: ack},
		map[string]Handler{"billing.refund": h}, nil)
	if !ack.nacked || ack.requeued {
		t.Fatalf("want nack without requeue, got %+v", ack)
	}
}

// refuseAll is a liveness.Reporter that refuses every report.
type refuseAll struct{}

func (refuseAll) CheckStep(context.Context, uuid.UUID, string, int) error {
	return liveness.ErrStepNotAwaited
}
func (refuseAll) CompleteStep(context.Context, uuid.UUID, string, int, map[string]any) error {
	return liveness.ErrStepNotAwaited
}
func (refuseAll) FailStep(context.Context, uuid.UUID, string, int, string, string, bool) error {
	return liveness.ErrStepNotAwaited
}

// TestProcessDelivery_RefusedReportIsAckedAndDropped acks a dispatch the
// engine refuses (stale or duplicate) instead of requeueing it forever, and
// never runs the handler.
func TestProcessDelivery_RefusedReportIsAckedAndDropped(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	liveness.Register(srv, liveness.NewServer(refuseAll{}, nil))
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var mu sync.Mutex
	ran := false
	h := HandlerFunc(func(context.Context, ActionPayload) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		ran = true
		return nil, nil
	})
	body, _ := json.Marshal(ActionPayload{RunID: uuid.NewString(), StepID: "s", Attempt: 1, Action: "billing.charge"})
	ack := &fakeAck{}
	processDelivery(context.Background(), amqp.Delivery{Body: body, Acknowledger: ack},
		map[string]Handler{"billing.charge": h}, pb.NewWorkerLivenessClient(conn))
	if !ack.acked || ack.nacked {
		t.Fatalf("want ack, got %+v", ack)
	}
	mu.Lock()
	defer mu.Unlock()
	if ran {
		t.Error("handler ran for a refused report")
	}
}
