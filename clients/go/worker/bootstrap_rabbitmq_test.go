package worker_test

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
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Bugs5382/go-saga-orchestration/clients/go/worker"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/liveness"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/Bugs5382/go-saga-orchestration/topology"
)

// amqpDispatch publishes the engine's dispatches to action.direct, as the
// engine binary does.
type amqpDispatch struct {
	mu sync.Mutex
	ch *amqp.Channel
}

func (p *amqpDispatch) PublishActionDispatch(ctx context.Context, key string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch.PublishWithContext(ctx, topology.ExchangeAction, key, true, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
}

// inProcAdvance advances runs in-process.
type inProcAdvance struct{ coord *engine.Coordinator }

func (a *inProcAdvance) PublishSagaAdvance(_ context.Context, runID string) error {
	go func() { _ = a.coord.Advance(context.Background(), runID) }()
	return nil
}

// TestBootstrap_EmbeddedEngine runs a worker the way an application that
// embeds the engine does: its verbs are registered elsewhere (no HTTP
// registration), their names are fully qualified, and it reaches the
// liveness server through caller-supplied dial options. A workflow with two
// action steps runs to completion over a real RabbitMQ, and a failure on the
// second run compensates the first step through the same queue.
func TestBootstrap_EmbeddedEngine(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("set TEST_RABBITMQ_URL to run the worker against RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	suffix := uuid.NewString()[:8]
	reserve := "com.example-" + suffix + ".inventory.reserve"
	release := "com.example-" + suffix + ".inventory.release"
	charge := "com.example-" + suffix + ".billing.charge"
	queue := "embedded-worker-" + suffix
	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	s := memory.New()
	pub := &amqpDispatch{ch: ch}
	adv := &inProcAdvance{}
	coord := engine.NewCoordinator(s, adv, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, pub, nil)
	adv.coord = coord

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	liveness.Register(srv, liveness.NewServer(&liveness.StoreReporter{Store: s, Advance: adv, Compensator: coord}, nil))
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)

	var mu sync.Mutex
	var released []string
	failCharge := false
	handlers := map[string]worker.HandlerFunc{
		reserve: func(_ context.Context, p worker.ActionPayload) (worker.Result, error) {
			return worker.Result{"reserved": p.Inputs["sku"]}, nil
		},
		release: func(_ context.Context, p worker.ActionPayload) (worker.Result, error) {
			mu.Lock()
			defer mu.Unlock()
			released = append(released, p.RunID)
			return nil, nil
		},
		charge: func(context.Context, worker.ActionPayload) (worker.Result, error) {
			mu.Lock()
			defer mu.Unlock()
			if failCharge {
				return nil, worker.Errorf("declined", false, "card declined")
			}
			return worker.Result{"charged": true}, nil
		},
	}
	cfg := worker.BootstrapConfig{
		Service:              "embedded",
		RmqURL:               url,
		GrpcURL:              "passthrough://bufnet",
		SkipRegistration:     true,
		QualifiedActionNames: true,
		Queue:                queue,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	for name, h := range handlers {
		cfg.Actions = append(cfg.Actions, worker.Action{Name: name, Handler: h})
	}
	// Declare the queue the worker will consume up front (the call is
	// idempotent), so no dispatch is published before it is bound.
	if err := topology.DeclareActionQueue(ch, queue, cfg.RoutingKeys()...); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	wctx, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Bootstrap(wctx, cfg) }()
	t.Cleanup(func() {
		stopWorker()
		<-done
	})

	def := domain.WorkflowDefinition{
		ID: "wf_embedded_" + suffix, Version: 1, Name: "embedded", Start: "reserve", Published: true,
		Steps: []domain.Step{
			{ID: "reserve", Type: domain.StepTypeAction, Action: reserve, Inputs: map[string]any{"sku": "A1"},
				Next: "charge", Compensation: &domain.Compensation{Action: release}},
			{ID: "charge", Type: domain.StepTypeAction, Action: charge, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	startRun := func() uuid.UUID {
		run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
		if err := s.CreateRun(ctx, run); err != nil {
			t.Fatalf("create run: %v", err)
		}
		if err := coord.Advance(ctx, run.ID.String()); err != nil {
			t.Fatalf("first advance: %v", err)
		}
		return run.ID
	}
	waitFor := func(id uuid.UUID, want domain.RunState) domain.SagaRun {
		for ctx.Err() == nil {
			got, _ := s.GetRun(ctx, id)
			if got.State == want {
				return got
			}
			time.Sleep(20 * time.Millisecond)
		}
		got, _ := s.GetRun(ctx, id)
		t.Fatalf("run %s is %s on %s, want %s", id, got.State, got.CurrentStep, want)
		return got
	}

	ok := waitFor(startRun(), domain.RunStateSucceeded)
	if ok.Variables["reserved"] != "A1" || ok.Variables["charged"] != true {
		t.Errorf("variables = %v, want both step results", ok.Variables)
	}

	mu.Lock()
	failCharge = true
	mu.Unlock()
	failedID := startRun()
	waitFor(failedID, domain.RunStateFailed)
	for ctx.Err() == nil {
		mu.Lock()
		n := len(released)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(released) != 1 || released[0] != failedID.String() {
		t.Errorf("released = %v, want one release for %s", released, failedID)
	}
}
