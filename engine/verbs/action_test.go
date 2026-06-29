package verbs

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
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// actionPubCapture captures PublishActionDispatch calls for assertion.
type actionPubCapture struct {
	routingKeys []string
	payloads    [][]byte
}

func (p *actionPubCapture) PublishActionDispatch(_ context.Context, routingKey string, payload []byte) error {
	p.routingKeys = append(p.routingKeys, routingKey)
	p.payloads = append(p.payloads, payload)
	return nil
}

// httpDispatchCapture captures DispatchHTTP calls.
type httpDispatchCapture struct {
	addresses []string
	payloads  [][]byte
}

func (d *httpDispatchCapture) DispatchHTTP(_ context.Context, address string, payload []byte) error {
	d.addresses = append(d.addresses, address)
	d.payloads = append(d.payloads, payload)
	return nil
}

// rmqDispatchCapture captures DispatchRMQQueue calls.
type rmqDispatchCapture struct {
	queues   []string
	payloads [][]byte
}

func (d *rmqDispatchCapture) DispatchRMQQueue(_ context.Context, queue string, payload []byte) error {
	d.queues = append(d.queues, queue)
	d.payloads = append(d.payloads, payload)
	return nil
}

// registerAction upserts an ActionRegistration with the given dispatch descriptor.
func registerAction(t *testing.T, s *memory.Store, service, name string, version int, transport, address string) {
	t.Helper()
	reg := domain.ActionRegistration{
		Service:      service,
		ActionName:   name,
		Version:      version,
		Compensable:  false,
		InputSchema:  map[string]any{},
		OutputSchema: map[string]any{},
		Transport:    transport,
		Address:      address,
	}
	if err := s.UpsertActionRegistration(context.Background(), reg); err != nil {
		t.Fatalf("register action: %v", err)
	}
}

func TestActionVerb_GRPCDefault_NoDescriptor_Publishes(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	httpD := &httpDispatchCapture{}
	rmqD := &rmqDispatchCapture{}
	v := ActionVerb{S: s, Publisher: pub, HTTPDispatcher: httpD, RMQDispatcher: rmqD}

	// No registration at all -> grpc default.
	run, step := makeActionRun(t, s, "example.set_state", 0)
	if _, err := v.Execute(ctx, run, step); !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if len(pub.routingKeys) != 1 || pub.routingKeys[0] != "example.set_state" {
		t.Errorf("grpc routing keys = %v, want [example.set_state]", pub.routingKeys)
	}
	if len(httpD.addresses) != 0 || len(rmqD.queues) != 0 {
		t.Errorf("http/rmq should not be called for grpc default")
	}
}

func TestActionVerb_GRPCDescriptor_Publishes(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	v := ActionVerb{S: s, Publisher: pub}
	registerAction(t, s, "example", "set_state", 1, domain.TransportGRPC, "")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	if _, err := v.Execute(ctx, run, step); !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if len(pub.routingKeys) != 1 {
		t.Errorf("expected grpc publish, got %v", pub.routingKeys)
	}
}

func TestActionVerb_HTTPTransport_Dispatches(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	httpD := &httpDispatchCapture{}
	v := ActionVerb{S: s, Publisher: pub, HTTPDispatcher: httpD}
	registerAction(t, s, "example", "set_state", 1, domain.TransportHTTP, "https://worker.local/cb")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	if _, err := v.Execute(ctx, run, step); !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if len(httpD.addresses) != 1 || httpD.addresses[0] != "https://worker.local/cb" {
		t.Errorf("http addresses = %v, want [https://worker.local/cb]", httpD.addresses)
	}
	if len(pub.routingKeys) != 0 {
		t.Errorf("grpc publisher should not be called for http transport")
	}
	if len(httpD.payloads[0]) == 0 {
		t.Errorf("http payload missing")
	}
}

func TestActionVerb_RMQTransport_Dispatches(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	rmqD := &rmqDispatchCapture{}
	v := ActionVerb{S: s, Publisher: pub, RMQDispatcher: rmqD}
	registerAction(t, s, "example", "set_state", 1, domain.TransportRMQ, "worker.set_state.q")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	if _, err := v.Execute(ctx, run, step); !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if len(rmqD.queues) != 1 || rmqD.queues[0] != "worker.set_state.q" {
		t.Errorf("rmq queues = %v, want [worker.set_state.q]", rmqD.queues)
	}
	if len(pub.routingKeys) != 0 {
		t.Errorf("grpc publisher should not be called for rmq transport")
	}
}

func TestActionVerb_LatestVersionDescriptorWins(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	httpD := &httpDispatchCapture{}
	rmqD := &rmqDispatchCapture{}
	v := ActionVerb{S: s, HTTPDispatcher: httpD, RMQDispatcher: rmqD}
	// v1 http, v2 rmq -> latest (v2) wins.
	registerAction(t, s, "example", "set_state", 1, domain.TransportHTTP, "https://old/cb")
	registerAction(t, s, "example", "set_state", 2, domain.TransportRMQ, "new.q")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	if _, err := v.Execute(ctx, run, step); !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if len(rmqD.queues) != 1 || rmqD.queues[0] != "new.q" {
		t.Errorf("latest version should route to rmq new.q, got rmq=%v http=%v", rmqD.queues, httpD.addresses)
	}
}

func TestActionVerb_HTTPTransport_NoAddress_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	v := ActionVerb{S: s, HTTPDispatcher: &httpDispatchCapture{}}
	registerAction(t, s, "example", "set_state", 1, domain.TransportHTTP, "")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	_, err := v.Execute(ctx, run, step)
	if err == nil || errors.Is(err, ErrSagaPaused) {
		t.Errorf("expected error for http transport with no address, got %v", err)
	}
}

func TestActionVerb_HTTPTransport_NoDispatcher_Errors(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	v := ActionVerb{S: s} // no HTTPDispatcher
	registerAction(t, s, "example", "set_state", 1, domain.TransportHTTP, "https://worker.local/cb")

	run, step := makeActionRun(t, s, "example.set_state", 0)
	_, err := v.Execute(ctx, run, step)
	if err == nil || errors.Is(err, ErrSagaPaused) {
		t.Errorf("expected error for http transport with no dispatcher, got %v", err)
	}
}

func makeActionRun(t *testing.T, s *memory.Store, action string, currentAttempt int) (domain.SagaRun, domain.Step) {
	t.Helper()
	ctx := context.Background()
	defID := uuid.New()
	run := domain.NewSagaRun("wf", defID, nil, map[string]any{})
	run.CurrentAttempt = currentAttempt
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	step := domain.Step{
		ID:     "act",
		Type:   domain.StepTypeAction,
		Action: action,
		Next:   "end",
		Inputs: map[string]any{"ticket_id": "INC-1"},
	}
	return run, step
}

func TestActionVerb_PublishesAndPauses(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	v := ActionVerb{S: s, Publisher: pub}

	run, step := makeActionRun(t, s, "example.set_state", 0)
	result, err := v.Execute(ctx, run, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	if result != nil {
		t.Errorf("result = %v, want nil", result)
	}

	// Verify the store was marked.
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.AwaitedActionDispatch == nil || *got.AwaitedActionDispatch != "example.set_state" {
		t.Errorf("awaited_action_dispatch = %v, want example.set_state", got.AwaitedActionDispatch)
	}
	if got.CurrentAttempt != 1 {
		t.Errorf("current_attempt = %d, want 1", got.CurrentAttempt)
	}

	// Verify RabbitMQ publish.
	if len(pub.routingKeys) != 1 || pub.routingKeys[0] != "example.set_state" {
		t.Errorf("routing keys = %v, want [example.set_state]", pub.routingKeys)
	}
	if len(pub.payloads) != 1 || len(pub.payloads[0]) == 0 {
		t.Errorf("payload missing")
	}
}

func TestActionVerb_NilPublisher_StillPauses(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	v := ActionVerb{S: s, Publisher: nil} // nil publisher — no publish, still pauses

	run, step := makeActionRun(t, s, "svc.do_thing", 0)
	_, err := v.Execute(ctx, run, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
}

func TestActionVerb_EmptyAction_ReturnsError(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	v := ActionVerb{S: s, Publisher: nil}

	defID := uuid.New()
	run := domain.NewSagaRun("wf", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	step := domain.Step{ID: "act", Type: domain.StepTypeAction, Action: ""}

	_, err := v.Execute(ctx, run, step)
	if err == nil || errors.Is(err, ErrSagaPaused) {
		t.Errorf("expected validation error for empty action, got %v", err)
	}
}

func TestActionVerb_NoDot_ReturnsError(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	v := ActionVerb{S: s, Publisher: nil}

	defID := uuid.New()
	run := domain.NewSagaRun("wf", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	step := domain.Step{ID: "act", Type: domain.StepTypeAction, Action: "nodot"}

	_, err := v.Execute(ctx, run, step)
	if err == nil || errors.Is(err, ErrSagaPaused) {
		t.Errorf("expected bad-format error for action without dot, got %v", err)
	}
}

func TestActionVerb_AttemptBumped(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	pub := &actionPubCapture{}
	v := ActionVerb{S: s, Publisher: pub}

	// Simulate run already at attempt 2.
	run, step := makeActionRun(t, s, "svc.act", 2)
	_, err := v.Execute(ctx, run, step)
	if !errors.Is(err, ErrSagaPaused) {
		t.Fatalf("Execute err = %v, want ErrSagaPaused", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.CurrentAttempt != 3 {
		t.Errorf("current_attempt = %d, want 3 (2+1)", got.CurrentAttempt)
	}
}

func TestGenerateIdempotencyKey_Deterministic(t *testing.T) {
	k1 := generateIdempotencyKey("run-1", "step-1", 1)
	k2 := generateIdempotencyKey("run-1", "step-1", 1)
	if k1 != k2 {
		t.Errorf("expected same key, got %q vs %q", k1, k2)
	}
	k3 := generateIdempotencyKey("run-1", "step-1", 2)
	if k1 == k3 {
		t.Errorf("different attempts should produce different keys")
	}
}
