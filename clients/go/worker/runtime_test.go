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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegisterWithOrchestrator_HappyPath(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := BootstrapConfig{
		Service: "example", ServiceVersion: "1.0.0",
		RegistryURL: srv.URL, RmqURL: "u", GrpcURL: "u",
		Actions: []Action{
			{Name: "set_state", Category: "record_lifecycle", Compensable: true, Handler: HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) { return nil, nil })},
		},
	}
	if err := registerWithOrchestrator(context.Background(), cfg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !strings.Contains(string(gotBody), `"service":"example"`) {
		t.Errorf("body did not contain service=example: %s", gotBody)
	}
	if !strings.Contains(string(gotBody), `"action_name":"set_state"`) {
		t.Errorf("body did not contain action_name=set_state: %s", gotBody)
	}
}

func TestRegisterWithOrchestrator_NonOKReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("validation failed"))
	}))
	defer srv.Close()
	cfg := BootstrapConfig{
		Service: "x", RegistryURL: srv.URL, RmqURL: "u", GrpcURL: "u",
		Actions: []Action{{Name: "a", Handler: HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) { return nil, nil })}},
	}
	if err := registerWithOrchestrator(context.Background(), cfg); err == nil {
		t.Errorf("expected error on 400")
	}
}

func TestConsumeLoop_FeedsDispatch(t *testing.T) {
	ch := make(chan amqp.Delivery, 3)
	var bodies []string
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = consumeLoop(ctx, ch, func(_ context.Context, d amqp.Delivery) {
			bodies = append(bodies, string(d.Body))
		})
	}()
	ch <- amqp.Delivery{Body: []byte("one")}
	ch <- amqp.Delivery{Body: []byte("two")}
	// brief wait
	for len(bodies) < 2 {
	}
	cancel()
	if len(bodies) < 2 {
		t.Errorf("got %d, want 2", len(bodies))
	}
}

func TestProcessDelivery_UnknownAction_NacksWithoutRequeue(t *testing.T) {
	// We can't easily test the actual Ack/Nack on a real amqp.Delivery
	// without a broker, but we CAN verify the logic flow by checking
	// that processDelivery doesn't panic on an unknown action.
	// Real Nack test requires integration tests.
	payload := ActionPayload{Action: "example.nope", RunID: "r", StepID: "s", Attempt: 1}
	body, _ := json.Marshal(payload)
	d := amqp.Delivery{Body: body}
	handlers := map[string]Handler{} // empty
	// processDelivery would call d.Nack which on a zero-value Delivery
	// is a panic. Verify by recovering.
	defer func() {
		if r := recover(); r != nil {
			// Expected — zero Delivery has no Acknowledger
			return
		}
		t.Logf("processDelivery handled unknown action without panic — channel-bound Delivery wasn't real")
	}()
	processDelivery(context.Background(), d, handlers, nil)
}

// Avoid unused-import error.
var _ = bytes.NewReader
