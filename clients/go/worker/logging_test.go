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
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

// fakeAck records how a delivery was settled.
type fakeAck struct {
	acked, nacked, requeued bool
}

func (f *fakeAck) Ack(uint64, bool) error { f.acked = true; return nil }
func (f *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacked, f.requeued = true, requeue
	return nil
}
func (f *fakeAck) Reject(_ uint64, requeue bool) error {
	f.nacked, f.requeued = true, requeue
	return nil
}

const workerSecret = "WORKER-INPUT-SECRET"

func TestProcessDelivery_LogsUnknownActionWithoutPayload(t *testing.T) {
	rec := sagalogtest.New()
	ctx := sagalog.NewContext(context.Background(), rec)
	body, _ := json.Marshal(ActionPayload{
		Action: "example.nope", RunID: "r1", StepID: "s1", Attempt: 1,
		Inputs: map[string]any{"card": workerSecret},
	})
	ack := &fakeAck{}
	processDelivery(ctx, amqp.Delivery{Body: body, Acknowledger: ack}, map[string]Handler{}, nil)

	if !ack.nacked || ack.requeued {
		t.Fatalf("want nack without requeue, got %+v", ack)
	}
	e, ok := rec.Find("worker: no handler for action")
	if !ok || e.Level != sagalogtest.LevelError {
		t.Fatalf("missing error line; got %v", rec.Entries())
	}
	if e.Fields["run_id"] != "r1" || e.Fields["action"] != "example.nope" {
		t.Errorf("fields = %v", e.Fields)
	}
	if !rec.Has(sagalogtest.LevelDebug, "worker: delivery received") {
		t.Errorf("missing debug line; got %v", rec.Entries())
	}
	if rec.Contains(workerSecret) {
		t.Fatalf("payload leaked into the log: %v", rec.Entries())
	}
}

func TestProcessDelivery_LogsBadPayload(t *testing.T) {
	rec := sagalogtest.New()
	ctx := sagalog.NewContext(context.Background(), rec)
	ack := &fakeAck{}
	processDelivery(ctx, amqp.Delivery{Body: []byte(`{"inputs":"` + workerSecret), Acknowledger: ack}, nil, nil)

	if !rec.Has(sagalogtest.LevelError, "worker: bad payload; nacking") {
		t.Fatalf("missing error line; got %v", rec.Entries())
	}
	if rec.Contains(workerSecret) {
		t.Fatalf("payload leaked into the log: %v", rec.Entries())
	}
}

// Without a logger on the context or config, nothing is recorded anywhere and
// nothing panics.
func TestProcessDelivery_SilentWithoutLogger(t *testing.T) {
	ack := &fakeAck{}
	processDelivery(context.Background(), amqp.Delivery{Body: []byte("{"), Acknowledger: ack}, nil, nil)
	if !ack.nacked {
		t.Fatalf("want nack, got %+v", ack)
	}
}
