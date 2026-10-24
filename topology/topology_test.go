package topology_test

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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Bugs5382/go-saga-orchestration/topology"
)

// recChannel records declarations; it is a fake of topology.Channel.
type recChannel struct {
	calls []string
}

func (r *recChannel) ExchangeDeclare(name, kind string, durable, _, _, _ bool, _ amqp.Table) error {
	r.calls = append(r.calls, "exchange "+name+" "+kind+" durable="+boolStr(durable))
	return nil
}

func (r *recChannel) QueueDeclare(name string, durable, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	r.calls = append(r.calls, "queue "+name+" durable="+boolStr(durable))
	return amqp.Queue{Name: name}, nil
}

func (r *recChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	r.calls = append(r.calls, "bind "+name+" "+key+" "+exchange)
	return nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestDeclare_EngineTopology(t *testing.T) {
	ch := &recChannel{}
	if err := topology.Declare(ch); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	want := []string{
		"exchange action.direct direct durable=true",
		"exchange workflow.events topic durable=true",
		"queue saga.advance durable=true",
		"queue saga.dlq durable=true",
		"queue action.dlq durable=true",
	}
	if strings.Join(ch.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(ch.calls, "\n"), strings.Join(want, "\n"))
	}
}

func TestDeclareActionQueue_BindsEachKeyExactly(t *testing.T) {
	ch := &recChannel{}
	if err := topology.DeclareActionQueue(ch, "billing.actions", "billing.charge", "billing.refund"); err != nil {
		t.Fatalf("DeclareActionQueue: %v", err)
	}
	want := []string{
		"exchange action.direct direct durable=true",
		"queue billing.actions durable=true",
		"bind billing.actions billing.charge action.direct",
		"bind billing.actions billing.refund action.direct",
	}
	if strings.Join(ch.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(ch.calls, "\n"), strings.Join(want, "\n"))
	}
}

func TestDeclareActionQueue_Validates(t *testing.T) {
	if err := topology.DeclareActionQueue(&recChannel{}, "", "a.b"); err == nil {
		t.Error("empty queue: want an error")
	}
	if err := topology.DeclareActionQueue(&recChannel{}, "q"); err == nil {
		t.Error("no routing keys: want an error")
	}
	if err := topology.DeclareActionQueue(&recChannel{}, "q", "a.b", ""); err == nil {
		t.Error("empty routing key: want an error")
	}
}

// TestDeclareActionQueue_RabbitMQ checks against a real broker that a dispatch
// published to action.direct with the action's name reaches the queue.
func TestDeclareActionQueue_RabbitMQ(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("set TEST_RABBITMQ_URL to run the RabbitMQ topology test")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	suffix := uuid.NewString()
	queue := "topology-test." + suffix
	key := "svc-" + suffix + ".do"
	t.Cleanup(func() { _, _ = ch.QueueDelete(queue, false, false, false) })

	if err := topology.DeclareActionQueue(ch, queue, key); err != nil {
		t.Fatalf("DeclareActionQueue: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, topology.ExchangeAction, key, true, false,
		amqp.Publishing{ContentType: "application/json", Body: []byte(`{"run_id":"r"}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if ok {
			if string(d.Body) != `{"run_id":"r"}` {
				t.Errorf("body = %s", d.Body)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the dispatch never reached the action queue")
}
