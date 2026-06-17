package mq

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

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher publishes messages to the RabbitMQ broker. Owns its own
// channel (channels are NOT safe for concurrent use; one publisher per
// goroutine that publishes).
type Publisher struct {
	ch *amqp.Channel
}

// NewPublisher allocates a channel on conn for publishing. Caller MUST
// defer p.Close().
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &Publisher{ch: ch}, nil
}

// Close releases the channel.
func (p *Publisher) Close() error {
	if p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

// PublishSagaAdvance enqueues a "please advance saga X" message on the
// saga.advance work queue. Body is JSON-encoded.
func (p *Publisher) PublishSagaAdvance(ctx context.Context, runID string) error {
	body, err := json.Marshal(map[string]string{"saga_run_id": runID})
	if err != nil {
		return err
	}
	return p.ch.PublishWithContext(ctx, "" /*default exchange*/, QueueSagaAdvance, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// PublishActionDispatch publishes a payload to ExchangeAction with the given
// routing key (typically "<service>.<action_name>"). Workers consume from
// per-service queues bound to this exchange.
func (p *Publisher) PublishActionDispatch(ctx context.Context, routingKey string, payload []byte) error {
	return p.ch.PublishWithContext(ctx, ExchangeAction, routingKey, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         payload,
		})
}

// PublishEvent publishes a named event to ExchangeWorkflowEvents with topic
// as the routing key. Headers are published as AMQP headers. Payload is
// JSON-encoded as the body (nil payload encodes as "null").
func (p *Publisher) PublishEvent(ctx context.Context, topic string, headers map[string]string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tbl := amqp.Table{}
	for k, v := range headers {
		tbl[k] = v
	}
	return p.ch.PublishWithContext(ctx, ExchangeWorkflowEvents, topic, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Headers:      tbl,
		Body:         body,
	})
}
