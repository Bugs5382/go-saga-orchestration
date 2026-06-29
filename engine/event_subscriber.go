package engine

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
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// EventDelivery is the minimum shape EventSubscriber needs from a
// RabbitMQ delivery. Production wires from amqp.Delivery; tests
// synthesise it directly.
type EventDelivery struct {
	Topic   string            // RabbitMQ routing key
	Headers map[string]string // string-keyed headers (RMQ amqp.Table values stringified)
	Body    []byte
}

// EventSubscriber matches incoming RabbitMQ events to paused sagas
// awaiting matching topic + header subset. On match: clear pause +
// publish saga.advance.
type EventSubscriber struct {
	S          store.Store
	Publisher  TimerPublisher     // same interface as Timer
	Dispatcher *TriggerDispatcher // optional; nil = wake-only mode
}

// Deliver processes one event delivery. Returns nil if no matching
// run; returns the first error encountered while updating store /
// publishing.
func (e *EventSubscriber) Deliver(ctx context.Context, d EventDelivery) error {
	runs, err := e.S.FindRunsByAwaitedEvent(ctx, d.Topic)
	if err != nil {
		return fmt.Errorf("find runs by awaited event: %w", err)
	}
	for _, run := range runs {
		if !headersSubset(run.AwaitedEventHeaders, d.Headers) {
			continue
		}
		if err := e.S.WakeFromExternal(ctx, run.ID); err != nil {
			return fmt.Errorf("wake from external for %s: %w", run.ID, err)
		}
		if e.Publisher != nil {
			if err := e.Publisher.PublishSagaAdvance(ctx, run.ID.String()); err != nil {
				return fmt.Errorf("publish advance for %s: %w", run.ID, err)
			}
		}
	}
	return nil
}

// headersSubset reports whether all keys in `want` are present in
// `have` with matching string values. Empty `want` always matches.
func headersSubset(want, have map[string]string) bool {
	for k, v := range want {
		if hv, ok := have[k]; !ok || hv != v {
			return false
		}
	}
	return true
}

// RunRMQ binds a per-pod queue to the workflow.events exchange and
// consumes deliveries indefinitely. ACKs every delivery (events are
// fire-and-forget; redelivery semantics are not useful here).
func (e *EventSubscriber) RunRMQ(ctx context.Context, conn *amqp.Connection, queueName string) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	q, err := ch.QueueDeclare(queueName, false /*durable*/, true /*autoDelete*/, true /*exclusive*/, false, nil)
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}
	if err := ch.QueueBind(q.Name, "#", mq.ExchangeWorkflowEvents, false, nil); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}

	deliveries, err := ch.Consume(q.Name, "" /*consumer tag*/, true /*autoAck*/, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("event subscriber channel closed")
			}
			hdrs := map[string]string{}
			for k, v := range d.Headers {
				hdrs[k] = fmt.Sprint(v)
			}
			env := EventDelivery{Topic: d.RoutingKey, Headers: hdrs, Body: d.Body}
			if err := e.Deliver(ctx, env); err != nil {
				log.Error().Err(err).Str("topic", d.RoutingKey).Msg("event subscriber: deliver")
			}
			if e.Dispatcher != nil {
				if err := e.Dispatcher.Dispatch(ctx, env); err != nil {
					log.Error().Err(err).Str("topic", d.RoutingKey).Msg("trigger dispatcher")
				}
			}
		}
	}
}
