// Package topology names and declares the RabbitMQ exchanges and queues the
// engine and its workers share.
//
// The engine publishes every action and compensation dispatch to the direct
// exchange ExchangeAction with the action's full name as the routing key
// (for example "billing.charge"). A worker declares a durable queue and binds
// it once per action name with DeclareActionQueue. Direct exchanges match
// routing keys exactly, so wildcard bindings such as "billing.*" never match.
package topology

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
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchange and queue names.
const (
	// ExchangeAction is the direct exchange action and compensation
	// dispatches are published to, keyed by action name.
	ExchangeAction = "action.direct"
	// ExchangeWorkflowEvents is the topic exchange emit_event and
	// wait_for_event use.
	ExchangeWorkflowEvents = "workflow.events"
	// QueueSagaAdvance carries saga.advance messages between engine replicas.
	QueueSagaAdvance = "saga.advance"
	// QueueSagaDLQ holds saga.advance messages that could not be processed.
	QueueSagaDLQ = "saga.dlq"
	// QueueActionDLQ holds action dispatches a worker rejected.
	QueueActionDLQ = "action.dlq"
)

// Channel is the part of *amqp.Channel the declarations use.
type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

var _ Channel = (*amqp.Channel)(nil)

// Declare declares the exchanges and queues the engine owns. It is
// idempotent.
func Declare(ch Channel) error {
	if err := ch.ExchangeDeclare(ExchangeAction, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("topology: exchange %s: %w", ExchangeAction, err)
	}
	if err := ch.ExchangeDeclare(ExchangeWorkflowEvents, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("topology: exchange %s: %w", ExchangeWorkflowEvents, err)
	}
	for _, name := range []string{QueueSagaAdvance, QueueSagaDLQ, QueueActionDLQ} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("topology: queue %s: %w", name, err)
		}
	}
	return nil
}

// DeclareActionQueue declares ExchangeAction, a durable queue, and one exact
// binding per routing key (each an action's full name). It is idempotent.
func DeclareActionQueue(ch Channel, queue string, routingKeys ...string) error {
	if queue == "" {
		return errors.New("topology: queue name required")
	}
	if len(routingKeys) == 0 {
		return errors.New("topology: at least one routing key required")
	}
	for i, k := range routingKeys {
		if k == "" {
			return fmt.Errorf("topology: routing key %d is empty", i)
		}
	}
	if err := ch.ExchangeDeclare(ExchangeAction, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("topology: exchange %s: %w", ExchangeAction, err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("topology: queue %s: %w", queue, err)
	}
	for _, k := range routingKeys {
		if err := ch.QueueBind(queue, k, ExchangeAction, false, nil); err != nil {
			return fmt.Errorf("topology: bind %s to %s: %w", queue, k, err)
		}
	}
	return nil
}
