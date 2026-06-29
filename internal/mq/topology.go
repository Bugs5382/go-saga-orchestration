// Package mq wraps the platform RabbitMQ for go-saga-orchestration's needs. Two
// surfaces: the saga.advance work queue (engine consumes), and the
// action.* exchange used by step dispatch (workers consume per-service
// queues — declared by their owning service, not by the orchestrator).
package mq

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
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Queue names.
const (
	QueueSagaAdvance = "saga.advance"
	QueueSagaDLQ     = "saga.dlq"
	QueueActionDLQ   = "action.dlq"

	ExchangeAction         = "action.direct"
	ExchangeWorkflowEvents = "workflow.events"
)

// Connect dials RabbitMQ and returns the connection. Caller MUST defer
// conn.Close().
func Connect(url string) (*amqp.Connection, error) {
	if url == "" {
		return nil, fmt.Errorf("rabbitmq: URL is empty")
	}
	return amqp.Dial(url)
}

// DeclareTopology sets up the queues + exchange go-saga-orchestration owns. Idempotent.
// Workers declare their own per-service action queues bound to
// ExchangeAction with their routing key (e.g. example.set_state).
func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeAction, "direct", true /*durable*/, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %s: %w", ExchangeAction, err)
	}
	if err := ch.ExchangeDeclare(ExchangeWorkflowEvents, "topic", true /*durable*/, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %s: %w", ExchangeWorkflowEvents, err)
	}
	for _, name := range []string{QueueSagaAdvance, QueueSagaDLQ, QueueActionDLQ} {
		_, err := ch.QueueDeclare(name, true /*durable*/, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("queue %s: %w", name, err)
		}
	}
	return nil
}
