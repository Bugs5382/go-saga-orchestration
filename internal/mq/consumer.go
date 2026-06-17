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
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SagaAdvanceMsg is the body of a saga.advance message.
type SagaAdvanceMsg struct {
	SagaRunID string `json:"saga_run_id"`
}

// AdvanceHandler processes one saga.advance message. Return nil to ACK,
// non-nil to NACK (which redelivers per RabbitMQ defaults).
type AdvanceHandler func(ctx context.Context, msg SagaAdvanceMsg) error

// ConsumeSagaAdvance starts a competing-consumer on QueueSagaAdvance.
// Blocks until ctx is cancelled or the channel errors out.
func ConsumeSagaAdvance(ctx context.Context, conn *amqp.Connection, h AdvanceHandler) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	// Fairness: prefetch=1 means each consumer holds one in-flight msg at a time.
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := ch.Consume(QueueSagaAdvance, "" /*consumer tag*/, false /*autoAck*/, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			var msg SagaAdvanceMsg
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				// Malformed message → reject without requeue → DLQ.
				_ = d.Reject(false)
				continue
			}
			if err := h(ctx, msg); err != nil {
				_ = d.Nack(false, true) // requeue
				continue
			}
			_ = d.Ack(false)
		}
	}
}
