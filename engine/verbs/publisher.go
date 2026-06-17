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

import "context"

// Publisher is the minimum surface verbs need to enqueue saga.advance
// for child runs. Real impl: mq.Publisher. Tests: a fake.
// Structurally compatible with engine.Publisher so *mq.Publisher satisfies both.
type Publisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// ActionDispatchPublisher is a separate interface for publishing action dispatch
// messages to RabbitMQ. Kept separate so existing test pubs that only implement
// PublishSagaAdvance do not need to change.
type ActionDispatchPublisher interface {
	PublishActionDispatch(ctx context.Context, routingKey string, payload []byte) error
}

// EventEmitter publishes an event other sagas/triggers can match. Real impls:
// an in-process matcher (embedded) or a RabbitMQ publisher (service mode).
type EventEmitter interface {
	EmitEvent(ctx context.Context, topic string, headers map[string]string, payload map[string]any) error
}
