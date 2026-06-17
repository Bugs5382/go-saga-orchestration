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
	"fmt"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// WaitForEventVerb pauses the saga until a RabbitMQ event with a
// matching topic + header subset arrives. v1 only does string-equality
// header filtering (CEL on payload deferred to a later batch).
//
// Inputs:
//   - "topic"     (required, string): RabbitMQ routing key the saga awaits.
//   - "headers"   (optional, map[string]any): header→value pairs the
//     incoming event's headers must all match (values stringified).
//   - "timeout_s" (optional, number): max seconds to wait. On timeout the
//     engine routes to the step's "timeout" branch if defined, else to Next.
//     Omitted = wait indefinitely.
type WaitForEventVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute persists the awaited topic, header filter, and optional timeout
// deadline on the run and returns ErrSagaPaused until a matching event arrives
// or the deadline elapses.
func (v WaitForEventVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	topic, _ := step.Inputs["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("wait_for_event: topic required")
	}
	hdrs := map[string]string{}
	if h, ok := step.Inputs["headers"].(map[string]any); ok {
		for k, val := range h {
			hdrs[k] = fmt.Sprint(val)
		}
	}

	var deadline *time.Time
	if to, ok := step.Inputs["timeout_s"].(float64); ok && to > 0 && v.Clock != nil {
		d := v.Clock.Now().Add(time.Duration(to * float64(time.Second)))
		deadline = &d
	}

	if err := v.S.SetPausedAwaitingEventWithDeadline(ctx, run.ID, topic, hdrs, deadline); err != nil {
		return nil, fmt.Errorf("wait_for_event: persist: %w", err)
	}
	return nil, ErrSagaPaused
}
