package verbs

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

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// EmitEventVerb publishes an event via the configured EventEmitter. Inputs:
//   - "topic"   (required, string)
//   - "headers" (optional, map[string]any -> stringified)
//   - "payload" (optional, map[string]any)
type EmitEventVerb struct {
	Emitter EventEmitter
}

// Execute implements Handler.
func (v EmitEventVerb) Execute(ctx context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
	topic, _ := step.Inputs["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("emit_event: topic required")
	}
	if v.Emitter == nil {
		return nil, fmt.Errorf("emit_event: no EventEmitter configured")
	}
	hdrs := map[string]string{}
	if h, ok := step.Inputs["headers"].(map[string]any); ok {
		for k, val := range h {
			hdrs[k] = fmt.Sprint(val)
		}
	}
	payload, _ := step.Inputs["payload"].(map[string]any)
	if err := v.Emitter.EmitEvent(ctx, topic, hdrs, payload); err != nil {
		return nil, fmt.Errorf("emit_event: %w", err)
	}
	return map[string]any{}, nil
}
