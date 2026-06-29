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
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// LogVerb appends a saga_run_events row of type `log`. Inputs:
//   - "message" (required, string)
//   - "level"   (optional, string: "info" | "warn" | "error"; default "info")
type LogVerb struct {
	S store.Store
}

// Execute appends a log event carrying the message and level to the run's
// event stream.
func (v LogVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	msg, _ := step.Inputs["message"].(string)
	if msg == "" {
		return nil, fmt.Errorf("log: message required")
	}
	level, _ := step.Inputs["level"].(string)
	if level == "" {
		level = "info"
	}
	evt := domain.NewEvent(run.ID, step.ID, 0, domain.EventLog, "workflow")
	evt.Metadata = map[string]any{"message": msg, "level": level}
	if err := v.S.AppendEvent(ctx, evt); err != nil {
		return nil, fmt.Errorf("log: append event: %w", err)
	}
	return map[string]any{}, nil
}
