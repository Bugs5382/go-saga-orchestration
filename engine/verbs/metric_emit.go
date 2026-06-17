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

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// MetricEmitVerb appends a saga_run_events row of type `metric`. Inputs:
//   - "name"   (required, string)
//   - "value"  (required, number)
//   - "labels" (optional, map[string]string)
//
// Prometheus side-channel wiring is future work; for now the
// event is enough — admin UI surfaces metric events in the run inspector.
type MetricEmitVerb struct {
	S store.Store
}

// Execute appends a metric event carrying the name, value, and labels to the
// run's event stream.
func (v MetricEmitVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	name, _ := step.Inputs["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("metric_emit: name required")
	}
	value, ok := step.Inputs["value"]
	if !ok {
		return nil, fmt.Errorf("metric_emit: value required")
	}
	labels, _ := step.Inputs["labels"].(map[string]any)
	evt := domain.NewEvent(run.ID, step.ID, 0, domain.EventMetric, "workflow")
	evt.Metadata = map[string]any{"name": name, "value": value, "labels": labels}
	if err := v.S.AppendEvent(ctx, evt); err != nil {
		return nil, fmt.Errorf("metric_emit: append event: %w", err)
	}
	return map[string]any{}, nil
}
