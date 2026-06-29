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
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestMetricEmit_AppendsEvent(t *testing.T) {
	s := memory.New()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(context.Background(), run)
	_, err := MetricEmitVerb{S: s}.Execute(context.Background(), run,
		domain.Step{ID: "m", Inputs: map[string]any{"name": "decision.evaluated", "value": 1}})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	events, _ := s.ListEventsByRun(context.Background(), run.ID)
	var found bool
	for _, e := range events {
		if e.EventType == domain.EventMetric && e.Metadata["name"] == "decision.evaluated" {
			found = true
		}
	}
	if !found {
		t.Errorf("EventMetric not found in %+v", events)
	}
}
