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
	"errors"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// fakeEmitter captures calls to EmitEvent.
type fakeEmitter struct {
	topic   string
	headers map[string]string
	payload map[string]any
	err     error
}

func (f *fakeEmitter) EmitEvent(_ context.Context, topic string, headers map[string]string, payload map[string]any) error {
	f.topic = topic
	f.headers = headers
	f.payload = payload
	return f.err
}

func TestEmitEvent_HappyPath(t *testing.T) {
	fe := &fakeEmitter{}
	v := EmitEventVerb{Emitter: fe}
	step := domain.Step{
		ID:   "s1",
		Type: domain.StepTypeEmitEvent,
		Inputs: map[string]any{
			"topic":   "order.placed",
			"headers": map[string]any{"env": "test"},
			"payload": map[string]any{"order_id": "123"},
		},
	}
	out, err := v.Execute(context.Background(), domain.SagaRun{}, step)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output, got %v", out)
	}
	if fe.topic != "order.placed" {
		t.Errorf("topic = %q, want order.placed", fe.topic)
	}
	if fe.headers["env"] != "test" {
		t.Errorf("headers = %v, want env=test", fe.headers)
	}
	if fe.payload["order_id"] != "123" {
		t.Errorf("payload = %v, want order_id=123", fe.payload)
	}
}

func TestEmitEvent_EmptyTopic_Errors(t *testing.T) {
	v := EmitEventVerb{Emitter: &fakeEmitter{}}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"topic": ""},
	})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestEmitEvent_NilEmitter_Errors(t *testing.T) {
	v := EmitEventVerb{Emitter: nil}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"topic": "x"},
	})
	if err == nil {
		t.Fatal("expected error for nil emitter")
	}
}

func TestEmitEvent_EmitterError_Propagates(t *testing.T) {
	fe := &fakeEmitter{err: errors.New("broker down")}
	v := EmitEventVerb{Emitter: fe}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"topic": "x"},
	})
	if err == nil {
		t.Fatal("expected error from emitter")
	}
}

func TestEmitEvent_NoHeadersOrPayload_OK(t *testing.T) {
	fe := &fakeEmitter{}
	v := EmitEventVerb{Emitter: fe}
	_, err := v.Execute(context.Background(), domain.SagaRun{}, domain.Step{
		Inputs: map[string]any{"topic": "ping"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fe.headers) != 0 {
		t.Errorf("expected empty headers, got %v", fe.headers)
	}
}
