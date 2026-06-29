package engine

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

func TestEventSubscriber_MatchingTopicAndHeaders_Publishes(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.SetPausedAwaitingEvent(ctx, r.ID, "foo.bar", map[string]string{"x": "1"})

	pub := &recordingPublisher{}
	sub := &EventSubscriber{S: s, Publisher: pub}

	err := sub.Deliver(ctx, EventDelivery{
		Topic:   "foo.bar",
		Headers: map[string]string{"x": "1", "y": "extra"},
		Body:    []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(pub.runs) != 1 || pub.runs[0] != r.ID.String() {
		t.Errorf("publisher saw %v, want one publish of %s", pub.runs, r.ID)
	}

	// State should be cleared back to running.
	got, _ := s.GetRun(ctx, r.ID)
	if got.AwaitedEventTopic != nil {
		t.Errorf("AwaitedEventTopic should be cleared, got %v", *got.AwaitedEventTopic)
	}
}

func TestEventSubscriber_TopicMismatch_NoPublish(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.SetPausedAwaitingEvent(ctx, r.ID, "foo.bar", nil)

	pub := &recordingPublisher{}
	sub := &EventSubscriber{S: s, Publisher: pub}

	_ = sub.Deliver(ctx, EventDelivery{Topic: "no.match", Headers: nil})

	if len(pub.runs) != 0 {
		t.Errorf("expected no publish, got %v", pub.runs)
	}
}

func TestEventSubscriber_HeaderMismatch_NoPublish(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.SetPausedAwaitingEvent(ctx, r.ID, "foo.bar", map[string]string{"x": "1"})

	pub := &recordingPublisher{}
	sub := &EventSubscriber{S: s, Publisher: pub}

	_ = sub.Deliver(ctx, EventDelivery{
		Topic:   "foo.bar",
		Headers: map[string]string{"x": "2"}, // wrong value
	})
	if len(pub.runs) != 0 {
		t.Errorf("expected no publish on header mismatch, got %v", pub.runs)
	}
}

func TestEventSubscriber_EmptyAwaitedHeaders_MatchesAny(t *testing.T) {
	s := memory.New()
	ctx := context.Background()
	r := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	_ = s.CreateRun(ctx, r)
	_ = s.SetPausedAwaitingEvent(ctx, r.ID, "foo.bar", nil)

	pub := &recordingPublisher{}
	sub := &EventSubscriber{S: s, Publisher: pub}

	_ = sub.Deliver(ctx, EventDelivery{Topic: "foo.bar", Headers: map[string]string{"anything": "goes"}})
	if len(pub.runs) != 1 {
		t.Errorf("expected match with empty awaited headers, got %d publishes", len(pub.runs))
	}
}
