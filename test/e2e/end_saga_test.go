// Package e2e exercises the saga engine end-to-end against the in-memory
// store. Real RabbitMQ + Postgres integration tests are future work.
package e2e

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
	"os"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// loopbackPub captures saga.advance publishes and immediately calls back
// into the coordinator. Lets us drive the full advance loop without
// RabbitMQ in tests. The `end` verb terminates synchronously,
// so this is unused for the trivial test but kept here for symmetry
// with the pattern multi-step sagas need.
type loopbackPub struct {
	coord *engine.Coordinator
	ctx   context.Context
}

func (p *loopbackPub) PublishSagaAdvance(_ context.Context, runID string) error {
	return p.coord.Advance(p.ctx, runID)
}

func TestTrivialSagaEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	raw, err := os.ReadFile("../fixtures/wf_trivial.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	defID, err := s.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		t.Fatalf("upsert definition: %v", err)
	}

	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil) // publisher unused; end verb terminates synchronously
	pub := &loopbackPub{coord: coord, ctx: ctx}
	_ = pub

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.State != domain.RunStateSucceeded {
		t.Errorf("state = %s, want succeeded", got.State)
	}

	events, err := s.ListEventsByRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) < 4 {
		t.Errorf("expected >= 4 events (saga.started, step.dispatched, step.succeeded, run.succeeded), got %d: %+v", len(events), events)
	}
}
