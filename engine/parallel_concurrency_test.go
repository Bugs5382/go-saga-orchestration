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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// inlineChildPublisher runs every child advance synchronously, inside the
// parallel verb's publish call, which is the worst case for ordering: each
// child finishes before the verb returns. Advances of any other run (the
// parent's wakeup) go to a goroutine, as the in-process publisher does.
type inlineChildPublisher struct {
	coord  *Coordinator
	parent uuid.UUID
	wg     sync.WaitGroup
}

func (p *inlineChildPublisher) PublishSagaAdvance(ctx context.Context, runID string) error {
	if runID == p.parent.String() {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			_ = p.coord.Advance(ctx, runID)
		}()
		return nil
	}
	return p.coord.Advance(ctx, runID)
}

func parallelDef(n int) domain.WorkflowDefinition {
	branches := make([]any, n)
	for i := range branches {
		branches[i] = map[string]any{"type": "set_var", "inputs": map[string]any{"out_var": "v", "value": i}}
	}
	return domain.WorkflowDefinition{
		ID: "wf_par_conc", Version: 1, Start: "fanout", Published: true,
		Steps: []domain.Step{
			{ID: "fanout", Type: domain.StepTypeParallel, Inputs: map[string]any{"join_strategy": "all", "branches": branches}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}
}

func waitTerminal(t *testing.T, s *memory.Store, id uuid.UUID) domain.SagaRun {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r, _ := s.GetRun(context.Background(), id)
		if r.State.IsTerminal() || time.Now().After(deadline) {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Children that finish before the parallel step has returned must still wake
// the parent. Before the fix the parent was marked paused only after every
// child had been published, so fast children found it running, skipped the
// wakeup, and the parent stayed paused forever.
func TestParallel_ChildrenFinishingFirstStillWakeParent(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	defID, _ := s.UpsertWorkflowDefinition(ctx, parallelDef(3))
	parent := domain.NewSagaRun("wf_par_conc", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)

	pub := &inlineChildPublisher{parent: parent.ID}
	c := NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, nil, nil)
	pub.coord = c

	if err := c.Advance(ctx, parent.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	pub.wg.Wait()
	got := waitTerminal(t, s, parent.ID)
	if got.State != domain.RunStateSucceeded {
		t.Fatalf("parent state = %s at step %q, want succeeded (lost wakeup)", got.State, got.CurrentStep)
	}
	if n := len(got.Variables["_parallel"].(map[string]any)["fanout"].(map[string]any)["branches"].([]any)); n != 3 {
		t.Fatalf("aggregated %d branches, want 3", n)
	}
}

// Two siblings finishing together can both wake the parent. The parent's
// parallel step must run once: never spawn a second set of children.
func TestParallel_ConcurrentWakeupsRunParentOnce(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		ctx := context.Background()
		s := memory.New()
		defID, _ := s.UpsertWorkflowDefinition(ctx, parallelDef(2))
		parent := domain.NewSagaRun("wf_par_conc", defID, nil, map[string]any{})
		_ = s.CreateRun(ctx, parent)
		c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, nil, nil)

		// Run the parallel step: it spawns two children and pauses. With a
		// nil publisher nothing advances the children yet.
		if err := c.Advance(ctx, parent.ID.String()); err != nil {
			t.Fatalf("advance: %v", err)
		}
		children, _ := s.ListChildrenByParent(ctx, parent.ID, "fanout")
		for _, ch := range children {
			if err := c.Advance(ctx, ch.ID.String()); err != nil {
				t.Fatalf("advance child: %v", err)
			}
		}
		// The last child woke the parent. Deliver the same wakeup many times at
		// once, as duplicate saga.advance messages would.
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = c.Advance(ctx, parent.ID.String())
			}()
		}
		wg.Wait()

		children, _ = s.ListChildrenByParent(ctx, parent.ID, "fanout")
		if len(children) != 2 {
			t.Fatalf("iter %d: %d children, want 2 (parallel step ran more than once)", iter, len(children))
		}
		got, _ := s.GetRun(ctx, parent.ID)
		if got.State != domain.RunStateSucceeded {
			t.Fatalf("iter %d: parent state = %s, want succeeded", iter, got.State)
		}
		events, _ := s.ListEventsByRun(ctx, parent.ID)
		succeeded := 0
		for _, e := range events {
			if e.StepID == "fanout" && e.EventType == domain.EventStepSucceeded {
				succeeded++
			}
		}
		if succeeded != 1 {
			t.Fatalf("iter %d: fanout step.succeeded recorded %d times, want 1", iter, succeeded)
		}
		if n := c.runLocks.size(); n != 0 {
			t.Fatalf("iter %d: %d run locks left after all advances returned, want 0", iter, n)
		}
	}
}

// An advance that reaches a parent before its children are done (a stray or
// duplicate message) must leave the parent paused on the spawning step.
func TestParallel_EarlyAdvanceDoesNotResumeParent(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	defID, _ := s.UpsertWorkflowDefinition(ctx, parallelDef(2))
	parent := domain.NewSagaRun("wf_par_conc", defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, parent)
	c := NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, nil, nil)
	if err := c.Advance(ctx, parent.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	children, _ := s.ListChildrenByParent(ctx, parent.ID, "fanout")
	// Only one of the two children finishes.
	if err := c.Advance(ctx, children[0].ID.String()); err != nil {
		t.Fatalf("advance child: %v", err)
	}
	if err := c.Advance(ctx, parent.ID.String()); err != nil {
		t.Fatalf("early advance: %v", err)
	}
	got, _ := s.GetRun(ctx, parent.ID)
	if got.State != domain.RunStatePaused || got.CurrentStep != "fanout" {
		t.Fatalf("parent = %s at %q, want paused at fanout until both children finish", got.State, got.CurrentStep)
	}
	// The second child finishing wakes it for real.
	if err := c.Advance(ctx, children[1].ID.String()); err != nil {
		t.Fatalf("advance child: %v", err)
	}
	if err := c.Advance(ctx, parent.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got, _ := s.GetRun(ctx, parent.ID); got.State != domain.RunStateSucceeded {
		t.Fatalf("parent = %s, want succeeded", got.State)
	}
}
