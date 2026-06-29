package e2e

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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// subSagaPub drives child and parent advances in-process, satisfying both
// engine.Publisher and verbs.Publisher (same method shape as parallelPub).
type subSagaPub struct {
	coord *engine.Coordinator
}

func (p *subSagaPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

func TestSubSagaVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Register child workflow first so the parent can resolve it.
	childRaw, err := os.ReadFile("../fixtures/wf_sub_saga_child.json")
	if err != nil {
		t.Fatalf("read child fixture: %v", err)
	}
	var childDef domain.WorkflowDefinition
	if err := json.Unmarshal(childRaw, &childDef); err != nil {
		t.Fatalf("parse child fixture: %v", err)
	}
	_, _ = s.UpsertWorkflowDefinition(ctx, childDef)

	// Register parent workflow.
	parentRaw, err := os.ReadFile("../fixtures/wf_sub_saga_parent.json")
	if err != nil {
		t.Fatalf("read parent fixture: %v", err)
	}
	var parentDef domain.WorkflowDefinition
	if err := json.Unmarshal(parentRaw, &parentDef); err != nil {
		t.Fatalf("parse parent fixture: %v", err)
	}
	parentDefID, _ := s.UpsertWorkflowDefinition(ctx, parentDef)

	pub := &subSagaPub{}
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	pub.coord = coord

	run := domain.NewSagaRun(parentDef.ID, parentDefID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// After the initial advance, the parent is paused (sub_saga spawned the
	// child). The goroutine-driven Advance for the child will complete the child,
	// which triggers checkParentJoin â†’ WakeFromExternal + PublishSagaAdvance(parent),
	// allowing the parent to resume and reach "end" â†’ succeeded.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("parent did not reach succeeded; state=%s", got.State)
}
