// Package engine contains the saga coordinator and the built-in verb
// dispatch table.
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
	"fmt"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// Publisher is the minimum surface the Coordinator needs from mq.Publisher:
// enqueue a saga.advance message so the next step runs. Using an interface
// (instead of *mq.Publisher directly) lets in-process tests supply a fake.
// *mq.Publisher satisfies this interface.
type Publisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// Coordinator wires the saga.advance consumer to the per-run advance
// loop. It owns no goroutines beyond the consumer; Advance does its
// work synchronously per message.
type Coordinator struct {
	store     store.Store
	publisher Publisher // interface; *mq.Publisher in production, fake in tests
	verbs     verbs.Registry
	clock     clock.Clock
	secrets   secrets.Resolver
	licensing licensing.Resolver
}

// NewCoordinator constructs a Coordinator. pub is the Publisher used to
// re-enqueue saga.advance (multi-step sagas advance one queue message at a
// time) and to start child runs spawned by the parallel verb. pub may be nil
// in tests that only exercise synchronous / non-spawning verbs.
// actionPub is the ActionDispatchPublisher for the action verb. Pass
// nil in tests that do not exercise action steps.
// lr is the license resolver; pass licensing.StubAllowAll{} for dev/test.
// A nil lr is normalised to StubAllowAll so callers need not guard.
// emitter is the EventEmitter used by emit_event steps. Pass nil in tests
// that do not exercise emit_event — EmitEventVerb checks for nil.
// opts wire optional verb dependencies, e.g. the http/rmq action dispatchers
// for the dispatch-descriptor feature (verbs.WithHTTPDispatcher /
// verbs.WithRMQDispatcher); omit them for the gRPC-only default. (issue #59)
func NewCoordinator(s store.Store, pub Publisher, clk clock.Clock, sec secrets.Resolver, lr licensing.Resolver, actionPub verbs.ActionDispatchPublisher, emitter verbs.EventEmitter, opts ...verbs.DefaultOption) *Coordinator {
	if lr == nil {
		lr = licensing.StubAllowAll{}
	}
	c := &Coordinator{
		store:     s,
		publisher: pub,
		verbs:     verbs.Default(s, clk, sec, pub, actionPub, emitter, opts...),
		clock:     clk,
		secrets:   sec,
		licensing: lr,
	}
	// Re-register cancel with the coordinator as its parent-join checker, so a
	// target-cancel re-evaluates a join-waiting parent. (Default has no checker
	// because the coordinator doesn't exist yet when Default runs.)
	c.RegisterVerb(domain.StepTypeCancel, verbs.CancelVerb{S: s, JoinChecker: c}, "loops_and_recovery")
	return c
}

// RegisterVerb adds or replaces a verb handler in the coordinator's registry,
// extending the engine with custom step types without rebuilding it.
func (c *Coordinator) RegisterVerb(stepType domain.StepType, handler verbs.Handler, licenseGroup string) {
	c.verbs[stepType] = verbs.RegistryEntry{Handler: handler, LicenseGroup: licenseGroup}
}

// CheckParentJoin re-evaluates the parent join of run's parent (if any), waking
// the parent when the join is satisfied. Exported for verbs (e.g. cancel) that
// terminate a run outside the normal Advance flow.
func (c *Coordinator) CheckParentJoin(ctx context.Context, run domain.SagaRun) {
	c.checkParentJoin(ctx, run)
}

// Cancel terminates an in-flight run from outside the run — the run-level
// counterpart to the in-step cancel verb. An external caller (e.g. an
// approval policy withdrawing or re-submitting while a run is paused at a
// manual_approval) uses this to abort the prior run so its pending user
// tasks leave the approver's inbox and a fresh run can start, instead of
// reaching around the API into the store.
//
// The store transition is atomic: terminal cancelled + terminal_at, reason
// recorded in last_error, open user tasks closed, awaited signal/event and
// pending wakeup cleared. Idempotent — a no-op when the run is already
// terminal. If the cancelled run is a child, its parent's join is
// re-evaluated so the parent is not left waiting. See issue #80.
func (c *Coordinator) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	run, err := c.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("cancel run %s: %w", runID, err)
	}
	if run.State.IsTerminal() {
		return nil // idempotent — nothing to cancel
	}
	if err := c.store.Cancel(ctx, runID, reason); err != nil {
		return fmt.Errorf("cancel run %s: %w", runID, err)
	}
	if run.ParentRunID != nil {
		c.checkParentJoin(ctx, run)
	}
	return nil
}
