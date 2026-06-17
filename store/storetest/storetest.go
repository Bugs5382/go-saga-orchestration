// Package storetest provides a backend-agnostic conformance ("contract") test
// suite for the store.Store interface. Each store implementation (memory,
// postgres, redis, ...) wires it up via a _test.go file that calls RunSuite
// with a factory returning a fresh, empty store. The suite exercises the full
// behavioral contract of store.Store so every backend stays in lockstep.
package storetest

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// RunSuite runs every behavior group in the store contract against the
// implementation produced by newStore. Each group obtains its own fresh store
// so the groups are fully isolated from one another.
func RunSuite(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()
	groups := []struct {
		name string
		fn   func(t *testing.T, s store.Store)
	}{
		{"Definitions", testDefinitions},
		{"PublishedWorkflow", testPublishedWorkflow},
		{"Runs", testRuns},
		{"RunState", testRunState},
		{"RunVariables", testRunVariables},
		{"Events", testEvents},
		{"Rules", testRules},
		{"PauseWakeup", testPauseWakeup},
		{"Signals", testSignals},
		{"AwaitedEvent", testAwaitedEvent},
		{"ClearAndWake", testClearAndWake},
		{"ChildRuns", testChildRuns},
		{"TryCatch", testTryCatch},
		{"UserTasks", testUserTasks},
		{"ActionRegistry", testActionRegistry},
		{"ActionDispatch", testActionDispatch},
		{"Triggers", testTriggers},
		{"ListRuns", testListRuns},
		{"Stats", testStats},
	}
	for _, g := range groups {
		g := g
		t.Run(g.name, func(t *testing.T) {
			g.fn(t, newStore(t))
		})
	}
}

// --- helpers ----------------------------------------------------------------

func ctx() context.Context { return context.Background() }

// isNotFound reports whether err is (wraps) a store.ErrNotFound.
func isNotFound(err error) bool {
	var nf store.ErrNotFound
	return errors.As(err, &nf)
}

// requireNotFound fails the test unless err is a store.ErrNotFound.
func requireNotFound(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected ErrNotFound, got nil", what)
	}
	if !isNotFound(err) {
		t.Fatalf("%s: expected store.ErrNotFound, got %T: %v", what, err, err)
	}
}

// requireNoErr fails the test if err is non-nil.
func requireNoErr(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", what, err)
	}
}

// newRun builds a minimal pending run for workflowID with a fresh def ID.
func newRun(workflowID string) domain.SagaRun {
	return domain.NewSagaRun(workflowID, uuid.New(), nil, map[string]any{})
}

// seedRun creates a run in s and returns it (failing the test on error).
func seedRun(t *testing.T, s store.Store, workflowID string) domain.SagaRun {
	t.Helper()
	r := newRun(workflowID)
	requireNoErr(t, s.CreateRun(ctx(), r), "CreateRun")
	return r
}

// seedDef builds a single-step "end" workflow definition.
func seedDef(workflowID string, version int, published bool) domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID:        workflowID,
		Version:   version,
		Name:      workflowID,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: published,
	}
}

// decisionRule builds a published decision-table rule definition.
func decisionRule(ruleID string, version int, published bool) domain.RuleDefinition {
	def := domain.NewRuleDefinition(ruleID, version, ruleID, domain.RuleTypeDecisionTable, domain.RuleSpec{
		HitPolicy:     domain.HitPolicyFirst,
		Rows:          []domain.DecisionTableRow{{When: "x==1", Then: map[string]any{"branch": "a"}}},
		DefaultOutput: map[string]any{"branch": "b"},
	}, "by")
	def.Published = published
	return def
}

// --- groups -----------------------------------------------------------------

func testDefinitions(t *testing.T, s store.Store) {
	id, err := s.UpsertWorkflowDefinition(ctx(), seedDef("wf", 1, true))
	requireNoErr(t, err, "UpsertWorkflowDefinition")
	if id == uuid.Nil {
		t.Fatal("UpsertWorkflowDefinition returned the nil UUID")
	}

	got, err := s.GetWorkflowDefinition(ctx(), id)
	requireNoErr(t, err, "GetWorkflowDefinition")
	if got.ID != "wf" || got.Version != 1 {
		t.Errorf("GetWorkflowDefinition = %+v, want id=wf version=1", got)
	}

	_, err = s.GetWorkflowDefinition(ctx(), uuid.New())
	requireNotFound(t, err, "GetWorkflowDefinition(random)")
}

func testPublishedWorkflow(t *testing.T, s store.Store) {
	// v1 published, then v2 unpublished. The newest *published* version is v1.
	_, err := s.UpsertWorkflowDefinition(ctx(), seedDef("wf", 1, true))
	requireNoErr(t, err, "upsert v1")
	_, err = s.UpsertWorkflowDefinition(ctx(), seedDef("wf", 2, false))
	requireNoErr(t, err, "upsert v2")

	got, err := s.GetPublishedWorkflowByID(ctx(), "wf", nil)
	requireNoErr(t, err, "GetPublishedWorkflowByID")
	if got.Version != 1 || !got.Published {
		t.Errorf("published lookup = version %d published=%v, want newest published (v1)", got.Version, got.Published)
	}

	// No published versions at all → fall back to the most recently upserted.
	_, err = s.UpsertWorkflowDefinition(ctx(), seedDef("draft", 1, false))
	requireNoErr(t, err, "upsert draft v1")
	_, err = s.UpsertWorkflowDefinition(ctx(), seedDef("draft", 2, false))
	requireNoErr(t, err, "upsert draft v2")
	got, err = s.GetPublishedWorkflowByID(ctx(), "draft", nil)
	requireNoErr(t, err, "GetPublishedWorkflowByID(draft)")
	if got.Version != 2 {
		t.Errorf("fallback lookup = version %d, want most recent (v2)", got.Version)
	}

	_, err = s.GetPublishedWorkflowByID(ctx(), "unknown", nil)
	requireNotFound(t, err, "GetPublishedWorkflowByID(unknown)")
}

func testRuns(t *testing.T, s store.Store) {
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{"seed": 1})
	requireNoErr(t, s.CreateRun(ctx(), run), "CreateRun")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.ID != run.ID || got.WorkflowID != "wf" {
		t.Errorf("GetRun = %+v, want id=%s wf=wf", got, run.ID)
	}
	if got.Inputs["seed"] != 1 {
		t.Errorf("GetRun inputs = %+v, want seed=1", got.Inputs)
	}

	_, err = s.GetRun(ctx(), uuid.New())
	requireNotFound(t, err, "GetRun(random)")
}

func testRunState(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.UpdateRunState(ctx(), run.ID, domain.RunStateRunning, "step2"), "UpdateRunState")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.State != domain.RunStateRunning || got.CurrentStep != "step2" {
		t.Errorf("after UpdateRunState: state=%s step=%s, want running/step2", got.State, got.CurrentStep)
	}

	err = s.UpdateRunState(ctx(), uuid.New(), domain.RunStateRunning, "x")
	requireNotFound(t, err, "UpdateRunState(random)")
}

func testRunVariables(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.UpdateRunVariables(ctx(), run.ID, map[string]any{"a": 1, "scope.x": 2}), "UpdateRunVariables")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.Variables["a"] != 1 {
		t.Errorf("Variables[a] = %v, want 1", got.Variables["a"])
	}
	scope, ok := got.Variables["scope"].(map[string]any)
	if !ok {
		t.Fatalf("Variables[scope] = %T, want map[string]any", got.Variables["scope"])
	}
	if scope["x"] != 2 {
		t.Errorf("Variables[scope][x] = %v, want 2", scope["x"])
	}

	err = s.UpdateRunVariables(ctx(), uuid.New(), map[string]any{"a": 1})
	requireNotFound(t, err, "UpdateRunVariables(random)")
}

func testEvents(t *testing.T, s store.Store) {
	runID := uuid.New()
	e1 := domain.NewEvent(runID, "s1", 0, domain.EventStepSucceeded, "engine")
	e2 := domain.NewEvent(runID, "s2", 1, domain.EventStepSucceeded, "engine")
	requireNoErr(t, s.AppendEvent(ctx(), e1), "AppendEvent e1")
	requireNoErr(t, s.AppendEvent(ctx(), e2), "AppendEvent e2")

	evts, err := s.ListEventsByRun(ctx(), runID)
	requireNoErr(t, err, "ListEventsByRun")
	if len(evts) != 2 || evts[0].ID != e1.ID || evts[1].ID != e2.ID {
		t.Errorf("ListEventsByRun did not return events in append order: %+v", evts)
	}

	// Unknown run → empty slice, NOT an error.
	empty, err := s.ListEventsByRun(ctx(), uuid.New())
	requireNoErr(t, err, "ListEventsByRun(unknown)")
	if len(empty) != 0 {
		t.Errorf("ListEventsByRun(unknown) = %+v, want empty", empty)
	}

	got, err := s.GetEventByID(ctx(), e1.ID)
	requireNoErr(t, err, "GetEventByID")
	if got.ID != e1.ID {
		t.Errorf("GetEventByID = %s, want %s", got.ID, e1.ID)
	}

	_, err = s.GetEventByID(ctx(), uuid.New())
	requireNotFound(t, err, "GetEventByID(random)")
}

func testRules(t *testing.T, s store.Store) {
	// Zero ID → generated and returned.
	def := decisionRule("r", 1, true)
	def.ID = uuid.Nil
	gen, err := s.UpsertRuleDefinition(ctx(), def)
	requireNoErr(t, err, "UpsertRuleDefinition(zero id)")
	if gen == uuid.Nil {
		t.Fatal("UpsertRuleDefinition(zero id) returned the nil UUID")
	}

	// Caller-set ID → preserved.
	preset := decisionRule("r2", 1, true)
	preset.ID = uuid.New()
	ret, err := s.UpsertRuleDefinition(ctx(), preset)
	requireNoErr(t, err, "UpsertRuleDefinition(preset id)")
	if ret != preset.ID {
		t.Errorf("UpsertRuleDefinition returned %s, want preserved %s", ret, preset.ID)
	}
	// Re-upsert the same UUID: must not create a duplicate version entry.
	_, err = s.UpsertRuleDefinition(ctx(), preset)
	requireNoErr(t, err, "re-upsert preset id")
	got, err := s.GetPublishedRuleByID(ctx(), "r2", nil)
	requireNoErr(t, err, "GetPublishedRuleByID(r2)")
	if got.ID != preset.ID {
		t.Errorf("GetPublishedRuleByID(r2) = %s, want %s", got.ID, preset.ID)
	}

	// Published path: newest published version wins.
	got, err = s.GetPublishedRuleByID(ctx(), "r", nil)
	requireNoErr(t, err, "GetPublishedRuleByID(r)")
	if !got.Published || got.RuleID != "r" {
		t.Errorf("GetPublishedRuleByID(r) = %+v, want published rule r", got)
	}

	// Fallback path: none published → most recent.
	d1 := decisionRule("draft", 1, false)
	d1.ID = uuid.New()
	d2 := decisionRule("draft", 2, false)
	d2.ID = uuid.New()
	_, err = s.UpsertRuleDefinition(ctx(), d1)
	requireNoErr(t, err, "upsert draft v1")
	_, err = s.UpsertRuleDefinition(ctx(), d2)
	requireNoErr(t, err, "upsert draft v2")
	got, err = s.GetPublishedRuleByID(ctx(), "draft", nil)
	requireNoErr(t, err, "GetPublishedRuleByID(draft)")
	if got.Version != 2 {
		t.Errorf("rule fallback = version %d, want most recent (v2)", got.Version)
	}

	_, err = s.GetPublishedRuleByID(ctx(), "unknown", nil)
	requireNotFound(t, err, "GetPublishedRuleByID(unknown)")
}

func testPauseWakeup(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	wakeup := time.Now().UTC().Add(time.Minute)
	requireNoErr(t, s.SetPausedWithWakeup(ctx(), run.ID, wakeup), "SetPausedWithWakeup")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.WakeupAt == nil || !got.WakeupAt.Equal(wakeup) {
		t.Errorf("WakeupAt = %v, want %v", got.WakeupAt, wakeup)
	}

	// now after the wakeup → included.
	ids, err := s.FindRunsByDueWakeup(ctx(), wakeup.Add(time.Second), 10)
	requireNoErr(t, err, "FindRunsByDueWakeup(after)")
	if !containsID(ids, run.ID) {
		t.Errorf("FindRunsByDueWakeup(after) = %v, want to include %s", ids, run.ID)
	}

	// now before the wakeup → excluded.
	ids, err = s.FindRunsByDueWakeup(ctx(), wakeup.Add(-time.Minute), 10)
	requireNoErr(t, err, "FindRunsByDueWakeup(before)")
	if containsID(ids, run.ID) {
		t.Errorf("FindRunsByDueWakeup(before) = %v, want to exclude %s", ids, run.ID)
	}

	// Limit is respected.
	for i := 0; i < 3; i++ {
		r := seedRun(t, s, "wf")
		requireNoErr(t, s.SetPausedWithWakeup(ctx(), r.ID, wakeup), "SetPausedWithWakeup extra")
	}
	limited, err := s.FindRunsByDueWakeup(ctx(), wakeup.Add(time.Second), 2)
	requireNoErr(t, err, "FindRunsByDueWakeup(limit)")
	if len(limited) > 2 {
		t.Errorf("FindRunsByDueWakeup with limit 2 returned %d ids", len(limited))
	}
}

func testSignals(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.SetPausedAwaitingSignal(ctx(), run.ID, "go", nil), "SetPausedAwaitingSignal")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.AwaitedSignal == nil || *got.AwaitedSignal != "go" {
		t.Errorf("AwaitedSignal = %v, want go", got.AwaitedSignal)
	}

	// Wrong signal name → (false, nil).
	ok, err := s.TryConsumeAwaitedSignal(ctx(), run.ID, "nope")
	requireNoErr(t, err, "TryConsumeAwaitedSignal(wrong)")
	if ok {
		t.Error("TryConsumeAwaitedSignal(wrong name) = true, want false")
	}

	// Correct name → (true, nil) and clears markers.
	ok, err = s.TryConsumeAwaitedSignal(ctx(), run.ID, "go")
	requireNoErr(t, err, "TryConsumeAwaitedSignal(go)")
	if !ok {
		t.Error("TryConsumeAwaitedSignal(go) = false, want true")
	}
	got, _ = s.GetRun(ctx(), run.ID)
	if got.AwaitedSignal != nil {
		t.Errorf("AwaitedSignal after consume = %v, want nil", got.AwaitedSignal)
	}

	// Second call → (false, nil) (already consumed).
	ok, err = s.TryConsumeAwaitedSignal(ctx(), run.ID, "go")
	requireNoErr(t, err, "TryConsumeAwaitedSignal(second)")
	if ok {
		t.Error("second TryConsumeAwaitedSignal = true, want false")
	}

	// AppendSignal does not error.
	sig := domain.SagaSignal{ID: uuid.New(), RunID: run.ID, SignalName: "go", ReceivedAt: time.Now().UTC()}
	requireNoErr(t, s.AppendSignal(ctx(), sig), "AppendSignal")
}

func testAwaitedEvent(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.SetPausedAwaitingEvent(ctx(), run.ID, "topic", map[string]string{"k": "v"}), "SetPausedAwaitingEvent")

	got, err := s.GetRun(ctx(), run.ID)
	requireNoErr(t, err, "GetRun")
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}

	runs, err := s.FindRunsByAwaitedEvent(ctx(), "topic")
	requireNoErr(t, err, "FindRunsByAwaitedEvent(topic)")
	if !containsRun(runs, run.ID) {
		t.Errorf("FindRunsByAwaitedEvent(topic) did not include %s", run.ID)
	}
	other, err := s.FindRunsByAwaitedEvent(ctx(), "other")
	requireNoErr(t, err, "FindRunsByAwaitedEvent(other)")
	if containsRun(other, run.ID) {
		t.Errorf("FindRunsByAwaitedEvent(other) incorrectly included %s", run.ID)
	}

	// WithDeadline also sets WakeupAt.
	run2 := seedRun(t, s, "wf")
	deadline := time.Now().UTC().Add(time.Hour)
	requireNoErr(t, s.SetPausedAwaitingEventWithDeadline(ctx(), run2.ID, "topic2", map[string]string{"a": "b"}, &deadline), "SetPausedAwaitingEventWithDeadline")
	got2, _ := s.GetRun(ctx(), run2.ID)
	if got2.WakeupAt == nil || !got2.WakeupAt.Equal(deadline) {
		t.Errorf("WakeupAt = %v, want %v", got2.WakeupAt, deadline)
	}
}

func testClearAndWake(t *testing.T, s store.Store) {
	// ClearPause returns the run to running and clears all markers.
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.SetPausedAwaitingEventWithDeadline(ctx(), run.ID, "topic", map[string]string{"k": "v"}, ptrTime(time.Now().Add(time.Hour))), "set awaiting event")
	requireNoErr(t, s.ClearPause(ctx(), run.ID), "ClearPause")
	got, _ := s.GetRun(ctx(), run.ID)
	if got.State != domain.RunStateRunning {
		t.Errorf("ClearPause state = %s, want running", got.State)
	}
	if got.WakeupAt != nil || got.AwaitedSignal != nil || got.AwaitedEventTopic != nil || got.AwaitedEventHeaders != nil {
		t.Errorf("ClearPause did not clear markers: %+v", got)
	}
	requireNotFound(t, s.ClearPause(ctx(), uuid.New()), "ClearPause(random)")

	// WakeFromExternal clears markers but LEAVES state=paused.
	run2 := seedRun(t, s, "wf")
	requireNoErr(t, s.SetPausedAwaitingSignal(ctx(), run2.ID, "go", ptrTime(time.Now().Add(time.Hour))), "set awaiting signal")
	requireNoErr(t, s.WakeFromExternal(ctx(), run2.ID), "WakeFromExternal")
	got2, _ := s.GetRun(ctx(), run2.ID)
	if got2.State != domain.RunStatePaused {
		t.Errorf("WakeFromExternal state = %s, want paused", got2.State)
	}
	if got2.WakeupAt != nil || got2.AwaitedSignal != nil || got2.AwaitedEventTopic != nil || got2.AwaitedEventHeaders != nil {
		t.Errorf("WakeFromExternal did not clear markers: %+v", got2)
	}
	requireNotFound(t, s.WakeFromExternal(ctx(), uuid.New()), "WakeFromExternal(random)")
}

func testChildRuns(t *testing.T, s store.Store) {
	parent := seedRun(t, s, "wf-parent")
	childDef := seedDef("wf-child", 1, true)

	childID, err := s.SpawnChildRun(ctx(), parent.ID, "stepA", "b0", childDef, map[string]any{"in": 1})
	requireNoErr(t, err, "SpawnChildRun")

	child, err := s.GetRun(ctx(), childID)
	requireNoErr(t, err, "GetRun(child)")
	if child.ParentRunID == nil || *child.ParentRunID != parent.ID {
		t.Errorf("child ParentRunID = %v, want %s", child.ParentRunID, parent.ID)
	}
	if child.ParentStepID == nil || *child.ParentStepID != "stepA" {
		t.Errorf("child ParentStepID = %v, want stepA", child.ParentStepID)
	}

	// An unrelated child of a different step must be excluded.
	_, err = s.SpawnChildRun(ctx(), parent.ID, "stepB", "b0", childDef, nil)
	requireNoErr(t, err, "SpawnChildRun(other step)")
	children, err := s.ListChildrenByParent(ctx(), parent.ID, "stepA")
	requireNoErr(t, err, "ListChildrenByParent")
	if len(children) != 1 || children[0].ID != childID {
		t.Errorf("ListChildrenByParent(stepA) = %+v, want only %s", children, childID)
	}

	// SpawnChildRunAt sets the child's CurrentStep to startStep.
	atID, err := s.SpawnChildRunAt(ctx(), parent.ID, "stepC", "b0", childDef, nil, "startStep")
	requireNoErr(t, err, "SpawnChildRunAt")
	at, _ := s.GetRun(ctx(), atID)
	if at.CurrentStep != "startStep" {
		t.Errorf("SpawnChildRunAt CurrentStep = %s, want startStep", at.CurrentStep)
	}
}

func testTryCatch(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")

	// Up to 3 frames succeed.
	for i := 1; i <= 3; i++ {
		err := s.PushTryCatch(ctx(), run.ID, domain.TryCatchFrame{StepID: frameID("t", i), CatchStep: frameID("c", i)})
		requireNoErr(t, err, "PushTryCatch")
	}
	// 4th frame exceeds max depth 3 → error.
	if err := s.PushTryCatch(ctx(), run.ID, domain.TryCatchFrame{StepID: "t4", CatchStep: "c4"}); err == nil {
		t.Error("PushTryCatch 4th frame = nil error, want max-depth error")
	}

	// Pop LIFO.
	f, ok, err := s.PopTryCatch(ctx(), run.ID)
	requireNoErr(t, err, "PopTryCatch")
	if !ok || f.StepID != "t3" {
		t.Errorf("PopTryCatch = (%+v, %v), want (t3, true)", f, ok)
	}
	f, ok, _ = s.PopTryCatch(ctx(), run.ID)
	if !ok || f.StepID != "t2" {
		t.Errorf("PopTryCatch = (%+v, %v), want (t2, true)", f, ok)
	}
	_, ok, _ = s.PopTryCatch(ctx(), run.ID)
	if !ok {
		t.Error("PopTryCatch t1 = false, want true")
	}

	// Empty stack → (zero, false, nil) with NO error.
	zero, ok, err := s.PopTryCatch(ctx(), run.ID)
	requireNoErr(t, err, "PopTryCatch(empty)")
	if ok || zero != (domain.TryCatchFrame{}) {
		t.Errorf("PopTryCatch(empty) = (%+v, %v), want (zero, false)", zero, ok)
	}

	// Random run → ErrNotFound on both push and pop.
	requireNotFound(t, s.PushTryCatch(ctx(), uuid.New(), domain.TryCatchFrame{StepID: "x"}), "PushTryCatch(random)")
	_, _, err = s.PopTryCatch(ctx(), uuid.New())
	requireNotFound(t, err, "PopTryCatch(random)")
}

func testUserTasks(t *testing.T, s store.Store) {
	run := seedRun(t, s, "wf")
	task := domain.UserTask{ID: uuid.New(), RunID: run.ID, StepID: "approve", Assignee: "bob"}
	requireNoErr(t, s.CreateUserTask(ctx(), task), "CreateUserTask")

	got, err := s.GetUserTask(ctx(), task.ID)
	requireNoErr(t, err, "GetUserTask")
	if got.ID != task.ID || got.Assignee != "bob" {
		t.Errorf("GetUserTask = %+v, want id=%s assignee=bob", got, task.ID)
	}
	_, err = s.GetUserTask(ctx(), uuid.New())
	requireNotFound(t, err, "GetUserTask(random)")

	result := map[string]any{"vote": "approve"}
	requireNoErr(t, s.SubmitUserTask(ctx(), task.ID, "alice", result), "SubmitUserTask")
	got, _ = s.GetUserTask(ctx(), task.ID)
	if got.SubmittedAt == nil {
		t.Error("SubmittedAt = nil after submit, want set")
	}
	if got.SubmittedBy != "alice" {
		t.Errorf("SubmittedBy = %q, want alice", got.SubmittedBy)
	}
	if got.Result["vote"] != "approve" {
		t.Errorf("Result = %+v, want vote=approve", got.Result)
	}
	requireNotFound(t, s.SubmitUserTask(ctx(), uuid.New(), "x", nil), "SubmitUserTask(random)")

	// ListUserTasksByRun returns this run's tasks only.
	task2 := domain.UserTask{ID: uuid.New(), RunID: run.ID, StepID: "approve2", Assignee: "carol"}
	requireNoErr(t, s.CreateUserTask(ctx(), task2), "CreateUserTask task2")
	otherRun := seedRun(t, s, "wf")
	otherTask := domain.UserTask{ID: uuid.New(), RunID: otherRun.ID, StepID: "x", Assignee: "dan"}
	requireNoErr(t, s.CreateUserTask(ctx(), otherTask), "CreateUserTask other")

	tasks, err := s.ListUserTasksByRun(ctx(), run.ID)
	requireNoErr(t, err, "ListUserTasksByRun")
	if len(tasks) != 2 {
		t.Errorf("ListUserTasksByRun = %d tasks, want 2", len(tasks))
	}
	for _, tk := range tasks {
		if tk.RunID != run.ID {
			t.Errorf("ListUserTasksByRun returned task for run %s, want %s", tk.RunID, run.ID)
		}
	}
}

func testActionRegistry(t *testing.T, s store.Store) {
	regs := []domain.ActionRegistration{
		{Service: "svc1", ActionName: "create_order", Version: 1, Category: "orders"},
		{Service: "svc1", ActionName: "cancel_order", Version: 1, Category: "orders"},
		{Service: "svc2", ActionName: "send_email", Version: 1, Category: "comms"},
	}
	for _, r := range regs {
		requireNoErr(t, s.UpsertActionRegistration(ctx(), r), "UpsertActionRegistration")
	}

	got, err := s.GetAction(ctx(), "svc1", "create_order", 1)
	requireNoErr(t, err, "GetAction")
	if got.Service != "svc1" || got.ActionName != "create_order" {
		t.Errorf("GetAction = %+v, want svc1/create_order", got)
	}
	_, err = s.GetAction(ctx(), "svc1", "missing", 1)
	requireNotFound(t, err, "GetAction(missing)")

	// Empty filter returns all.
	all, err := s.ListActions(ctx(), store.ActionFilter{})
	requireNoErr(t, err, "ListActions(empty)")
	if len(all) != 3 {
		t.Errorf("ListActions(empty) = %d, want 3", len(all))
	}

	// Service exact.
	bySvc, _ := s.ListActions(ctx(), store.ActionFilter{Service: "svc1"})
	if len(bySvc) != 2 {
		t.Errorf("ListActions(Service=svc1) = %d, want 2", len(bySvc))
	}

	// Category exact.
	byCat, _ := s.ListActions(ctx(), store.ActionFilter{Category: "comms"})
	if len(byCat) != 1 || byCat[0].ActionName != "send_email" {
		t.Errorf("ListActions(Category=comms) = %+v, want only send_email", byCat)
	}

	// Search = case-sensitive substring of ActionName.
	bySearch, _ := s.ListActions(ctx(), store.ActionFilter{Search: "order"})
	if len(bySearch) != 2 {
		t.Errorf("ListActions(Search=order) = %d, want 2", len(bySearch))
	}
	caseSensitive, _ := s.ListActions(ctx(), store.ActionFilter{Search: "ORDER"})
	if len(caseSensitive) != 0 {
		t.Errorf("ListActions(Search=ORDER) = %d, want 0 (case-sensitive)", len(caseSensitive))
	}
}

func testActionDispatch(t *testing.T, s store.Store) {
	// MarkAwaitingAction sets state=paused; calling again with same args is a no-op.
	run := seedRun(t, s, "wf")
	requireNoErr(t, s.MarkAwaitingAction(ctx(), run.ID, "svc.name", 1), "MarkAwaitingAction")
	got, _ := s.GetRun(ctx(), run.ID)
	if got.State != domain.RunStatePaused {
		t.Errorf("state = %s, want paused", got.State)
	}
	if got.CurrentAttempt != 1 {
		t.Errorf("CurrentAttempt = %d, want 1", got.CurrentAttempt)
	}
	requireNoErr(t, s.MarkAwaitingAction(ctx(), run.ID, "svc.name", 1), "MarkAwaitingAction(repeat)")
	got, _ = s.GetRun(ctx(), run.ID)
	if got.CurrentAttempt != 1 {
		t.Errorf("CurrentAttempt after repeat = %d, want 1", got.CurrentAttempt)
	}

	// CompleteAction with matching attempt merges variables.
	requireNoErr(t, s.CompleteAction(ctx(), run.ID, 1, map[string]any{"k": "v"}), "CompleteAction(match)")
	got, _ = s.GetRun(ctx(), run.ID)
	if got.Variables["k"] != "v" {
		t.Errorf("Variables[k] = %v, want v", got.Variables["k"])
	}

	// CompleteAction with mismatched attempt is a silent no-op.
	run2 := seedRun(t, s, "wf")
	requireNoErr(t, s.MarkAwaitingAction(ctx(), run2.ID, "svc.name", 1), "MarkAwaitingAction run2")
	requireNoErr(t, s.CompleteAction(ctx(), run2.ID, 99, map[string]any{"late": true}), "CompleteAction(mismatch)")
	got2, _ := s.GetRun(ctx(), run2.ID)
	if got2.Variables["late"] != nil {
		t.Errorf("mismatched CompleteAction merged variables: %+v", got2.Variables)
	}
	requireNotFound(t, s.CompleteAction(ctx(), uuid.New(), 1, nil), "CompleteAction(random)")

	// FailAction with matching attempt → state=failed + EventStepFailed appended.
	run3 := seedRun(t, s, "wf")
	requireNoErr(t, s.MarkAwaitingAction(ctx(), run3.ID, "svc.name", 1), "MarkAwaitingAction run3")
	requireNoErr(t, s.FailAction(ctx(), run3.ID, 1, "code", "msg", false), "FailAction(match)")
	got3, _ := s.GetRun(ctx(), run3.ID)
	if got3.State != domain.RunStateFailed {
		t.Errorf("FailAction state = %s, want failed", got3.State)
	}
	evts, _ := s.ListEventsByRun(ctx(), run3.ID)
	if !hasEventType(evts, domain.EventStepFailed) {
		t.Errorf("FailAction did not append an EventStepFailed event: %+v", evts)
	}

	// FailAction with mismatched attempt → no-op.
	run4 := seedRun(t, s, "wf")
	requireNoErr(t, s.MarkAwaitingAction(ctx(), run4.ID, "svc.name", 1), "MarkAwaitingAction run4")
	requireNoErr(t, s.FailAction(ctx(), run4.ID, 99, "code", "msg", false), "FailAction(mismatch)")
	got4, _ := s.GetRun(ctx(), run4.ID)
	if got4.State == domain.RunStateFailed {
		t.Error("mismatched FailAction transitioned run to failed")
	}
	requireNotFound(t, s.FailAction(ctx(), uuid.New(), 1, "c", "m", false), "FailAction(random)")
}

func testTriggers(t *testing.T, s store.Store) {
	tenant := uuid.New()
	trig := domain.SagaTrigger{
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf",
		Enabled:     true,
		TenantID:    &tenant,
	}
	id, err := s.UpsertTrigger(ctx(), trig)
	requireNoErr(t, err, "UpsertTrigger(zero id)")
	if id == uuid.Nil {
		t.Fatal("UpsertTrigger returned the nil UUID")
	}

	got, err := s.GetTrigger(ctx(), id)
	requireNoErr(t, err, "GetTrigger")
	if got.WorkflowID != "wf" || got.TriggerType != domain.TriggerRecordTransition {
		t.Errorf("GetTrigger = %+v, want wf/record_transition", got)
	}
	_, err = s.GetTrigger(ctx(), uuid.New())
	requireNotFound(t, err, "GetTrigger(random)")

	// Add a disabled trigger of the same type, different tenant.
	otherTenant := uuid.New()
	_, err = s.UpsertTrigger(ctx(), domain.SagaTrigger{TriggerType: domain.TriggerRecordTransition, WorkflowID: "wf2", Enabled: false, TenantID: &otherTenant})
	requireNoErr(t, err, "UpsertTrigger(disabled)")

	// Type filter.
	byType, _ := s.ListTriggers(ctx(), store.TriggerFilter{Type: domain.TriggerRecordTransition})
	if len(byType) != 2 {
		t.Errorf("ListTriggers(Type) = %d, want 2", len(byType))
	}
	// Enabled filter.
	enabled := true
	byEnabled, _ := s.ListTriggers(ctx(), store.TriggerFilter{Enabled: &enabled})
	if len(byEnabled) != 1 || byEnabled[0].ID != id {
		t.Errorf("ListTriggers(Enabled=true) = %+v, want only %s", byEnabled, id)
	}
	// TenantID filter.
	byTenant, _ := s.ListTriggers(ctx(), store.TriggerFilter{TenantID: &tenant})
	if len(byTenant) != 1 || byTenant[0].ID != id {
		t.Errorf("ListTriggers(TenantID) = %+v, want only %s", byTenant, id)
	}

	// DeleteTrigger removes it.
	requireNoErr(t, s.DeleteTrigger(ctx(), id), "DeleteTrigger")
	_, err = s.GetTrigger(ctx(), id)
	requireNotFound(t, err, "GetTrigger(after delete)")
	requireNotFound(t, s.DeleteTrigger(ctx(), uuid.New()), "DeleteTrigger(random)")
}

func testListRuns(t *testing.T, s store.Store) {
	now := time.Now().UTC()
	// Seed wf-a: 3 runs at distinct times, one failed.
	mk := func(wf string, state domain.RunState, startedAt time.Time) domain.SagaRun {
		r := newRun(wf)
		r.State = state
		r.StartedAt = startedAt
		r.LastEventAt = startedAt
		return r
	}
	a1 := mk("wf-a", domain.RunStateSucceeded, now.Add(-1*time.Minute))
	a2 := mk("wf-a", domain.RunStateRunning, now.Add(-2*time.Minute))
	a3 := mk("wf-a", domain.RunStateFailed, now.Add(-3*time.Minute))
	b1 := mk("wf-b", domain.RunStateSucceeded, now.Add(-30*time.Minute))
	for _, r := range []domain.SagaRun{a1, a2, a3, b1} {
		requireNoErr(t, s.CreateRun(ctx(), r), "CreateRun")
	}

	// WorkflowID filter + DESC ordering by StartedAt.
	runs, err := s.ListRuns(ctx(), store.RunFilter{WorkflowID: "wf-a"})
	requireNoErr(t, err, "ListRuns(WorkflowID)")
	if len(runs) != 3 {
		t.Fatalf("ListRuns(wf-a) = %d, want 3", len(runs))
	}
	for _, r := range runs {
		if r.WorkflowID != "wf-a" {
			t.Errorf("ListRuns returned run for %s, want wf-a", r.WorkflowID)
		}
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].StartedAt.After(runs[i-1].StartedAt) {
			t.Errorf("ListRuns not sorted StartedAt DESC at %d", i)
		}
	}

	// State filter.
	failed, _ := s.ListRuns(ctx(), store.RunFilter{State: string(domain.RunStateFailed)})
	if len(failed) != 1 || failed[0].ID != a3.ID {
		t.Errorf("ListRuns(State=failed) = %+v, want only %s", failed, a3.ID)
	}

	// Since filter.
	since := now.Add(-10 * time.Minute)
	recent, _ := s.ListRuns(ctx(), store.RunFilter{Since: &since})
	if len(recent) != 3 {
		t.Errorf("ListRuns(Since=-10m) = %d, want 3 (excludes wf-b at -30m)", len(recent))
	}

	// Pagination: Limit + Offset.
	page1, _ := s.ListRuns(ctx(), store.RunFilter{WorkflowID: "wf-a", Limit: 2, Offset: 0})
	page2, _ := s.ListRuns(ctx(), store.RunFilter{WorkflowID: "wf-a", Limit: 2, Offset: 2})
	if len(page1) != 2 || len(page2) != 1 {
		t.Errorf("pagination: page1=%d page2=%d, want 2 and 1", len(page1), len(page2))
	}

	// Default limit when 0 returns everything (4 total).
	deflt, _ := s.ListRuns(ctx(), store.RunFilter{})
	if len(deflt) != 4 {
		t.Errorf("ListRuns(default limit) = %d, want 4", len(deflt))
	}

	// CountRuns ignores Limit/Offset.
	count, err := s.CountRuns(ctx(), store.RunFilter{WorkflowID: "wf-a", Limit: 1, Offset: 1})
	requireNoErr(t, err, "CountRuns")
	if count != 3 {
		t.Errorf("CountRuns(wf-a) = %d, want 3", count)
	}
}

func testStats(t *testing.T, s store.Store) {
	now := time.Now().UTC()
	mk := func(state domain.RunState, startedAt time.Time) domain.SagaRun {
		r := newRun("wf-stat")
		r.State = state
		r.StartedAt = startedAt
		r.LastEventAt = startedAt
		return r
	}
	// In window: 3 succeeded, 1 failed → rate 0.75.
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateSucceeded, now.Add(-1*time.Hour))), "seed")
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateSucceeded, now.Add(-2*time.Hour))), "seed")
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateSucceeded, now.Add(-3*time.Hour))), "seed")
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateFailed, now.Add(-4*time.Hour))), "seed")
	// In-flight: running + paused (not terminal).
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateRunning, now.Add(-10*time.Minute))), "seed")
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStatePaused, now.Add(-20*time.Minute))), "seed")
	// Out of window: should not affect rate, but LastRunAt is overall max so
	// keep it older than the most recent in-window run.
	requireNoErr(t, s.CreateRun(ctx(), mk(domain.RunStateSucceeded, now.Add(-30*time.Hour))), "seed")

	stats, err := s.StatsForWorkflow(ctx(), "wf-stat")
	requireNoErr(t, err, "StatsForWorkflow")
	if stats.WorkflowID != "wf-stat" {
		t.Errorf("WorkflowID = %q, want wf-stat", stats.WorkflowID)
	}
	if stats.InFlight != 2 {
		t.Errorf("InFlight = %d, want 2", stats.InFlight)
	}
	if stats.SuccessRate24h == nil {
		t.Fatal("SuccessRate24h = nil, want ~0.75")
	}
	if diff := *stats.SuccessRate24h - 0.75; diff > 0.001 || diff < -0.001 {
		t.Errorf("SuccessRate24h = %f, want 0.75", *stats.SuccessRate24h)
	}
	if stats.LastRunAt == nil {
		t.Fatal("LastRunAt = nil, want most recent StartedAt")
	}
	// LastRunAt should be the running run at -10m (the most recent overall).
	want := now.Add(-10 * time.Minute)
	if !stats.LastRunAt.Equal(want) {
		t.Errorf("LastRunAt = %v, want %v", stats.LastRunAt, want)
	}

	// No runs in window → nil SuccessRate24h.
	empty, err := s.StatsForWorkflow(ctx(), "no-such-wf")
	requireNoErr(t, err, "StatsForWorkflow(empty)")
	if empty.SuccessRate24h != nil {
		t.Errorf("SuccessRate24h(empty) = %v, want nil", empty.SuccessRate24h)
	}
	if empty.LastRunAt != nil {
		t.Errorf("LastRunAt(empty) = %v, want nil", empty.LastRunAt)
	}
	if empty.InFlight != 0 {
		t.Errorf("InFlight(empty) = %d, want 0", empty.InFlight)
	}
}

// --- small local helpers ----------------------------------------------------

func ptrTime(t time.Time) *time.Time { return &t }

func containsID(ids []uuid.UUID, want uuid.UUID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsRun(runs []domain.SagaRun, want uuid.UUID) bool {
	for _, r := range runs {
		if r.ID == want {
			return true
		}
	}
	return false
}

func hasEventType(evts []domain.SagaRunEvent, want domain.EventType) bool {
	for _, e := range evts {
		if e.EventType == want {
			return true
		}
	}
	return false
}

func frameID(prefix string, n int) string {
	return prefix + string(rune('0'+n))
}
