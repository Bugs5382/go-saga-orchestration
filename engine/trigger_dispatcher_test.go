package engine

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
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// helpers ----------------------------------------------------------------

// minDef builds a minimal published WorkflowDefinition for testing.
func minDef(id string) domain.WorkflowDefinition {
	return domain.WorkflowDefinition{
		ID:        id,
		Version:   1,
		Name:      id,
		Start:     "end",
		Steps:     []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
		Published: true,
	}
}

// triggerBody builds a JSON-encoded event body for a record.transitioned event.
func triggerBody(t *testing.T, recordType, fromState, toState string, extra map[string]any) []byte {
	t.Helper()
	m := map[string]any{
		"record_id":   "rec-1",
		"record_type": recordType,
		"from_state":  fromState,
		"to_state":    toState,
		"actor":       "user-1",
	}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return b
}

// makeDispatcher returns a TriggerDispatcher using an in-memory store pre-seeded
// with def (if non-nil) and triggers.
func makeDispatcher(t *testing.T, def *domain.WorkflowDefinition, triggers []domain.SagaTrigger) (*TriggerDispatcher, *memory.Store, *recordingPublisher) {
	t.Helper()
	s := memory.New()
	ctx := context.Background()
	if def != nil {
		if _, err := s.UpsertWorkflowDefinition(ctx, *def); err != nil {
			t.Fatalf("seed def: %v", err)
		}
	}
	for _, trig := range triggers {
		if _, err := s.UpsertTrigger(ctx, trig); err != nil {
			t.Fatalf("seed trigger: %v", err)
		}
	}
	pub := &recordingPublisher{}
	d := &TriggerDispatcher{S: s, Publisher: pub}
	return d, s, pub
}

// tests ------------------------------------------------------------------

func TestTriggerDispatcher_NonTriggerTopic(t *testing.T) {
	d, s, pub := makeDispatcher(t, nil, nil)
	ctx := context.Background()

	for _, topic := range []string{"thing.created", "thing.updated", "foo", "a.b.c"} {
		err := d.Dispatch(ctx, EventDelivery{Topic: topic, Body: []byte(`{}`)})
		if err != nil {
			t.Errorf("topic %q: unexpected error: %v", topic, err)
		}
	}
	runs, _ := s.FindRunsByAwaitedEvent(ctx, "")
	if len(runs) != 0 {
		t.Errorf("expected no runs, got %d", len(runs))
	}
	if len(pub.runs) != 0 {
		t.Errorf("expected no publishes, got %d", len(pub.runs))
	}
}

func TestTriggerDispatcher_TriggerTopic_NoMatchingTrigger(t *testing.T) {
	def := minDef("wf-order")
	d, _, pub := makeDispatcher(t, &def, nil) // no trigger rows seeded

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "created", "pending_review", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 0 {
		t.Errorf("expected no publishes, got %d", len(pub.runs))
	}
}

func TestTriggerDispatcher_TriggerTopic_DisabledTrigger(t *testing.T) {
	def := minDef("wf-order")
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-order",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "created",
			"to_state":    "pending_review",
		},
		Enabled: false, // disabled
	}
	d, _, pub := makeDispatcher(t, &def, []domain.SagaTrigger{trig})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "created", "pending_review", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 0 {
		t.Errorf("disabled trigger must not start a saga; got %d publishes", len(pub.runs))
	}
}

func TestTriggerDispatcher_TriggerTopic_MatchingEnabledTrigger(t *testing.T) {
	def := minDef("wf-order")
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-order",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "created",
			"to_state":    "pending_review",
			"input_mapping": map[string]any{
				"order_id":     "$.record_id",
				"requester_id": "$.actor",
			},
		},
		Enabled: true,
	}
	d, s, pub := makeDispatcher(t, &def, []domain.SagaTrigger{trig})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "created", "pending_review", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(pub.runs))
	}

	// Verify the run exists in the store with the expected workflow_id and inputs.
	runID, err := uuid.Parse(pub.runs[0])
	if err != nil {
		t.Fatalf("published run ID is not a UUID: %v", err)
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.WorkflowID != "wf-order" {
		t.Errorf("WorkflowID = %q, want %q", run.WorkflowID, "wf-order")
	}
	// Inputs must reflect the input_mapping.
	if run.Inputs["order_id"] != "rec-1" {
		t.Errorf("inputs[order_id] = %v, want rec-1", run.Inputs["order_id"])
	}
	if run.Inputs["requester_id"] != "user-1" {
		t.Errorf("inputs[requester_id] = %v, want user-1", run.Inputs["requester_id"])
	}
}

func TestTriggerDispatcher_BodyRecordTypeWinsOverTopic(t *testing.T) {
	// Topic says "order" but body says "ticket"; trigger is for "ticket".
	def := minDef("wf-ticket")
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-ticket",
		Version:     1,
		Config: map[string]any{
			"record_type": "ticket",
			"from_state":  "new",
			"to_state":    "assigned",
		},
		Enabled: true,
	}
	d, _, pub := makeDispatcher(t, &def, []domain.SagaTrigger{trig})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		// Topic's trailing segment says "order" — body overrides it.
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "ticket", "new", "assigned", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 1 {
		t.Fatalf("body record_type should win; expected 1 publish, got %d", len(pub.runs))
	}
}

func TestTriggerDispatcher_MultipleMatchingTriggers(t *testing.T) {
	def1 := minDef("wf-a")
	def2 := minDef("wf-b")
	trig1 := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-a",
		Version:     1,
		Config:      map[string]any{"record_type": "order", "from_state": "new", "to_state": "open"},
		Enabled:     true,
	}
	trig2 := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-b",
		Version:     1,
		Config:      map[string]any{"record_type": "order", "from_state": "new", "to_state": "open"},
		Enabled:     true,
	}

	s := memory.New()
	ctx := context.Background()
	for _, df := range []domain.WorkflowDefinition{def1, def2} {
		if _, err := s.UpsertWorkflowDefinition(ctx, df); err != nil {
			t.Fatalf("seed def: %v", err)
		}
	}
	for _, tg := range []domain.SagaTrigger{trig1, trig2} {
		if _, err := s.UpsertTrigger(ctx, tg); err != nil {
			t.Fatalf("seed trigger: %v", err)
		}
	}
	pub := &recordingPublisher{}
	d := &TriggerDispatcher{S: s, Publisher: pub}

	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "new", "open", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 2 {
		t.Fatalf("expected 2 publishes for 2 matching triggers, got %d", len(pub.runs))
	}
}

// mapInputs unit tests ---------------------------------------------------

func TestMapInputs_Identity(t *testing.T) {
	body := map[string]any{"a": "1", "b": "2"}
	out := mapInputs(body, nil)
	if out["a"] != "1" || out["b"] != "2" {
		t.Errorf("identity: got %v", out)
	}
}

func TestMapInputs_Reference(t *testing.T) {
	body := map[string]any{"record_id": "c1", "actor": "u1"}
	mapping := map[string]any{"order_id": "$.record_id"}
	out := mapInputs(body, mapping)
	if out["order_id"] != "c1" {
		t.Errorf("reference: order_id = %v, want c1", out["order_id"])
	}
	if _, ok := out["actor"]; ok {
		t.Errorf("unmapped key actor should not be in output")
	}
}

func TestMapInputs_Literal(t *testing.T) {
	body := map[string]any{}
	mapping := map[string]any{"tag": "static"}
	out := mapInputs(body, mapping)
	if out["tag"] != "static" {
		t.Errorf("literal: tag = %v, want static", out["tag"])
	}
}

func TestMapInputs_MissingSource(t *testing.T) {
	body := map[string]any{"present": "yes"}
	mapping := map[string]any{"x": "$.nonexistent"}
	out := mapInputs(body, mapping)
	if _, ok := out["x"]; ok {
		t.Errorf("missing source: key x should be absent, got %v", out["x"])
	}
}

func TestTriggerDispatcher_WorkflowNotFound_SkipsTrigger(t *testing.T) {
	// Trigger references a workflow that is not seeded in the store.
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-missing",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "new",
			"to_state":    "open",
		},
		Enabled: true,
	}
	// seed a SECOND trigger that references a real workflow; must still fire.
	defGood := minDef("wf-good")
	trigGood := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-good",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "new",
			"to_state":    "open",
		},
		Enabled: true,
	}
	d, _, pub := makeDispatcher(t, &defGood, []domain.SagaTrigger{trig, trigGood})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "new", "open", nil),
	})
	// workflow not found is non-fatal; Dispatch must return nil.
	if err != nil {
		t.Fatalf("expected nil error on workflow-not-found, got: %v", err)
	}
	// The good trigger must still have fired.
	if len(pub.runs) != 1 {
		t.Fatalf("expected 1 publish (good trigger), got %d", len(pub.runs))
	}
}

func TestTriggerDispatcher_Entrypoint_StartsRunAtNamedStep(t *testing.T) {
	// Workflow has an alt entrypoint "alt" mapped to step "s2".
	def := domain.WorkflowDefinition{
		ID:      "wf-entry",
		Version: 1,
		Name:    "wf-entry",
		Start:   "s1",
		Steps: []domain.Step{
			{ID: "s1", Type: domain.StepTypeEnd},
			{ID: "s2", Type: domain.StepTypeEnd},
		},
		Entrypoints: map[string]string{"alt": "s2"},
		Published:   true,
	}
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-entry",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "new",
			"to_state":    "open",
			"entrypoint":  "alt",
		},
		Enabled: true,
	}
	d, s, pub := makeDispatcher(t, &def, []domain.SagaTrigger{trig})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "new", "open", nil),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.runs) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(pub.runs))
	}

	runID, err := uuid.Parse(pub.runs[0])
	if err != nil {
		t.Fatalf("published run ID is not a UUID: %v", err)
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.CurrentStep != "s2" {
		t.Errorf("CurrentStep = %q, want %q", run.CurrentStep, "s2")
	}
}

func TestTriggerDispatcher_InvalidEntrypoint_SkipsTrigger(t *testing.T) {
	// Trigger names an entrypoint that does not exist in the workflow.
	def := domain.WorkflowDefinition{
		ID:          "wf-entry-bad",
		Version:     1,
		Name:        "wf-entry-bad",
		Start:       "s1",
		Steps:       []domain.Step{{ID: "s1", Type: domain.StepTypeEnd}},
		Entrypoints: map[string]string{"alt": "s2"},
		Published:   true,
	}
	trig := domain.SagaTrigger{
		ID:          uuid.New(),
		TriggerType: domain.TriggerRecordTransition,
		WorkflowID:  "wf-entry-bad",
		Version:     1,
		Config: map[string]any{
			"record_type": "order",
			"from_state":  "new",
			"to_state":    "open",
			"entrypoint":  "nope",
		},
		Enabled: true,
	}
	d, _, pub := makeDispatcher(t, &def, []domain.SagaTrigger{trig})

	ctx := context.Background()
	err := d.Dispatch(ctx, EventDelivery{
		Topic: "example.record.transitioned.order",
		Body:  triggerBody(t, "order", "new", "open", nil),
	})
	// Invalid entrypoint is non-fatal; Dispatch must return nil.
	if err != nil {
		t.Fatalf("expected nil error on invalid entrypoint, got: %v", err)
	}
	// Must not have started any run.
	if len(pub.runs) != 0 {
		t.Errorf("expected 0 publishes for invalid entrypoint, got %d", len(pub.runs))
	}
}
