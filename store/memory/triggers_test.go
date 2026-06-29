package memory

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
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

func boolPtr(b bool) *bool { return &b }

func makeTrigger(trigType domain.TriggerType, enabled bool) domain.SagaTrigger {
	return domain.SagaTrigger{
		TriggerType: trigType,
		WorkflowID:  "wf-approval",
		Version:     1,
		Config: map[string]any{
			"record_type": "change",
			"from_state":  "scheduled",
			"to_state":    "pending_approval",
		},
		Enabled:   enabled,
		CreatedBy: "test",
	}
}

// TestTrigger_UpsertWithNilID_GeneratesID verifies that passing ID==uuid.Nil
// produces a non-nil returned ID.
func TestTrigger_UpsertWithNilID_GeneratesID(t *testing.T) {
	s := New()
	ctx := context.Background()

	trig := makeTrigger(domain.TriggerRecordTransition, true)
	id, err := s.UpsertTrigger(ctx, trig)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if id == uuid.Nil {
		t.Fatal("expected non-nil UUID, got uuid.Nil")
	}
}

// TestTrigger_UpsertWithSetID_Replaces verifies that a second upsert with the
// same ID overwrites the stored row.
func TestTrigger_UpsertWithSetID_Replaces(t *testing.T) {
	s := New()
	ctx := context.Background()

	trig := makeTrigger(domain.TriggerRecordTransition, true)
	id, err := s.UpsertTrigger(ctx, trig)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	// Update with same ID but different WorkflowID.
	trig.ID = id
	trig.WorkflowID = "wf-replaced"
	id2, err := s.UpsertTrigger(ctx, trig)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if id2 != id {
		t.Fatalf("expected same ID %s, got %s", id, id2)
	}

	got, err := s.GetTrigger(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.WorkflowID != "wf-replaced" {
		t.Errorf("WorkflowID = %q, want wf-replaced", got.WorkflowID)
	}
}

// TestTrigger_GetByID_RoundTrip verifies that GetTrigger returns the upserted row.
func TestTrigger_GetByID_RoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()

	trig := makeTrigger(domain.TriggerRecordTransition, true)
	id, _ := s.UpsertTrigger(ctx, trig)

	got, err := s.GetTrigger(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != id {
		t.Errorf("ID = %s, want %s", got.ID, id)
	}
	if got.TriggerType != domain.TriggerRecordTransition {
		t.Errorf("TriggerType = %s, want record_transition", got.TriggerType)
	}
	if got.WorkflowID != "wf-approval" {
		t.Errorf("WorkflowID = %q, want wf-approval", got.WorkflowID)
	}
	if got.Config["record_type"] != "change" {
		t.Errorf("config.record_type = %v, want change", got.Config["record_type"])
	}
}

// TestTrigger_GetUnknownID_ReturnsErrNotFound verifies ErrNotFound on a missing row.
func TestTrigger_GetUnknownID_ReturnsErrNotFound(t *testing.T) {
	s := New()
	ctx := context.Background()

	_, err := s.GetTrigger(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}
	if _, ok := err.(store.ErrNotFound); !ok {
		t.Errorf("expected store.ErrNotFound, got %T: %v", err, err)
	}
}

// TestTrigger_ListFilterByType verifies that TriggerFilter.Type excludes
// non-matching rows.
func TestTrigger_ListFilterByType(t *testing.T) {
	s := New()
	ctx := context.Background()

	_, _ = s.UpsertTrigger(ctx, makeTrigger(domain.TriggerRecordTransition, true))
	// Insert a second with a different (hypothetical) type.
	other := makeTrigger("cron", true)
	_, _ = s.UpsertTrigger(ctx, other)

	results, err := s.ListTriggers(ctx, store.TriggerFilter{Type: domain.TriggerRecordTransition})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].TriggerType != domain.TriggerRecordTransition {
		t.Errorf("unexpected TriggerType: %s", results[0].TriggerType)
	}
}

// TestTrigger_ListFilterByEnabled verifies that TriggerFilter.Enabled=true
// excludes disabled triggers.
func TestTrigger_ListFilterByEnabled(t *testing.T) {
	s := New()
	ctx := context.Background()

	_, _ = s.UpsertTrigger(ctx, makeTrigger(domain.TriggerRecordTransition, true))
	_, _ = s.UpsertTrigger(ctx, makeTrigger(domain.TriggerRecordTransition, false))

	results, err := s.ListTriggers(ctx, store.TriggerFilter{Enabled: boolPtr(true)})
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 enabled trigger, got %d", len(results))
	}
	if !results[0].Enabled {
		t.Errorf("expected enabled=true in result")
	}

	// Verify disabled filter also works.
	disabled, err := s.ListTriggers(ctx, store.TriggerFilter{Enabled: boolPtr(false)})
	if err != nil {
		t.Fatalf("list disabled: %v", err)
	}
	if len(disabled) != 1 {
		t.Fatalf("expected 1 disabled trigger, got %d", len(disabled))
	}
}

// TestTrigger_Delete_RemovesRow verifies that DeleteTrigger removes the row
// and subsequent Get returns ErrNotFound.
func TestTrigger_Delete_RemovesRow(t *testing.T) {
	s := New()
	ctx := context.Background()

	id, _ := s.UpsertTrigger(ctx, makeTrigger(domain.TriggerRecordTransition, true))

	if err := s.DeleteTrigger(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err := s.GetTrigger(ctx, id)
	if err == nil {
		t.Fatal("expected ErrNotFound after delete, got nil")
	}
	if _, ok := err.(store.ErrNotFound); !ok {
		t.Errorf("expected store.ErrNotFound, got %T: %v", err, err)
	}

	// Deleting a non-existent ID also returns ErrNotFound.
	if err := s.DeleteTrigger(ctx, uuid.New()); err == nil {
		t.Error("expected ErrNotFound for missing ID, got nil")
	}
}

func TestListDueCronTriggers(t *testing.T) {
	s := New()
	ctx := context.Background()
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	future := now.Add(time.Minute)
	mk := func(enabled bool, fire time.Time) uuid.UUID {
		id, _ := s.UpsertTrigger(ctx, domain.SagaTrigger{
			TriggerType: domain.TriggerCron, WorkflowID: "wf", Version: 1,
			Config: map[string]any{"schedule": "* * * * *"}, Enabled: enabled,
			NextFireAt: &fire, CreatedBy: "t",
		})
		return id
	}
	dueID := mk(true, due)
	mk(false, due)   // disabled -> excluded
	mk(true, future) // not due -> excluded
	got, err := s.ListDueCronTriggers(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != dueID {
		t.Fatalf("want [%s], got %+v", dueID, got)
	}
}

func TestClaimCronFire_SingleWinner(t *testing.T) {
	s := New()
	ctx := context.Background()
	cur := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	next := cur.Add(time.Minute)
	id, _ := s.UpsertTrigger(ctx, domain.SagaTrigger{
		TriggerType: domain.TriggerCron, WorkflowID: "wf", Version: 1,
		Config: map[string]any{"schedule": "* * * * *"}, Enabled: true,
		NextFireAt: &cur, CreatedBy: "t",
	})
	won1, err := s.ClaimCronFire(ctx, id, cur, next)
	if err != nil || !won1 {
		t.Fatalf("first claim should win: won=%v err=%v", won1, err)
	}
	won2, err := s.ClaimCronFire(ctx, id, cur, next) // stale expected -> lose
	if err != nil || won2 {
		t.Fatalf("second claim on stale expected should lose: won=%v err=%v", won2, err)
	}
}
