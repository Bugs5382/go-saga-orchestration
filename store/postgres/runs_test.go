package postgres

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
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// newTestDB creates a fresh, migrated database on the server TEST_POSTGRES_DSN
// points at, opens a Store on it, and drops the database when the test ends.
func newTestDB(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres store tests")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	name := "saga_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse TEST_POSTGRES_DSN (a postgres:// URL is required): %v", err)
	}
	u.Path = "/" + name
	if err := Migrate(u.String()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s, err := Open(ctx, u.String())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// seedPGRun stores a definition and a pending run of it.
func seedPGRun(t *testing.T, s *Store, mutate func(*domain.SagaRun)) domain.SagaRun {
	t.Helper()
	ctx := context.Background()
	defID, err := s.UpsertWorkflowDefinition(ctx, domain.WorkflowDefinition{
		ID: "wf-" + uuid.NewString(), Version: 1, Name: "wf", Start: "end",
		Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}}, Published: true,
	})
	if err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	run := domain.NewSagaRun("wf", defID, nil, map[string]any{})
	if mutate != nil {
		mutate(&run)
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	return run
}

func strPtr(s string) *string { return &s }

// TestGetRun_RoundTripsCreateFields checks GetRun returns the parent and
// try_catch fields CreateRun stores.
func TestGetRun_RoundTripsCreateFields(t *testing.T) {
	s := newTestDB(t)
	ctx := context.Background()
	parent := seedPGRun(t, s, nil)
	frames := []domain.TryCatchFrame{{StepID: "try", CatchStep: "catch"}}
	run := seedPGRun(t, s, func(r *domain.SagaRun) {
		r.ParentRunID = &parent.ID
		r.ParentStepID = strPtr("fan")
		r.ParentBranchID = strPtr("b1")
		r.TryCatchStack = frames
	})

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.ParentStepID == nil || *got.ParentStepID != "fan" {
		t.Errorf("ParentStepID = %v, want fan", got.ParentStepID)
	}
	if got.ParentBranchID == nil || *got.ParentBranchID != "b1" {
		t.Errorf("ParentBranchID = %v, want b1", got.ParentBranchID)
	}
	if len(got.TryCatchStack) != 1 || got.TryCatchStack[0] != frames[0] {
		t.Errorf("TryCatchStack = %+v, want %+v", got.TryCatchStack, frames)
	}
}

// TestGetRun_RoundTripsAwaitedAction checks the attempt and awaited action
// survive a GetRun, so a second action step dispatches attempt 2 and the run
// is paused on it.
func TestGetRun_RoundTripsAwaitedAction(t *testing.T) {
	s := newTestDB(t)
	ctx := context.Background()
	run := seedPGRun(t, s, nil)

	if err := s.MarkAwaitingAction(ctx, run.ID, "svc.first", 1); err != nil {
		t.Fatalf("MarkAwaitingAction(1): %v", err)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.CurrentAttempt != 1 {
		t.Fatalf("CurrentAttempt = %d, want 1", got.CurrentAttempt)
	}
	if got.AwaitedActionDispatch == nil || *got.AwaitedActionDispatch != "svc.first" {
		t.Fatalf("AwaitedActionDispatch = %v, want svc.first", got.AwaitedActionDispatch)
	}

	// The worker reports the first step; the run wakes with no awaited action.
	if err := s.CompleteAction(ctx, run.ID, 1, map[string]any{"a": 1}); err != nil {
		t.Fatalf("CompleteAction(1): %v", err)
	}
	if err := s.ClearPause(ctx, run.ID); err != nil {
		t.Fatalf("ClearPause: %v", err)
	}
	got, err = s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun after complete: %v", err)
	}
	if got.AwaitedActionDispatch != nil {
		t.Errorf("AwaitedActionDispatch after complete = %v, want nil", *got.AwaitedActionDispatch)
	}

	// The second action step bumps the attempt the way ActionVerb does.
	next := got.CurrentAttempt + 1
	if next != 2 {
		t.Fatalf("next attempt = %d, want 2", next)
	}
	if err := s.MarkAwaitingAction(ctx, run.ID, "svc.second", next); err != nil {
		t.Fatalf("MarkAwaitingAction(2): %v", err)
	}
	got, err = s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun second step: %v", err)
	}
	if got.State != domain.RunStatePaused || got.CurrentAttempt != 2 ||
		got.AwaitedActionDispatch == nil || *got.AwaitedActionDispatch != "svc.second" {
		t.Errorf("second step = state %s attempt %d dispatch %v, want paused 2 svc.second",
			got.State, got.CurrentAttempt, got.AwaitedActionDispatch)
	}
}

// TestGetRun_RoundTripsPauseMarkers checks the wakeup, signal and event
// markers survive a GetRun.
func TestGetRun_RoundTripsPauseMarkers(t *testing.T) {
	s := newTestDB(t)
	ctx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	wake := seedPGRun(t, s, nil)
	if err := s.SetPausedWithWakeup(ctx, wake.ID, deadline); err != nil {
		t.Fatalf("SetPausedWithWakeup: %v", err)
	}
	got, err := s.GetRun(ctx, wake.ID)
	if err != nil {
		t.Fatalf("GetRun(wakeup): %v", err)
	}
	if got.WakeupAt == nil || !got.WakeupAt.Equal(deadline) {
		t.Errorf("WakeupAt = %v, want %v", got.WakeupAt, deadline)
	}

	sig := seedPGRun(t, s, nil)
	if err := s.SetPausedAwaitingSignal(ctx, sig.ID, "approve", &deadline); err != nil {
		t.Fatalf("SetPausedAwaitingSignal: %v", err)
	}
	got, err = s.GetRun(ctx, sig.ID)
	if err != nil {
		t.Fatalf("GetRun(signal): %v", err)
	}
	if got.AwaitedSignal == nil || *got.AwaitedSignal != "approve" {
		t.Errorf("AwaitedSignal = %v, want approve", got.AwaitedSignal)
	}
	if got.WakeupAt == nil || !got.WakeupAt.Equal(deadline) {
		t.Errorf("signal WakeupAt = %v, want %v", got.WakeupAt, deadline)
	}

	ev := seedPGRun(t, s, nil)
	headers := map[string]string{"tenant": "t1"}
	if err := s.SetPausedAwaitingEventWithDeadline(ctx, ev.ID, "orders.created", headers, nil); err != nil {
		t.Fatalf("SetPausedAwaitingEventWithDeadline: %v", err)
	}
	got, err = s.GetRun(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetRun(event): %v", err)
	}
	if got.AwaitedEventTopic == nil || *got.AwaitedEventTopic != "orders.created" {
		t.Errorf("AwaitedEventTopic = %v, want orders.created", got.AwaitedEventTopic)
	}
	if got.AwaitedEventHeaders["tenant"] != "t1" {
		t.Errorf("AwaitedEventHeaders = %v, want tenant=t1", got.AwaitedEventHeaders)
	}
	if got.WakeupAt != nil {
		t.Errorf("event WakeupAt = %v, want nil", got.WakeupAt)
	}

	// ClearPause removes every marker.
	if err := s.ClearPause(ctx, ev.ID); err != nil {
		t.Fatalf("ClearPause: %v", err)
	}
	got, err = s.GetRun(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetRun(cleared): %v", err)
	}
	if got.AwaitedEventTopic != nil || got.AwaitedEventHeaders != nil || got.AwaitedSignal != nil || got.WakeupAt != nil {
		t.Errorf("markers after ClearPause = topic %v headers %v signal %v wakeup %v, want none",
			got.AwaitedEventTopic, got.AwaitedEventHeaders, got.AwaitedSignal, got.WakeupAt)
	}
}
