// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// checkTTLBounds asserts that the Redis key has a TTL in (0, max].
func checkTTLBounds(t *testing.T, ctx context.Context, s *Store, key string, max time.Duration) {
	t.Helper()
	ttl, err := s.rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("TTL(%s) failed: %v", key, err)
	}
	if ttl <= 0 {
		t.Errorf("expected TTL > 0 for key %s after terminal transition, got %v", key, ttl)
	}
	if ttl > max {
		t.Errorf("TTL %v for key %s exceeds configured runTTL %v", ttl, key, max)
	}
}

// TestFailAction_RemovesFromWakeupIndex verifies that FailAction prunes a run
// from idx:wakeup (and idx:awaitevent when applicable) so a timer loop cannot
// re-emit saga.advance for a failed run with a past deadline.
//
// Reproduction path: SetPausedAwaitingSignal with a past deadline registers the
// run in idx:wakeup. MarkAwaitingAction + FailAction transitions the run to
// failed (terminal). Before the fix, FailAction did not remove the run from
// idx:wakeup, causing FindRunsByDueWakeup to keep returning its ID.
func TestFailAction_RemovesFromWakeupIndex(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to run Redis-specific tests")
	}

	ctx := context.Background()
	s, err := Open(ctx, url, WithPrefix("saga-failaction:"+uuid.NewString()+":"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	run := domain.NewSagaRun("wf-failaction-test", uuid.New(), nil, nil)
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Register the run in idx:wakeup with a past deadline.
	pastDeadline := time.Now().Add(-10 * time.Minute)
	if err := s.SetPausedAwaitingSignal(ctx, run.ID, "go", &pastDeadline); err != nil {
		t.Fatalf("set paused awaiting signal: %v", err)
	}

	// Transition to awaiting-action, then fail it (this is the path that
	// leaves a stale wakeup entry before the fix).
	const attempt = 1
	if err := s.MarkAwaitingAction(ctx, run.ID, "svc.x", attempt); err != nil {
		t.Fatalf("mark awaiting action: %v", err)
	}
	if err := s.FailAction(ctx, run.ID, attempt, "ERR", "injected failure", false); err != nil {
		t.Fatalf("fail action: %v", err)
	}

	// The run is now terminal; FindRunsByDueWakeup must NOT return it.
	ids, err := s.FindRunsByDueWakeup(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("find runs by due wakeup: %v", err)
	}
	for _, id := range ids {
		if id == run.ID {
			t.Errorf("FindRunsByDueWakeup returned failed run %s — stale wakeup index entry not pruned", run.ID)
		}
	}
}

func TestTerminalRunTTL(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to run Redis-specific tests")
	}

	ctx := context.Background()
	runTTL := 2 * time.Second

	s, err := Open(ctx, url,
		WithPrefix("saga-ttltest:"+uuid.NewString()+":"),
		WithRunTTL(runTTL),
	)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	run := domain.NewSagaRun("wf-ttl-test", uuid.New(), nil, nil)
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	evt := domain.NewEvent(run.ID, "start", 0, domain.EventStepStarted, "test")
	if err := s.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}

	sig := domain.SagaSignal{
		ID:         uuid.New(),
		RunID:      run.ID,
		SignalName: "test-signal",
		ReceivedAt: time.Now().UTC(),
	}
	if err := s.AppendSignal(ctx, sig); err != nil {
		t.Fatalf("append signal: %v", err)
	}

	// Transition to a terminal state; this should set TTL on the run keys.
	if err := s.UpdateRunState(ctx, run.ID, domain.RunStateSucceeded, "done"); err != nil {
		t.Fatalf("update run state: %v", err)
	}

	id := run.ID.String()

	// Assert run, events, and signals keys each have a positive TTL
	// immediately after the terminal transition (no sleep needed).
	checkTTLBounds(t, ctx, s, s.key("run", id), runTTL)
	checkTTLBounds(t, ctx, s, s.key("events", id), runTTL)
	checkTTLBounds(t, ctx, s, s.key("signals", id), runTTL)
}
