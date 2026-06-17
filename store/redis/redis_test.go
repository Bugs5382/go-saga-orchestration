// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

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

	// Transition to a terminal state; this should set TTL on the run keys.
	if err := s.UpdateRunState(ctx, run.ID, domain.RunStateSucceeded, "done"); err != nil {
		t.Fatalf("update run state: %v", err)
	}

	// Assert the run key has a positive TTL immediately after transition.
	runKey := s.key("run", run.ID.String())
	ttl, err := s.rdb.TTL(ctx, runKey).Result()
	if err != nil {
		t.Fatalf("TTL command failed: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("expected run key TTL > 0 after terminal transition, got %v (key may have no expiry set)", ttl)
	}
	if ttl > runTTL {
		t.Errorf("TTL %v exceeds configured runTTL %v", ttl, runTTL)
	}
}
