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
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// TimerAdvisoryLockID is the Postgres advisory lock the timer
// dispatcher holds while it's the leader.
const TimerAdvisoryLockID = int64(0xBA70C420)

// TimerPublisher is the surface Timer needs from mq.Publisher. Allows
// in-process tests to supply a fake.
type TimerPublisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// Timer polls saga_runs for due wakeups and republishes saga.advance.
// Run it as a goroutine from cmd/engine; one leader-elected instance
// per cluster at a time (advisory-lock guarded by the caller via
// AcquireLeaderLock).
type Timer struct {
	S         store.Store
	Publisher TimerPublisher
	Clock     clock.Clock
	Tick      time.Duration // default 1s; tests use 10ms with FakeClock
	BatchSize int           // default 100
}

// Run loops until ctx is cancelled. Each tick: query due-wakeup runs,
// publish saga.advance for each.
func (t *Timer) Run(ctx context.Context) error {
	tick := t.Tick
	if tick == 0 {
		tick = time.Second
	}
	batch := t.BatchSize
	if batch == 0 {
		batch = 100
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.Clock.After(tick):
		}
		ids, err := t.S.FindRunsByDueWakeup(ctx, t.Clock.Now(), batch)
		if err != nil {
			log.Error().Err(err).Msg("timer: find due wakeups")
			continue
		}
		for _, id := range ids {
			if err := t.Publisher.PublishSagaAdvance(ctx, id.String()); err != nil {
				log.Error().Err(err).Str("run_id", id.String()).Msg("timer: publish saga.advance")
			}
		}
	}
}

// AcquireLeaderLock blocks until it can acquire the timer-dispatcher
// advisory lock, then returns. Caller MUST release the lock (or close
// the conn) on shutdown. v1 uses pgx directly; the helper is in the
// postgres pkg so the engine pkg stays Postgres-agnostic.
//
// This is a stub — wired by cmd/engine via a postgres
// helper. The Timer itself is decoupled from the lock; tests run
// without a leader-election step.
