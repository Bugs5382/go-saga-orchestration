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
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
)

// DefaultRetryPolicy returns the spec default per § 3.4.
func DefaultRetryPolicy() domain.RetryPolicy {
	return domain.RetryPolicy{
		MaxAttempts:      3,
		InitialBackoffMS: 1000,
		MaxBackoffMS:     60000,
		Multiplier:       2.0,
		Jitter:           true,
	}
}

// Backoff returns the wait duration for `attempt` (zero-indexed) under
// policy p. Caps at MaxBackoffMS. If jitter is true, applies ±25% noise.
func Backoff(p domain.RetryPolicy, attempt int, jitter bool) time.Duration {
	if p.Multiplier <= 0 {
		p.Multiplier = 2.0
	}
	base := float64(p.InitialBackoffMS) * math.Pow(p.Multiplier, float64(attempt))
	capMS := float64(p.MaxBackoffMS)
	if capMS == 0 {
		capMS = 60000
	}
	if base > capMS {
		base = capMS
	}
	if jitter {
		noise := (rand.Float64()*0.5 - 0.25) // -25%..+25%
		base = base * (1 + noise)
	}
	return time.Duration(base) * time.Millisecond
}

// executeStep dispatches a step to its handler, applying the step's RetryPolicy
// to synchronous errors. A nil step.Retry means "run once". On a retryable
// error it waits Backoff(policy, attempt, jitter) using the injected clock — so
// FakeClock can drive the wait deterministically in tests — then re-dispatches,
// up to policy.MaxAttempts total attempts. A paused or cancelled sentinel is
// returned immediately and never retried; those are not failures. The last
// attempt's result/error is returned to the caller, which then runs the normal
// try_catch / compensation path on a final error.
func (c *Coordinator) executeStep(ctx context.Context, run domain.SagaRun, step domain.Step, handler verbs.Handler) (map[string]any, error) {
	maxAttempts := 1
	policy := domain.RetryPolicy{}
	if step.Retry != nil {
		policy = *step.Retry
		if policy.MaxAttempts > 1 {
			maxAttempts = policy.MaxAttempts
		}
	}

	var result map[string]any
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err = handler.Execute(ctx, run, step)
		if err == nil {
			return result, nil
		}
		// Paused / cancelled are control signals, not failures: never retry them.
		if isControlSignal(err) {
			return result, err
		}
		// Out of attempts: surface the final error to the caller.
		if attempt == maxAttempts-1 {
			return result, err
		}
		// Record the failed attempt, then wait the backoff before retrying.
		_ = c.store.AppendEvent(ctx, domain.NewEvent(run.ID, step.ID, attempt+1, domain.EventStepFailed, "engine-retry"))
		wait := Backoff(policy, attempt, policy.Jitter)
		log.Debug().Str("run_id", run.ID.String()).Str("step_id", step.ID).
			Int("attempt", attempt+1).Dur("backoff", wait).Err(err).Msg("step failed; retrying")
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-c.clock.After(wait):
		}
	}
	return result, err
}

// isControlSignal reports whether err is a non-failure control sentinel
// (paused or cancelled) that must bypass the retry loop.
func isControlSignal(err error) bool {
	return errors.Is(err, verbs.ErrSagaPaused) || errors.Is(err, verbs.ErrSagaCancelled)
}
