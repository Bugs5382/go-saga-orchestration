package verbs

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
	"fmt"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// WaitUntilVerb pauses the saga until a wall-clock instant. Inputs:
//   - "timestamp" (required, string): RFC3339 timestamp (e.g. "2026-12-31T23:59:00Z").
//
// If the timestamp is in the past relative to the engine's clock, the
// verb sets wakeup_at to clock.Now() so the next timer tick wakes the
// saga immediately. This mirrors `wait_duration` but with an absolute
// rather than relative deadline.
type WaitUntilVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute parses the RFC3339 timestamp, persists it as wakeup_at (clamped to
// now if in the past), and returns ErrSagaPaused for the timer to resume.
func (v WaitUntilVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	tsStr, _ := step.Inputs["timestamp"].(string)
	if tsStr == "" {
		return nil, fmt.Errorf("wait_until: timestamp required")
	}
	t, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return nil, fmt.Errorf("wait_until: parse %q: %w", tsStr, err)
	}
	t = t.UTC()
	now := v.Clock.Now()
	if t.Before(now) {
		t = now // past deadline → wake at next tick
	}
	if err := v.S.SetPausedWithWakeup(ctx, run.ID, t); err != nil {
		return nil, fmt.Errorf("wait_until: persist wakeup: %w", err)
	}
	return nil, ErrSagaPaused
}
