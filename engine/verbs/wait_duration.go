package verbs

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
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// WaitDurationVerb pauses the saga for a fixed duration. Inputs:
//   - "duration" (required, string): Go duration syntax e.g. "5s", "1h30m".
//
// Persists wakeup_at on the saga run and returns ErrSagaPaused; the
// coordinator catches the sentinel, appends EventStepPaused, and ACKs
// the queue message. The timer dispatcher polls for due wakeups and
// republishes saga.advance.
type WaitDurationVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute parses the duration, persists wakeup_at = now + duration on the run,
// and returns ErrSagaPaused for the timer dispatcher to resume.
func (v WaitDurationVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	dStr, _ := step.Inputs["duration"].(string)
	if dStr == "" {
		return nil, fmt.Errorf("wait_duration: duration required")
	}
	d, err := time.ParseDuration(dStr)
	if err != nil {
		return nil, fmt.Errorf("wait_duration: parse %q: %w", dStr, err)
	}
	if d < 0 {
		return nil, fmt.Errorf("wait_duration: negative duration not allowed: %s", dStr)
	}
	wakeup := v.Clock.Now().Add(d)
	if err := v.S.SetPausedWithWakeup(ctx, run.ID, wakeup); err != nil {
		return nil, fmt.Errorf("wait_duration: persist wakeup: %w", err)
	}
	return nil, ErrSagaPaused
}
