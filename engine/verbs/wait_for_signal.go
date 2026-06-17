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

// WaitForSignalVerb pauses the saga until a named external signal arrives
// via POST /api/v1/sagas/{run_id}/signal/{name}. Inputs:
//   - "name"      (required, string): signal name to await.
//   - "timeout_s" (optional, float64): max seconds to wait before the
//     timer dispatcher wakes the saga regardless. If omitted the saga
//     waits indefinitely (no wakeup_at set by the verb itself; the signal
//     handler sets wakeup_at=now() when it arrives).
//
// On signal arrival: TryConsumeAwaitedSignal clears the await markers and
// sets wakeup_at=now(). The signal REST handler publishes saga.advance;
// Advance sees paused+due-wakeup and resumes from the next step uniformly.
type WaitForSignalVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute persists the awaited signal name (and optional timeout deadline) on
// the run and returns ErrSagaPaused until the signal arrives or times out.
func (v WaitForSignalVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	name, _ := step.Inputs["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("wait_for_signal: name required")
	}

	var deadline *time.Time
	if to, ok := step.Inputs["timeout_s"].(float64); ok && to > 0 {
		d := v.Clock.Now().Add(time.Duration(to * float64(time.Second)))
		deadline = &d
	}

	if err := v.S.SetPausedAwaitingSignal(ctx, run.ID, name, deadline); err != nil {
		return nil, fmt.Errorf("wait_for_signal: persist: %w", err)
	}
	return nil, ErrSagaPaused
}
