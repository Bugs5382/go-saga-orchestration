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

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// EmitSignalVerb sends a signal to a target run (the send-side of
// wait_for_signal). If the target run is currently paused awaiting that
// signal, it is consumed and a saga.advance message is published so the
// engine resumes the target immediately.
//
// Inputs:
//   - "run_id"  (required, string): target run UUID.
//   - "name"    (required, string): signal name.
//   - "payload" (optional, map[string]any): arbitrary signal payload.
type EmitSignalVerb struct {
	S         store.Store
	Publisher Publisher
}

// Execute implements Handler.
func (v EmitSignalVerb) Execute(ctx context.Context, _ domain.SagaRun, step domain.Step) (map[string]any, error) {
	runIDStr, _ := step.Inputs["run_id"].(string)
	name, _ := step.Inputs["name"].(string)
	if runIDStr == "" || name == "" {
		return nil, fmt.Errorf("emit_signal: run_id and name required")
	}
	targetID, err := uuid.Parse(runIDStr)
	if err != nil {
		return nil, fmt.Errorf("emit_signal: bad run_id: %w", err)
	}
	payload, _ := step.Inputs["payload"].(map[string]any)

	sig := domain.SagaSignal{
		ID:         uuid.New(),
		RunID:      targetID,
		SignalName: name,
		Payload:    payload,
		ReceivedAt: time.Now().UTC(),
	}
	if err := v.S.AppendSignal(ctx, sig); err != nil {
		return nil, fmt.Errorf("emit_signal: append: %w", err)
	}

	ok, err := v.S.TryConsumeAwaitedSignal(ctx, targetID, name)
	if err != nil {
		return nil, fmt.Errorf("emit_signal: consume: %w", err)
	}
	if ok && v.Publisher != nil {
		if err := v.Publisher.PublishSagaAdvance(ctx, targetID.String()); err != nil {
			return nil, fmt.Errorf("emit_signal: publish: %w", err)
		}
	}
	return map[string]any{}, nil
}
