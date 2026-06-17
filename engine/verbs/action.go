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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// ActionPayload is the body of a saga.advance → action dispatch message.
// Workers deserialise this to drive their handler.
type ActionPayload struct {
	RunID          string         `json:"run_id"`
	StepID         string         `json:"step_id"`
	Attempt        int            `json:"attempt"`
	IdempotencyKey string         `json:"idempotency_key"`
	Action         string         `json:"action"` // "<service>.<action_name>"
	Inputs         map[string]any `json:"inputs"`
	DryRun         bool           `json:"dry_run,omitempty"`
}

// ActionVerb dispatches a registered action to a worker via RabbitMQ.
//
// Inputs:
//   - step.Action (required, string): "<service>.<action_name>". Must contain a dot.
//   - step.Inputs (any): forwarded verbatim to the worker.
//
// The verb:
//  1. Bumps current_attempt + persists the awaiting state via MarkAwaitingAction.
//  2. Publishes to ExchangeAction with routing key = step.Action.
//  3. Returns ErrSagaPaused.
//
// The worker's Complete/Error reply (via gRPC ExecuteStep) routes
// through CompleteAction / FailAction store hooks to resume or fail the saga.
type ActionVerb struct {
	S         store.Store
	Publisher ActionDispatchPublisher
}

// Execute validates the action name, persists the awaiting-action state with
// a bumped attempt, publishes the dispatch message to the worker, and returns
// ErrSagaPaused so the saga waits for the worker's reply.
func (v ActionVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	action := step.Action
	if action == "" {
		return nil, fmt.Errorf("action: step.Action required (format service.name)")
	}
	if !strings.Contains(action, ".") {
		return nil, fmt.Errorf("action: bad format %q (want service.name)", action)
	}

	// Bump attempt BEFORE publish so a late Complete sees the right attempt number.
	attempt := run.CurrentAttempt + 1
	idemKey := generateIdempotencyKey(run.ID.String(), step.ID, attempt)

	payload := ActionPayload{
		RunID:          run.ID.String(),
		StepID:         step.ID,
		Attempt:        attempt,
		IdempotencyKey: idemKey,
		Action:         action,
		Inputs:         step.Inputs,
		DryRun:         run.DryRun,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("action: marshal payload: %w", err)
	}
	if err := v.S.MarkAwaitingAction(ctx, run.ID, action, attempt); err != nil {
		return nil, fmt.Errorf("action: mark awaiting: %w", err)
	}
	if v.Publisher != nil {
		if err := v.Publisher.PublishActionDispatch(ctx, action, body); err != nil {
			return nil, fmt.Errorf("action: publish: %w", err)
		}
	}
	return nil, ErrSagaPaused
}

// generateIdempotencyKey returns a deterministic hex key for (run, step, attempt).
// Workers store recent keys to dedupe redeliveries.
func generateIdempotencyKey(runID, stepID string, attempt int) string {
	h := sha256.Sum256([]byte(runID + "|" + stepID + "|" + fmt.Sprintf("%d", attempt)))
	return hex.EncodeToString(h[:])
}

// Compile-time: confirm mq.ExchangeAction constant exists so the package
// reference in the import is meaningful and caught by the compiler.
var _ = mq.ExchangeAction
