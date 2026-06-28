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

// ActionVerb dispatches a registered action to a worker over its declared
// transport. The transport comes from the action's ActionRegistration
// dispatch descriptor (issue #59):
//
//   - "" / "grpc": the zero-config default. Publishes to ExchangeAction with
//     routing key = step.Action; the worker is connected over the gRPC
//     ExecuteStep stream.
//   - "http": POSTs the ActionPayload to the registration's Address (a
//     callback URL).
//   - "rmq": publishes the ActionPayload to the RabbitMQ queue named by the
//     registration's Address.
//
// Inputs:
//   - step.Action (required, string): "<service>.<action_name>". Must contain a dot.
//   - step.Inputs (any): forwarded verbatim to the worker.
//
// The verb:
//  1. Bumps current_attempt + persists the awaiting state via MarkAwaitingAction.
//  2. Resolves the action's dispatch descriptor (latest registered version) and
//     dispatches over the declared transport.
//  3. Returns ErrSagaPaused.
//
// gRPC workers reply via the ExecuteStep stream (Complete/Error). http and rmq
// workers have no return stream; they report their result asynchronously via
// the result-callback REST endpoint
// (POST /api/v1/sagas/{run_id}/actions/{step_id}/result). Both paths land on
// the same CompleteAction / FailAction store hooks that resume or fail the saga.
type ActionVerb struct {
	S         store.Store
	Publisher ActionDispatchPublisher
	// HTTPDispatcher delivers the payload for transport="http" registrations.
	// nil disables http dispatch (resolution falls back to an error).
	HTTPDispatcher ActionHTTPDispatcher
	// RMQDispatcher delivers the payload for transport="rmq" registrations.
	// nil disables rmq dispatch.
	RMQDispatcher ActionRMQDispatcher
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
	if err := v.dispatch(ctx, action, body); err != nil {
		return nil, err
	}
	return nil, ErrSagaPaused
}

// dispatch routes the marshalled ActionPayload to the transport declared on
// the action's registration. gRPC (or no descriptor) is the default; http and
// rmq require a non-empty Address and a configured dispatcher. (issue #59)
func (v ActionVerb) dispatch(ctx context.Context, action string, body []byte) error {
	transport, address := v.resolveDispatch(ctx, action)
	switch transport {
	case "", domain.TransportGRPC:
		if v.Publisher == nil {
			return nil // no grpc publisher wired (e.g. tests); still pauses
		}
		if err := v.Publisher.PublishActionDispatch(ctx, action, body); err != nil {
			return fmt.Errorf("action: grpc dispatch: %w", err)
		}
		return nil
	case domain.TransportHTTP:
		if address == "" {
			return fmt.Errorf("action %q: transport http requires an address", action)
		}
		if v.HTTPDispatcher == nil {
			return fmt.Errorf("action %q: transport http not supported (no http dispatcher configured)", action)
		}
		if err := v.HTTPDispatcher.DispatchHTTP(ctx, address, body); err != nil {
			return fmt.Errorf("action: http dispatch: %w", err)
		}
		return nil
	case domain.TransportRMQ:
		if address == "" {
			return fmt.Errorf("action %q: transport rmq requires an address", action)
		}
		if v.RMQDispatcher == nil {
			return fmt.Errorf("action %q: transport rmq not supported (no rmq dispatcher configured)", action)
		}
		if err := v.RMQDispatcher.DispatchRMQQueue(ctx, address, body); err != nil {
			return fmt.Errorf("action: rmq dispatch: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("action %q: unknown transport %q", action, transport)
	}
}

// resolveDispatch looks up the action's registration (latest registered
// version) and returns its transport + address. An unregistered action — or
// one with no descriptor — resolves to the gRPC default (empty, empty), so the
// zero-config path keeps working without a registry entry.
func (v ActionVerb) resolveDispatch(ctx context.Context, action string) (transport, address string) {
	service, name, ok := splitAction(action)
	if !ok || v.S == nil {
		return "", ""
	}
	regs, err := v.S.ListActions(ctx, store.ActionFilter{Service: service})
	if err != nil {
		return "", ""
	}
	var best *domain.ActionRegistration
	for i := range regs {
		if regs[i].ActionName != name {
			continue
		}
		if best == nil || regs[i].Version > best.Version {
			best = &regs[i]
		}
	}
	if best == nil {
		return "", ""
	}
	return best.Transport, best.Address
}

// splitAction splits "<service>.<action_name>" into its parts. The action name
// may itself contain dots; only the first dot separates service from name.
func splitAction(action string) (service, name string, ok bool) {
	i := strings.Index(action, ".")
	if i <= 0 || i == len(action)-1 {
		return "", "", false
	}
	return action[:i], action[i+1:], true
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
