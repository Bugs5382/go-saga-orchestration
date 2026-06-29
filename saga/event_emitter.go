package saga

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
	"encoding/json"

	"github.com/rs/zerolog"

	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// InProcessEventEmitter delivers an emitted event in-process, with no broker,
// mirroring service mode where the same event feeds both the event subscriber
// (which wakes awaiting runs) and the trigger dispatcher (which starts new runs):
//   - wakes runs awaiting the topic, applying the same header-subset match as
//     engine.EventSubscriber, then
//   - runs the trigger dispatcher so matching triggers start new runs.
type InProcessEventEmitter struct {
	store      store.Store
	publisher  engine.Publisher
	dispatcher *engine.TriggerDispatcher
	log        zerolog.Logger
}

// EmitEvent wakes paused runs awaiting topic (header-subset match) and then runs
// the trigger dispatcher against the event so matching triggers start new runs.
func (e *InProcessEventEmitter) EmitEvent(ctx context.Context, topic string, headers map[string]string, payload map[string]any) error {
	runs, err := e.store.FindRunsByAwaitedEvent(ctx, topic)
	if err != nil {
		return err
	}
	for _, r := range runs {
		// Replicate engine/event_subscriber.go headersSubset: all keys in
		// r.AwaitedEventHeaders must be present in the incoming headers with
		// equal values. Empty AwaitedEventHeaders always matches.
		if !inprocHeadersSubset(r.AwaitedEventHeaders, headers) {
			continue
		}
		// Best-effort per run: unlike engine.EventSubscriber.Deliver (which
		// returns on the first error), one failing run must not block waking
		// the rest, so we log and continue.
		if err := e.store.WakeFromExternal(ctx, r.ID); err != nil {
			e.log.Warn().Err(err).Str("run_id", r.ID.String()).Msg("emit_event: wake failed")
			continue
		}
		if e.publisher != nil {
			if err := e.publisher.PublishSagaAdvance(ctx, r.ID.String()); err != nil {
				e.log.Warn().Err(err).Str("run_id", r.ID.String()).Msg("emit_event: publish advance failed")
			}
		}
	}

	// Also fan the event into the trigger dispatcher so matching triggers start
	// new runs (parity with service mode, where the same delivery feeds both the
	// subscriber and the dispatcher).
	if e.dispatcher != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			e.log.Warn().Err(err).Str("topic", topic).Msg("emit_event: marshal payload for triggers failed")
			return nil
		}
		if err := e.dispatcher.Dispatch(ctx, engine.EventDelivery{Topic: topic, Headers: headers, Body: body}); err != nil {
			e.log.Warn().Err(err).Str("topic", topic).Msg("emit_event: trigger dispatch failed")
		}
	}
	return nil
}

// inprocHeadersSubset reports whether all keys in want are present in have
// with matching values. Empty want always matches.
func inprocHeadersSubset(want, have map[string]string) bool {
	for k, v := range want {
		if hv, ok := have[k]; !ok || hv != v {
			return false
		}
	}
	return true
}
