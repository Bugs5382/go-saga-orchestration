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
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// TriggerDispatcher matches incoming events to saga_triggers rows and
// starts a new saga for each match. Peer of EventSubscriber: the same
// RabbitMQ delivery feeds both — EventSubscriber wakes paused sagas,
// TriggerDispatcher starts new ones.
type TriggerDispatcher struct {
	S                store.Store
	Publisher        TimerPublisher // reuse — same PublishSagaAdvance method
	StartupProviders []StartupVariableProvider
}

// Dispatch inspects one delivery. For each enabled record_transition
// trigger whose config matches the event's record_type, from_state,
// and to_state, it creates a new saga run and publishes saga.advance.
// Returns the first error encountered (from store/publish failures);
// logs and continues on per-trigger failures so one bad trigger doesn't
// block the others.
func (d *TriggerDispatcher) Dispatch(ctx context.Context, evt EventDelivery) error {
	// 1. Only proceed for "*.record.transitioned.*" routing keys.
	if !isRecordTransitionTopic(evt.Topic) {
		return nil
	}

	// 2. Decode event body.
	var body map[string]any
	if err := json.Unmarshal(evt.Body, &body); err != nil {
		// Malformed body — skip silently; nothing to match against.
		return nil
	}

	// record_type from body is authoritative; topic's trailing segment is
	// only a delivery hint.
	recordType, _ := body["record_type"].(string)
	fromState, _ := body["from_state"].(string)
	toState, _ := body["to_state"].(string)

	// 3. List enabled record_transition triggers.
	enabled := true
	triggers, err := d.S.ListTriggers(ctx, store.TriggerFilter{
		Type:    domain.TriggerRecordTransition,
		Enabled: &enabled,
	})
	if err != nil {
		return err
	}

	// 4. For each trigger, compare config fields and start the saga if all three match.
	var firstErr error
	for _, trig := range triggers {
		cfgRT, _ := trig.Config["record_type"].(string)
		cfgFrom, _ := trig.Config["from_state"].(string)
		cfgTo, _ := trig.Config["to_state"].(string)

		if cfgRT != recordType || cfgFrom != fromState || cfgTo != toState {
			continue
		}

		// Build inputs from body + trigger's optional input_mapping.
		var inputMapping map[string]any
		if im, ok := trig.Config["input_mapping"].(map[string]any); ok {
			inputMapping = im
		}
		inputs := mapInputs(body, inputMapping)

		// 6. Tenant resolution: trigger's tenant_id takes precedence; fall back
		//    to body's "tenant_id"; then nil (global / no-tenant).
		tenantID := trig.TenantID
		if tenantID == nil {
			if tidStr, ok := body["tenant_id"].(string); ok && tidStr != "" {
				if tid, parseErr := uuid.Parse(tidStr); parseErr == nil {
					tid := tid // copy
					tenantID = &tid
				}
			}
		}

		// 5. Start the saga: resolve definition, upsert, create run, publish.
		def, err := d.S.GetPublishedWorkflowByID(ctx, trig.WorkflowID, tenantID)
		if err != nil {
			log.Error().Err(err).
				Str("workflow_id", trig.WorkflowID).
				Str("trigger_id", trig.ID.String()).
				Msg("trigger dispatcher: workflow not found, skipping")
			// ErrNotFound on the workflow is non-fatal — log and move on.
			continue
		}

		// Optional entrypoint in trigger config — "" resolves to the definition's Start.
		trigEntrypoint, _ := trig.Config["entrypoint"].(string)
		startStep, err := def.ResolveEntry(trigEntrypoint)
		if err != nil {
			log.Error().Err(err).
				Str("trigger_id", trig.ID.String()).
				Str("entrypoint", trigEntrypoint).
				Msg("trigger dispatcher: invalid entrypoint, skipping")
			continue
		}

		defRowID, err := d.S.UpsertWorkflowDefinition(ctx, def)
		if err != nil {
			log.Error().Err(err).Str("trigger_id", trig.ID.String()).Msg("trigger dispatcher: upsert definition")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		run := domain.NewSagaRun(def.ID, defRowID, tenantID, inputs)
		run.CurrentStep = startStep
		trigID := trig.ID
		run.TriggerID = &trigID
		if err := d.S.CreateRun(ctx, run); err != nil {
			log.Error().Err(err).Str("trigger_id", trig.ID.String()).Msg("trigger dispatcher: create run")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		InjectStartupVariables(ctx, d.S, run.ID, tenantID, log.Logger, d.StartupProviders...)

		if d.Publisher != nil {
			if err := d.Publisher.PublishSagaAdvance(ctx, run.ID.String()); err != nil {
				log.Error().Err(err).Str("run_id", run.ID.String()).Msg("trigger dispatcher: publish advance")
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}

		if recErr := d.S.RecordTriggerFire(ctx, trig.ID, trig.WorkflowID, &run.ID, ""); recErr != nil {
			log.Warn().Err(recErr).Str("trigger_id", trig.ID.String()).Msg("trigger dispatcher: record trigger fire")
		}
	}

	return firstErr
}

// isRecordTransitionTopic returns true iff topic matches the shape
// "*.record.transitioned.*" — exactly 4 dot-separated segments where the
// middle two are "record" and "transitioned" and the last is non-empty.
// Uses simple strings.Split; no regex.
func isRecordTransitionTopic(topic string) bool {
	parts := strings.Split(topic, ".")
	if len(parts) != 4 {
		return false
	}
	return parts[1] == "record" && parts[2] == "transitioned" && parts[3] != ""
}

// mapInputs builds the saga's input map from the event body and the
// trigger's input_mapping. v1 supports only top-level field references
// of the form "$.fieldname" — no nested paths. Unmapped values are
// passed through as literals. If input_mapping is nil or empty, the
// event body itself becomes the inputs.
//
// Example:
//
//	eventBody:     {"record_id":"c1","actor":"u1","extra":"x"}
//	input_mapping: {"change_id":"$.record_id","requester_id":"$.actor","tag":"static"}
//	result:        {"change_id":"c1","requester_id":"u1","tag":"static"}
//
// Full JSONPath / nested paths are deferred to a later iteration.
func mapInputs(body map[string]any, mapping map[string]any) map[string]any {
	if len(mapping) == 0 {
		return body
	}
	out := make(map[string]any, len(mapping))
	for k, raw := range mapping {
		switch v := raw.(type) {
		case string:
			if strings.HasPrefix(v, "$.") {
				if got, ok := body[strings.TrimPrefix(v, "$.")]; ok {
					out[k] = got
				}
				// if the source field is absent, omit the key entirely (not nil)
				continue
			}
			out[k] = v
		default:
			out[k] = raw
		}
	}
	return out
}
