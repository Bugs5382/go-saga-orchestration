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
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// RegistryEntry is what each registered StepType resolves to. LicenseGroup
// lets the engine gate dispatch by the tenant's license features.
type RegistryEntry struct {
	Handler      Handler
	LicenseGroup string // canonical group name, e.g. "external_io_advanced"
}

// Registry maps step.Type → entry. The engine's Advance loop looks
// up the entry, applies the license gate (if non-common), then runs
// Handler.Execute.
type Registry map[domain.StepType]RegistryEntry

// Default builds the verb registry with license
// groups attached. The store/clock/secrets/publisher deps are threaded
// to verbs that need them. The `end` step is intentionally NOT in the
// registry — Advance short-circuits it.
//
// actionPub is the publisher for action dispatch messages. Pass nil
// in tests that do not exercise action steps — ActionVerb checks for nil.
//
// emitter is the EventEmitter used by emit_event steps. Pass nil in tests
// that do not exercise emit_event — EmitEventVerb checks for nil.
func Default(s store.Store, clk clock.Clock, sec secrets.Resolver, pub Publisher, actionPub ActionDispatchPublisher, emitter EventEmitter, opts ...DefaultOption) Registry {
	cfg := defaultConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return Registry{
		domain.StepTypeAction:         {ActionVerb{S: s, Publisher: actionPub, HTTPDispatcher: cfg.httpDispatcher, RMQDispatcher: cfg.rmqDispatcher}, "common"},
		domain.StepTypeNoop:           {NoopVerb{}, "common"},
		domain.StepTypeSetVar:         {SetVarVerb{}, "common"},
		domain.StepTypeTransform:      {TransformVerb{}, "common"},
		domain.StepTypeMerge:          {MergeVerb{}, "common"},
		domain.StepTypeFilter:         {FilterVerb{}, "common"},
		domain.StepTypeMap:            {MapVerb{}, "common"},
		domain.StepTypeAssert:         {AssertVerb{}, "common"},
		domain.StepTypeError:          {ErrorVerb{}, "common"},
		domain.StepTypeLog:            {LogVerb{S: s}, "common"},
		domain.StepTypeMetricEmit:     {MetricEmitVerb{S: s}, "observability"},
		domain.StepTypeDecision:       {DecisionVerb{S: s}, "common"},
		domain.StepTypeHTTPRequest:    {HTTPRequestVerb{Secrets: sec}, "external_io_advanced"}, // overridden dynamically by LicenseGroupForStep for GET-no-auth
		domain.StepTypeWebhookEmit:    {WebhookEmitVerb{Secrets: sec}, "external_io_advanced"},
		domain.StepTypeWaitDuration:   {WaitDurationVerb{S: s, Clock: clk}, "waits"},
		domain.StepTypeWaitUntil:      {WaitUntilVerb{S: s, Clock: clk}, "waits"},
		domain.StepTypeWaitForSignal:  {WaitForSignalVerb{S: s, Clock: clk}, "events_and_signals"},
		domain.StepTypeWaitForEvent:   {WaitForEventVerb{S: s, Clock: clk}, "events_and_signals"},
		domain.StepTypeEmitSignal:     {EmitSignalVerb{S: s, Publisher: pub}, "events_and_signals"},
		domain.StepTypeParallel:       {ParallelVerb{S: s, Publisher: pub}, "parallel_control"},
		domain.StepTypeForeach:        {ForeachVerb{S: s, Publisher: pub}, "parallel_control"},
		domain.StepTypeWhile:          {WhileVerb{}, "loops_and_recovery"},
		domain.StepTypeSwitch:         {SwitchVerb{}, "common"},
		domain.StepTypeTryCatch:       {TryCatchVerb{S: s}, "loops_and_recovery"},
		domain.StepTypeSubSaga:        {SubSagaVerb{S: s, Publisher: pub}, "compositions"},
		domain.StepTypeSpawnSaga:      {SpawnSagaVerb{S: s, Publisher: pub}, "compositions"},
		domain.StepTypeManualApproval: {ManualApprovalVerb{S: s, Clock: clk}, "human_interaction"},
		domain.StepTypeCollectInput:   {CollectInputVerb{S: s, Clock: clk}, "human_interaction"},
		domain.StepTypeCancel:         {CancelVerb{S: s}, "loops_and_recovery"},
		domain.StepTypeEmitEvent:      {EmitEventVerb{Emitter: emitter}, "events_and_signals"},
	}
}

// defaultConfig holds the optional dependencies applied to Default via
// DefaultOption. (issue #59)
type defaultConfig struct {
	httpDispatcher ActionHTTPDispatcher
	rmqDispatcher  ActionRMQDispatcher
}

// DefaultOption configures optional verb dependencies (e.g. the http/rmq
// action dispatchers) without breaking Default's positional signature.
type DefaultOption func(*defaultConfig)

// WithHTTPDispatcher wires the http action dispatcher for transport="http"
// action registrations. (issue #59)
func WithHTTPDispatcher(d ActionHTTPDispatcher) DefaultOption {
	return func(c *defaultConfig) { c.httpDispatcher = d }
}

// WithRMQDispatcher wires the rmq action dispatcher for transport="rmq"
// action registrations. (issue #59)
func WithRMQDispatcher(d ActionRMQDispatcher) DefaultOption {
	return func(c *defaultConfig) { c.rmqDispatcher = d }
}
