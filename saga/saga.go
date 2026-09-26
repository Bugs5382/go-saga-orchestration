// Package saga is the embedding entrypoint: construct an in-process saga engine,
// register workflows and custom verbs, and drive runs — without running the
// engine binaries.
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
	"errors"
	golog "github.com/Bugs5382/go-log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

var errMissingStore = errors.New("saga: Options.Store is required")

// Options configures a Saga engine instance.
type Options struct {
	Store            store.Store
	Clock            clock.Clock
	Licensing        licensing.Resolver
	Secrets          secrets.Resolver
	Publisher        engine.Publisher
	StartupProviders []engine.StartupVariableProvider
	// Logger receives the engine's logs: saga start and finish, step
	// execution, retries, compensation, timeouts and errors. Optional: nil
	// means the library writes nothing. A logger carried on a call's context
	// (sagalog.NewContext) takes precedence for that call. Build one with
	// golog.NewLogger to honour LOG_LEVEL and LOG_FORMAT.
	Logger golog.Logger
	// Context is the base context for background work (parallel/foreach/spawn
	// child advances run on a cancellable context derived from it). Defaults to
	// context.Background(). Shutdown cancels the derived context.
	Context context.Context
}

// Saga is the embedding facade around the coordinator and store.
type Saga struct {
	coord     *engine.Coordinator
	store     store.Store
	providers []engine.StartupVariableProvider
	log       golog.Logger       // nil = silent
	cancel    context.CancelFunc // cancels background advances; called by Shutdown
	wg        *sync.WaitGroup    // tracks in-flight background advances
}

// New constructs a Saga from opts. opts.Store is required; all other fields
// have sensible defaults (SystemClock, in-memory secrets, StubAllowAll
// licensing, in-process publisher).
func New(opts Options) (*Saga, error) {
	if opts.Store == nil {
		return nil, errMissingStore
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.SystemClock{}
	}
	sec := opts.Secrets
	if sec == nil {
		sec = secrets.NewMemory(nil)
	}
	lg := opts.Logger
	base := opts.Context
	if base == nil {
		base = context.Background()
	}
	bgCtx, cancel := context.WithCancel(base)
	wg := &sync.WaitGroup{}

	inproc := &InProcessPublisher{ctx: bgCtx, wg: wg, log: lg}
	pub := opts.Publisher
	if pub == nil {
		pub = inproc
	}
	actionPub := actionPublisher(opts.Publisher, inproc)
	dispatcher := &engine.TriggerDispatcher{S: opts.Store, Publisher: pub, StartupProviders: opts.StartupProviders, Logger: lg}
	emitter := &InProcessEventEmitter{store: opts.Store, publisher: pub, dispatcher: dispatcher, log: lg}
	coord := engine.NewCoordinator(opts.Store, pub, clk, sec, opts.Licensing, actionPub, emitter)
	coord.SetLogger(lg)
	inproc.coord = coord
	sagalog.Or(lg).Debug("saga engine created", golog.F("in_process_publisher", opts.Publisher == nil),
		golog.F("startup_providers", len(opts.StartupProviders)))
	return &Saga{coord: coord, store: opts.Store, providers: opts.StartupProviders, log: lg, cancel: cancel, wg: wg}, nil
}

// Shutdown cancels the Saga's background context (so in-flight background
// advances stop between steps) and waits for them to drain, bounded by ctx.
// Returns ctx.Err() if the drain does not complete before ctx is done. After
// Shutdown the Saga should not be reused.
func (s *Saga) Shutdown(ctx context.Context) error {
	lg := sagalog.For(ctx, s.log)
	lg.Info("saga engine shutting down")
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		lg.Info("saga engine shut down")
		return nil
	case <-ctx.Done():
		lg.Warn("saga engine shutdown: background advances still running at deadline", golog.F("error", ctx.Err().Error()))
		return ctx.Err()
	}
}

func actionPublisher(p engine.Publisher, fallback verbs.ActionDispatchPublisher) verbs.ActionDispatchPublisher {
	if ap, ok := p.(verbs.ActionDispatchPublisher); ok && ap != nil {
		return ap
	}
	return fallback
}

// InMemory returns a Saga backed by an in-memory store with all defaults.
// Convenient for tests and examples.
func InMemory() *Saga {
	s, _ := New(Options{Store: memory.New()})
	return s
}

// Register upserts a workflow definition into the store so it can be started.
func (s *Saga) Register(def domain.WorkflowDefinition) error {
	_, err := s.store.UpsertWorkflowDefinition(context.Background(), def)
	return err
}

// RegisterVerb adds or replaces a verb handler identified by stepType in the
// coordinator's registry.
func (s *Saga) RegisterVerb(stepType string, licenseGroup string, h verbs.Handler) {
	s.coord.RegisterVerb(domain.StepType(stepType), h, licenseGroup)
}

func (s *Saga) Start(ctx context.Context, workflowID string, inputs map[string]any) (uuid.UUID, error) {
	return s.StartAt(ctx, workflowID, "", inputs)
}

// StartAt creates a run beginning at the named entry point ("" => default/Start)
// and advances it once (synchronously to the first pause or terminal state).
func (s *Saga) StartAt(ctx context.Context, workflowID, entrypoint string, inputs map[string]any) (uuid.UUID, error) {
	lg := sagalog.For(ctx, s.log).With(golog.F("workflow_id", workflowID), golog.F("entrypoint", entrypoint))
	def, err := s.store.GetPublishedWorkflowByID(ctx, workflowID, nil)
	if err != nil {
		lg.Error(err, "start: published workflow not found")
		return uuid.Nil, err
	}
	startStep, err := def.ResolveEntry(entrypoint)
	if err != nil {
		lg.Error(err, "start: invalid entrypoint")
		return uuid.Nil, err
	}
	defRowID, err := s.store.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		lg.Error(err, "start: upsert definition failed")
		return uuid.Nil, err
	}
	run := domain.NewSagaRun(def.ID, defRowID, nil, inputs)
	run.CurrentStep = startStep
	if err := s.store.CreateRun(ctx, run); err != nil {
		lg.Error(err, "start: create run failed")
		return uuid.Nil, err
	}
	lg.Debug("run created", golog.F("run_id", run.ID.String()), golog.F("start_step", startStep),
		golog.F("inputs", len(inputs)))
	engine.InjectStartupVariables(ctx, s.store, run.ID, nil, s.log, s.providers...)
	if err := s.coord.Advance(ctx, run.ID.String()); err != nil {
		return run.ID, err
	}
	return run.ID, nil
}

// Get returns the current state of a run by ID.
func (s *Saga) Get(ctx context.Context, runID uuid.UUID) (domain.SagaRun, error) {
	return s.store.GetRun(ctx, runID)
}

// Cancel terminates an in-flight run from outside the run — e.g. an approval
// policy withdrawing or re-submitting while a run is paused at a
// manual_approval. The run transitions to terminal cancelled, its open user
// tasks are closed (so none linger pending), and any awaited signal/event or
// pending wakeup is cleared so a stray advance cannot resurrect it. reason is
// recorded on the run's last_error. Idempotent: a no-op when the run is
// already terminal. See issue #80.
func (s *Saga) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	return s.coord.Cancel(ctx, runID, reason)
}

// Signal delivers an external signal to a run. If the run was paused awaiting
// exactly this signal name, it is consumed and the run advances.
func (s *Saga) Signal(ctx context.Context, runID uuid.UUID, name string, payload map[string]any) error {
	sig := domain.SagaSignal{
		ID:         uuid.New(),
		RunID:      runID,
		SignalName: name,
		Payload:    payload,
		ReceivedAt: s.now(),
	}
	lg := sagalog.For(ctx, s.log).With(golog.F("run_id", runID.String()), golog.F("signal", name))
	if err := s.store.AppendSignal(ctx, sig); err != nil {
		lg.Error(err, "signal: append failed")
		return err
	}
	ok, err := s.store.TryConsumeAwaitedSignal(ctx, runID, name)
	if err != nil {
		lg.Error(err, "signal: consume failed")
		return err
	}
	if ok {
		lg.Debug("signal consumed; advancing run")
		return s.coord.Advance(ctx, runID.String())
	}
	lg.Debug("signal recorded; run was not waiting for it")
	return nil
}

func (s *Saga) now() time.Time { return time.Now().UTC() }

// Coordinator returns the underlying engine.Coordinator for advanced use.
func (s *Saga) Coordinator() *engine.Coordinator { return s.coord }

// Store returns the underlying store.Store for direct access.
func (s *Saga) Store() store.Store { return s.store }
