package saga_test

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
	"sync/atomic"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/saga"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

// Payload values that must never reach a log line.
const (
	secretInput    = "4111-1111-SECRET-INPUT"
	secretVariable = "s3cr3t-variable-value"
	secretResult   = "s3cr3t-result-value"
)

func newLoggedSaga(t *testing.T, rec *sagalogtest.Recorder) *saga.Saga {
	t.Helper()
	opts := saga.Options{Store: memory.New()}
	if rec != nil {
		opts.Logger = rec
	}
	sc, err := saga.New(opts)
	if err != nil {
		t.Fatalf("saga.New: %v", err)
	}
	return sc
}

// flakyVerb fails its first call and succeeds after, returning a secret value.
func flakyVerb() verbs.Handler {
	var calls atomic.Int32
	return verbs.HandlerFunc(func(context.Context, domain.SagaRun, domain.Step) (map[string]any, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("transient failure")
		}
		return map[string]any{"token": secretResult}, nil
	})
}

func requireEntry(t *testing.T, rec *sagalogtest.Recorder, level sagalogtest.Level, msg string) sagalogtest.Entry {
	t.Helper()
	for _, e := range rec.Entries() {
		if e.Level == level && e.Msg == msg {
			return e
		}
	}
	t.Fatalf("no %s line %q; got:\n%s", level, msg, dump(rec))
	return sagalogtest.Entry{}
}

func dump(rec *sagalogtest.Recorder) string {
	out := ""
	for _, e := range rec.Entries() {
		out += e.String() + "\n"
	}
	return out
}

func requireNoPayload(t *testing.T, rec *sagalogtest.Recorder) {
	t.Helper()
	for _, s := range []string{secretInput, secretVariable, secretResult} {
		if rec.Contains(s) {
			t.Fatalf("payload value %q leaked into the log:\n%s", s, dump(rec))
		}
	}
}

// A successful run with one retry logs at every level, and never logs a
// payload value.
func TestLogging_SuccessfulRunCoversEveryLevel(t *testing.T) {
	ctx := context.Background()
	rec := sagalogtest.New()
	sc := newLoggedSaga(t, rec)
	sc.RegisterVerb("flaky", "common", flakyVerb())

	if err := sc.Register(domain.WorkflowDefinition{
		ID: "wf_log", Version: 1, Start: "set", Published: true,
		Steps: []domain.Step{
			{ID: "set", Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "hidden", "value": secretVariable}, Next: "call"},
			{ID: "call", Type: "flaky", Retry: &domain.RetryPolicy{MaxAttempts: 2, InitialBackoffMS: 1}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	runID, err := sc.Start(ctx, "wf_log", map[string]any{"card": secretInput})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	run, _ := sc.Get(ctx, runID)
	if run.State != domain.RunStateSucceeded {
		t.Fatalf("state = %s, want succeeded", run.State)
	}

	started := requireEntry(t, rec, sagalogtest.LevelInfo, "saga started")
	if started.Fields["run_id"] != runID.String() || started.Fields["workflow_id"] != "wf_log" {
		t.Errorf("saga started fields = %v", started.Fields)
	}
	requireEntry(t, rec, sagalogtest.LevelInfo, "saga succeeded")
	exec := requireEntry(t, rec, sagalogtest.LevelDebug, "step executing")
	if exec.Fields["step_id"] == nil || exec.Fields["step_type"] == nil {
		t.Errorf("step executing fields = %v", exec.Fields)
	}
	requireEntry(t, rec, sagalogtest.LevelDebug, "step succeeded")
	retry := requireEntry(t, rec, sagalogtest.LevelWarn, "step failed; retrying")
	if retry.Fields["attempt"] != 1 || retry.Fields["error"] == nil {
		t.Errorf("retry line = %s", retry)
	}
	requireEntry(t, rec, sagalogtest.LevelTrace, "step result")
	requireNoPayload(t, rec)
}

// A failing run logs the step error, the compensation pass and the failed
// run, and never logs a payload value.
func TestLogging_FailedRunLogsErrorAndCompensation(t *testing.T) {
	ctx := context.Background()
	rec := sagalogtest.New()
	sc := newLoggedSaga(t, rec)
	sc.RegisterVerb("ok", "common", verbs.HandlerFunc(func(context.Context, domain.SagaRun, domain.Step) (map[string]any, error) {
		return map[string]any{"token": secretResult}, nil
	}))
	sc.RegisterVerb("boom", "common", verbs.HandlerFunc(func(context.Context, domain.SagaRun, domain.Step) (map[string]any, error) {
		return nil, errors.New("downstream refused")
	}))

	if err := sc.Register(domain.WorkflowDefinition{
		ID: "wf_fail", Version: 1, Start: "a", Published: true,
		Steps: []domain.Step{
			{ID: "a", Type: "ok", Next: "b"},
			{ID: "b", Type: "boom", Inputs: map[string]any{"card": secretInput}, Next: "done"},
			{ID: "done", Type: domain.StepTypeEnd},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	runID, _ := sc.Start(ctx, "wf_fail", map[string]any{"card": secretInput})
	run, _ := sc.Get(ctx, runID)
	if run.State != domain.RunStateFailed {
		t.Fatalf("state = %s, want failed", run.State)
	}

	failed := requireEntry(t, rec, sagalogtest.LevelError, "step failed")
	if failed.Fields["step_id"] != "b" || failed.Err == nil {
		t.Errorf("step failed line = %s", failed)
	}
	requireEntry(t, rec, sagalogtest.LevelInfo, "compensation started")
	requireEntry(t, rec, sagalogtest.LevelWarn, "compensation: step has no compensation; skipping")
	requireEntry(t, rec, sagalogtest.LevelInfo, "compensation finished")
	requireEntry(t, rec, sagalogtest.LevelError, "saga failed")
	requireNoPayload(t, rec)
}

// A logger carried on the context is used when no option is set.
func TestLogging_LoggerFromContext(t *testing.T) {
	rec := sagalogtest.New()
	sc := newLoggedSaga(t, nil)
	if err := sc.Register(domain.WorkflowDefinition{
		ID: "wf_ctx", Version: 1, Start: "done", Published: true,
		Steps: []domain.Step{{ID: "done", Type: domain.StepTypeEnd}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx := sagalog.NewContext(context.Background(), rec)
	if _, err := sc.Start(ctx, "wf_ctx", nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEntry(t, rec, sagalogtest.LevelInfo, "saga started")
	requireEntry(t, rec, sagalogtest.LevelInfo, "saga succeeded")
}

// With no logger configured the library writes nothing, even when a run fails.
func TestLogging_QuietByDefault(t *testing.T) {
	out := captureOutput(t, func() {
		ctx := context.Background()
		sc := saga.InMemory()
		sc.RegisterVerb("boom", "common", verbs.HandlerFunc(func(context.Context, domain.SagaRun, domain.Step) (map[string]any, error) {
			return nil, errors.New("downstream refused")
		}))
		_ = sc.Register(domain.WorkflowDefinition{
			// "a" completes before "b" fails, so the rollback pass runs and
			// warns about a step with no compensation. That warning used to
			// reach stderr through zerolog's global logger.
			ID: "wf_quiet", Version: 1, Start: "a", Published: true,
			Steps: []domain.Step{
				{ID: "a", Type: domain.StepTypeNoop, Next: "b"},
				{ID: "b", Type: "boom", Next: "done"},
				{ID: "done", Type: domain.StepTypeEnd},
			},
		})
		_, _ = sc.Start(ctx, "wf_quiet", nil)
	})
	if out != "" {
		t.Fatalf("expected no output without a configured logger, got:\n%s", out)
	}
}
