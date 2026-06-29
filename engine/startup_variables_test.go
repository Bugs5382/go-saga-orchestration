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
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func discardLogger() zerolog.Logger { return zerolog.Nop() }

// fakeProvider returns a fixed map (or error) for testing the merge loop.
type fakeProvider struct {
	vars map[string]any
	err  error
}

func (f fakeProvider) StartupVariables(_ context.Context, _ *uuid.UUID) (map[string]any, error) {
	return f.vars, f.err
}

// newPendingRun seeds a minimal run and returns its ID.
func newPendingRun(t *testing.T, s *memory.Store, ctx context.Context) uuid.UUID {
	t.Helper()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	return run.ID
}

func TestInjectStartupVariables_NoProviders_NoOp(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	runID := newPendingRun(t, s, ctx)

	InjectStartupVariables(ctx, s, runID, nil, discardLogger())

	run, _ := s.GetRun(ctx, runID)
	if len(run.Variables) != 0 {
		t.Errorf("expected no variables injected, got %v", run.Variables)
	}
}

func TestInjectStartupVariables_MergesProviders(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	runID := newPendingRun(t, s, ctx)

	p1 := fakeProvider{vars: map[string]any{"_a": 1}}
	p2 := fakeProvider{vars: map[string]any{"_b": 2}}
	InjectStartupVariables(ctx, s, runID, nil, discardLogger(), p1, p2)

	run, _ := s.GetRun(ctx, runID)
	if run.Variables["_a"] != 1 || run.Variables["_b"] != 2 {
		t.Errorf("expected merged _a=1,_b=2, got %v", run.Variables)
	}
}

func TestInjectStartupVariables_ProviderErrorSkipped(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	runID := newPendingRun(t, s, ctx)

	bad := fakeProvider{err: errors.New("boom")}
	good := fakeProvider{vars: map[string]any{"_ok": true}}
	InjectStartupVariables(ctx, s, runID, nil, discardLogger(), bad, good)

	run, _ := s.GetRun(ctx, runID)
	if run.Variables["_ok"] != true {
		t.Errorf("expected good provider to still apply, got %v", run.Variables)
	}
}
