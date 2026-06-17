package e2e

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
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

type denyAllResolver struct{}

func (denyAllResolver) IsFeatureEnabled(_ context.Context, _ *uuid.UUID, feature string, overrides map[string]bool) (bool, error) {
	if v, ok := overrides[feature]; ok {
		return v, nil
	}
	return false, nil
}

type lgPub struct {
	coord *engine.Coordinator
	calls atomic.Int32
}

func (p *lgPub) PublishSagaAdvance(ctx context.Context, runID string) error {
	p.calls.Add(1)
	go func() { _ = p.coord.Advance(context.Background(), runID) }()
	return nil
}

// 1. Common-only workflow + allow-all licensing → runs to succeeded.
func TestLicenseGate_CommonOnly_AllowAll(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	raw, _ := os.ReadFile("../fixtures/wf_license_gate_common.json")
	var def domain.WorkflowDefinition
	_ = json.Unmarshal(raw, &def)
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	// Validate at publish time.
	reg := verbs.Default(s, clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	if err := engine.ValidateDefinitionWithLicense(def, reg, licensing.StubAllowAll{}, nil, nil); err != nil {
		t.Fatalf("expected publish to pass for common-only: %v", err)
	}

	// Run.
	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(nil), licensing.StubAllowAll{}, nil, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("state = %s, want succeeded", got.State)
	}
}

// 2. Parallel workflow + deny resolver → publish rejected with LicenseGateError;
//
//	runtime advance also fails the saga with the gate.
func TestLicenseGate_ParallelLocked_RejectedAtPublishAndRuntime(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	raw, _ := os.ReadFile("../fixtures/wf_license_gate_parallel.json")
	var def domain.WorkflowDefinition
	_ = json.Unmarshal(raw, &def)

	reg := verbs.Default(s, clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	err := engine.ValidateDefinitionWithLicense(def, reg, denyAllResolver{}, nil, nil)
	if err == nil {
		t.Fatalf("expected publish LicenseGateError, got nil")
	}
	var lge engine.LicenseGateError
	if !errors.As(err, &lge) {
		t.Fatalf("expected LicenseGateError, got %T: %v", err, err)
	}
	if lge.Feature != "wf.parallel" {
		t.Errorf("feature = %q, want wf.parallel", lge.Feature)
	}

	// Now bypass the publish validator (simulate an admin who has
	// somehow committed a definition without validation, e.g. via a
	// direct migration) — runtime guard must still reject.
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)
	pub := &lgPub{}
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(nil), denyAllResolver{}, nil, nil)
	pub.coord = coord

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	advErr := coord.Advance(ctx, run.ID.String())
	if advErr == nil {
		t.Fatalf("expected runtime gate error, got nil")
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateFailed {
		t.Errorf("state = %s, want failed (license_gate)", got.State)
	}
	// Audit should have a license.gate.rejected event.
	events, _ := s.ListEventsByRun(ctx, run.ID)
	var found bool
	for _, e := range events {
		if e.EventType == domain.EventLicenseGateRejected {
			found = true
		}
	}
	if !found {
		t.Errorf("expected EventLicenseGateRejected, events = %+v", events)
	}
}

// 3. Parallel workflow + deny resolver + X-Feature-Override {wf.parallel: true}
//
//	→ both publish and runtime pass.
func TestLicenseGate_FeatureOverrideUnlocksParallel(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	raw, _ := os.ReadFile("../fixtures/wf_license_gate_parallel.json")
	var def domain.WorkflowDefinition
	_ = json.Unmarshal(raw, &def)
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	reg := verbs.Default(s, clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	overrides := map[string]bool{"wf.parallel": true}
	if err := engine.ValidateDefinitionWithLicense(def, reg, denyAllResolver{}, nil, overrides); err != nil {
		t.Fatalf("expected feature override to pass publish: %v", err)
	}

	pub := &lgPub{}
	coord := engine.NewCoordinator(s, pub, clock.SystemClock{}, secrets.NewMemory(nil), denyAllResolver{}, nil, nil)
	pub.coord = coord

	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	run.FeatureOverrides = overrides
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.GetRun(ctx, run.ID)
		if got.State == domain.RunStateSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.GetRun(ctx, run.ID)
	t.Fatalf("simulation-unlock saga did not reach succeeded; state=%s", got.State)
}
