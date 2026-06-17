package engine

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
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestValidateDefinition_TryCatchContainsParallel_Rejected(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Name: "t", Start: "t1", Published: true,
		Steps: []domain.Step{
			{ID: "t1", Type: domain.StepTypeTryCatch, Inputs: map[string]any{"try": []any{"p1"}, "catch": "h"}, Next: "end"},
			{ID: "p1", Type: domain.StepTypeParallel, Inputs: map[string]any{"branches": []any{}}, Next: "end"},
			{ID: "h", Type: domain.StepTypeEnd},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	err := ValidateDefinition(def)
	if err == nil || !strings.Contains(err.Error(), "parallel") {
		t.Errorf("expected parallel-in-try error, got %v", err)
	}
}

func TestValidateDefinition_NoTryCatch_AcceptsTrivial(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Name: "t", Start: "end", Published: true,
		Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	if err := ValidateDefinition(def); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDefinition_TryWithSafeInner_Accepts(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Name: "t", Start: "t1", Published: true,
		Steps: []domain.Step{
			{ID: "t1", Type: domain.StepTypeTryCatch, Inputs: map[string]any{"try": []any{"s1"}, "catch": "h"}, Next: "end"},
			{ID: "s1", Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "x", "value": 1}, Next: "end"},
			{ID: "h", Type: domain.StepTypeEnd},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	if err := ValidateDefinition(def); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDefinition_EntrypointMissingStep_Rejected(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Name: "t", Start: "end", Published: true,
		Entrypoints: map[string]string{
			"alt": "nonexistent",
		},
		Steps: []domain.Step{{ID: "end", Type: domain.StepTypeEnd}},
	}
	err := ValidateDefinition(def)
	if err == nil || !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("expected missing-step entrypoint error, got %v", err)
	}
}

// denyAll is a test Resolver that rejects every feature unless an override allows it.
type denyAll struct{}

func (denyAll) IsFeatureEnabled(_ context.Context, _ *uuid.UUID, feature string, overrides map[string]bool) (bool, error) {
	if v, ok := overrides[feature]; ok {
		return v, nil
	}
	return false, nil
}

func TestValidateDefinitionWithLicense_CommonOnly_AllowAll(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Start: "s1", Published: true,
		Steps: []domain.Step{
			{ID: "s1", Type: domain.StepTypeSetVar, Inputs: map[string]any{"out_var": "x", "value": 1}, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	reg := verbs.Default(memory.New(), clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	if err := ValidateDefinitionWithLicense(def, reg, licensing.StubAllowAll{}, nil, nil); err != nil {
		t.Errorf("unexpected: %v", err)
	}
}

func TestValidateDefinitionWithLicense_ParallelLocked_Rejected(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Start: "p", Published: true,
		Steps: []domain.Step{
			{ID: "p", Type: domain.StepTypeParallel, Inputs: map[string]any{"branches": []any{}}, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	reg := verbs.Default(memory.New(), clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	err := ValidateDefinitionWithLicense(def, reg, denyAll{}, nil, nil)
	if err == nil {
		t.Fatalf("expected LicenseGateError, got nil")
	}
	var lge LicenseGateError
	if !errors.As(err, &lge) {
		t.Fatalf("expected LicenseGateError, got %T: %v", err, err)
	}
	if lge.Group != "parallel_control" || lge.Feature != "wf.parallel" {
		t.Errorf("got %+v, want parallel_control/wf.parallel", lge)
	}
}

func TestValidateDefinitionWithLicense_FeatureOverrideUnlocks(t *testing.T) {
	def := domain.WorkflowDefinition{
		ID: "wf", Version: 1, Start: "p", Published: true,
		Steps: []domain.Step{
			{ID: "p", Type: domain.StepTypeParallel, Inputs: map[string]any{"branches": []any{}}, Next: "end"},
			{ID: "end", Type: domain.StepTypeEnd},
		},
	}
	reg := verbs.Default(memory.New(), clock.SystemClock{}, secrets.NewMemory(nil), nil, nil, nil)
	if err := ValidateDefinitionWithLicense(def, reg, denyAll{}, nil, map[string]bool{"wf.parallel": true}); err != nil {
		t.Errorf("feature override should allow parallel; got %v", err)
	}
}
