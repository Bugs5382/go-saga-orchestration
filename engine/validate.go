// Package engine — workflow-definition validators run before persisting.
// Reject structural errors (forbidden compositions, missing references)
// at publish/create time so runtime never sees them.
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
	"fmt"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
)

// LicenseGateError is returned by ValidateDefinitionWithLicense when a
// step references a verb in a license-group whose feature flag isn't
// enabled for the tenant. The API publish endpoint returns this as a
// 422 with the structured body.
type LicenseGateError struct {
	StepID  string `json:"step_id"`
	Group   string `json:"group"`
	Feature string `json:"feature"`
}

// Error formats the gate failure naming the step, group, and required feature.
func (e LicenseGateError) Error() string {
	return fmt.Sprintf("license_gate: step %q in group %q requires feature %q (not enabled for tenant)",
		e.StepID, e.Group, e.Feature)
}

// ValidateDefinition runs structural checks on a workflow definition.
// Returns nil if valid. Currently checks:
//   - try_catch cannot contain a parallel step (per v1 spec §12.3).
//
// Future checks: license-group gates,
// max nesting depth, missing-step-reference detection.
//
// TODO: call ValidateDefinition from the POST /workflows publish
// handler so invalid definitions are rejected at the API boundary.
func ValidateDefinition(def domain.WorkflowDefinition) error {
	stepIndex := map[string]domain.Step{}
	for _, s := range def.Steps {
		stepIndex[s.ID] = s
	}
	for name, stepID := range def.Entrypoints {
		if _, ok := stepIndex[stepID]; !ok {
			return fmt.Errorf("validate: entrypoint %q references missing step %q", name, stepID)
		}
	}
	for _, s := range def.Steps {
		if s.Type != domain.StepTypeTryCatch {
			continue
		}
		tryAny, ok := s.Inputs["try"].([]any)
		if !ok {
			continue // malformed try_catch; let runtime surface the issue
		}
		for _, idAny := range tryAny {
			id, _ := idAny.(string)
			inner, ok := stepIndex[id]
			if !ok {
				continue
			}
			if inner.Type == domain.StepTypeParallel {
				return fmt.Errorf("validate: try_catch step %q contains parallel step %q (forbidden)", s.ID, id)
			}
		}
	}
	return nil
}

// ValidateDefinitionWithLicense runs the existing structural checks
// (try-contains-parallel etc) AND walks every step to verify the
// tenant's license includes the verb's feature flag. First license-gate
// violation is returned; structural errors take precedence.
//
// Pass a Resolver from licensing. For non-licensed environments
// (dev/test) pass licensing.StubAllowAll{}, which approves everything.
//
// overrides: per-saga feature overrides (X-Feature-Override header).
// Pass nil for none.
func ValidateDefinitionWithLicense(
	def domain.WorkflowDefinition,
	registry verbs.Registry,
	resolver licensing.Resolver,
	tenantID *uuid.UUID,
	overrides map[string]bool,
) error {
	if err := ValidateDefinition(def); err != nil {
		return err
	}
	for _, s := range def.Steps {
		entry, ok := registry[s.Type]
		if !ok {
			continue // unknown step type — caught at runtime
		}
		group := verbs.LicenseGroupForStep(s, entry.LicenseGroup)
		feature := verbs.GroupToFeature[group]
		if feature == "" {
			continue // "common" — no gate
		}
		enabled, err := resolver.IsFeatureEnabled(context.Background(), tenantID, feature, overrides)
		if err != nil {
			return fmt.Errorf("validate: license check for step %q: %w", s.ID, err)
		}
		if !enabled {
			return LicenseGateError{StepID: s.ID, Group: group, Feature: feature}
		}
	}
	return nil
}
