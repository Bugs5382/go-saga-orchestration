package domain

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
	"time"

	"github.com/google/uuid"
)

// RuleType is the discriminant for the union of rule shapes. v1 ships
// only `decision_table`; v1.5+ adds `script` and `expression_tree`.
type RuleType string

// RuleTypeDecisionTable is the only rule type shipped in v1.
const (
	RuleTypeDecisionTable RuleType = "decision_table"
)

// HitPolicy controls how rows in a decision_table are evaluated. v1
// supports only HitPolicyFirst (return the first matched row's output).
type HitPolicy string

// HitPolicyFirst returns the first matched row's output; the only policy in v1.
const (
	HitPolicyFirst HitPolicy = "first"
)

// RuleDefinition is one version of one rule. Stored in
// definitions.rule_definitions.
type RuleDefinition struct {
	ID        uuid.UUID `json:"id"`
	RuleID    string    `json:"rule_id"`
	Version   int       `json:"version"`
	TenantID  *string   `json:"tenant_id,omitempty"`
	Name      string    `json:"name"`
	RuleType  RuleType  `json:"rule_type"`
	Spec      RuleSpec  `json:"spec"`
	Published bool      `json:"published"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
}

// RuleSpec is the union of rule-type-specific bodies. v1 only fills in
// DecisionTable; later rule types add their own fields.
type RuleSpec struct {
	HitPolicy     HitPolicy          `json:"hit_policy"`
	Rows          []DecisionTableRow `json:"rows"`
	DefaultOutput map[string]any     `json:"default_output,omitempty"`
}

// DecisionTableRow is one row in a decision_table. The When expression
// is CEL; on truthy result the Then map becomes the rule's output.
type DecisionTableRow struct {
	When string         `json:"when"`
	Then map[string]any `json:"then"`
}

// NewRuleDefinition returns a fresh RuleDefinition with a generated ID
// and current timestamp. Caller fills in the spec.
func NewRuleDefinition(ruleID string, version int, name string, ruleType RuleType, spec RuleSpec, createdBy string) RuleDefinition {
	return RuleDefinition{
		ID:        uuid.New(),
		RuleID:    ruleID,
		Version:   version,
		Name:      name,
		RuleType:  ruleType,
		Spec:      spec,
		Published: true,
		CreatedAt: time.Now().UTC(),
		CreatedBy: createdBy,
	}
}
