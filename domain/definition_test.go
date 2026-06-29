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
	"encoding/json"
	"testing"
)

func TestParseTrivialWorkflow(t *testing.T) {
	raw := `{
		"id": "wf_trivial",
		"version": 1,
		"name": "Trivial",
		"start": "end",
		"steps": [
			{ "id": "end", "type": "end" }
		]
	}`
	var d WorkflowDefinition
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.ID != "wf_trivial" || d.Version != 1 || d.Start != "end" {
		t.Errorf("definition fields: %+v", d)
	}
	if len(d.Steps) != 1 || d.Steps[0].ID != "end" || d.Steps[0].Type != StepTypeEnd {
		t.Errorf("steps: %+v", d.Steps)
	}
}

func TestResolveEntry(t *testing.T) {
	d := WorkflowDefinition{
		ID:    "wf_test",
		Start: "s1",
		Entrypoints: map[string]string{
			"alt": "s2",
		},
	}

	// "" resolves to Start
	got, err := d.ResolveEntry("")
	if err != nil || got != "s1" {
		t.Errorf("empty entrypoint: got %q, err %v; want s1, nil", got, err)
	}

	// "default" resolves to Start
	got, err = d.ResolveEntry("default")
	if err != nil || got != "s1" {
		t.Errorf(`"default" entrypoint: got %q, err %v; want s1, nil`, got, err)
	}

	// named entrypoint resolves to mapped step
	got, err = d.ResolveEntry("alt")
	if err != nil || got != "s2" {
		t.Errorf(`"alt" entrypoint: got %q, err %v; want s2, nil`, got, err)
	}

	// unknown name returns error
	_, err = d.ResolveEntry("nope")
	if err == nil {
		t.Error("unknown entrypoint: expected error, got nil")
	}

	// empty Start returns error for default resolution
	noStart := WorkflowDefinition{ID: "wf_nostart"}
	_, err = noStart.ResolveEntry("")
	if err == nil {
		t.Error("empty Start with default entrypoint: expected error, got nil")
	}
}

func TestStepLookup(t *testing.T) {
	d := WorkflowDefinition{
		Steps: []Step{
			{ID: "a", Type: StepTypeEnd},
			{ID: "b", Type: StepTypeEnd},
		},
	}
	step, ok := d.StepByID("b")
	if !ok || step.ID != "b" {
		t.Errorf("expected step b, got %+v ok=%v", step, ok)
	}
	if _, ok := d.StepByID("missing"); ok {
		t.Error("expected missing lookup to fail")
	}
}
