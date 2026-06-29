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
	"context"
	"strings"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func TestSwitch_ReturnsMatchingBranchKey(t *testing.T) {
	out, err := SwitchVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{"tier": "gold"}},
		domain.Step{Inputs: map[string]any{"expr": "tier"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out["branch"] != "gold" {
		t.Errorf("branch = %v, want gold", out["branch"])
	}
}

func TestSwitch_EmptyExprReturnsError(t *testing.T) {
	_, err := SwitchVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{}},
		domain.Step{Inputs: map[string]any{"expr": ""}})
	if err == nil || !strings.Contains(err.Error(), "expr required") {
		t.Errorf("expected expr required error, got %v", err)
	}
}

func TestSwitch_NonStringResultReturnsError(t *testing.T) {
	_, err := SwitchVerb{}.Execute(context.Background(),
		domain.SagaRun{Variables: map[string]any{"count": int64(42)}},
		domain.Step{Inputs: map[string]any{"expr": "count"}})
	if err == nil || !strings.Contains(err.Error(), "must evaluate to string") {
		t.Errorf("expected string type error, got %v", err)
	}
}
