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
	"os"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func TestWhileVerbEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	raw, err := os.ReadFile("../fixtures/wf_while.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var def domain.WorkflowDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	defID, _ := s.UpsertWorkflowDefinition(ctx, def)

	coord := engine.NewCoordinator(s, nil, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, nil, nil)
	run := domain.NewSagaRun(def.ID, defID, nil, map[string]any{})
	_ = s.CreateRun(ctx, run)
	if err := coord.Advance(ctx, run.ID.String()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	got, _ := s.GetRun(ctx, run.ID)
	if got.State != domain.RunStateSucceeded {
		t.Errorf("state = %s, want succeeded", got.State)
	}
	final, ok := got.Variables["counter"].(float64)
	if !ok {
		t.Fatalf("counter not float64, got %T = %v", got.Variables["counter"], got.Variables["counter"])
	}
	if final < 3 {
		t.Errorf("counter = %v, want >= 3", final)
	}
}
