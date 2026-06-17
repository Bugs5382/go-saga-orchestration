package domain

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
	"testing"

	"github.com/google/uuid"
)

func TestRunStateIsTerminal(t *testing.T) {
	cases := []struct {
		state    RunState
		terminal bool
	}{
		{RunStatePending, false},
		{RunStateRunning, false},
		{RunStatePaused, false},
		{RunStateCompensating, false},
		{RunStateSucceeded, true},
		{RunStateFailed, true},
		{RunStateCancelled, true},
	}
	for _, c := range cases {
		if got := c.state.IsTerminal(); got != c.terminal {
			t.Errorf("%s.IsTerminal() = %v, want %v", c.state, got, c.terminal)
		}
	}
}

func TestNewRun(t *testing.T) {
	defID := uuid.New()
	r := NewSagaRun("wf_trivial", defID, nil, map[string]any{"k": "v"})
	if r.ID == uuid.Nil {
		t.Error("expected ID to be generated")
	}
	if r.State != RunStatePending {
		t.Errorf("state = %s, want pending", r.State)
	}
	if r.WorkflowID != "wf_trivial" {
		t.Errorf("workflow_id = %q", r.WorkflowID)
	}
	if r.DefinitionID != defID {
		t.Errorf("definition_id mismatch")
	}
}
