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
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// CollectInputVerb is the same shape as ManualApprovalVerb but
// `form_schema` is REQUIRED. Use this verb when the workflow needs
// structured data from the user (e.g., remediation plan, additional
// context) — vs. manual_approval which is typically approve/reject.
//
// Inputs:
//   - "assignee"    (required, string)
//   - "form_schema" (required, map[string]any)
//   - "due_in"      (optional, string Go duration)
type CollectInputVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute creates a user task carrying the required form schema, then pauses
// the saga awaiting the task's submitted signal, returning ErrSagaPaused.
func (v CollectInputVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	assignee, _ := step.Inputs["assignee"].(string)
	if assignee == "" {
		return nil, fmt.Errorf("collect_input: assignee required")
	}
	formSchema, ok := step.Inputs["form_schema"].(map[string]any)
	if !ok || len(formSchema) == 0 {
		return nil, fmt.Errorf("collect_input: form_schema required and non-empty")
	}
	var dueAt *time.Time
	if dueIn, ok := step.Inputs["due_in"].(string); ok && dueIn != "" {
		d, err := time.ParseDuration(dueIn)
		if err != nil {
			return nil, fmt.Errorf("collect_input: bad due_in %q: %w", dueIn, err)
		}
		t := v.Clock.Now().Add(d)
		dueAt = &t
	}
	task := domain.UserTask{
		ID:         uuid.New(),
		RunID:      run.ID,
		StepID:     step.ID,
		Assignee:   assignee,
		DueAt:      dueAt,
		FormSchema: formSchema,
	}
	if err := v.S.CreateUserTask(ctx, task); err != nil {
		return nil, fmt.Errorf("collect_input: create task: %w", err)
	}
	signal := "user_task." + task.ID.String() + ".submitted"
	if err := v.S.SetPausedAwaitingSignal(ctx, run.ID, signal, dueAt); err != nil {
		return nil, fmt.Errorf("collect_input: set awaited signal: %w", err)
	}
	return nil, ErrSagaPaused
}
