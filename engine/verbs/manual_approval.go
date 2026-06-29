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

// ManualApprovalVerb creates a user task and pauses the saga awaiting
// its submission via POST /api/v1/sagas/{run_id}/user_task/{task_id}/submit.
// The submit handler appends a signal of name
// `user_task.{task_id}.submitted` which wakes this saga.
//
// Inputs:
//   - "assignee"    (required, string): user ID or role expected to submit.
//   - "due_in"      (optional, string, Go duration): sets due_at = clock.Now() + due_in.
//   - "form_schema" (optional, map[string]any): rendered to the assignee
//     in the UI (admin panel). For manual_approval the form is
//     typically a simple {approve|reject} radio; the schema is optional.
type ManualApprovalVerb struct {
	S     store.Store
	Clock clock.Clock
}

// Execute creates a user task for the assignee and pauses the saga awaiting
// its submitted signal, returning ErrSagaPaused.
func (v ManualApprovalVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	assignee, _ := step.Inputs["assignee"].(string)
	if assignee == "" {
		return nil, fmt.Errorf("manual_approval: assignee required")
	}
	var dueAt *time.Time
	if dueIn, ok := step.Inputs["due_in"].(string); ok && dueIn != "" {
		d, err := time.ParseDuration(dueIn)
		if err != nil {
			return nil, fmt.Errorf("manual_approval: bad due_in %q: %w", dueIn, err)
		}
		t := v.Clock.Now().Add(d)
		dueAt = &t
	}
	formSchema, _ := step.Inputs["form_schema"].(map[string]any)

	task := domain.UserTask{
		ID:         uuid.New(),
		RunID:      run.ID,
		StepID:     step.ID,
		Assignee:   assignee,
		DueAt:      dueAt,
		FormSchema: formSchema,
	}
	if err := v.S.CreateUserTask(ctx, task); err != nil {
		return nil, fmt.Errorf("manual_approval: create task: %w", err)
	}
	signal := "user_task." + task.ID.String() + ".submitted"
	if err := v.S.SetPausedAwaitingSignal(ctx, run.ID, signal, dueAt); err != nil {
		return nil, fmt.Errorf("manual_approval: set awaited signal: %w", err)
	}
	return nil, ErrSagaPaused
}
