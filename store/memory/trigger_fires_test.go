package memory

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
OUT OF OR IN CONNECTION WITH THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestRecordTriggerFire_PersistsRow(t *testing.T) {
	s := New()
	ctx := context.Background()

	triggerID := uuid.New()
	runID := uuid.New()

	if err := s.RecordTriggerFire(ctx, triggerID, "wf-test", &runID, ""); err != nil {
		t.Fatalf("RecordTriggerFire: %v", err)
	}

	fires := s.TriggerFires()
	if len(fires) != 1 {
		t.Fatalf("want 1 trigger fire row, got %d", len(fires))
	}
	got := fires[0]
	if got.TriggerID != triggerID {
		t.Errorf("TriggerID = %v, want %v", got.TriggerID, triggerID)
	}
	if got.WorkflowID != "wf-test" {
		t.Errorf("WorkflowID = %q, want %q", got.WorkflowID, "wf-test")
	}
	if got.ResultingRunID == nil || *got.ResultingRunID != runID {
		t.Errorf("ResultingRunID = %v, want %v", got.ResultingRunID, runID)
	}
	if got.Error != "" {
		t.Errorf("Error = %q, want empty", got.Error)
	}
	if got.FiredAt.IsZero() {
		t.Error("FiredAt must not be zero")
	}
}

func TestRecordTriggerFire_NoRunID_PersistsRow(t *testing.T) {
	s := New()
	ctx := context.Background()

	triggerID := uuid.New()

	if err := s.RecordTriggerFire(ctx, triggerID, "wf-fail", nil, "workflow not found"); err != nil {
		t.Fatalf("RecordTriggerFire: %v", err)
	}

	fires := s.TriggerFires()
	if len(fires) != 1 {
		t.Fatalf("want 1 trigger fire row, got %d", len(fires))
	}
	got := fires[0]
	if got.TriggerID != triggerID {
		t.Errorf("TriggerID = %v, want %v", got.TriggerID, triggerID)
	}
	if got.ResultingRunID != nil {
		t.Errorf("ResultingRunID = %v, want nil", got.ResultingRunID)
	}
	if got.Error != "workflow not found" {
		t.Errorf("Error = %q, want %q", got.Error, "workflow not found")
	}
}
