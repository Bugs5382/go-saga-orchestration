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
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
)

func seedCronWorkflowAndTrigger(t *testing.T, s *memory.Store, fireAt time.Time) {
	t.Helper()
	ctx := context.Background()
	_, _ = s.UpsertWorkflowDefinition(ctx, domain.WorkflowDefinition{
		ID: "wf-cron", Version: 1, Start: "done", Published: true,
		Steps: []domain.Step{{ID: "done", Type: domain.StepTypeEnd}},
	})
	_, _ = s.UpsertTrigger(ctx, domain.SagaTrigger{
		TriggerType: domain.TriggerCron, WorkflowID: "wf-cron", Version: 1,
		Config: map[string]any{"schedule": "* * * * *"}, Enabled: true,
		NextFireAt: &fireAt, CreatedBy: "t",
	})
}

func TestCronDispatcher_FiresDueTrigger(t *testing.T) {
	s := memory.New()
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	seedCronWorkflowAndTrigger(t, s, now.Add(-time.Second))
	pub := &recordingPublisher{}
	d := &CronDispatcher{S: s, Publisher: pub, Clock: clock.NewFakeClock(now), Licensing: licensing.StubAllowAll{}}
	if err := d.fireDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.runs) != 1 {
		t.Fatalf("want 1 run advanced, got %d", len(pub.runs))
	}
}

func TestCronDispatcher_SkipsWhenUnlicensed(t *testing.T) {
	s := memory.New()
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	seedCronWorkflowAndTrigger(t, s, now.Add(-time.Second))
	pub := &recordingPublisher{}
	d := &CronDispatcher{S: s, Publisher: pub, Clock: clock.NewFakeClock(now), Licensing: denyAll{}}
	if err := d.fireDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.runs) != 0 {
		t.Fatalf("unlicensed tenant must not fire, got %d", len(pub.runs))
	}
}
