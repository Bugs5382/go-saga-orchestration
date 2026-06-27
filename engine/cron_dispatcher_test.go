package engine

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
