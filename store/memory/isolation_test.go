package memory

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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// Readers of a run returned by GetRun must be able to read its variables
// while another goroutine updates the run. Before the fix GetRun handed out
// the store's own Variables map, so this raced (visible under -race).
func TestConcurrentGetRunAndUpdateRunVariables(t *testing.T) {
	ctx := context.Background()
	s := New()
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{})
	run.Variables = map[string]any{"scope": map[string]any{"n": 0}}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = s.UpdateRunVariables(ctx, run.ID, map[string]any{
					fmt.Sprintf("k%d_%d", w, i): i,
					"scope.n":                   i,
				})
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := s.GetRun(ctx, run.ID)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				// Read every key and the nested map, the way a verb's CEL
				// evaluation reads run.Variables.
				for k, v := range got.Variables {
					_, _ = k, v
				}
				if scope, ok := got.Variables["scope"].(map[string]any); ok {
					_ = scope["n"]
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Mutating anything a read method returns must never change the store.
func TestReadsReturnIsolatedCopies(t *testing.T) {
	ctx := context.Background()
	s := New()
	signal := "approve"
	run := domain.NewSagaRun("wf", uuid.New(), nil, map[string]any{"in": map[string]any{"a": 1}})
	run.Variables = map[string]any{"list": []any{map[string]any{"x": 1}}, "nested": map[string]any{"y": 1}}
	run.AwaitedSignal = &signal
	run.AwaitedEventHeaders = map[string]string{"h": "1"}
	run.FeatureOverrides = map[string]bool{"f": true}
	run.TryCatchStack = []domain.TryCatchFrame{{StepID: "t", CatchStep: "c"}}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}
	// The caller's own copy must not alias the stored one either.
	run.Variables["nested"].(map[string]any)["y"] = "caller-mutated"

	got, _ := s.GetRun(ctx, run.ID)
	got.Variables["nested"].(map[string]any)["y"] = "mutated"
	got.Variables["list"].([]any)[0].(map[string]any)["x"] = "mutated"
	got.Variables["added"] = true
	got.Inputs["in"].(map[string]any)["a"] = "mutated"
	*got.AwaitedSignal = "mutated"
	got.AwaitedEventHeaders["h"] = "mutated"
	got.FeatureOverrides["f"] = false
	got.TryCatchStack[0].CatchStep = "mutated"

	again, _ := s.GetRun(ctx, run.ID)
	if again.Variables["nested"].(map[string]any)["y"] != 1 {
		t.Errorf("nested variable changed: %v", again.Variables["nested"])
	}
	if again.Variables["list"].([]any)[0].(map[string]any)["x"] != 1 {
		t.Errorf("list element changed: %v", again.Variables["list"])
	}
	if _, ok := again.Variables["added"]; ok {
		t.Error("key added through a returned run reached the store")
	}
	if again.Inputs["in"].(map[string]any)["a"] != 1 {
		t.Errorf("inputs changed: %v", again.Inputs)
	}
	if *again.AwaitedSignal != "approve" {
		t.Errorf("awaited signal changed: %q", *again.AwaitedSignal)
	}
	if again.AwaitedEventHeaders["h"] != "1" || !again.FeatureOverrides["f"] {
		t.Errorf("headers or overrides changed: %v %v", again.AwaitedEventHeaders, again.FeatureOverrides)
	}
	if again.TryCatchStack[0].CatchStep != "c" {
		t.Errorf("try_catch stack changed: %v", again.TryCatchStack)
	}

	// The list reads hand out copies too.
	def := domain.WorkflowDefinition{ID: "child", Start: "done", Steps: []domain.Step{{ID: "done", Type: domain.StepTypeEnd}}}
	childID, _ := s.SpawnChildRun(ctx, run.ID, "fan", "b0", def, map[string]any{"c": map[string]any{"v": 1}})
	_ = s.SetPausedAwaitingEvent(ctx, childID, "topic", nil)
	lists := map[string]func() []domain.SagaRun{
		"ListChildrenByParent": func() []domain.SagaRun {
			r, _ := s.ListChildrenByParent(ctx, run.ID, "fan")
			return r
		},
		"FindRunsByAwaitedEvent": func() []domain.SagaRun {
			r, _ := s.FindRunsByAwaitedEvent(ctx, "topic")
			return r
		},
		"ListRuns": func() []domain.SagaRun {
			r, _ := s.ListRuns(ctx, store.RunFilter{WorkflowID: "child"})
			return r
		},
	}
	for name, list := range lists {
		got := list()
		if len(got) != 1 {
			t.Fatalf("%s returned %d runs, want 1", name, len(got))
		}
		got[0].Inputs["c"].(map[string]any)["v"] = "mutated"
		if list()[0].Inputs["c"].(map[string]any)["v"] != 1 {
			t.Errorf("%s: mutating a returned run changed the store", name)
		}
	}
}

// Workflow definitions, events, signals, user tasks, rules, actions and
// triggers are handed out as copies as well.
func TestOtherReadsReturnIsolatedCopies(t *testing.T) {
	ctx := context.Background()
	s := New()

	defID, _ := s.UpsertWorkflowDefinition(ctx, domain.WorkflowDefinition{
		ID: "wf", Version: 1, Start: "a", Published: true,
		Steps: []domain.Step{{ID: "a", Type: domain.StepTypeEnd, Inputs: map[string]any{"k": 1}}},
	})
	def, _ := s.GetWorkflowDefinition(ctx, defID)
	def.Steps[0].Inputs["k"] = "mutated"
	def.Steps[0].ID = "mutated"
	def2, _ := s.GetWorkflowDefinition(ctx, defID)
	if def2.Steps[0].Inputs["k"] != 1 || def2.Steps[0].ID != "a" {
		t.Errorf("definition changed through a returned copy: %+v", def2.Steps[0])
	}

	run := domain.NewSagaRun("wf", defID, nil, nil)
	_ = s.CreateRun(ctx, run)
	evt := domain.NewEvent(run.ID, "a", 0, domain.EventStepSucceeded, "engine")
	evt.Metadata = map[string]any{"m": 1}
	_ = s.AppendEvent(ctx, evt)
	events, _ := s.ListEventsByRun(ctx, run.ID)
	events[0].Metadata["m"] = "mutated"
	events2, _ := s.ListEventsByRun(ctx, run.ID)
	if events2[0].Metadata["m"] != 1 {
		t.Errorf("event metadata changed: %v", events2[0].Metadata)
	}

	task := domain.UserTask{ID: uuid.New(), RunID: run.ID, FormSchema: map[string]any{"f": 1}}
	_ = s.CreateUserTask(ctx, task)
	gt, _ := s.GetUserTask(ctx, task.ID)
	gt.FormSchema["f"] = "mutated"
	tasks, _ := s.ListUserTasksByRun(ctx, run.ID)
	if tasks[0].FormSchema["f"] != 1 {
		t.Errorf("user task changed: %v", tasks[0].FormSchema)
	}

	trigID, _ := s.UpsertTrigger(ctx, domain.SagaTrigger{WorkflowID: "wf", Config: map[string]any{"c": 1}})
	tr, _ := s.GetTrigger(ctx, trigID)
	tr.Config["c"] = "mutated"
	tr2, _ := s.GetTrigger(ctx, trigID)
	if tr2.Config["c"] != 1 {
		t.Errorf("trigger config changed: %v", tr2.Config)
	}
}
