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
	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// The store keeps domain values whose maps, slices and pointers would
// otherwise be shared with callers. Every value is deep-copied on the way in
// and on the way out, so a caller can never read or write the store's own
// data outside the mutex. A durable store gets this for free by
// serialising; the in-memory store has to do it by hand.

// cloneAny deep-copies the JSON-shaped values that live in variables,
// inputs, payloads and schemas. Other types (scalars, uuid.UUID, time.Time)
// are values already and are returned as is.
func cloneAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneMap(t)
	case []any:
		if t == nil {
			return t
		}
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneAny(e)
		}
		return out
	case map[string]string:
		return cloneStrMap(t)
	case []string:
		return cloneStrs(t)
	case []map[string]any:
		if t == nil {
			return t
		}
		out := make([]map[string]any, len(t))
		for i, e := range t {
			out[i] = cloneMap(e)
		}
		return out
	default:
		return v
	}
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneAny(v)
	}
	return out
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneBoolMap(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneStrs(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s...)
}

// clonePtr copies the value behind p so the copy does not share it.
func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneRun(r domain.SagaRun) domain.SagaRun {
	r.TenantID = clonePtr(r.TenantID)
	r.Inputs = cloneMap(r.Inputs)
	r.Variables = cloneMap(r.Variables)
	r.TerminalAt = clonePtr(r.TerminalAt)
	r.TriggerID = clonePtr(r.TriggerID)
	r.ParentRunID = clonePtr(r.ParentRunID)
	r.ParentStepID = clonePtr(r.ParentStepID)
	r.ParentBranchID = clonePtr(r.ParentBranchID)
	if r.TryCatchStack != nil {
		r.TryCatchStack = append([]domain.TryCatchFrame(nil), r.TryCatchStack...)
	}
	r.WakeupAt = clonePtr(r.WakeupAt)
	r.AwaitedSignal = clonePtr(r.AwaitedSignal)
	r.AwaitedEventTopic = clonePtr(r.AwaitedEventTopic)
	r.AwaitedEventHeaders = cloneStrMap(r.AwaitedEventHeaders)
	r.FeatureOverrides = cloneBoolMap(r.FeatureOverrides)
	r.AwaitedActionDispatch = clonePtr(r.AwaitedActionDispatch)
	r.LastError = clonePtr(r.LastError)
	return r
}

func cloneRuns(rs []domain.SagaRun) []domain.SagaRun {
	if rs == nil {
		return nil
	}
	out := make([]domain.SagaRun, len(rs))
	for i, r := range rs {
		out[i] = cloneRun(r)
	}
	return out
}

func cloneStep(st domain.Step) domain.Step {
	st.Inputs = cloneMap(st.Inputs)
	if st.Compensation != nil {
		c := *st.Compensation
		c.Inputs = cloneMap(c.Inputs)
		st.Compensation = &c
	}
	st.Retry = clonePtr(st.Retry)
	if st.Branches != nil {
		b := make(map[string]domain.Branch, len(st.Branches))
		for k, v := range st.Branches {
			b[k] = v
		}
		st.Branches = b
	}
	st.Extra = cloneMap(st.Extra)
	return st
}

func cloneDef(d domain.WorkflowDefinition) domain.WorkflowDefinition {
	d.TenantID = clonePtr(d.TenantID)
	d.Entrypoints = cloneStrMap(d.Entrypoints)
	if d.Steps != nil {
		steps := make([]domain.Step, len(d.Steps))
		for i, st := range d.Steps {
			steps[i] = cloneStep(st)
		}
		d.Steps = steps
	}
	return d
}

func cloneDefs(ds []domain.WorkflowDefinition) []domain.WorkflowDefinition {
	if ds == nil {
		return nil
	}
	out := make([]domain.WorkflowDefinition, len(ds))
	for i, d := range ds {
		out[i] = cloneDef(d)
	}
	return out
}

func cloneEvent(e domain.SagaRunEvent) domain.SagaRunEvent {
	e.Metadata = cloneMap(e.Metadata)
	return e
}

func cloneEvents(es []domain.SagaRunEvent) []domain.SagaRunEvent {
	if es == nil {
		return nil
	}
	out := make([]domain.SagaRunEvent, len(es))
	for i, e := range es {
		out[i] = cloneEvent(e)
	}
	return out
}

func cloneSignal(s domain.SagaSignal) domain.SagaSignal {
	s.Payload = cloneMap(s.Payload)
	s.ConsumedAt = clonePtr(s.ConsumedAt)
	return s
}

func cloneUserTask(t domain.UserTask) domain.UserTask {
	t.DueAt = clonePtr(t.DueAt)
	t.FormSchema = cloneMap(t.FormSchema)
	t.SubmittedAt = clonePtr(t.SubmittedAt)
	t.Result = cloneMap(t.Result)
	return t
}

func cloneAction(a domain.ActionRegistration) domain.ActionRegistration {
	a.InputSchema = cloneMap(a.InputSchema)
	a.OutputSchema = cloneMap(a.OutputSchema)
	a.ErrorCodes = cloneStrs(a.ErrorCodes)
	a.DefaultRetry = clonePtr(a.DefaultRetry)
	return a
}

func cloneRule(r domain.RuleDefinition) domain.RuleDefinition {
	r.TenantID = clonePtr(r.TenantID)
	r.Spec = cloneRuleSpec(r.Spec)
	return r
}

func cloneRuleSpec(s domain.RuleSpec) domain.RuleSpec {
	if s.Rows != nil {
		rows := make([]domain.DecisionTableRow, len(s.Rows))
		for i, row := range s.Rows {
			row.Then = cloneMap(row.Then)
			rows[i] = row
		}
		s.Rows = rows
	}
	s.DefaultOutput = cloneMap(s.DefaultOutput)
	return s
}

func cloneTrigger(t domain.SagaTrigger) domain.SagaTrigger {
	t.Config = cloneMap(t.Config)
	t.TenantID = clonePtr(t.TenantID)
	t.NextFireAt = clonePtr(t.NextFireAt)
	t.LastFiredAt = clonePtr(t.LastFiredAt)
	return t
}

func cloneTriggerFire(f domain.TriggerFireRow) domain.TriggerFireRow {
	f.ResultingRunID = clonePtr(f.ResultingRunID)
	return f
}
