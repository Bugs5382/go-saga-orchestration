// Package memory is an in-process store used by unit tests.
// Production code uses store/postgres.
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

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// Store is the in-memory store. Safe for concurrent use.
type Store struct {
	mu         sync.RWMutex
	defs       map[uuid.UUID]domain.WorkflowDefinition
	defsByName map[string][]uuid.UUID // workflow_id -> versions ordered
	runs       map[uuid.UUID]domain.SagaRun
	events     map[uuid.UUID][]domain.SagaRunEvent
	rules      map[uuid.UUID]domain.RuleDefinition // key: rule UUID
	rulesByID  map[string][]uuid.UUID              // rule_id -> versions
	signals    map[uuid.UUID][]domain.SagaSignal
	userTasks  map[uuid.UUID]domain.UserTask
	actions    map[string]map[int]domain.ActionRegistration // key: service.name -> version
	triggers      map[uuid.UUID]domain.SagaTrigger
	triggerFires  []domain.TriggerFireRow
}

// New returns an empty Store. Compile-time check confirms it satisfies the interface.
func New() *Store {
	return &Store{
		defs:         map[uuid.UUID]domain.WorkflowDefinition{},
		defsByName:   map[string][]uuid.UUID{},
		runs:         map[uuid.UUID]domain.SagaRun{},
		events:       map[uuid.UUID][]domain.SagaRunEvent{},
		rules:        map[uuid.UUID]domain.RuleDefinition{},
		rulesByID:    map[string][]uuid.UUID{},
		signals:      map[uuid.UUID][]domain.SagaSignal{},
		userTasks:    map[uuid.UUID]domain.UserTask{},
		actions:      map[string]map[int]domain.ActionRegistration{},
		triggers:     map[uuid.UUID]domain.SagaTrigger{},
		triggerFires: []domain.TriggerFireRow{},
	}
}

var _ store.Store = (*Store)(nil)

// UpsertWorkflowDefinition stores def under a fresh ID and records it under
// the workflow ID's version list, returning the new storage ID.
func (s *Store) UpsertWorkflowDefinition(_ context.Context, def domain.WorkflowDefinition) (uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := uuid.New()
	s.defs[id] = def
	s.defsByName[def.ID] = append(s.defsByName[def.ID], id)
	return id, nil
}

// GetWorkflowDefinition returns the definition with the given storage ID, or
// ErrNotFound.
func (s *Store) GetWorkflowDefinition(_ context.Context, id uuid.UUID) (domain.WorkflowDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.defs[id]
	if !ok {
		return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: id.String()}
	}
	return d, nil
}

// GetPublishedWorkflowByID returns the newest published version of workflowID,
// falling back to the most recent version if none is published, or ErrNotFound.
func (s *Store) GetPublishedWorkflowByID(_ context.Context, workflowID string, _ *uuid.UUID) (domain.WorkflowDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.defsByName[workflowID]
	// Walk newest-first; return the first Published==true.
	for i := len(ids) - 1; i >= 0; i-- {
		d := s.defs[ids[i]]
		if d.Published {
			return d, nil
		}
	}
	// Fall back to most recent if none published (test convenience).
	if len(ids) > 0 {
		return s.defs[ids[len(ids)-1]], nil
	}
	return domain.WorkflowDefinition{}, store.ErrNotFound{Entity: "workflow_definition", ID: workflowID}
}

// CreateRun stores run keyed by its ID.
func (s *Store) CreateRun(_ context.Context, run domain.SagaRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[run.ID] = run
	return nil
}

// GetRun returns the run with the given ID, or ErrNotFound.
func (s *Store) GetRun(_ context.Context, id uuid.UUID) (domain.SagaRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[id]
	if !ok {
		return domain.SagaRun{}, store.ErrNotFound{Entity: "saga_run", ID: id.String()}
	}
	return r, nil
}

// UpdateRunState sets the run's state and current step, or returns ErrNotFound.
func (s *Store) UpdateRunState(_ context.Context, id uuid.UUID, state domain.RunState, currentStep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: id.String()}
	}
	r.State = state
	r.CurrentStep = currentStep
	s.runs[id] = r
	return nil
}

// AppendEvent appends evt to the event list for evt.RunID.
func (s *Store) AppendEvent(_ context.Context, evt domain.SagaRunEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[evt.RunID] = append(s.events[evt.RunID], evt)
	return nil
}

// ListEventsByRun returns a copy of the events recorded for runID.
func (s *Store) ListEventsByRun(_ context.Context, runID uuid.UUID) ([]domain.SagaRunEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.SagaRunEvent, len(s.events[runID]))
	copy(out, s.events[runID])
	return out, nil
}

// GetEventByID returns the first event whose ID matches, or ErrNotFound.
func (s *Store) GetEventByID(_ context.Context, id uuid.UUID) (domain.SagaRunEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, evts := range s.events {
		for _, e := range evts {
			if e.ID == id {
				return e, nil
			}
		}
	}
	return domain.SagaRunEvent{}, store.ErrNotFound{Entity: "saga_run_event", ID: id.String()}
}

// UpsertRuleDefinition stores def, preserving a caller-supplied ID (or
// generating one) and replacing any prior entry with the same ID.
func (s *Store) UpsertRuleDefinition(_ context.Context, def domain.RuleDefinition) (uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rules == nil {
		s.rules = map[uuid.UUID]domain.RuleDefinition{}
		s.rulesByID = map[string][]uuid.UUID{}
	}
	// Preserve caller-supplied ID; generate one only when the zero UUID is passed.
	id := def.ID
	if id == uuid.Nil {
		id = uuid.New()
		def.ID = id
	}
	// If this ID already exists in the store (re-seed / idempotent call), remove
	// the old rulesByID entry for that ID so we don't accumulate duplicates.
	if existing, ok := s.rules[id]; ok {
		oldIDs := s.rulesByID[existing.RuleID]
		filtered := oldIDs[:0]
		for _, eid := range oldIDs {
			if eid != id {
				filtered = append(filtered, eid)
			}
		}
		s.rulesByID[existing.RuleID] = filtered
	}
	s.rules[id] = def
	s.rulesByID[def.RuleID] = append(s.rulesByID[def.RuleID], id)
	return id, nil
}

// GetPublishedRuleByID returns the newest published version of ruleID, falling
// back to the most recent version if none is published, or ErrNotFound.
func (s *Store) GetPublishedRuleByID(_ context.Context, ruleID string, _ *uuid.UUID) (domain.RuleDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.rulesByID[ruleID]
	for i := len(ids) - 1; i >= 0; i-- {
		r := s.rules[ids[i]]
		if r.Published {
			return r, nil
		}
	}
	if len(ids) > 0 {
		return s.rules[ids[len(ids)-1]], nil
	}
	return domain.RuleDefinition{}, store.ErrNotFound{Entity: "rule_definition", ID: ruleID}
}

// UpdateRunVariables merges the entries of merge into the run's variables,
// honouring dotted keys for nested writes, or returns ErrNotFound.
func (s *Store) UpdateRunVariables(_ context.Context, runID uuid.UUID, merge map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	if r.Variables == nil {
		r.Variables = map[string]any{}
	}
	for k, v := range merge {
		applyDottedKey(r.Variables, k, v)
	}
	s.runs[runID] = r
	return nil
}

// SetPausedWithWakeup marks the run paused and sets its wakeup_at, or returns
// ErrNotFound.
func (s *Store) SetPausedWithWakeup(_ context.Context, runID uuid.UUID, wakeupAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	r.State = domain.RunStatePaused
	w := wakeupAt
	r.WakeupAt = &w
	s.runs[runID] = r
	return nil
}

// SetPausedAwaitingSignal marks the run paused awaiting signalName, setting an
// optional wakeup deadline, or returns ErrNotFound.
func (s *Store) SetPausedAwaitingSignal(_ context.Context, runID uuid.UUID, signalName string, deadline *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	r.State = domain.RunStatePaused
	name := signalName
	r.AwaitedSignal = &name
	if deadline != nil {
		d := *deadline
		r.WakeupAt = &d
	}
	s.runs[runID] = r
	return nil
}

// SetPausedAwaitingEvent marks the run paused awaiting an event matching topic
// and the given header filter, or returns ErrNotFound.
func (s *Store) SetPausedAwaitingEvent(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string) error {
	return s.SetPausedAwaitingEventWithDeadline(ctx, runID, topic, headers, nil)
}

// SetPausedAwaitingEventWithDeadline is SetPausedAwaitingEvent plus an optional
// wakeup deadline (nil = wait indefinitely).
func (s *Store) SetPausedAwaitingEventWithDeadline(_ context.Context, runID uuid.UUID, topic string, headers map[string]string, deadline *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	r.State = domain.RunStatePaused
	t := topic
	r.AwaitedEventTopic = &t
	hdrs := map[string]string{}
	for k, v := range headers {
		hdrs[k] = v
	}
	r.AwaitedEventHeaders = hdrs
	r.WakeupAt = deadline
	s.runs[runID] = r
	return nil
}

// ClearPause returns the run to the running state and clears all wakeup and
// await markers, or returns ErrNotFound.
func (s *Store) ClearPause(_ context.Context, runID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	r.State = domain.RunStateRunning
	r.WakeupAt = nil
	r.AwaitedSignal = nil
	r.AwaitedEventTopic = nil
	r.AwaitedEventHeaders = nil
	s.runs[runID] = r
	return nil
}

// FindRunsByDueWakeup returns up to limit IDs of paused runs whose wakeup_at is
// at or before now.
func (s *Store) FindRunsByDueWakeup(_ context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []uuid.UUID{}
	for id, r := range s.runs {
		if r.State == domain.RunStatePaused && r.WakeupAt != nil && !r.WakeupAt.After(now) {
			out = append(out, id)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// FindRunsByAwaitedEvent returns paused runs awaiting an event on topic.
func (s *Store) FindRunsByAwaitedEvent(_ context.Context, topic string) ([]domain.SagaRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.SagaRun{}
	for _, r := range s.runs {
		if r.State == domain.RunStatePaused && r.AwaitedEventTopic != nil && *r.AwaitedEventTopic == topic {
			out = append(out, r)
		}
	}
	return out, nil
}

// TryConsumeAwaitedSignal clears the await markers and marks any matching
// unconsumed signal consumed when the run is paused awaiting signalName,
// reporting whether it did so.
func (s *Store) TryConsumeAwaitedSignal(_ context.Context, runID uuid.UUID, signalName string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || r.State != domain.RunStatePaused || r.AwaitedSignal == nil || *r.AwaitedSignal != signalName {
		return false, nil
	}
	// Clear all await markers and wakeup_at. The Advance paused-handling
	// block detects "paused + no pending awaits + wakeup_at==nil" as a
	// signal/event wakeup and resumes from the next step.
	r.AwaitedSignal = nil
	r.AwaitedEventTopic = nil
	r.AwaitedEventHeaders = nil
	r.WakeupAt = nil
	// mark any matching unconsumed signal as consumed
	now := time.Now().UTC()
	if sigs, ok := s.signals[runID]; ok {
		for i := range sigs {
			if sigs[i].SignalName == signalName && sigs[i].ConsumedAt == nil {
				sigs[i].ConsumedAt = &now
			}
		}
		s.signals[runID] = sigs
	}
	s.runs[runID] = r
	return true, nil
}

// WakeFromExternal clears all await markers and wakeup_at while leaving the run
// paused, so the Advance loop resumes it, or returns ErrNotFound.
func (s *Store) WakeFromExternal(_ context.Context, runID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	// Leave state=paused; clear all await markers and wakeup_at. The
	// Advance paused-handling block detects "paused + no pending awaits +
	// wakeup_at==nil" as a signal/event wakeup and resumes the saga.
	r.AwaitedSignal = nil
	r.AwaitedEventTopic = nil
	r.AwaitedEventHeaders = nil
	r.WakeupAt = nil
	s.runs[runID] = r
	return nil
}

// AppendSignal appends sig to the signal list for sig.RunID.
func (s *Store) AppendSignal(_ context.Context, sig domain.SagaSignal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.signals == nil {
		s.signals = map[uuid.UUID][]domain.SagaSignal{}
	}
	s.signals[sig.RunID] = append(s.signals[sig.RunID], sig)
	return nil
}

// SpawnChildRunAt creates a child run linked to parentID / parentStepID / branchKey,
// beginning at startStep (empty string means the child definition's Start field).
func (s *Store) SpawnChildRunAt(_ context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any, startStep string) (uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Resolve (or upsert) the definition — reuse defsByName to find a stored ID.
	var defStoredID uuid.UUID
	if ids := s.defsByName[def.ID]; len(ids) > 0 {
		defStoredID = ids[len(ids)-1]
	} else {
		defStoredID = uuid.New()
		s.defs[defStoredID] = def
		s.defsByName[def.ID] = append(s.defsByName[def.ID], defStoredID)
	}

	child := domain.NewSagaRun(def.ID, defStoredID, nil, inputs)
	if startStep != "" {
		child.CurrentStep = startStep
	}
	pid := parentID
	psid := parentStepID
	bid := branchKey
	child.ParentRunID = &pid
	child.ParentStepID = &psid
	child.ParentBranchID = &bid
	s.runs[child.ID] = child
	return child.ID, nil
}

// SpawnChildRun creates a child run linked to parentID / parentStepID / branchKey,
// beginning at the child definition's default Start step.
func (s *Store) SpawnChildRun(ctx context.Context, parentID uuid.UUID, parentStepID, branchKey string, def domain.WorkflowDefinition, inputs map[string]any) (uuid.UUID, error) {
	return s.SpawnChildRunAt(ctx, parentID, parentStepID, branchKey, def, inputs, "")
}

// ListChildrenByParent returns all runs whose ParentRunID == parentID and
// ParentStepID == parentStepID.
func (s *Store) ListChildrenByParent(_ context.Context, parentID uuid.UUID, parentStepID string) ([]domain.SagaRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.SagaRun{}
	for _, r := range s.runs {
		if r.ParentRunID != nil && *r.ParentRunID == parentID &&
			r.ParentStepID != nil && *r.ParentStepID == parentStepID {
			out = append(out, r)
		}
	}
	return out, nil
}

// PushTryCatch appends frame to the run's TryCatchStack. Returns an error if
// the stack is already at maximum depth (3).
func (s *Store) PushTryCatch(_ context.Context, runID uuid.UUID, frame domain.TryCatchFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	const maxDepth = 3
	if len(r.TryCatchStack) >= maxDepth {
		return fmt.Errorf("try_catch max nesting depth %d exceeded for run %s", maxDepth, runID)
	}
	r.TryCatchStack = append(r.TryCatchStack, frame)
	s.runs[runID] = r
	return nil
}

// PopTryCatch removes and returns the top TryCatchFrame. Returns (zero, false,
// nil) when the stack is empty.
func (s *Store) PopTryCatch(_ context.Context, runID uuid.UUID) (domain.TryCatchFrame, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return domain.TryCatchFrame{}, false, store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	if len(r.TryCatchStack) == 0 {
		return domain.TryCatchFrame{}, false, nil
	}
	top := r.TryCatchStack[len(r.TryCatchStack)-1]
	r.TryCatchStack = r.TryCatchStack[:len(r.TryCatchStack)-1]
	s.runs[runID] = r
	return top, true, nil
}

// CreateUserTask stores a new UserTask keyed by its ID.
func (s *Store) CreateUserTask(_ context.Context, task domain.UserTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userTasks[task.ID] = task
	return nil
}

// GetUserTask returns the task or ErrNotFound.
func (s *Store) GetUserTask(_ context.Context, taskID uuid.UUID) (domain.UserTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.userTasks[taskID]
	if !ok {
		return domain.UserTask{}, store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	return t, nil
}

// ListUserTasksByRun returns all user tasks whose RunID matches runID.
// Returned in insertion order by task.ID (domain.UserTask has no CreatedAt
// field, so ID order is used as a stable proxy for creation order).
func (s *Store) ListUserTasksByRun(_ context.Context, runID uuid.UUID) ([]domain.UserTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.UserTask{}
	for _, t := range s.userTasks {
		if t.RunID == runID {
			out = append(out, t)
		}
	}
	// Sort by ID bytes so the ordering is stable and deterministic.
	// UUIDs are generated in temporal order (v4 random), so this gives
	// a reasonable proxy for insertion order in test scenarios.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && uuidLess(out[j].ID, out[j-1].ID); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

// uuidLess compares two UUIDs lexicographically (byte by byte).
func uuidLess(a, b uuid.UUID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// SubmitUserTask marks the task as submitted. Idempotent (re-writes on
// repeated calls). Returns ErrNotFound if the task does not exist.
func (s *Store) SubmitUserTask(_ context.Context, taskID uuid.UUID, submittedBy string, result map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.userTasks[taskID]
	if !ok {
		return store.ErrNotFound{Entity: "user_task", ID: taskID.String()}
	}
	now := time.Now().UTC()
	t.SubmittedAt = &now
	t.SubmittedBy = submittedBy
	t.Result = result
	s.userTasks[taskID] = t
	return nil
}

// UpsertActionRegistration stores or replaces the registration keyed by
// service+name+version.
func (s *Store) UpsertActionRegistration(_ context.Context, reg domain.ActionRegistration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actions == nil {
		s.actions = map[string]map[int]domain.ActionRegistration{}
	}
	key := reg.Service + "." + reg.ActionName
	if s.actions[key] == nil {
		s.actions[key] = map[int]domain.ActionRegistration{}
	}
	if reg.ID == uuid.Nil {
		reg.ID = uuid.New()
	}
	if reg.RegisteredAt.IsZero() {
		reg.RegisteredAt = time.Now().UTC()
	}
	s.actions[key][reg.Version] = reg
	return nil
}

// ListActions returns all registrations matching the optional filter fields.
func (s *Store) ListActions(_ context.Context, filter store.ActionFilter) ([]domain.ActionRegistration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.ActionRegistration{}
	for _, byVer := range s.actions {
		for _, reg := range byVer {
			if filter.Service != "" && reg.Service != filter.Service {
				continue
			}
			if filter.Category != "" && reg.Category != filter.Category {
				continue
			}
			if filter.Search != "" && !strings.Contains(reg.ActionName, filter.Search) {
				continue
			}
			out = append(out, reg)
		}
	}
	return out, nil
}

// GetAction returns the registration for service+name+version, or ErrNotFound.
func (s *Store) GetAction(_ context.Context, service, name string, version int) (domain.ActionRegistration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if byVer, ok := s.actions[service+"."+name]; ok {
		if reg, ok := byVer[version]; ok {
			return reg, nil
		}
	}
	return domain.ActionRegistration{}, store.ErrNotFound{
		Entity: "action_registration",
		ID:     service + "." + name + ":" + strconv.Itoa(version),
	}
}

// MarkAwaitingAction sets state=paused and records the dispatch key + attempt.
// Idempotent on (runID, attempt): if the current_attempt already equals attempt
// and the dispatch key is the same, the call is a no-op.
func (s *Store) MarkAwaitingAction(_ context.Context, runID uuid.UUID, dispatch string, attempt int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	// Idempotent: same (attempt, dispatch) pair has no effect.
	if r.CurrentAttempt == attempt && r.AwaitedActionDispatch != nil && *r.AwaitedActionDispatch == dispatch {
		return nil
	}
	r.State = domain.RunStatePaused
	d := dispatch
	r.AwaitedActionDispatch = &d
	r.CurrentAttempt = attempt
	s.runs[runID] = r
	return nil
}

// CompleteAction merges result into Variables and sets wakeup_at=now() so the
// Advance paused-handling loop resumes the saga. If attempt != current_attempt
// it is a late/duplicate delivery and is silently ignored.
func (s *Store) CompleteAction(ctx context.Context, runID uuid.UUID, attempt int, result map[string]any) error {
	s.mu.Lock()
	r, ok := s.runs[runID]
	if !ok {
		s.mu.Unlock()
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	if r.CurrentAttempt != attempt {
		s.mu.Unlock()
		return nil // late delivery; no-op
	}
	r.AwaitedActionDispatch = nil
	now := time.Now().UTC()
	r.WakeupAt = &now
	if r.Variables == nil {
		r.Variables = map[string]any{}
	}
	for k, v := range result {
		r.Variables[k] = v
	}
	s.runs[runID] = r
	s.mu.Unlock()
	return nil
}

// FailAction transitions the run to failed and appends an audit event.
// If attempt != current_attempt it is a late delivery and is silently ignored.
func (s *Store) FailAction(ctx context.Context, runID uuid.UUID, attempt int, code, message string, retryable bool) error {
	s.mu.Lock()
	r, ok := s.runs[runID]
	if !ok {
		s.mu.Unlock()
		return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
	}
	if r.CurrentAttempt != attempt {
		s.mu.Unlock()
		return nil // late delivery; no-op
	}
	dispatch := ""
	if r.AwaitedActionDispatch != nil {
		dispatch = *r.AwaitedActionDispatch
	}
	r.AwaitedActionDispatch = nil
	r.State = domain.RunStateFailed
	s.runs[runID] = r
	s.mu.Unlock()
	// Append audit event AFTER releasing the lock (AppendEvent takes its own lock).
	evt := domain.NewEvent(runID, r.CurrentStep, attempt, domain.EventStepFailed, "engine")
	evt.Metadata = map[string]any{
		"code":      code,
		"message":   message,
		"retryable": retryable,
		"action":    dispatch,
	}
	_ = s.AppendEvent(ctx, evt)
	return nil
}

// ListRuns returns saga runs matching filter, sorted by StartedAt DESC.
// TriggerType filter: iterates s.triggers to build an id→type map, then
// checks each run's TriggerID.
func (s *Store) ListRuns(_ context.Context, filter store.RunFilter) ([]domain.SagaRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	triggerTypeByID := s.buildTriggerTypeMap()
	matched := s.filterRuns(filter, triggerTypeByID)

	// Sort by StartedAt DESC.
	for i := 1; i < len(matched); i++ {
		for j := i; j > 0 && matched[j].StartedAt.After(matched[j-1].StartedAt); j-- {
			matched[j], matched[j-1] = matched[j-1], matched[j]
		}
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(matched) {
		return []domain.SagaRun{}, nil
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], nil
}

// CountRuns returns the total count matching filter (ignoring Limit/Offset).
func (s *Store) CountRuns(_ context.Context, filter store.RunFilter) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	triggerTypeByID := s.buildTriggerTypeMap()
	matched := s.filterRuns(filter, triggerTypeByID)
	return len(matched), nil
}

// buildTriggerTypeMap builds a uuid→trigger_type map from s.triggers.
// Caller must hold at least a read lock.
func (s *Store) buildTriggerTypeMap() map[uuid.UUID]string {
	m := map[uuid.UUID]string{}
	for id, t := range s.triggers {
		m[id] = string(t.TriggerType)
	}
	return m
}

// filterRuns applies RunFilter (excluding Limit/Offset) and returns matching runs.
// Caller must hold at least a read lock.
func (s *Store) filterRuns(filter store.RunFilter, triggerTypeByID map[uuid.UUID]string) []domain.SagaRun {
	out := []domain.SagaRun{}
	for _, r := range s.runs {
		if filter.WorkflowID != "" && r.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.State != "" && string(r.State) != filter.State {
			continue
		}
		if filter.Since != nil && r.StartedAt.Before(*filter.Since) {
			continue
		}
		// HasError: v1 uses state==failed as the indicator (see RunFilter comment).
		if filter.HasError != nil {
			isFailed := r.State == domain.RunStateFailed
			if *filter.HasError != isFailed {
				continue
			}
		}
		if filter.RequiresReview != nil && r.RequiresManualReview != *filter.RequiresReview {
			continue
		}
		if filter.TriggerType != "" {
			if r.TriggerID == nil {
				continue
			}
			tt, ok := triggerTypeByID[*r.TriggerID]
			if !ok || tt != filter.TriggerType {
				continue
			}
		}
		out = append(out, r)
	}
	return out
}

// StatsForWorkflow computes aggregate metrics for workflowID by iterating s.runs.
func (s *Store) StatsForWorkflow(_ context.Context, workflowID string) (store.WorkflowStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()
	window := now.Add(-24 * time.Hour)

	stats := store.WorkflowStats{WorkflowID: workflowID}
	var succeeded24h, failed24h int

	for _, r := range s.runs {
		if r.WorkflowID != workflowID {
			continue
		}
		// last_run_at = most recent StartedAt overall.
		if stats.LastRunAt == nil || r.StartedAt.After(*stats.LastRunAt) {
			t := r.StartedAt
			stats.LastRunAt = &t
		}
		// in_flight = state NOT in (succeeded, failed, cancelled).
		if r.State != domain.RunStateSucceeded && r.State != domain.RunStateFailed && r.State != domain.RunStateCancelled {
			stats.InFlight++
		}
		// success_rate_24h: only runs with started_at >= window.
		if !r.StartedAt.Before(window) {
			switch r.State {
			case domain.RunStateSucceeded:
				succeeded24h++
			case domain.RunStateFailed:
				failed24h++
			}
		}
	}

	total24h := succeeded24h + failed24h
	if total24h > 0 {
		rate := float64(succeeded24h) / float64(total24h)
		stats.SuccessRate24h = &rate
	}
	return stats, nil
}

// applyDottedKey writes value at the dot-walked path within target.
// "scope.subkey" walks into a nested map. Top-level keys without a dot
// are a plain map assignment.
func applyDottedKey(target map[string]any, key string, value any) {
	parts := []string{}
	cur := ""
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			parts = append(parts, cur)
			cur = ""
			continue
		}
		cur += string(key[i])
	}
	parts = append(parts, cur)

	t := target
	for i, p := range parts {
		if i == len(parts)-1 {
			t[p] = value
			return
		}
		nested, ok := t[p].(map[string]any)
		if !ok {
			nested = map[string]any{}
			t[p] = nested
		}
		t = nested
	}
}
