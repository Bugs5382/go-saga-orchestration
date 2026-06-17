// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// loadCandidates fetches all candidate SagaRun blobs for the given filter.
// If filter.WorkflowID is set it uses the byworkflow SET index; otherwise
// it uses the global idx:runs ZSET.
func (s *Store) loadCandidates(ctx context.Context, workflowID string) ([]domain.SagaRun, error) {
	var members []string
	var err error
	if workflowID != "" {
		members, err = s.rdb.SMembers(ctx, s.key("idx", "runs", "byworkflow", workflowID)).Result()
	} else {
		members, err = s.rdb.ZRange(ctx, s.key("idx", "runs"), 0, -1).Result()
	}
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.SagaRun{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("run", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.SagaRun, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var r domain.SagaRun
		if err := unmarshalJSON([]byte(v.(string)), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// buildTriggerTypeMap loads all trigger blobs and returns a uuid -> TriggerType map.
func (s *Store) buildTriggerTypeMap(ctx context.Context) (map[string]string, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "triggers")).Result()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(members))
	if len(members) == 0 {
		return m, nil
	}
	keys := make([]string, len(members))
	for i, mem := range members {
		keys[i] = s.key("trigger", mem)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for _, v := range vals {
		if v == nil {
			continue
		}
		var t domain.SagaTrigger
		if err := unmarshalJSON([]byte(v.(string)), &t); err != nil {
			continue
		}
		m[t.ID.String()] = string(t.TriggerType)
	}
	return m, nil
}

// filterRuns applies all RunFilter predicates (excluding Limit/Offset) and
// returns matching runs sorted StartedAt DESC.
func filterRuns(runs []domain.SagaRun, filter store.RunFilter, triggerTypeByID map[string]string) []domain.SagaRun {
	out := make([]domain.SagaRun, 0, len(runs))
	for _, r := range runs {
		if filter.WorkflowID != "" && r.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.State != "" && string(r.State) != filter.State {
			continue
		}
		if filter.Since != nil && r.StartedAt.Before(*filter.Since) {
			continue
		}
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
			tt, ok := triggerTypeByID[r.TriggerID.String()]
			if !ok || tt != filter.TriggerType {
				continue
			}
		}
		out = append(out, r)
	}
	// Sort StartedAt DESC (insertion sort; suits small-to-medium result sets).
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].StartedAt.After(out[j-1].StartedAt); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// ListRuns returns saga runs matching filter, sorted StartedAt DESC, paginated.
func (s *Store) ListRuns(ctx context.Context, filter store.RunFilter) ([]domain.SagaRun, error) {
	candidates, err := s.loadCandidates(ctx, filter.WorkflowID)
	if err != nil {
		return nil, err
	}
	triggerTypeByID, err := s.buildTriggerTypeMap(ctx)
	if err != nil {
		return nil, err
	}
	matched := filterRuns(candidates, filter, triggerTypeByID)

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
func (s *Store) CountRuns(ctx context.Context, filter store.RunFilter) (int, error) {
	candidates, err := s.loadCandidates(ctx, filter.WorkflowID)
	if err != nil {
		return 0, err
	}
	triggerTypeByID, err := s.buildTriggerTypeMap(ctx)
	if err != nil {
		return 0, err
	}
	matched := filterRuns(candidates, filter, triggerTypeByID)
	return len(matched), nil
}

// StatsForWorkflow computes aggregate metrics for workflowID.
func (s *Store) StatsForWorkflow(ctx context.Context, workflowID string) (store.WorkflowStats, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "runs", "byworkflow", workflowID)).Result()
	if err != nil {
		return store.WorkflowStats{}, err
	}
	stats := store.WorkflowStats{WorkflowID: workflowID}
	if len(members) == 0 {
		return stats, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("run", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return store.WorkflowStats{}, err
	}

	now := time.Now().UTC()
	window := now.Add(-24 * time.Hour)
	var succeeded24h, failed24h int

	for _, v := range vals {
		if v == nil {
			continue
		}
		var r domain.SagaRun
		if err := unmarshalJSON([]byte(v.(string)), &r); err != nil {
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
