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

	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// ResolveJoinStreams normalises the join verb's "streams" input into a list
// of upstream step IDs. Accepts a literal []any of strings, a []string, or a
// CEL string that evaluates against vars to a list of strings. The list must
// be non-empty. Exported so the engine's join-barrier hook resolves the same
// stream set the join verb watched.
func ResolveJoinStreams(raw any, vars map[string]any) ([]string, error) {
	var list []any
	switch s := raw.(type) {
	case string:
		evaluated, err := evalBranchesCEL(s, vars)
		if err != nil {
			return nil, fmt.Errorf("streams CEL eval: %w", err)
		}
		list = evaluated
	case []any:
		list = s
	case []string:
		out := make([]string, 0, len(s))
		for _, v := range s {
			if v == "" {
				return nil, fmt.Errorf("streams entries must be non-empty step IDs")
			}
			out = append(out, v)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("streams required and must be a non-empty list of step IDs")
		}
		return out, nil
	default:
		return nil, fmt.Errorf("streams must be a list of step IDs or a CEL string, got %T", raw)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("streams required and must be a non-empty list of step IDs (after CEL eval if applicable)")
	}
	out := make([]string, 0, len(list))
	for i, v := range list {
		sid, ok := v.(string)
		if !ok || sid == "" {
			return nil, fmt.Errorf("streams[%d] must be a non-empty step ID string, got %T", i, v)
		}
		out = append(out, sid)
	}
	return out, nil
}

// validateJoinStrategy checks join_strategy / quorum_n before a barrier
// pauses, so a misconfigured barrier fails fast rather than pausing forever.
// watchedCount is the number of children the barrier is watching.
func validateJoinStrategy(inputs map[string]any, vars map[string]any, watchedCount int) error {
	joinStrategy, _ := inputs["join_strategy"].(string)
	if joinStrategy == "" {
		joinStrategy = "all"
	}
	switch joinStrategy {
	case "all":
		return nil
	case "quorum":
		var quorumN int
		switch qv := inputs["quorum_n"].(type) {
		case string:
			val, err := EvalQuorumNCEL(qv, vars)
			if err != nil {
				return fmt.Errorf("quorum_n CEL eval: %w", err)
			}
			n, ok := ToIntFromAny(val)
			if !ok {
				return fmt.Errorf("quorum_n CEL result is not numeric: %T %v", val, val)
			}
			quorumN = n
		default:
			n, ok := ToInt(inputs["quorum_n"])
			if !ok {
				return fmt.Errorf("quorum_n required and must be positive when join_strategy=quorum")
			}
			quorumN = n
		}
		if quorumN <= 0 {
			return fmt.Errorf("quorum_n must be positive")
		}
		if quorumN > watchedCount {
			return fmt.Errorf("quorum_n=%d exceeds watched-child count=%d", quorumN, watchedCount)
		}
		return nil
	default:
		return fmt.Errorf("join_strategy %q not supported (use 'all' or 'quorum')", joinStrategy)
	}
}

// JoinConditionMet reports whether the join/parallel barrier described by
// inputs is satisfied by the given group of runs. It reads "join_strategy"
// ("all" default, or "quorum") and "quorum_n" (int or CEL string) exactly as
// the parallel and join verbs configure them, so the parallel child-join
// hook and the join-barrier hook in the engine share one implementation.
//
//   - "all": every run in the group must be terminal.
//   - "quorum": at least quorum_n runs must be in RunStateSucceeded. A
//     missing/invalid quorum_n falls back to "all" (matching the historical
//     child-join behaviour), logged.
//
// An empty group is treated as not-met.
func JoinConditionMet(inputs map[string]any, vars map[string]any, group []domain.SagaRun) bool {
	if len(group) == 0 {
		return false
	}
	allTerminal := func() bool {
		for _, r := range group {
			if !r.State.IsTerminal() {
				return false
			}
		}
		return true
	}

	joinStrategy, _ := inputs["join_strategy"].(string)
	if joinStrategy == "" {
		joinStrategy = "all"
	}

	switch joinStrategy {
	case "all":
		return allTerminal()
	case "quorum":
		var quorumN int
		switch qv := inputs["quorum_n"].(type) {
		case string:
			val, err := EvalQuorumNCEL(qv, vars)
			if err != nil {
				log.Warn().Err(err).Msg("join: quorum_n CEL eval failed - falling back to 'all'")
				return allTerminal()
			}
			n, ok := ToIntFromAny(val)
			if !ok || n <= 0 {
				log.Warn().Msgf("join: quorum_n CEL result non-numeric (%T %v) - falling back to 'all'", val, val)
				return allTerminal()
			}
			quorumN = n
		default:
			n, ok := ToInt(inputs["quorum_n"])
			if !ok || n <= 0 {
				log.Warn().Msg("join: quorum_n missing or invalid - falling back to 'all'")
				return allTerminal()
			}
			quorumN = n
		}
		succeeded := 0
		for _, r := range group {
			if r.State == domain.RunStateSucceeded {
				succeeded++
			}
		}
		return succeeded >= quorumN
	default:
		log.Warn().Str("join_strategy", joinStrategy).Msg("join: unknown join_strategy - no wake")
		return false
	}
}

// AggregateJoinResults builds the per-child result list written into a run's
// Variables under "_join.<step_id>.branches" (join) or
// "_parallel.<step_id>.branches" (parallel). For each child:
//   - key: the child's ParentBranchID (or "b{index}" fallback)
//   - variables: the child's final Variables map
//   - state: the child's terminal state ("succeeded" or "failed")
//   - _user_task: the first submitted user_task owned by the child (if any),
//     as {id, result, submitted_by, submitted_at}. First by ID order wins.
func AggregateJoinResults(ctx context.Context, s store.Store, children []domain.SagaRun) []any {
	out := make([]any, 0, len(children))
	for i, child := range children {
		key := ""
		if child.ParentBranchID != nil {
			key = *child.ParentBranchID
		}
		if key == "" {
			key = fmt.Sprintf("b%d", i)
		}
		entry := map[string]any{
			"key":       key,
			"variables": child.Variables,
			"state":     string(child.State),
		}
		tasks, err := s.ListUserTasksByRun(ctx, child.ID)
		if err != nil {
			log.Warn().Err(err).Str("child_run_id", child.ID.String()).Msg("join: list user_tasks failed")
		}
		for _, t := range tasks {
			if t.SubmittedAt == nil {
				continue
			}
			entry["_user_task"] = map[string]any{
				"id":           t.ID.String(),
				"result":       t.Result,
				"submitted_by": t.SubmittedBy,
				"submitted_at": t.SubmittedAt.UTC().Format(time.RFC3339),
			}
			break // first submitted wins
		}
		out = append(out, entry)
	}
	return out
}
