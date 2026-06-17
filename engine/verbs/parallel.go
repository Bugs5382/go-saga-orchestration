package verbs

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
	"encoding/json"
	"fmt"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/internal/cel"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// ParallelVerb fans out N child runs and pauses the parent until the join
// strategy is satisfied. Inputs:
//   - "branches" (required, []any or CEL string): each element is a
//     workflow-fragment object {"start": "step_id", "steps": [...step
//     objects...]}. Short-form {"type": "...", "inputs": {...}} is also
//     accepted and normalised on the fly. When a string is supplied it is
//     evaluated as a CEL expression against run.Variables and the result
//     must be a non-empty list. Each branch becomes a child run.
//   - "join_strategy" (optional, string, default "all"): "all" waits for
//     every branch to reach a terminal state. "quorum" wakes the parent
//     once quorum_n branches have succeeded (remaining branches keep
//     running but no longer gate the parent). "first_terminal" and other
//     values are rejected.
//   - "quorum_n" (required when join_strategy=="quorum"): positive integer,
//     must be ≤ len(branches).
//
// The coordinator's child-terminal hook (engine/advance.go) wakes the
// parent when all children are terminal. The parent then advances to
// step.Next.
type ParallelVerb struct {
	S         store.Store
	Publisher Publisher
}

// Execute resolves the branches (literal list or CEL expression), validates
// the join strategy, spawns one child run per branch, then pauses the parent
// awaiting the join and returns ErrSagaPaused.
func (v ParallelVerb) Execute(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
	rawBranches := step.Inputs["branches"]
	switch b := rawBranches.(type) {
	case string:
		// CEL expression — evaluate against run.Variables, expect a []any result.
		evaluated, err := evalBranchesCEL(b, run.Variables)
		if err != nil {
			return nil, fmt.Errorf("parallel: branches CEL eval: %w", err)
		}
		rawBranches = evaluated
	case []any:
		// already a literal list — fall through
	default:
		return nil, fmt.Errorf("parallel: branches must be []any or CEL string, got %T", rawBranches)
	}
	branchesAny, ok := rawBranches.([]any)
	if !ok || len(branchesAny) == 0 {
		return nil, fmt.Errorf("parallel: branches required and must be non-empty list (after CEL eval if applicable)")
	}
	joinStrategy, _ := step.Inputs["join_strategy"].(string)
	if joinStrategy == "" {
		joinStrategy = "all"
	}
	switch joinStrategy {
	case "all":
		// no extra validation
	case "quorum":
		quorumNRaw := step.Inputs["quorum_n"]
		var quorumN int
		switch qv := quorumNRaw.(type) {
		case string:
			val, err := EvalQuorumNCEL(qv, run.Variables)
			if err != nil {
				return nil, fmt.Errorf("parallel: quorum_n CEL eval: %w", err)
			}
			n, ok := ToIntFromAny(val)
			if !ok {
				return nil, fmt.Errorf("parallel: quorum_n CEL result is not numeric: %T %v", val, val)
			}
			quorumN = n
		default:
			n, ok := ToInt(quorumNRaw)
			if !ok {
				return nil, fmt.Errorf("parallel: quorum_n required and must be positive when join_strategy=quorum")
			}
			quorumN = n
		}
		if quorumN <= 0 {
			return nil, fmt.Errorf("parallel: quorum_n must be positive")
		}
		if quorumN > len(branchesAny) {
			return nil, fmt.Errorf("parallel: quorum_n=%d exceeds branch count=%d", quorumN, len(branchesAny))
		}
	default:
		return nil, fmt.Errorf("parallel: join_strategy %q not supported (use 'all' or 'quorum')", joinStrategy)
	}

	// For each branch, build a synthetic WorkflowDefinition and spawn a child run.
	for i, b := range branchesAny {
		bm, ok := b.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("parallel: branch %d must be an object", i)
		}
		bm = normalizeBranch(bm, i)
		start, _ := bm["start"].(string)
		if start == "" {
			return nil, fmt.Errorf("parallel: branch %d missing start", i)
		}
		stepsAny, _ := bm["steps"].([]any)
		if len(stepsAny) == 0 {
			return nil, fmt.Errorf("parallel: branch %d missing steps", i)
		}
		stepsParsed, err := parseSteps(stepsAny)
		if err != nil {
			return nil, fmt.Errorf("parallel: branch %d parse: %w", i, err)
		}
		branchKey := fmt.Sprintf("b%d", i)
		if k, _ := bm["key"].(string); k != "" {
			branchKey = k
		}
		branchDef := domain.WorkflowDefinition{
			ID:        run.WorkflowID + "@" + step.ID + "/" + branchKey,
			Version:   1,
			Name:      "synthetic-branch",
			Start:     start,
			Steps:     stepsParsed,
			Published: true,
		}
		// Persist the synthetic def so SpawnChildRun's definition_id resolution works.
		if _, err := v.S.UpsertWorkflowDefinition(ctx, branchDef); err != nil {
			return nil, fmt.Errorf("parallel: upsert branch %d def: %w", i, err)
		}
		// Carry the parent's variables in as the child's inputs.
		childInputs := map[string]any{}
		for k, val := range run.Variables {
			childInputs[k] = val
		}
		childID, err := v.S.SpawnChildRun(ctx, run.ID, step.ID, branchKey, branchDef, childInputs)
		if err != nil {
			return nil, fmt.Errorf("parallel: spawn branch %d: %w", i, err)
		}
		// Publish saga.advance to start the child immediately.
		if v.Publisher != nil {
			if err := v.Publisher.PublishSagaAdvance(ctx, childID.String()); err != nil {
				return nil, fmt.Errorf("parallel: publish branch %d: %w", i, err)
			}
		}
	}

	// Mark parent as paused awaiting children. Use step.ID (not run.CurrentStep)
	// because the coordinator has already written UpdateRunState(running, step.ID)
	// before dispatching Execute — we need the paused record to carry the correct
	// CurrentStep so the wakeup path advances to step.Next, not back to def.Start.
	//
	// The child-terminal hook in coordinator/advance.go calls WakeFromExternal on
	// the parent once all siblings terminate. Advance detects "paused + no pending
	// awaits + wakeup_at==nil" as an external wake, emits step.succeeded, and
	// advances CurrentStep to step.Next.
	if err := v.S.UpdateRunState(ctx, run.ID, domain.RunStatePaused, step.ID); err != nil {
		return nil, fmt.Errorf("parallel: pause parent: %w", err)
	}
	return nil, ErrSagaPaused
}

// parseSteps converts []any (JSON-decoded step objects) into []domain.Step.
// Used to reconstruct synthetic branch definitions from the parallel verb's inputs.
func parseSteps(in []any) ([]domain.Step, error) {
	bs, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out []domain.Step
	if err := json.Unmarshal(bs, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// evalBranchesCEL compiles expr as a CEL expression against the given
// variables and returns the result coerced to []any. The expression is
// expected to evaluate to a list of branch-definition maps (each with
// "start" and "steps", or the short form {"type":"manual_approval", ...}
// — though for v1 we accept any map shape and let the iteration below
// validate). Common shape: collection.map(_, {...}).
func evalBranchesCEL(expr string, vars map[string]any) ([]any, error) {
	varNames := make([]string, 0, len(vars))
	for k := range vars {
		varNames = append(varNames, k)
	}
	env, err := cel.NewEnv(varNames...)
	if err != nil {
		return nil, fmt.Errorf("new env: %w", err)
	}
	prg, err := env.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	raw, err := prg.Eval(vars)
	if err != nil {
		return nil, fmt.Errorf("eval: %w", err)
	}
	// CEL returns Go-native values via refValueToNative (see internal/cel/cel.go).
	// A list result should be []any already.
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("branches expression must evaluate to a list, got %T", raw)
	}
	return list, nil
}

// ToInt coerces v to an int. Accepts int, int64, and float64 (JSON-decoded
// numbers). Returns (0, false) for any other type. Exported so the engine
// package can reuse it when reading quorum_n from a step's Inputs.
func ToInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// EvalQuorumNCEL evaluates expr as a CEL expression against vars and returns
// the result as any (expected to be numeric — int64 from CEL). Mirrors
// evalBranchesCEL but expects a scalar numeric result. Exported for use by
// the engine's checkParentJoin path in advance.go.
func EvalQuorumNCEL(expr string, vars map[string]any) (any, error) {
	varNames := make([]string, 0, len(vars))
	for k := range vars {
		varNames = append(varNames, k)
	}
	env, err := cel.NewEnv(varNames...)
	if err != nil {
		return nil, fmt.Errorf("new env: %w", err)
	}
	prg, err := env.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return prg.Eval(vars)
}

// ToIntFromAny coerces v to int, accepting the same types as ToInt plus
// int32 and uint64 edge cases from CEL numeric coercions. Exported for use
// by the engine's checkParentJoin path in advance.go.
func ToIntFromAny(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case uint64:
		return int(n), true
	}
	return 0, false
}

// normalizeBranch turns a short-form {type, inputs} into the long-form
// {key, start, steps} the existing iteration expects. If already long-form,
// returns as-is. Used when CEL-generated branches use the short shape.
func normalizeBranch(b map[string]any, index int) map[string]any {
	if _, has := b["start"]; has {
		return b
	}
	verbType, _ := b["type"].(string)
	if verbType == "" {
		return b // will fail the existing validation; let it
	}
	inputs, _ := b["inputs"].(map[string]any)
	key, _ := b["key"].(string)
	if key == "" {
		key = fmt.Sprintf("b%d", index)
	}
	stepID := key + "_" + verbType
	endID := key + "_end"
	return map[string]any{
		"key":   key,
		"start": stepID,
		"steps": []any{
			map[string]any{"id": stepID, "type": verbType, "inputs": inputs, "next": endID},
			map[string]any{"id": endID, "type": "end"},
		},
	}
}
