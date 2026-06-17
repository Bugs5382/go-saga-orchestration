package cel

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
	"sort"
	"strings"
	"sync"
)

// programCache memoises compiled programs by (declared variable set, expr).
// Stored *Program values are immutable and cel-go programs are safe for
// concurrent Eval, so entries can be shared freely across goroutines.
//
// The cache is unbounded. Workflow and rule definitions are finite and
// published, so the set of (variable set, expression) pairs is bounded by the
// deployed definitions in practice.
var programCache sync.Map // string key -> *Program

// CompiledProgram returns a compiled program for expr evaluated against the
// given variable names, building it once and reusing it on subsequent calls.
// It replaces the per-call NewEnv + Compile the verbs previously paid on every
// dispatch: because every variable is declared as a dyn type and the v1 subset
// is constant, a program is fully determined by its declared variable set and
// expression, so two runs with the same shape can share one compiled program
// and supply their own values at Eval time.
//
// The variable names are sorted and de-duplicated before keying, so callers
// may pass them in any order (e.g. from keysOf, which iterates a map).
func CompiledProgram(varNames []string, expr string) (*Program, error) {
	names := sortedUnique(varNames)
	// \x1f separates names and \x1e separates the name set from the
	// expression; neither byte can appear in a CEL identifier, so distinct
	// inputs cannot collide on a shared key.
	key := strings.Join(names, "\x1f") + "\x1e" + expr
	if cached, ok := programCache.Load(key); ok {
		return cached.(*Program), nil
	}
	env, err := NewEnv(names...)
	if err != nil {
		return nil, err
	}
	prg, err := env.Compile(expr)
	if err != nil {
		return nil, err
	}
	// LoadOrStore collapses a race where two goroutines built the same
	// program: both get a valid program, only one is retained.
	actual, _ := programCache.LoadOrStore(key, prg)
	return actual.(*Program), nil
}

// sortedUnique returns a sorted copy of names with duplicates removed, leaving
// the caller's slice untouched.
func sortedUnique(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	copy(out, names)
	sort.Strings(out)
	dedup := out[:1]
	for _, n := range out[1:] {
		if n != dedup[len(dedup)-1] {
			dedup = append(dedup, n)
		}
	}
	return dedup
}
