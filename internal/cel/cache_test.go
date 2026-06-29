package cel

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
	"sync"
	"testing"
)

// CompiledProgram must evaluate identically to the uncached NewEnv+Compile
// path it replaces.
func TestCompiledProgram_EvaluatesCorrectly(t *testing.T) {
	prg, err := CompiledProgram([]string{"a", "b"}, "a + b")
	if err != nil {
		t.Fatalf("compiled program: %v", err)
	}
	got, err := prg.Eval(map[string]any{"a": 2, "b": 3})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != int64(5) {
		t.Errorf("got %v (%T), want int64(5)", got, got)
	}
}

// A repeat call with the same variable set and expression must return the
// cached program, not rebuild it.
func TestCompiledProgram_CachesByVarsAndExpr(t *testing.T) {
	a, err := CompiledProgram([]string{"a", "b"}, "a + b")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	b, err := CompiledProgram([]string{"a", "b"}, "a + b")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if a != b {
		t.Errorf("expected cached program to be reused (same pointer)")
	}
}

// keysOf returns variable names in arbitrary order, so the cache key must be
// order-insensitive: the same names in a different order hit the same entry.
func TestCompiledProgram_VarOrderInsensitive(t *testing.T) {
	a, err := CompiledProgram([]string{"a", "b", "c"}, "a + b + c")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	b, err := CompiledProgram([]string{"c", "a", "b"}, "a + b + c")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if a != b {
		t.Errorf("expected var-order-insensitive cache hit (same pointer)")
	}
}

// Different variable sets or expressions must not collide on a shared entry.
func TestCompiledProgram_DistinctKeysDoNotCollide(t *testing.T) {
	base, err := CompiledProgram([]string{"a"}, "a + 1")
	if err != nil {
		t.Fatalf("base: %v", err)
	}
	diffExpr, err := CompiledProgram([]string{"a"}, "a + 2")
	if err != nil {
		t.Fatalf("diff expr: %v", err)
	}
	diffVars, err := CompiledProgram([]string{"a", "b"}, "a + 1")
	if err != nil {
		t.Fatalf("diff vars: %v", err)
	}
	if base == diffExpr {
		t.Errorf("different expressions shared a program")
	}
	if base == diffVars {
		t.Errorf("different variable sets shared a program")
	}
}

// A reference to an undeclared variable must error (and must not be cached as
// a usable program), matching the uncached Compile behavior.
func TestCompiledProgram_UndeclaredVarErrors(t *testing.T) {
	if _, err := CompiledProgram([]string{"a"}, "a + missing"); err == nil {
		t.Errorf("expected error for undeclared variable")
	}
}

// Concurrent callers must all succeed and converge on the same cached program.
func TestCompiledProgram_ConcurrentSafe(t *testing.T) {
	const n = 50
	var wg sync.WaitGroup
	got := make([]*Program, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = CompiledProgram([]string{"x", "y"}, "x * y")
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if got[i] != got[0] {
			t.Errorf("goroutine %d got a different program pointer", i)
		}
	}
}
