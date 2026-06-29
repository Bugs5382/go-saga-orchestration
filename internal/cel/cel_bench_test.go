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

// Benchmarks for the CEL primitives (issue #19). The built-in verbs call
// NewEnv + Compile on every Execute, so NewEnvCompileEval is the per-call
// cost the follow-up cache aims to amortise.

import (
	"fmt"
	"testing"
)

// celVarNames returns n declared variable names v0..v(n-1).
func celVarNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("v%d", i)
	}
	return names
}

// celVarValues returns a value map matching celVarNames(n).
func celVarValues(n int) map[string]any {
	vals := make(map[string]any, n)
	for i := 0; i < n; i++ {
		vals[fmt.Sprintf("v%d", i)] = i
	}
	return vals
}

// sumExpr builds "v0 + v1 + ... + v(n-1)" (n >= 1).
func sumExpr(n int) string {
	expr := "v0"
	for i := 1; i < n; i++ {
		expr += fmt.Sprintf(" + v%d", i)
	}
	return expr
}

// BenchmarkNewEnv measures environment construction cost as the declared
// variable set grows.
func BenchmarkNewEnv(b *testing.B) {
	for _, n := range []int{0, 5, 20} {
		names := celVarNames(n)
		b.Run(fmt.Sprintf("vars_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := NewEnv(names...); err != nil {
					b.Fatalf("new env: %v", err)
				}
			}
		})
	}
}

// BenchmarkCompile measures compiling an expression against a prebuilt env.
func BenchmarkCompile(b *testing.B) {
	env, err := NewEnv(celVarNames(5)...)
	if err != nil {
		b.Fatalf("new env: %v", err)
	}
	expr := sumExpr(5)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := env.Compile(expr); err != nil {
			b.Fatalf("compile: %v", err)
		}
	}
}

// BenchmarkEval measures repeated evaluation of a prebuilt program — the
// steady-state cost once env + program are cached.
func BenchmarkEval(b *testing.B) {
	env, err := NewEnv(celVarNames(5)...)
	if err != nil {
		b.Fatalf("new env: %v", err)
	}
	prg, err := env.Compile(sumExpr(5))
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	vals := celVarValues(5)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := prg.Eval(vals); err != nil {
			b.Fatalf("eval: %v", err)
		}
	}
}

// BenchmarkNewEnvCompileEval measures the full per-call cost the verbs pay
// today: a fresh env + compile + eval on every dispatch.
func BenchmarkNewEnvCompileEval(b *testing.B) {
	expr := sumExpr(5)
	vals := celVarValues(5)
	names := celVarNames(5)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env, err := NewEnv(names...)
		if err != nil {
			b.Fatalf("new env: %v", err)
		}
		prg, err := env.Compile(expr)
		if err != nil {
			b.Fatalf("compile: %v", err)
		}
		if _, err := prg.Eval(vals); err != nil {
			b.Fatalf("eval: %v", err)
		}
	}
}

// BenchmarkNewEnvParallel measures env construction under concurrency, the
// access pattern a shared cache must remain safe and contention-free under.
func BenchmarkNewEnvParallel(b *testing.B) {
	names := celVarNames(5)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := NewEnv(names...); err != nil {
				b.Fatalf("new env: %v", err)
			}
		}
	})
}
