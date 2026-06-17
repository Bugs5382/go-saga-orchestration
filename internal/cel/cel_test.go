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
	"strings"
	"testing"
)

func TestCompileAndEvalArithmetic(t *testing.T) {
	env, err := NewEnv("x", "y")
	if err != nil {
		t.Fatalf("new env: %v", err)
	}
	prg, err := env.Compile("x * 2 + y")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := prg.Eval(map[string]any{"x": 5, "y": 3})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got.(int64) != 13 {
		t.Errorf("got %v, want 13", got)
	}
}

func TestCompileAndEvalStringOps(t *testing.T) {
	env, _ := NewEnv("name")
	prg, err := env.Compile("name.startsWith('inc-')")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, _ := prg.Eval(map[string]any{"name": "inc-1234"})
	if got.(bool) != true {
		t.Errorf("got %v, want true", got)
	}
}

func TestCompileAndEvalListFilter(t *testing.T) {
	env, _ := NewEnv("xs")
	prg, err := env.Compile("xs.filter(_, _ > 2)")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := prg.Eval(map[string]any{"xs": []any{1, 2, 3, 4}})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	list := got.([]any)
	if len(list) != 2 {
		t.Errorf("got len %d, want 2", len(list))
	}
}

func TestCompileEmptyExpressionRejected(t *testing.T) {
	env, _ := NewEnv()
	if _, err := env.Compile(""); err == nil {
		t.Errorf("expected error on empty expression, got nil")
	}
}

func TestCompileBadSyntaxRejected(t *testing.T) {
	env, _ := NewEnv("x")
	_, err := env.Compile("x +")
	if err == nil {
		t.Fatalf("expected compile error, got nil")
	}
	if !strings.Contains(err.Error(), "compile") {
		t.Errorf("error message did not mention compile: %v", err)
	}
}

func TestEvalMissingVariableRejected(t *testing.T) {
	env, _ := NewEnv("x")
	prg, err := env.Compile("x + 1")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = prg.Eval(map[string]any{})
	if err == nil {
		t.Errorf("expected eval error on missing var, got nil")
	}
}

func TestEvalTypeMismatchRejected(t *testing.T) {
	env, _ := NewEnv("x")
	prg, _ := env.Compile("x + 1")
	_, err := prg.Eval(map[string]any{"x": "not-a-number"})
	if err == nil {
		t.Errorf("expected type-mismatch error, got nil")
	}
}
