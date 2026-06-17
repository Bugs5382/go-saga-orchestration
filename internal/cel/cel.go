// Package cel embeds google/cel-go to give go-saga-orchestration built-in verbs
// and rule definitions one shared expression language. It exposes three
// operations: NewEnv (build an environment with the variables in scope),
// Compile (parse + type-check an expression once), Eval (run a compiled
// program against a map of variable values). A deliberately small subset
// of functions is enabled; see subset.go for the allow-list.
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
	"errors"
	"fmt"

	celpkg "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// Env wraps a cel-go Environment with the v1 subset already applied.
type Env struct {
	inner *celpkg.Env
}

// Program is a compiled expression ready for repeated evaluation.
type Program struct {
	inner celpkg.Program
}

// NewEnv builds a CEL environment whose top-level identifiers come from
// the provided variable names. All variables are typed as dyn so the
// runtime can accept any JSON-shaped value from saga variables. The
// returned Env enforces the v1 subset (see subset.go).
func NewEnv(varNames ...string) (*Env, error) {
	opts := []celpkg.EnvOption{}
	for _, name := range varNames {
		opts = append(opts, celpkg.Variable(name, celpkg.DynType))
	}
	opts = append(opts, subsetOptions()...)
	env, err := celpkg.NewEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("cel: new env: %w", err)
	}
	return &Env{inner: env}, nil
}

// Compile parses, type-checks, and prepares an expression for eval.
// Returns a Program that can be Eval'd many times against different
// variable maps.
func (e *Env) Compile(expr string) (*Program, error) {
	if expr == "" {
		return nil, errors.New("cel: empty expression")
	}
	ast, iss := e.inner.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("cel: compile: %w", iss.Err())
	}
	prg, err := e.inner.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("cel: program: %w", err)
	}
	return &Program{inner: prg}, nil
}

// Eval runs a compiled program against the variable map and returns the
// Go-native value of the result, or an error if evaluation failed.
func (p *Program) Eval(vars map[string]any) (any, error) {
	out, _, err := p.inner.Eval(vars)
	if err != nil {
		return nil, fmt.Errorf("cel: eval: %w", err)
	}
	return refValueToNative(out)
}

// refValueToNative deep-converts a CEL ref.Val into a Go-native value.
// CEL's Value() returns []ref.Val for lists and map[ref.Val]ref.Val for
// maps; this function recursively unwraps those so callers see []any
// and map[string]any. Returns an error if the result has no native
// representation.
func refValueToNative(v ref.Val) (any, error) {
	if v == nil {
		return nil, nil
	}
	if lister, ok := v.(traits.Lister); ok {
		out := []any{}
		it := lister.Iterator()
		for it.HasNext() == types.True {
			elem, err := refValueToNative(it.Next())
			if err != nil {
				return nil, err
			}
			out = append(out, elem)
		}
		return out, nil
	}
	if mapper, ok := v.(traits.Mapper); ok {
		out := map[string]any{}
		it := mapper.Iterator()
		for it.HasNext() == types.True {
			k := it.Next()
			ks, ok := k.Value().(string)
			if !ok {
				return nil, fmt.Errorf("cel: map key is not a string: %T", k.Value())
			}
			val, ok := mapper.Find(k)
			if !ok {
				return nil, fmt.Errorf("cel: map key %q not found during iteration", ks)
			}
			conv, err := refValueToNative(val)
			if err != nil {
				return nil, err
			}
			out[ks] = conv
		}
		return out, nil
	}
	native := v.Value()
	if _, isRef := native.(ref.Val); isRef {
		return nil, fmt.Errorf("cel: unsupported result type %T", v)
	}
	return native, nil
}
