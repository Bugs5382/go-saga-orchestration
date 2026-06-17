package worker

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
	"errors"
	"sync/atomic"
	"testing"
)

func TestIdempotency_FirstCallExecutes(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32
	inner := HandlerFunc(func(_ context.Context, p ActionPayload) (Result, error) {
		calls.Add(1)
		return Result{"echo": p.Inputs["x"]}, nil
	})
	wrapped := Wrap(store, inner)
	out, err := wrapped.Execute(context.Background(), ActionPayload{
		IdempotencyKey: "k1", Inputs: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if out["echo"] != 1 || calls.Load() != 1 {
		t.Errorf("out=%v calls=%d", out, calls.Load())
	}
}

func TestIdempotency_SecondCallReturnsCached(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32
	inner := HandlerFunc(func(_ context.Context, p ActionPayload) (Result, error) {
		calls.Add(1)
		return Result{"echo": p.Inputs["x"]}, nil
	})
	wrapped := Wrap(store, inner)
	_, _ = wrapped.Execute(context.Background(), ActionPayload{IdempotencyKey: "k", Inputs: map[string]any{"x": 1}})
	out, _ := wrapped.Execute(context.Background(), ActionPayload{IdempotencyKey: "k", Inputs: map[string]any{"x": 2}})
	if out["echo"] != 1 {
		t.Errorf("got %v, want cached value 1", out["echo"])
	}
	if calls.Load() != 1 {
		t.Errorf("inner called %d times, want 1", calls.Load())
	}
}

func TestIdempotency_ErrorsNotCached(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32
	inner := HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) {
		calls.Add(1)
		if calls.Load() == 1 {
			return nil, errors.New("first time fails")
		}
		return Result{"ok": true}, nil
	})
	wrapped := Wrap(store, inner)
	_, err := wrapped.Execute(context.Background(), ActionPayload{IdempotencyKey: "k"})
	if err == nil {
		t.Fatalf("expected first-call error")
	}
	out, err := wrapped.Execute(context.Background(), ActionPayload{IdempotencyKey: "k"})
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if out["ok"] != true {
		t.Errorf("got %v, want ok=true (error wasn't cached)", out)
	}
}

func TestIdempotency_NoKey_AlwaysExecutes(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	var calls atomic.Int32
	inner := HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) {
		calls.Add(1)
		return Result{}, nil
	})
	wrapped := Wrap(store, inner)
	_, _ = wrapped.Execute(context.Background(), ActionPayload{})
	_, _ = wrapped.Execute(context.Background(), ActionPayload{})
	if calls.Load() != 2 {
		t.Errorf("inner called %d times, want 2 (no key = no dedupe)", calls.Load())
	}
}
