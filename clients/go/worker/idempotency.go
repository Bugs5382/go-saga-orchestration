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
	"sync"
)

// IdempotencyStore is the contract a host service provides for action
// dedupe. Production impl: Postgres `idempotency_keys` table per service.
// In-memory impl: see MemoryIdempotencyStore for tests.
type IdempotencyStore interface {
	// Get returns (cached, true) if a prior execution with this key
	// recorded a result. Returns (zero, false) otherwise.
	Get(ctx context.Context, key string) (Result, bool, error)
	// Put records the result under the key. Idempotent — second call
	// with same key is a no-op (callers can also implement TTL cleanup).
	Put(ctx context.Context, key string, result Result) error
}

// MemoryIdempotencyStore is a test-time in-memory IdempotencyStore.
type MemoryIdempotencyStore struct {
	mu   sync.Mutex
	data map[string]Result
}

// NewMemoryIdempotencyStore returns an empty MemoryIdempotencyStore ready
// for use. The returned store is safe for concurrent use.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{data: map[string]Result{}}
}

// Get returns (cached, true, nil) if a result was previously stored under
// key, or (nil, false, nil) if no result is recorded. It never returns a
// non-nil error.
func (s *MemoryIdempotencyStore) Get(_ context.Context, key string) (Result, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.data[key]; ok {
		return r, true, nil
	}
	return nil, false, nil
}

// Put records result under key, overwriting any existing value. It never
// returns a non-nil error.
func (s *MemoryIdempotencyStore) Put(_ context.Context, key string, result Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = result
	return nil
}

// Wrap returns a new Handler that checks idempotency before invoking
// inner. On a key hit, the cached result is returned without re-running
// the handler. On a miss, inner runs and its result is cached. Errors
// from inner are NOT cached — the dispatch can retry via the engine's
// retry policy.
func Wrap(store IdempotencyStore, inner Handler) Handler {
	return HandlerFunc(func(ctx context.Context, payload ActionPayload) (Result, error) {
		if payload.IdempotencyKey != "" {
			if cached, ok, err := store.Get(ctx, payload.IdempotencyKey); err == nil && ok {
				return cached, nil
			}
		}
		result, err := inner.Execute(ctx, payload)
		if err != nil {
			return nil, err
		}
		if payload.IdempotencyKey != "" {
			_ = store.Put(ctx, payload.IdempotencyKey, result)
		}
		return result, nil
	})
}
