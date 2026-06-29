// Package licensing resolves feature flags for verb license-group gating.
// Engine consults a Resolver before dispatching any non-common verb to
// determine whether the tenant's license includes the feature the verb
// belongs to.
package licensing

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
	"sync"
	"time"

	"github.com/google/uuid"
)

// Resolver looks up whether a tenant has a feature flag enabled.
// `overrides` take precedence over the underlying source — supplied per
// request via the X-Feature-Override header so any environment (standalone,
// on-prem, dev, QA) can exercise gate paths without flipping real licenses.
type Resolver interface {
	IsFeatureEnabled(ctx context.Context, tenantID *uuid.UUID, feature string, overrides map[string]bool) (bool, error)
}

// StubAllowAll returns true for every feature. Used in dev / tests when
// licensing isn't wired or when running with --allow-all-features for
// non-licensed environments.
type StubAllowAll struct{}

// IsFeatureEnabled always reports the feature as enabled.
func (StubAllowAll) IsFeatureEnabled(_ context.Context, _ *uuid.UUID, _ string, _ map[string]bool) (bool, error) {
	return true, nil
}

// Cached wraps any Resolver with a per-tenant TTL cache of feature
// lookups. Feature overrides always bypass the cache (they're
// per-request) so the cache doesn't pollute across runs with different
// override flags.
type Cached struct {
	Inner Resolver
	TTL   time.Duration

	mu   sync.Mutex
	data map[string]cacheEntry
}

type cacheEntry struct {
	value   bool
	expires time.Time
}

// NewCached wraps inner with a TTL cache. Use TTL=60*time.Second for prod.
func NewCached(inner Resolver, ttl time.Duration) *Cached {
	return &Cached{Inner: inner, TTL: ttl, data: map[string]cacheEntry{}}
}

// IsFeatureEnabled returns a feature override if present, otherwise serves
// from the TTL cache, falling back to the wrapped Resolver and caching its result.
func (c *Cached) IsFeatureEnabled(ctx context.Context, tenant *uuid.UUID, feature string, overrides map[string]bool) (bool, error) {
	// Feature overrides win, always.
	if v, ok := overrides[feature]; ok {
		return v, nil
	}
	key := cacheKey(tenant, feature)
	c.mu.Lock()
	if e, ok := c.data[key]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		return e.value, nil
	}
	c.mu.Unlock()
	enabled, err := c.Inner.IsFeatureEnabled(ctx, tenant, feature, nil)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.data[key] = cacheEntry{value: enabled, expires: time.Now().Add(c.TTL)}
	c.mu.Unlock()
	return enabled, nil
}

func cacheKey(tenant *uuid.UUID, feature string) string {
	if tenant == nil {
		return "platform|" + feature
	}
	return tenant.String() + "|" + feature
}
