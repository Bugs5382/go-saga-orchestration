package licensing

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
	"testing"
	"time"

	"github.com/google/uuid"
)

type recordingResolver struct {
	calls int
	value bool
	err   error
}

func (r *recordingResolver) IsFeatureEnabled(_ context.Context, _ *uuid.UUID, _ string, _ map[string]bool) (bool, error) {
	r.calls++
	return r.value, r.err
}

func TestStubAllowAll_ReturnsTrue(t *testing.T) {
	r := StubAllowAll{}
	if v, err := r.IsFeatureEnabled(context.Background(), nil, "anything", nil); !v || err != nil {
		t.Errorf("got (%v, %v), want (true, nil)", v, err)
	}
}

func TestCached_FeatureOverride_BypassesCache(t *testing.T) {
	inner := &recordingResolver{value: false}
	c := NewCached(inner, time.Minute)
	v, _ := c.IsFeatureEnabled(context.Background(), nil, "wf.parallel", map[string]bool{"wf.parallel": true})
	if !v {
		t.Errorf("expected feature override true, got false")
	}
	if inner.calls != 0 {
		t.Errorf("feature override should bypass inner; got %d calls", inner.calls)
	}
}

func TestCached_HitsCacheOnSecondCall(t *testing.T) {
	inner := &recordingResolver{value: true}
	c := NewCached(inner, time.Minute)
	_, _ = c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	_, _ = c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	if inner.calls != 1 {
		t.Errorf("second call should hit cache; inner.calls = %d, want 1", inner.calls)
	}
}

func TestCached_ExpiresAfterTTL(t *testing.T) {
	inner := &recordingResolver{value: true}
	c := NewCached(inner, 5*time.Millisecond)
	_, _ = c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	time.Sleep(10 * time.Millisecond)
	_, _ = c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	if inner.calls != 2 {
		t.Errorf("expected 2 inner calls after TTL expiry, got %d", inner.calls)
	}
}

func TestCached_PropagatesError(t *testing.T) {
	inner := &recordingResolver{err: errors.New("upstream down")}
	c := NewCached(inner, time.Minute)
	_, err := c.IsFeatureEnabled(context.Background(), nil, "wf.timers", nil)
	if err == nil {
		t.Errorf("expected error to propagate")
	}
}
