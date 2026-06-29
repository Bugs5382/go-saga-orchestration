package engine

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
	"testing"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

func TestBackoffExponential(t *testing.T) {
	p := domain.RetryPolicy{MaxAttempts: 5, InitialBackoffMS: 1000, MaxBackoffMS: 60000, Multiplier: 2.0}
	cases := []struct {
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{0, 1000 * time.Millisecond, 1000 * time.Millisecond},
		{1, 2000 * time.Millisecond, 2000 * time.Millisecond},
		{2, 4000 * time.Millisecond, 4000 * time.Millisecond},
		{6, 60 * time.Second, 60 * time.Second}, // capped
	}
	for _, c := range cases {
		got := Backoff(p, c.attempt, false /*no jitter*/)
		if got < c.min || got > c.max {
			t.Errorf("attempt %d: got %v, want %v..%v", c.attempt, got, c.min, c.max)
		}
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	d := DefaultRetryPolicy()
	if d.MaxAttempts != 3 || d.InitialBackoffMS != 1000 {
		t.Errorf("defaults: %+v", d)
	}
}
