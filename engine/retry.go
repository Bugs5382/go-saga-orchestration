package engine

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
	"math"
	"math/rand"
	"time"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// DefaultRetryPolicy returns the spec default per § 3.4.
func DefaultRetryPolicy() domain.RetryPolicy {
	return domain.RetryPolicy{
		MaxAttempts:      3,
		InitialBackoffMS: 1000,
		MaxBackoffMS:     60000,
		Multiplier:       2.0,
		Jitter:           true,
	}
}

// Backoff returns the wait duration for `attempt` (zero-indexed) under
// policy p. Caps at MaxBackoffMS. If jitter is true, applies ±25% noise.
func Backoff(p domain.RetryPolicy, attempt int, jitter bool) time.Duration {
	if p.Multiplier <= 0 {
		p.Multiplier = 2.0
	}
	base := float64(p.InitialBackoffMS) * math.Pow(p.Multiplier, float64(attempt))
	capMS := float64(p.MaxBackoffMS)
	if capMS == 0 {
		capMS = 60000
	}
	if base > capMS {
		base = capMS
	}
	if jitter {
		noise := (rand.Float64()*0.5 - 0.25) // -25%..+25%
		base = base * (1 + noise)
	}
	return time.Duration(base) * time.Millisecond
}
