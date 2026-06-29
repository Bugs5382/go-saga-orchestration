// Package clock abstracts time so the engine + verbs can be tested
// without real wall-clock delays. Production uses SystemClock; tests
// use FakeClock which advances on demand.
package clock

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
	"time"
)

// Clock is the abstraction injected into the coordinator + wait verbs.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives the current time after d
	// elapses. FakeClock receives only when Advance(d) is called.
	After(d time.Duration) <-chan time.Time
}

// SystemClock delegates to the stdlib time package.
type SystemClock struct{}

// Now returns the current UTC wall-clock time.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// After returns a channel that fires after d elapses, delegating to time.After.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// FakeClock holds a virtual clock that advances on demand.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

// NewFakeClock starts at the given instant (use UTC).
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start.UTC()}
}

// Now returns the current virtual time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After registers a waiter that fires only when Advance moves the virtual
// clock to at or past now+d.
func (f *FakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	f.waiters = append(f.waiters, fakeWaiter{at: f.now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward and fires any waiters whose deadline
// is at or before the new time.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.at.After(f.now) {
			w.ch <- f.now
			close(w.ch)
			continue
		}
		kept = append(kept, w)
	}
	f.waiters = kept
}
