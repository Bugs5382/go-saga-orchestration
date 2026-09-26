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

import "sync"

// runLocks serialises Advance per run within one coordinator. Advance reads
// a run, runs a step and writes the next state; two advances of the same run
// interleaving there can both run the same step. Duplicate saga.advance
// deliveries (two siblings both waking a parent) make that routine, so each
// run gets a mutex for the length of an Advance. Entries are reference
// counted and removed when no advance holds or waits on them, so the map
// does not grow with the number of runs ever seen.
type runLocks struct {
	mu    sync.Mutex
	locks map[string]*runLock
}

type runLock struct {
	mu   sync.Mutex
	refs int
}

// lock blocks until the caller holds runID's lock and returns the unlock
// func.
func (l *runLocks) lock(runID string) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]*runLock{}
	}
	rl, ok := l.locks[runID]
	if !ok {
		rl = &runLock{}
		l.locks[runID] = rl
	}
	rl.refs++
	l.mu.Unlock()

	rl.mu.Lock()
	return func() {
		rl.mu.Unlock()
		l.mu.Lock()
		rl.refs--
		if rl.refs == 0 {
			delete(l.locks, runID)
		}
		l.mu.Unlock()
	}
}

// size reports how many runs have a lock entry (for tests).
func (l *runLocks) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.locks)
}
