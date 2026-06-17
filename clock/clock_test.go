package clock

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
	"testing"
	"time"
)

func TestSystemClock_NowAdvances(t *testing.T) {
	c := SystemClock{}
	t1 := c.Now()
	time.Sleep(2 * time.Millisecond)
	t2 := c.Now()
	if !t2.After(t1) {
		t.Errorf("expected t2 > t1; got %v, %v", t1, t2)
	}
}

func TestFakeClock_AdvanceFiresWaiters(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0).UTC())
	ch := c.After(5 * time.Second)
	select {
	case <-ch:
		t.Fatalf("waiter fired before advance")
	default:
	}
	c.Advance(5 * time.Second)
	select {
	case v := <-ch:
		if v != c.Now() {
			t.Errorf("got fire time %v, want %v", v, c.Now())
		}
	default:
		t.Fatalf("waiter did not fire after advance")
	}
}

func TestFakeClock_AdvancePastDeadline(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0).UTC())
	ch := c.After(1 * time.Second)
	c.Advance(10 * time.Second)
	select {
	case <-ch:
	default:
		t.Errorf("waiter did not fire")
	}
}
