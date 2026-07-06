package postgres

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
	"os"
	"testing"
	"time"
)

// testLockID is an arbitrary advisory-lock key scoped to this test.
const testLockID = int64(0x5A6A17E57)

// TestAcquireAdvisoryLock_RoundTrip verifies a single acquire followed by
// release succeeds and that the lock can be re-acquired once released.
func TestAcquireAdvisoryLock_RoundTrip(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres advisory-lock tests")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)

	release, err := AcquireAdvisoryLock(ctx, s.Pool(), testLockID)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	release()

	// After release the lock is free, so a second acquire returns promptly.
	release2, err := AcquireAdvisoryLock(ctx, s.Pool(), testLockID)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	release2()
}

// TestAcquireAdvisoryLock_MutualExclusion verifies that while one holder owns
// the lock a second acquire blocks, and only proceeds once the first releases.
func TestAcquireAdvisoryLock_MutualExclusion(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres advisory-lock tests")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)

	release1, err := AcquireAdvisoryLock(ctx, s.Pool(), testLockID)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// A second acquire must not complete while the first holder is active.
	acquired := make(chan struct{})
	go func() {
		release2, err := AcquireAdvisoryLock(ctx, s.Pool(), testLockID)
		if err != nil {
			t.Errorf("second acquire: %v", err)
			return
		}
		close(acquired)
		release2()
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire returned while first holder still owned the lock")
	case <-time.After(250 * time.Millisecond):
		// Expected: still blocked.
	}

	// Releasing the first holder should let the blocked acquire proceed.
	release1()
	select {
	case <-acquired:
		// Expected: lock handed off.
	case <-time.After(3 * time.Second):
		t.Fatal("second acquire did not proceed after first holder released")
	}
}
