//go:build unix

package saga_test

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
	"io"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// captureOutput runs fn with file descriptors 1 and 2 pointed at a pipe and
// returns everything written to either. Redirecting the descriptors, not just
// the os.Stdout and os.Stderr variables, also catches writers that captured
// the original *os.File at init time.
func captureOutput(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	savedOut, err := unix.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	savedErr, err := unix.Dup(2)
	if err != nil {
		t.Fatalf("dup stderr: %v", err)
	}
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	func() {
		defer func() {
			_ = unix.Dup2(savedOut, 1)
			_ = unix.Dup2(savedErr, 2)
			_ = unix.Close(savedOut)
			_ = unix.Close(savedErr)
		}()
		if err := unix.Dup2(int(w.Fd()), 1); err != nil {
			t.Fatalf("redirect stdout: %v", err)
		}
		if err := unix.Dup2(int(w.Fd()), 2); err != nil {
			t.Fatalf("redirect stderr: %v", err)
		}
		fn()
	}()
	_ = w.Close()
	return <-done
}
