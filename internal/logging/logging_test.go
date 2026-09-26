package logging

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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	golog "github.com/Bugs5382/go-log"
	"go.opentelemetry.io/otel/trace"

	"github.com/Bugs5382/go-saga-orchestration/sagalog"
)

// capture builds a logger with LOG_LEVEL=level while stdout points at a pipe
// (go-log binds its writer at construction), runs fn, and returns the decoded
// JSON lines.
func capture(t *testing.T, level string, fn func(golog.Logger)) []map[string]any {
	t.Helper()
	t.Setenv("LOG_LEVEL", level)
	t.Setenv("LOG_FORMAT", "json")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	l := New("test-svc")
	os.Stdout = old

	done := make(chan []map[string]any)
	go func() {
		var lines []map[string]any
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err == nil {
				lines = append(lines, m)
			}
		}
		_, _ = io.Copy(io.Discard, r)
		done <- lines
	}()
	fn(l)
	_ = w.Close()
	return <-done
}

func levels(lines []map[string]any) []string {
	out := make([]string, 0, len(lines))
	for _, m := range lines {
		out = append(out, m["level"].(string))
	}
	return out
}

func logEveryLevel(l golog.Logger) {
	sagalog.Trace(l, "t", golog.F("run_id", "r1"))
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error(errors.New("boom"), "e")
}

func TestNew_WritesEveryLevelAtTrace(t *testing.T) {
	lines := capture(t, "trace", logEveryLevel)
	got := levels(lines)
	want := []string{"trace", "debug", "info", "warn", "error"}
	if len(got) != len(want) {
		t.Fatalf("levels = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("levels = %v, want %v", got, want)
		}
	}
	if lines[0]["service"] != "test-svc" || lines[0]["run_id"] != "r1" {
		t.Errorf("trace line = %v", lines[0])
	}
	if lines[4]["error"] != "boom" {
		t.Errorf("error line = %v", lines[4])
	}
}

func TestNew_HonoursLogLevel(t *testing.T) {
	got := levels(capture(t, "warn", logEveryLevel))
	if len(got) != 2 || got[0] != "warn" || got[1] != "error" {
		t.Fatalf("LOG_LEVEL=warn levels = %v, want [warn error]", got)
	}
	got = levels(capture(t, "", logEveryLevel))
	if len(got) != 3 || got[0] != "info" {
		t.Fatalf("default levels = %v, want [info warn error]", got)
	}
}

func TestNew_CtxAttachesTraceAndSpan(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	lines := capture(t, "info", func(l golog.Logger) {
		l.With(golog.F("run_id", "r1")).Ctx(ctx).Info("correlated")
	})
	if len(lines) != 1 {
		t.Fatalf("lines = %v", lines)
	}
	if lines[0]["trace_id"] != sc.TraceID().String() || lines[0]["span_id"] != sc.SpanID().String() {
		t.Errorf("line = %v, want trace and span IDs", lines[0])
	}
	if lines[0]["run_id"] != "r1" {
		t.Errorf("With fields lost across Ctx: %v", lines[0])
	}
}
