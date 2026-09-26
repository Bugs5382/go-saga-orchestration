package sagalog_test

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
	"testing"

	golog "github.com/Bugs5382/go-log"

	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

func TestFor_ContextBeatsFallback(t *testing.T) {
	fromCtx, fallback := sagalogtest.New(), sagalogtest.New()
	ctx := sagalog.NewContext(context.Background(), fromCtx)
	sagalog.For(ctx, fallback).Info("hello")
	if !fromCtx.Has(sagalogtest.LevelInfo, "hello") || len(fallback.Entries()) != 0 {
		t.Fatalf("ctx=%v fallback=%v", fromCtx.Entries(), fallback.Entries())
	}
}

func TestFor_FallbackThenNop(t *testing.T) {
	fallback := sagalogtest.New()
	sagalog.For(context.Background(), fallback).Warn("w")
	if !fallback.Has(sagalogtest.LevelWarn, "w") {
		t.Fatalf("fallback not used: %v", fallback.Entries())
	}
	// No logger anywhere: a usable no-op, never nil.
	l := sagalog.For(context.Background(), nil)
	l.Error(nil, "dropped")
	l.With(golog.F("k", "v")).Ctx(context.Background()).Debug("dropped")
}

func TestNewContext_NilLoggerLeavesContext(t *testing.T) {
	ctx := sagalog.NewContext(context.Background(), nil)
	if _, ok := sagalog.FromContext(ctx); ok {
		t.Fatal("nil logger should not be stored")
	}
}

// Trace reaches loggers that implement TraceLogger, and is dropped for those
// that do not (such as go-log's neutral Logger).
func TestTrace_OnlyForTraceLoggers(t *testing.T) {
	rec := sagalogtest.New()
	sagalog.Trace(rec.With(golog.F("run_id", "r1")), "step", golog.F("n", 1))
	e, ok := rec.Find("step")
	if !ok || e.Level != sagalogtest.LevelTrace || e.Fields["run_id"] != "r1" || e.Fields["n"] != 1 {
		t.Fatalf("entries = %v", rec.Entries())
	}
	sagalog.Trace(debugOnly{t}, "dropped")
}

// debugOnly implements golog.Logger without Trace; any call fails the test.
type debugOnly struct{ t *testing.T }

func (d debugOnly) Debug(string, ...golog.Field)        { d.t.Fatal("unexpected Debug") }
func (d debugOnly) Info(string, ...golog.Field)         { d.t.Fatal("unexpected Info") }
func (d debugOnly) Warn(string, ...golog.Field)         { d.t.Fatal("unexpected Warn") }
func (d debugOnly) Error(error, string, ...golog.Field) { d.t.Fatal("unexpected Error") }
func (d debugOnly) Fatal(error, string, ...golog.Field) { d.t.Fatal("unexpected Fatal") }
func (d debugOnly) With(...golog.Field) golog.Logger    { return d }
func (d debugOnly) Ctx(context.Context) golog.Logger    { return d }
