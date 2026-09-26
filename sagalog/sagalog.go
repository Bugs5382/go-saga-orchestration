// Package sagalog carries the caller's github.com/Bugs5382/go-log logger
// through the engine.
//
// The engine never builds a logger of its own. A caller hands one in, either
// through an option (saga.Options.Logger, Coordinator.SetLogger, the worker's
// BootstrapConfig.Logger) or on the context with NewContext. With neither,
// every log call goes to Nop and nothing is written, so embedding the library
// never produces output the caller did not ask for.
//
// Loggers are always derived per call with the go-log Logger.Ctx method, so
// trace_id and span_id from the active OpenTelemetry span are attached.
//
// Log lines carry identifiers (run, step, workflow, trigger IDs, step types,
// states, attempts, durations), never run variables, step inputs or outputs,
// or signal and event payloads.
package sagalog

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

	golog "github.com/Bugs5382/go-log"
)

type ctxKey struct{}

// NewContext returns a copy of ctx that carries l. Engine code reached through
// ctx logs to l in preference to any logger set by option. A nil l returns ctx
// unchanged.
func NewContext(ctx context.Context, l golog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the logger carried by ctx, if any.
func FromContext(ctx context.Context) (golog.Logger, bool) {
	if ctx == nil {
		return nil, false
	}
	l, ok := ctx.Value(ctxKey{}).(golog.Logger)
	return l, ok && l != nil
}

// For picks the logger for a call: the one carried by ctx, else fallback, else
// Nop. The result is derived with Logger.Ctx so the active span's trace and
// span IDs are attached.
func For(ctx context.Context, fallback golog.Logger) golog.Logger {
	l, ok := FromContext(ctx)
	if !ok {
		l = fallback
	}
	if l == nil {
		return Nop()
	}
	if ctx == nil {
		return l
	}
	return l.Ctx(ctx)
}

// Or returns l, or Nop when l is nil.
func Or(l golog.Logger) golog.Logger {
	if l == nil {
		return Nop()
	}
	return l
}

// TraceLogger is implemented by loggers that support a trace level below
// debug. The go-log Logger interface stops at Debug, so trace lines are only
// written when the caller's logger also implements this interface. Otherwise
// they are dropped, which keeps debug output readable.
type TraceLogger interface {
	Trace(msg string, fields ...golog.Field)
}

// Trace writes a trace-level line when l implements TraceLogger, and does
// nothing otherwise.
func Trace(l golog.Logger, msg string, fields ...golog.Field) {
	if t, ok := l.(TraceLogger); ok {
		t.Trace(msg, fields...)
	}
}

// Nop returns a logger that discards everything. It is the default whenever
// the caller has not supplied a logger.
func Nop() golog.Logger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Debug(string, ...golog.Field)        {}
func (nopLogger) Info(string, ...golog.Field)         {}
func (nopLogger) Warn(string, ...golog.Field)         {}
func (nopLogger) Error(error, string, ...golog.Field) {}
func (nopLogger) Fatal(error, string, ...golog.Field) {}
func (n nopLogger) With(...golog.Field) golog.Logger  { return n }
func (n nopLogger) Ctx(context.Context) golog.Logger  { return n }
func (nopLogger) Trace(string, ...golog.Field)        {}
