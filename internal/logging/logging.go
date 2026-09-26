// Package logging builds the service binaries' logger on
// github.com/Bugs5382/go-log. The environment picks the output: LOG_LEVEL
// sets the minimum level (trace, debug, info, warn, error; default info) and
// LOG_FORMAT the rendering (json by default, console for local reading, or
// both). Library code never calls this; it logs only to a logger the caller
// supplies.
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
	"context"

	golog "github.com/Bugs5382/go-log"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

// New returns the logger for the named service binary, configured from
// LOG_LEVEL and LOG_FORMAT through go-log. Unlike go-log's neutral Logger it
// also implements sagalog.TraceLogger, so LOG_LEVEL=trace shows the engine's
// step-by-step lines.
func New(service string) golog.Logger {
	return svcLogger{l: golog.New(service)}
}

// svcLogger adapts the zerolog.Logger go-log builds to golog.Logger plus a
// Trace method. zerolog stays confined to this internal package.
type svcLogger struct {
	l zerolog.Logger
}

func withFields(e *zerolog.Event, fields []golog.Field) *zerolog.Event {
	for _, f := range fields {
		e = e.Interface(f.Key, f.Val)
	}
	return e
}

// Trace logs msg at trace level.
func (s svcLogger) Trace(msg string, fields ...golog.Field) {
	withFields(s.l.Trace(), fields).Msg(msg)
}

// Debug logs msg at debug level.
func (s svcLogger) Debug(msg string, fields ...golog.Field) {
	withFields(s.l.Debug(), fields).Msg(msg)
}

// Info logs msg at info level.
func (s svcLogger) Info(msg string, fields ...golog.Field) {
	withFields(s.l.Info(), fields).Msg(msg)
}

// Warn logs msg at warn level.
func (s svcLogger) Warn(msg string, fields ...golog.Field) {
	withFields(s.l.Warn(), fields).Msg(msg)
}

// Error logs msg at error level with err attached.
func (s svcLogger) Error(err error, msg string, fields ...golog.Field) {
	withFields(s.l.Error().Err(err), fields).Msg(msg)
}

// Fatal logs msg at fatal level with err attached, then exits.
func (s svcLogger) Fatal(err error, msg string, fields ...golog.Field) {
	withFields(s.l.Fatal().Err(err), fields).Msg(msg)
}

// With returns a child carrying fields on every line.
func (s svcLogger) With(fields ...golog.Field) golog.Logger {
	c := s.l.With()
	for _, f := range fields {
		c = c.Interface(f.Key, f.Val)
	}
	return svcLogger{l: c.Logger()}
}

// Ctx attaches the trace_id and span_id of ctx's active span, the same way
// go-log's Logger.Ctx does. Without a valid span it returns the receiver.
func (s svcLogger) Ctx(ctx context.Context) golog.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return s
	}
	return svcLogger{l: s.l.With().
		Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String()).
		Logger()}
}
