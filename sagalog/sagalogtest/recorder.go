// Package sagalogtest provides a recording go-log logger for tests.
package sagalogtest

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
	"fmt"
	"strings"
	"sync"

	golog "github.com/Bugs5382/go-log"
)

// Level names a recorded line's level.
type Level string

// Levels, finest first.
const (
	LevelTrace Level = "trace"
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
	LevelFatal Level = "fatal"
)

// Entry is one recorded line.
type Entry struct {
	Level  Level
	Msg    string
	Err    error
	Fields map[string]any
}

// String renders the entry with every field, for leak checks and failure
// messages.
func (e Entry) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %q", e.Level, e.Msg)
	if e.Err != nil {
		fmt.Fprintf(&b, " err=%q", e.Err.Error())
	}
	for k, v := range e.Fields {
		fmt.Fprintf(&b, " %s=%v", k, v)
	}
	return b.String()
}

// Recorder is a go-log Logger that also implements sagalog.TraceLogger and
// keeps every line in memory. It is safe for concurrent use. Fatal records
// the line and does not exit.
type Recorder struct {
	sink   *sink
	fields map[string]any
}

type sink struct {
	mu      sync.Mutex
	entries []Entry
}

// New returns an empty Recorder.
func New() *Recorder { return &Recorder{sink: &sink{}} }

func (r *Recorder) add(level Level, err error, msg string, fields []golog.Field) {
	all := make(map[string]any, len(r.fields)+len(fields))
	for k, v := range r.fields {
		all[k] = v
	}
	for _, f := range fields {
		all[f.Key] = f.Val
	}
	r.sink.mu.Lock()
	defer r.sink.mu.Unlock()
	r.sink.entries = append(r.sink.entries, Entry{Level: level, Msg: msg, Err: err, Fields: all})
}

// Trace records a trace-level line.
func (r *Recorder) Trace(msg string, fields ...golog.Field) { r.add(LevelTrace, nil, msg, fields) }

// Debug records a debug-level line.
func (r *Recorder) Debug(msg string, fields ...golog.Field) { r.add(LevelDebug, nil, msg, fields) }

// Info records an info-level line.
func (r *Recorder) Info(msg string, fields ...golog.Field) { r.add(LevelInfo, nil, msg, fields) }

// Warn records a warn-level line.
func (r *Recorder) Warn(msg string, fields ...golog.Field) { r.add(LevelWarn, nil, msg, fields) }

// Error records an error-level line.
func (r *Recorder) Error(err error, msg string, fields ...golog.Field) {
	r.add(LevelError, err, msg, fields)
}

// Fatal records a fatal-level line. It does not exit.
func (r *Recorder) Fatal(err error, msg string, fields ...golog.Field) {
	r.add(LevelFatal, err, msg, fields)
}

// With returns a child that shares the same entries and adds fields to each
// of its lines.
func (r *Recorder) With(fields ...golog.Field) golog.Logger {
	merged := make(map[string]any, len(r.fields)+len(fields))
	for k, v := range r.fields {
		merged[k] = v
	}
	for _, f := range fields {
		merged[f.Key] = f.Val
	}
	return &Recorder{sink: r.sink, fields: merged}
}

// Ctx returns the receiver. The recorder does not read spans.
func (r *Recorder) Ctx(context.Context) golog.Logger { return r }

// Entries returns a copy of every recorded line, oldest first.
func (r *Recorder) Entries() []Entry {
	r.sink.mu.Lock()
	defer r.sink.mu.Unlock()
	return append([]Entry(nil), r.sink.entries...)
}

// Has reports whether a line at level with message msg was recorded.
func (r *Recorder) Has(level Level, msg string) bool {
	for _, e := range r.Entries() {
		if e.Level == level && e.Msg == msg {
			return true
		}
	}
	return false
}

// Find returns the first line with message msg.
func (r *Recorder) Find(msg string) (Entry, bool) {
	for _, e := range r.Entries() {
		if e.Msg == msg {
			return e, true
		}
	}
	return Entry{}, false
}

// Contains reports whether any recorded line, rendered with all its fields,
// contains s. Use it to prove that payload values never reach the log.
func (r *Recorder) Contains(s string) bool {
	for _, e := range r.Entries() {
		if strings.Contains(e.String(), s) {
			return true
		}
	}
	return false
}
