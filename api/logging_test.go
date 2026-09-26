package api

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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

func TestLoggingMiddleware_LevelsByStatus(t *testing.T) {
	cases := []struct {
		status int
		level  sagalogtest.Level
		msg    string
	}{
		{http.StatusOK, sagalogtest.LevelDebug, "http request"},
		{http.StatusNotFound, sagalogtest.LevelWarn, "http request rejected"},
		{http.StatusInternalServerError, sagalogtest.LevelError, "http request failed"},
	}
	for _, c := range cases {
		rec := sagalogtest.New()
		h := LoggingMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil))
		e, ok := rec.Find(c.msg)
		if !ok || e.Level != c.level {
			t.Fatalf("status %d: want %s %q, got %v", c.status, c.level, c.msg, rec.Entries())
		}
		if e.Fields["status"] != c.status || e.Fields["method"] != http.MethodGet {
			t.Errorf("status %d: fields = %v", c.status, e.Fields)
		}
	}
}

// Handlers see the logger on the request context, and nothing from the query
// string or body reaches the log.
func TestLoggingMiddleware_CarriesLoggerAndHidesPayload(t *testing.T) {
	rec := sagalogtest.New()
	h := LoggingMiddleware(rec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := sagalog.FromContext(r.Context()); !ok {
			t.Error("handler context carries no logger")
		}
		sagalog.For(r.Context(), nil).Info("inside handler")
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sagas/start?token=QUERY-SECRET", strings.NewReader(`{"card":"BODY-SECRET"}`))
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !rec.Has(sagalogtest.LevelInfo, "inside handler") {
		t.Fatalf("handler line missing: %v", rec.Entries())
	}
	for _, s := range []string{"QUERY-SECRET", "BODY-SECRET"} {
		if rec.Contains(s) {
			t.Fatalf("%s leaked into the log: %v", s, rec.Entries())
		}
	}
}

func TestLoggingMiddleware_NilLoggerIsPassThrough(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	h := LoggingMiddleware(nil)(next)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}
