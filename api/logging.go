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
	"time"

	golog "github.com/Bugs5382/go-log"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Bugs5382/go-saga-orchestration/sagalog"
)

// LoggingMiddleware puts l on every request's context, so the handlers and
// the engine code they call log through it, and logs each request when it
// finishes: debug for success, warn for 4xx, error for 5xx. It logs the
// method, the route pattern when chi has resolved one (else the URL path,
// which holds at most resource IDs), the status, the duration and chi's
// request ID when set. It never logs headers, query strings or bodies. A nil
// l returns next unchanged.
func LoggingMiddleware(l golog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if l == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			reqLog := l
			if id := middleware.GetReqID(r.Context()); id != "" {
				reqLog = l.With(golog.F("request_id", id))
			}
			ctx := sagalog.NewContext(r.Context(), reqLog)
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(ctx))

			route := r.URL.Path
			if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
				route = rc.RoutePattern()
			}
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			fields := []golog.Field{
				golog.F("method", r.Method), golog.F("route", route), golog.F("status", status),
				golog.F("duration_ms", time.Since(start).Milliseconds()), golog.F("bytes", ww.BytesWritten()),
			}
			lg := reqLog.Ctx(ctx)
			switch {
			case status >= 500:
				lg.Error(nil, "http request failed", fields...)
			case status >= 400:
				lg.Warn("http request rejected", fields...)
			default:
				lg.Debug("http request", fields...)
			}
		})
	}
}
