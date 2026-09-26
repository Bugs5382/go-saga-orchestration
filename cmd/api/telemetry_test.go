package main

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
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gootel "github.com/Bugs5382/go-otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/internal/telemetry"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

// The API binary initialises telemetry with and without a collector endpoint.
func TestTelemetry_InitWithAndWithoutEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "127.0.0.1:4317"} {
		t.Run("endpoint="+endpoint, func(t *testing.T) {
			t.Setenv(telemetry.EndpointEnv, endpoint)
			shutdown, err := telemetry.Setup(context.Background(), serviceName, sagalog.Nop())
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = shutdown(ctx)
		})
	}
}

// The binary's handler chain gives each request a valid span, continues an
// incoming traceparent, puts the trace ID on log lines, and still hands the
// handler a writer that can hijack (websocket upgrade) and flush.
func TestHandlerChain_TracesAndKeepsWriterCapabilities(t *testing.T) {
	t.Setenv(telemetry.EndpointEnv, "")
	shutdown, err := telemetry.Setup(context.Background(), serviceName, sagalog.Nop())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	rec := sagalogtest.New()
	var gotTrace string
	var hijacker, flusher bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTrace = trace.SpanContextFromContext(r.Context()).TraceID().String()
		_, hijacker = w.(http.Hijacker)
		_, flusher = w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
	})
	h := tracingMiddleware(gootel.Metrics(api.LoggingMiddleware(rec)(inner)))

	const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil)
	req.Header.Set("traceparent", parent)
	h.ServeHTTP(hijackRecorder{httptest.NewRecorder()}, req)

	if gotTrace != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace ID = %q, want the incoming traceparent's", gotTrace)
	}
	if !hijacker || !flusher {
		t.Errorf("handler writer: hijacker=%v flusher=%v, want both", hijacker, flusher)
	}
	if !rec.Has(sagalogtest.LevelDebug, "http request") {
		t.Errorf("missing request log line: %v", rec.Entries())
	}
}

// hijackRecorder is an httptest.ResponseRecorder that also claims
// http.Hijacker, like a real server connection.
type hijackRecorder struct{ *httptest.ResponseRecorder }

func (hijackRecorder) Hijack() (conn net.Conn, rw *bufio.ReadWriter, err error) {
	return nil, nil, http.ErrNotSupported
}
