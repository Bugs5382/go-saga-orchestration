package telemetry

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
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

// assertTracing checks that the global tracer yields real span contexts and
// that the global propagator injects W3C trace context.
func assertTracing(t *testing.T) {
	t.Helper()
	ctx, span := otel.Tracer("telemetry-test").Start(context.Background(), "probe")
	defer span.End()
	if !span.SpanContext().IsValid() {
		t.Fatal("span context is not valid; tracer provider not installed")
	}
	h := http.Header{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
	if h.Get("traceparent") == "" {
		t.Fatal("no traceparent injected; propagator not installed")
	}
}

func TestSetup_WithoutEndpoint(t *testing.T) {
	t.Setenv(EndpointEnv, "")
	rec := sagalogtest.New()
	shutdown, err := Setup(context.Background(), "test-svc", rec)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	assertTracing(t)
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if !rec.Has("info", "telemetry: initialised without an exporter; trace context still propagates") {
		t.Errorf("missing init line: %v", rec.Entries())
	}
}

// With an endpoint set, Setup builds the OTLP exporters. The gRPC client is
// lazy, so nothing needs to listen on the address for Init to succeed.
func TestSetup_WithEndpoint(t *testing.T) {
	t.Setenv(EndpointEnv, "127.0.0.1:4317")
	rec := sagalogtest.New()
	shutdown, err := Setup(context.Background(), "test-svc", rec)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	assertTracing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// No collector is listening; the final flush may fail, which go-otel hands
	// to the global error handler. Shutdown itself must still return.
	_ = shutdown(ctx)
	e, ok := rec.Find("telemetry: exporting traces and metrics over OTLP gRPC")
	if !ok || e.Fields["endpoint"] != "127.0.0.1:4317" {
		t.Errorf("missing export line: %v", rec.Entries())
	}
}
