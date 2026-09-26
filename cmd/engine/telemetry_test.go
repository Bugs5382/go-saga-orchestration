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
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/Bugs5382/go-saga-orchestration/internal/telemetry"
	"github.com/Bugs5382/go-saga-orchestration/sagalog/sagalogtest"
)

// The engine binary initialises telemetry with and without a collector
// endpoint, and afterwards the global tracer yields real spans for the
// saga.advance handler.
func TestTelemetry_InitWithAndWithoutEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "127.0.0.1:4317"} {
		t.Run("endpoint="+endpoint, func(t *testing.T) {
			t.Setenv(telemetry.EndpointEnv, endpoint)
			rec := sagalogtest.New()
			shutdown, err := telemetry.Setup(context.Background(), serviceName, rec)
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			_, span := otel.Tracer(serviceName).Start(context.Background(), "saga.advance")
			if !span.SpanContext().IsValid() {
				t.Error("saga.advance span has no valid span context")
			}
			span.End()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = shutdown(ctx)
			if len(rec.Entries()) == 0 {
				t.Error("Setup logged nothing")
			}
		})
	}
}
