// Package telemetry sets up OpenTelemetry for the service binaries through
// github.com/Bugs5382/go-otel. Library packages never call it; they use the
// global OTel API only, so an embedder's own setup is left alone.
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
	"fmt"
	"os"
	"strings"

	golog "github.com/Bugs5382/go-log"
	gootel "github.com/Bugs5382/go-otel"
)

// EndpointEnv names the variable that holds the OTLP gRPC collector address,
// a bare host:port. Unset or empty means no exporter: spans still get real
// trace IDs and W3C trace context still propagates, but nothing is exported.
const EndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// Setup initialises the global tracer and meter providers and the W3C
// propagator for service, exporting to the endpoint in EndpointEnv when set.
// On success it returns the shutdown func that flushes both pipelines; the
// caller defers it. On error it returns nil and the caller must not defer
// anything.
func Setup(ctx context.Context, service string, logger golog.Logger) (func(context.Context) error, error) {
	endpoint := strings.TrimSpace(os.Getenv(EndpointEnv))
	shutdown, err := gootel.Init(ctx, service, endpoint)
	if err != nil {
		logger.Error(err, "telemetry: init failed", golog.F("endpoint", endpoint))
		return nil, fmt.Errorf("telemetry: init: %w", err)
	}
	if endpoint == "" {
		logger.Info("telemetry: initialised without an exporter; trace context still propagates",
			golog.F("endpoint_env", EndpointEnv))
	} else {
		logger.Info("telemetry: exporting traces and metrics over OTLP gRPC", golog.F("endpoint", endpoint))
	}
	return shutdown, nil
}
