package worker

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
)

// Compile-time: HandlerFunc satisfies Handler.
var _ Handler = HandlerFunc(nil)

func TestBootstrapConfig_ValidateRejectsEmpty(t *testing.T) {
	if err := (BootstrapConfig{}).Validate(); err == nil {
		t.Errorf("expected error for empty config")
	}
}

func TestBootstrapConfig_ValidateRejectsDuplicateActions(t *testing.T) {
	cfg := BootstrapConfig{
		Service: "x", RegistryURL: "u", RmqURL: "u", GrpcURL: "u",
		Actions: []Action{
			{Name: "a", Handler: HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) { return nil, nil })},
			{Name: "a", Handler: HandlerFunc(func(_ context.Context, _ ActionPayload) (Result, error) { return nil, nil })},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected duplicate-name error")
	}
}

func TestBootstrapConfig_ValidateRequiresHandler(t *testing.T) {
	cfg := BootstrapConfig{
		Service: "x", RegistryURL: "u", RmqURL: "u", GrpcURL: "u",
		Actions: []Action{{Name: "a"}}, // missing Handler
	}
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected missing-handler error")
	}
}

// TestBootstrap_NotImplemented was removed in Tasks 8+9 — Bootstrap now
// does real work (RabbitMQ + gRPC) so it cannot be unit-tested without a
// broker.  The pieces are covered by TestRegisterWithOrchestrator_*,
// TestConsumeLoop_*, and TestIdempotency_* in runtime_test.go /
// idempotency_test.go.
