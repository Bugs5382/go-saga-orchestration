package worker

/*
MIT License

Copyright (c) 2026 Bugs5382

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

import "fmt"

// Action declares one handler the worker will register with go-saga-orchestration
// on startup. Most fields mirror domain.ActionRegistration but the
// `Handler` field is the local implementation.
type Action struct {
	Name             string // e.g. "set_state"
	Description      string
	Category         string // "record_lifecycle" | "record_query" | "record_mutation" | "record_linking" | "notification" | "external_io" | "analytics" | "admin"
	Compensable      bool
	InputSchema      map[string]any // JSON-schema-lite
	OutputSchema     map[string]any
	ErrorCodes       []string
	DefaultRetry     map[string]any // {max_attempts, initial_backoff_ms}
	DefaultTimeoutMS int
	LicenseGroup     string // empty → registry default applies
	DryRunSupported  bool

	Handler Handler // required; the worker invokes this on dispatch
}

// BootstrapConfig is what cmd/worker/main.go fills in to spin up a worker.
type BootstrapConfig struct {
	Service        string // e.g. "example"
	ServiceVersion string // e.g. "1.0.0"

	// URLs:
	//   RegistryURL — go-saga-orchestration-api REST base (e.g. "http://go-saga-orchestration-api.platform.svc.cluster.local:8080")
	//   RmqURL      — RabbitMQ AMQP URL
	//   GrpcURL     — go-saga-orchestration-engine gRPC address (e.g. "go-saga-orchestration-engine.platform.svc.cluster.local:9090")
	RegistryURL string
	RmqURL      string
	GrpcURL     string

	// Actions to register on boot.
	Actions []Action
}

// Validate runs basic sanity checks on the config. Bootstrap calls this
// internally; tests can call it directly.
func (c BootstrapConfig) Validate() error {
	if c.Service == "" {
		return fmt.Errorf("worker: Service required")
	}
	if c.RegistryURL == "" {
		return fmt.Errorf("worker: RegistryURL required")
	}
	if c.RmqURL == "" {
		return fmt.Errorf("worker: RmqURL required")
	}
	if c.GrpcURL == "" {
		return fmt.Errorf("worker: GrpcURL required")
	}
	if len(c.Actions) == 0 {
		return fmt.Errorf("worker: at least one Action required")
	}
	seen := map[string]bool{}
	for i, a := range c.Actions {
		if a.Name == "" {
			return fmt.Errorf("worker: Actions[%d].Name required", i)
		}
		if seen[a.Name] {
			return fmt.Errorf("worker: duplicate Action name %q", a.Name)
		}
		seen[a.Name] = true
		if a.Handler == nil {
			return fmt.Errorf("worker: Actions[%d] (%s) missing Handler", i, a.Name)
		}
	}
	return nil
}

// Bootstrap is implemented in runtime.go (Tasks 8 + 9).
