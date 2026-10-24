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
	"fmt"
	"strings"

	golog "github.com/Bugs5382/go-log"
	"google.golang.org/grpc"
)

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

	// Logger receives the worker's logs: startup, each delivery, handler
	// outcome and every error. Optional: nil means the worker writes
	// nothing, unless the ctx passed to Bootstrap carries a logger
	// (sagalog.NewContext), which takes precedence. Build one with
	// golog.NewLogger to honour LOG_LEVEL and LOG_FORMAT.
	Logger golog.Logger

	// SkipRegistration skips the POST to the REST registry, for an engine
	// whose actions are registered somewhere else (an application that embeds
	// the engine and keeps its own verb registry). RegistryURL is then not
	// required.
	SkipRegistration bool

	// QualifiedActionNames makes each Action.Name the full action name the
	// engine dispatches (the step's action and the routing key), used as is,
	// for example "com.example.billing.charge". By default the name is
	// prefixed with Service: Name "charge" in Service "billing" is
	// "billing.charge".
	QualifiedActionNames bool

	// Queue is the durable RabbitMQ queue the worker consumes. Default
	// "<Service>.actions".
	Queue string

	// DialOptions replace the gRPC dial options used to reach GrpcURL, for
	// TLS or per-RPC credentials (a projected ServiceAccount token, for
	// example). Default: plaintext (insecure credentials).
	DialOptions []grpc.DialOption
}

// RoutingKeys returns the full action name of each Action, in order: the
// routing keys the worker's queue is bound to and the names its handlers
// answer to.
func (c BootstrapConfig) RoutingKeys() []string {
	keys := make([]string, 0, len(c.Actions))
	for _, a := range c.Actions {
		keys = append(keys, c.routingKey(a))
	}
	return keys
}

func (c BootstrapConfig) routingKey(a Action) string {
	if c.QualifiedActionNames {
		return a.Name
	}
	return c.Service + "." + a.Name
}

// QueueName returns Queue, or "<Service>.actions" when it is empty.
func (c BootstrapConfig) QueueName() string {
	if c.Queue != "" {
		return c.Queue
	}
	return c.Service + ".actions"
}

// Validate runs basic sanity checks on the config. Bootstrap calls this
// internally; tests can call it directly.
func (c BootstrapConfig) Validate() error {
	if c.Service == "" {
		return fmt.Errorf("worker: Service required")
	}
	if c.RegistryURL == "" && !c.SkipRegistration {
		return fmt.Errorf("worker: RegistryURL required (or set SkipRegistration)")
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
		if c.QualifiedActionNames && !strings.Contains(a.Name, ".") {
			return fmt.Errorf("worker: Actions[%d] qualified name %q has no dot (want service.name)", i, a.Name)
		}
	}
	return nil
}
