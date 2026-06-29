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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
)

// Bootstrap connects to go-saga-orchestration's registry, registers the actions,
// declares the per-service RabbitMQ queue, opens a long-lived gRPC
// client to the engine, and runs the consumer loop until ctx is
// cancelled.
func Bootstrap(ctx context.Context, cfg BootstrapConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	// 1. Register actions via REST.
	if err := registerWithOrchestrator(ctx, cfg); err != nil {
		return fmt.Errorf("worker: register: %w", err)
	}

	// 2. Open RabbitMQ + the per-service queue.
	rmqConn, err := amqp.Dial(cfg.RmqURL)
	if err != nil {
		return fmt.Errorf("worker: dial rabbitmq: %w", err)
	}
	defer func() { _ = rmqConn.Close() }()
	ch, err := rmqConn.Channel()
	if err != nil {
		return fmt.Errorf("worker: rabbitmq channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	queueName := cfg.Service + ".actions"
	if _, err := ch.QueueDeclare(queueName, true /*durable*/, false, false, false, nil); err != nil {
		return fmt.Errorf("worker: queue declare: %w", err)
	}
	if err := ch.QueueBind(queueName, cfg.Service+".*", "action.direct", false, nil); err != nil {
		return fmt.Errorf("worker: queue bind: %w", err)
	}
	if err := ch.Qos(1 /*prefetch*/, 0, false); err != nil {
		return fmt.Errorf("worker: qos: %w", err)
	}
	deliveries, err := ch.Consume(queueName, "", false /*autoAck*/, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("worker: consume: %w", err)
	}

	// 3. Open the long-lived gRPC client.
	gconn, err := grpc.NewClient(cfg.GrpcURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("worker: grpc dial: %w", err)
	}
	defer func() { _ = gconn.Close() }()
	client := pb.NewWorkerLivenessClient(gconn)

	// Build the per-action handler lookup.
	handlers := map[string]Handler{}
	for _, a := range cfg.Actions {
		handlers[a.Name] = a.Handler
	}

	// 4. Consumer loop.
	return consumeLoop(ctx, deliveries, func(ctx context.Context, d amqp.Delivery) {
		processDelivery(ctx, d, handlers, client)
	})
}

// registerWithOrchestrator POSTs the service's action declarations to the
// registry endpoint. Returns error on non-2xx.
func registerWithOrchestrator(ctx context.Context, cfg BootstrapConfig) error {
	type actionEnvelope struct {
		ActionName       string         `json:"action_name"`
		Version          int            `json:"version"`
		Description      string         `json:"description,omitempty"`
		Category         string         `json:"category,omitempty"`
		Compensable      bool           `json:"compensable,omitempty"`
		InputSchema      map[string]any `json:"input_schema,omitempty"`
		OutputSchema     map[string]any `json:"output_schema,omitempty"`
		ErrorCodes       []string       `json:"error_codes,omitempty"`
		DefaultRetry     map[string]any `json:"default_retry,omitempty"`
		DefaultTimeoutMS int            `json:"default_timeout_ms,omitempty"`
		LicenseGroup     string         `json:"license_group,omitempty"`
		DryRunSupported  bool           `json:"dry_run_supported,omitempty"`
	}
	type registerEnvelope struct {
		Service        string           `json:"service"`
		ServiceVersion string           `json:"service_version"`
		Actions        []actionEnvelope `json:"actions"`
	}
	envs := make([]actionEnvelope, 0, len(cfg.Actions))
	for _, a := range cfg.Actions {
		envs = append(envs, actionEnvelope{
			ActionName:       a.Name,
			Version:          1, // v1 — service-version handles iteration
			Description:      a.Description,
			Category:         a.Category,
			Compensable:      a.Compensable,
			InputSchema:      a.InputSchema,
			OutputSchema:     a.OutputSchema,
			ErrorCodes:       a.ErrorCodes,
			DefaultRetry:     a.DefaultRetry,
			DefaultTimeoutMS: a.DefaultTimeoutMS,
			LicenseGroup:     a.LicenseGroup,
			DryRunSupported:  a.DryRunSupported,
		})
	}
	body, err := json.Marshal(registerEnvelope{
		Service:        cfg.Service,
		ServiceVersion: cfg.ServiceVersion,
		Actions:        envs,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(cfg.RegistryURL, "/")+"/api/v1/registry/register",
		strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// consumeLoop drains the deliveries channel, calling dispatch for each.
// Returns when ctx is cancelled or the channel closes.
func consumeLoop(ctx context.Context, deliveries <-chan amqp.Delivery, dispatch func(context.Context, amqp.Delivery)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("worker: deliveries channel closed")
			}
			dispatch(ctx, d)
		}
	}
}

// processDelivery deserializes the ActionPayload, looks up the handler,
// drives the gRPC stream (Start -> Heartbeats -> Complete/Error), and ACKs
// the RabbitMQ message.
func processDelivery(ctx context.Context, d amqp.Delivery, handlers map[string]Handler, client pb.WorkerLivenessClient) {
	var payload ActionPayload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		log.Error().Err(err).Msg("worker: bad payload; nacking")
		_ = d.Nack(false /*multiple*/, false /*requeue=false -> DLQ*/)
		return
	}
	// Resolve handler by the suffix after the service prefix.
	// payload.Action format: "<service>.<action_name>" — handlers map is keyed by action_name only.
	name := payload.Action
	if dot := strings.Index(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	h, ok := handlers[name]
	if !ok {
		log.Error().Str("action", payload.Action).Msg("worker: no handler for action")
		_ = d.Nack(false, false)
		return
	}
	if err := driveStream(ctx, payload, h, client); err != nil {
		log.Error().Err(err).Msg("worker: stream failed; nacking with requeue")
		_ = d.Nack(false, true /*requeue=true -> retry via redelivery*/)
		return
	}
	_ = d.Ack(false)
}

// driveStream opens an ExecuteStep stream, sends Start, runs the handler,
// streams Heartbeats (optional in v1), sends Complete or Error, closes.
func driveStream(ctx context.Context, payload ActionPayload, h Handler, client pb.WorkerLivenessClient) error {
	stream, err := client.ExecuteStep(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	// 1. Start.
	if err := stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Start{Start: &pb.StartJob{
		RunId: payload.RunID, StepId: payload.StepID, Attempt: int32(payload.Attempt),
	}}}); err != nil {
		return fmt.Errorf("send start: %w", err)
	}
	// 2. Recv Ack.
	ev, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv ack: %w", err)
	}
	if ev.GetAck() == nil {
		return fmt.Errorf("expected ack, got %T", ev.GetEvent())
	}

	// 3. Run the handler.
	result, hErr := h.Execute(ctx, payload)
	if hErr != nil {
		// Map to Error.
		code := "handler_error"
		msg := hErr.Error()
		retryable := true
		if cw, ok := hErr.(coded); ok {
			code = cw.Code()
			retryable = cw.Retryable()
		}
		_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Error{Error: &pb.Error{
			Code: code, Message: msg, Retryable: retryable,
		}}})
		return nil // gRPC layer ok; engine handles the failure
	}
	bs, _ := json.Marshal(result)
	_ = stream.Send(&pb.WorkerEvent{Event: &pb.WorkerEvent_Complete{Complete: &pb.Complete{ResultJson: bs}}})
	return nil
}

// coded is implemented by error types that want to surface a stable
// error code and retryable flag.
type coded interface {
	Code() string
	Retryable() bool
}

// CodedError is the convenience error type a Handler returns to control
// how the engine records and retries a failed step. It carries a stable
// error code, a human-readable message, and a retryable flag. Construct one
// with Errorf rather than building the struct directly. When a handler
// returns any error that does not implement the code/retryable contract,
// the engine defaults to code "handler_error" with retryable=true.
type CodedError struct {
	C   string
	Msg string
	R   bool
}

// Error implements the error interface, returning the human-readable
// message (the Msg field).
func (e CodedError) Error() string { return e.Msg }

// Code returns the stable error code (the C field) that the engine records
// for the failed step.
func (e CodedError) Code() string { return e.C }

// Retryable reports whether the engine should retry the step (the R field).
func (e CodedError) Retryable() bool { return e.R }

// Errorf returns a CodedError with the given code + retryable flag.
func Errorf(code string, retryable bool, format string, args ...any) CodedError {
	return CodedError{C: code, Msg: fmt.Sprintf(format, args...), R: retryable}
}

// Compile-time assertion: zerolog log and time imports used.
var _ = log.Logger
var _ = time.Second
