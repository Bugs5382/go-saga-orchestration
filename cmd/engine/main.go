// Command go-saga-orchestration-engine runs the saga coordinator: it consumes
// saga.advance messages, dispatches steps, runs the timer dispatcher, and
// serves the gRPC worker API (ExecuteStep streams) on the configured port
// (default :9090).
package main

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

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	googlegrpc "google.golang.org/grpc"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	grpcsrv "github.com/Bugs5382/go-saga-orchestration/internal/grpc"
	"github.com/Bugs5382/go-saga-orchestration/internal/logging"
	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/internal/storefactory"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
)

var (
	Version = "dev"
	GitSHA  = "unknown"
)

// mqEventEmitter satisfies verbs.EventEmitter by publishing to RabbitMQ
// ExchangeWorkflowEvents. Other pods subscribed to that exchange (via
// EventSubscriber.RunRMQ) will receive the event and wake any paused runs.
type mqEventEmitter struct {
	pub *mq.Publisher
}

func (e *mqEventEmitter) EmitEvent(ctx context.Context, topic string, headers map[string]string, payload map[string]any) error {
	return e.pub.PublishEvent(ctx, topic, headers, payload)
}

func main() {
	logging.Init(false)
	cfg := config.Load()
	log.Info().Str("version", Version).Str("sha", GitSHA).Msg("starting go-saga-orchestration-engine")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, closeStore, err := storefactory.Open(ctx, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("store open")
	}
	defer func() { _ = closeStore() }()

	if cfg.StoreType == "" || cfg.StoreType == "postgres" {
		log.Info().Msg("postgres migrations applied")
	}

	conn, err := mq.Connect(cfg.RabbitMQURL)
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq connect")
	}
	defer func() { _ = conn.Close() }()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq publisher")
	}
	defer func() { _ = pub.Close() }()

	clk := clock.SystemClock{}
	sec := secrets.NewMemory(map[string]string{})
	lr := licensing.StubAllowAll{}
	mqEmitter := &mqEventEmitter{pub: pub}
	coord := engine.NewCoordinator(st, pub, clk, sec, lr, pub, mqEmitter)

	// Every engine pod runs the timer dispatcher. For multi-replica production
	// deployments, consider leader-elected single fire via pg_try_advisory_lock
	// to avoid duplicate wakeups. Single-pod dev and tests are fine without it.
	timer := &engine.Timer{
		S:         st,
		Publisher: pub,
		Clock:     clk,
		Tick:      time.Second,
	}
	go func() {
		if err := timer.Run(ctx); err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("timer dispatcher")
		}
	}()

	cronDispatcher := &engine.CronDispatcher{
		S:         st,
		Publisher: pub,
		Clock:     clock.SystemClock{},
		Tick:      time.Second,
		Licensing: lr,
	}
	go func() {
		if err := cronDispatcher.Run(ctx); err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("cron dispatcher stopped")
		}
	}()

	dispatcher := &engine.TriggerDispatcher{S: st, Publisher: pub}
	sub := &engine.EventSubscriber{S: st, Publisher: pub, Dispatcher: dispatcher}
	// Production: go sub.RunRMQ(ctx, conn, "go-saga-orchestration-events-"+podID)
	// Subscriber initialised; RunRMQ wiring deferred until a prod RMQ env is available.
	_ = sub

	// Start the gRPC server so workers can connect via ExecuteStep streams.
	grpcAddr := ":" + cfg.Engine.GRPCPort
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal().Err(err).Str("addr", grpcAddr).Msg("grpc listen")
	}
	grpcServer := googlegrpc.NewServer()
	grpcsrv.Register(grpcServer, st, pub)
	go func() {
		log.Info().Str("addr", grpcAddr).Msg("grpc server listening")
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Error().Err(err).Msg("grpc server stopped")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info().Msg("shutting down")
		grpcServer.GracefulStop()
		cancel()
	}()

	advance := func(ctx context.Context, msg mq.SagaAdvanceMsg) error {
		log.Info().Str("saga_run_id", msg.SagaRunID).Msg("advance received")
		if err := coord.Advance(ctx, msg.SagaRunID); err != nil {
			log.Error().Err(err).Str("saga_run_id", msg.SagaRunID).Msg("advance failed")
			return fmt.Errorf("advance: %w", err)
		}
		return nil
	}
	if err := mq.ConsumeSagaAdvance(ctx, conn, advance); err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("consume")
	}
}
