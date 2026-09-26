// Command go-saga-orchestration-engine runs the saga coordinator: it consumes
// saga.advance messages, dispatches steps, runs the timer dispatcher, and
// serves the gRPC worker API (ExecuteStep streams) on the configured port
// (default :9090).
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
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	golog "github.com/Bugs5382/go-log"
	"github.com/jackc/pgx/v5/pgxpool"
	googlegrpc "google.golang.org/grpc"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/engine/verbs"
	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	"github.com/Bugs5382/go-saga-orchestration/internal/dispatch"
	grpcsrv "github.com/Bugs5382/go-saga-orchestration/internal/grpc"
	"github.com/Bugs5382/go-saga-orchestration/internal/logging"
	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/internal/storefactory"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/postgres"
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
	// LOG_LEVEL (default info) and LOG_FORMAT (json, console or both) control
	// this logger; see internal/logging.
	logger := logging.New("go-saga-orchestration-engine")
	cfg := config.Load()
	logger.Info("starting go-saga-orchestration-engine", golog.F("version", Version), golog.F("sha", GitSHA))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = sagalog.NewContext(ctx, logger)

	st, closeStore, err := storefactory.Open(ctx, cfg)
	if err != nil {
		logger.Fatal(err, "store open")
	}
	defer func() { _ = closeStore() }()

	if cfg.StoreType == "" || cfg.StoreType == "postgres" {
		logger.Info("postgres migrations applied")
	}

	conn, err := mq.Connect(cfg.RabbitMQURL)
	if err != nil {
		logger.Fatal(err, "rabbitmq connect")
	}
	defer func() { _ = conn.Close() }()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		logger.Fatal(err, "rabbitmq publisher")
	}
	defer func() { _ = pub.Close() }()

	clk := clock.SystemClock{}
	sec := secrets.NewMemory(map[string]string{})
	lr := licensing.StubAllowAll{}
	mqEmitter := &mqEventEmitter{pub: pub}
	// Wire the optional dispatch-descriptor transports: http callbacks via the
	// http dispatcher, rmq via the publisher's named-queue method. gRPC stays
	// the zero-config default for actions with no descriptor. (issue #59)
	httpDispatcher := dispatch.NewHTTPDispatcher()
	coord := engine.NewCoordinator(st, pub, clk, sec, lr, pub, mqEmitter,
		verbs.WithHTTPDispatcher(httpDispatcher),
		verbs.WithRMQDispatcher(pub))
	coord.SetLogger(logger)

	// The timer dispatcher is leader-elected: exactly one engine replica runs
	// it at a time. Each pod races for the timer advisory lock and only the
	// winner runs timer.Run, so multi-replica deployments do not fire duplicate
	// wakeups. When the postgres store is not in use (dev/tests) the pool is nil
	// and the dispatcher runs unconditionally.
	timer := &engine.Timer{
		S:         st,
		Publisher: pub,
		Clock:     clk,
		Tick:      time.Second,
		Logger:    logger,
	}
	var enginePool *pgxpool.Pool
	if ps, ok := st.(*postgres.Store); ok {
		enginePool = ps.Pool()
	}
	go func() {
		if enginePool != nil {
			release, err := postgres.AcquireAdvisoryLock(ctx, enginePool, engine.TimerAdvisoryLockID)
			if err != nil {
				logger.Error(err, "timer dispatcher: acquire leader lock")
				return
			}
			logger.Info("timer dispatcher: leader acquired")
			defer release()
		}
		if err := timer.Run(ctx); err != nil && err != context.Canceled {
			logger.Error(err, "timer dispatcher")
		}
	}()

	// The cron dispatcher loop is gated by WORKFLOW_CRON_DISPATCHER (issue #69).
	// Firing is exactly-once across pods via the ClaimCronFire CAS, so this gate
	// is for operational isolation: run the dispatcher on a single dedicated
	// engine pod and disable it on the saga-processing replicas. The default is
	// on, preserving the single-deployment behavior.
	if cfg.Engine.EnableCronDispatcher {
		cronDispatcher := &engine.CronDispatcher{
			S:         st,
			Publisher: pub,
			Clock:     clock.SystemClock{},
			Tick:      time.Second,
			Licensing: lr,
			Logger:    logger,
		}
		go func() {
			if err := cronDispatcher.Run(ctx); err != nil && err != context.Canceled {
				logger.Error(err, "cron dispatcher stopped")
			}
		}()
		logger.Info("cron dispatcher enabled")
	} else {
		logger.Info("cron dispatcher disabled (WORKFLOW_CRON_DISPATCHER=false)")
	}

	dispatcher := &engine.TriggerDispatcher{S: st, Publisher: pub, Logger: logger}
	sub := &engine.EventSubscriber{S: st, Publisher: pub, Dispatcher: dispatcher, Logger: logger}
	// Production: go sub.RunRMQ(ctx, conn, "go-saga-orchestration-events-"+podID)
	// Subscriber initialised; RunRMQ wiring deferred until a prod RMQ env is available.
	_ = sub

	// Start the gRPC server so workers can connect via ExecuteStep streams.
	grpcAddr := ":" + cfg.Engine.GRPCPort
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal(err, "grpc listen", golog.F("addr", grpcAddr))
	}
	grpcServer := googlegrpc.NewServer()
	grpcsrv.RegisterWithLogger(grpcServer, st, pub, logger)
	go func() {
		logger.Info("grpc server listening", golog.F("addr", grpcAddr))
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error(err, "grpc server stopped")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Info("shutting down")
		grpcServer.GracefulStop()
		cancel()
	}()

	advance := func(ctx context.Context, msg mq.SagaAdvanceMsg) error {
		logger.Debug("advance received", golog.F("saga_run_id", msg.SagaRunID))
		if err := coord.Advance(ctx, msg.SagaRunID); err != nil {
			logger.Error(err, "advance failed", golog.F("saga_run_id", msg.SagaRunID))
			return fmt.Errorf("advance: %w", err)
		}
		return nil
	}
	if err := mq.ConsumeSagaAdvance(ctx, conn, advance); err != nil && err != context.Canceled {
		logger.Fatal(err, "consume")
	}
}
