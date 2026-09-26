// Command go-saga-orchestration-api is the REST surface for go-saga-orchestration.
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
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	golog "github.com/Bugs5382/go-log"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/engine"
	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	"github.com/Bugs5382/go-saga-orchestration/internal/logging"
	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/internal/storefactory"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/secrets"
	"github.com/Bugs5382/go-saga-orchestration/store/postgres"
)

var (
	Version = "dev"
	GitSHA  = "unknown"
)

// mqEventEmitter satisfies verbs.EventEmitter by publishing to RabbitMQ so the
// coordinator's emit_event steps reach subscribed pods.
type mqEventEmitter struct {
	pub *mq.Publisher
}

func (e *mqEventEmitter) EmitEvent(ctx context.Context, topic string, headers map[string]string, payload map[string]any) error {
	return e.pub.PublishEvent(ctx, topic, headers, payload)
}

func main() {
	// LOG_LEVEL (default info) and LOG_FORMAT (json, console or both) control
	// this logger; see internal/logging.
	logger := logging.New("go-saga-orchestration-api")
	cfg := config.Load()
	logger.Info("starting go-saga-orchestration-api", golog.F("version", Version), golog.F("sha", GitSHA))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	pubCh, err := conn.Channel()
	if err != nil {
		logger.Fatal(err, "rabbitmq channel")
	}
	if err := mq.DeclareTopology(pubCh); err != nil {
		logger.Fatal(err, "rabbitmq topology")
	}
	_ = pubCh.Close()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		logger.Fatal(err, "rabbitmq publisher")
	}
	defer func() { _ = pub.Close() }()

	// A coordinator backs the run-level cancel endpoint. It reuses the store
	// and publisher; Cancel only touches the store and re-evaluates a parent
	// join, so the action-dispatch opts are not wired here.
	coord := engine.NewCoordinator(st, pub, clock.SystemClock{}, secrets.NewMemory(map[string]string{}), licensing.StubAllowAll{}, pub, &mqEventEmitter{pub: pub})
	coord.SetLogger(logger)
	sagas := api.NewSagaHandler(st, pub).WithCanceller(coord)
	signals := api.NewSignalHandler(st, pub)
	userTasks := api.NewUserTaskHandler(st, pub)
	reg := api.NewRegistryHandler(st)
	actionResults := api.NewActionResultHandler(st, pub)
	rules := api.NewRulesHandler(st)
	triggers := api.NewTriggerHandler(st, licensing.StubAllowAll{}, clock.SystemClock{})
	var pgPool *pgxpool.Pool
	if ps, ok := st.(*postgres.Store); ok {
		pgPool = ps.Pool()
	}
	streamH := api.NewSagaStreamHandler(st, pgPool)
	workflows := api.NewWorkflowHandler(st)
	router := api.NewRouter(st, sagas, signals, userTasks, reg, rules, triggers, streamH, workflows, actionResults)
	srv := &http.Server{Addr: ":" + cfg.API.Port, Handler: api.LoggingMiddleware(logger)(router)}

	go func() {
		logger.Info("http listening", golog.F("port", cfg.API.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal(err, "http")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	shutCtx, c := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer c()
	_ = srv.Shutdown(shutCtx)
}
