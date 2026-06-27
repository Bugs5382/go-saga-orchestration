// Command go-saga-orchestration-api is the REST surface for go-saga-orchestration.
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
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/api"
	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	"github.com/Bugs5382/go-saga-orchestration/internal/logging"
	"github.com/Bugs5382/go-saga-orchestration/internal/mq"
	"github.com/Bugs5382/go-saga-orchestration/internal/storefactory"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/store/postgres"
)

var (
	Version = "dev"
	GitSHA  = "unknown"
)

func main() {
	logging.Init(false)
	cfg := config.Load()
	log.Info().Str("version", Version).Str("sha", GitSHA).Msg("starting go-saga-orchestration-api")

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
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq channel")
	}
	if err := mq.DeclareTopology(pubCh); err != nil {
		log.Fatal().Err(err).Msg("rabbitmq topology")
	}
	_ = pubCh.Close()

	pub, err := mq.NewPublisher(conn)
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq publisher")
	}
	defer func() { _ = pub.Close() }()

	sagas := api.NewSagaHandler(st, pub)
	signals := api.NewSignalHandler(st, pub)
	userTasks := api.NewUserTaskHandler(st, pub)
	reg := api.NewRegistryHandler(st)
	rules := api.NewRulesHandler(st)
	triggers := api.NewTriggerHandler(st, licensing.StubAllowAll{}, clock.SystemClock{})
	var pgPool *pgxpool.Pool
	if ps, ok := st.(*postgres.Store); ok {
		pgPool = ps.Pool()
	}
	streamH := api.NewSagaStreamHandler(st, pgPool)
	workflows := api.NewWorkflowHandler(st)
	router := api.NewRouter(st, sagas, signals, userTasks, reg, rules, triggers, streamH, workflows)
	srv := &http.Server{Addr: ":" + cfg.API.Port, Handler: router}

	go func() {
		log.Info().Str("port", cfg.API.Port).Msg("http listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("http")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info().Msg("shutting down")
	shutCtx, c := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer c()
	_ = srv.Shutdown(shutCtx)
}
