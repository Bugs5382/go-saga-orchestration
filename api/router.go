package api

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
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// NewRouter builds the chi router. Saga routes attach via SagaHandler;
// registry routes attach via RegistryHandler; rule evaluation via RulesHandler;
// trigger CRUD via TriggerHandler; live run inspector via SagaStreamHandler;
// workflow stats via WorkflowHandler.
func NewRouter(_ store.Store, sagas *SagaHandler, signals *SignalHandler, userTasks *UserTaskHandler, registryHandler *RegistryHandler, rulesHandler *RulesHandler, triggersHandler *TriggerHandler, streamHandler *SagaStreamHandler, workflows *WorkflowHandler, actionResults *ActionResultHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/health/live", HealthLive)
	r.Get("/health/ready", HealthReady)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/sagas", sagas.List)
		r.Post("/sagas/start", sagas.Start)
		r.Get("/sagas/{id}", sagas.Get)
		r.Post("/sagas/{run_id}/signal/{name}", signals.Post)
		r.Post("/sagas/{run_id}/user_task/{task_id}/submit", userTasks.Submit)
		r.Post("/sagas/{run_id}/actions/{step_id}/result", actionResults.Post)
		r.Get("/sagas/{run_id}/stream", streamHandler.Stream)

		r.Post("/registry/register", registryHandler.Register)
		r.Get("/registry/actions", registryHandler.List)

		r.Post("/rules/{rule_id}/evaluate", rulesHandler.Evaluate)

		r.Post("/triggers", triggersHandler.Create)
		r.Get("/triggers", triggersHandler.List)
		r.Get("/triggers/{id}", triggersHandler.Get)
		r.Delete("/triggers/{id}", triggersHandler.Delete)

		r.Get("/workflows/{wf_id}/stats", workflows.Stats)
	})

	return r
}
