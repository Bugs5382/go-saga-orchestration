// Package api — WebSocket stream handler for the run inspector.
//
// Auth is intentionally not enforced here; real authentication middleware
// should be added before production deployment. See the route comment in
// router.go.
package api

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
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// SagaStreamHandler upgrades GET /api/v1/sagas/{run_id}/stream to a
// WebSocket. On connect: sends the current SagaRun snapshot, then tails
// audit.saga_run_events via Postgres LISTEN/NOTIFY (channel
// "saga_event_<run_id_no_dashes>"). One LISTEN per connection — the
// per-connection pgx Conn is acquired from a dedicated channel pool the
// handler holds.
//
// Auth is intentionally not enforced today; wire real auth middleware before
// exposing this endpoint in production.
type SagaStreamHandler struct {
	S       store.Store
	Pool    *pgxpool.Pool // for LISTEN — acquire a dedicated conn per stream
	Upgrade websocket.Upgrader
}

// NewSagaStreamHandler constructs the handler.
func NewSagaStreamHandler(s store.Store, pool *pgxpool.Pool) *SagaStreamHandler {
	return &SagaStreamHandler{
		S:    s,
		Pool: pool,
		Upgrade: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true }, // TODO: tighten in auth batch
		},
	}
}

// frame is the JSON envelope sent over the WebSocket.
type frame struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// writeJSON marshals v into a WebSocket text message.
func writeJSON(conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, b)
}

// quoteIdent quotes a Postgres identifier with double-quotes, escaping any
// embedded double-quote characters. This avoids importing lib/pq for a
// one-liner.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Stream handles GET /api/v1/sagas/{run_id}/stream.
func (h *SagaStreamHandler) Stream(w http.ResponseWriter, r *http.Request) {
	runIDStr := chi.URLParam(r, "run_id")
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		WriteError(w, http.StatusBadRequest, CodeBadRequest, "bad run_id")
		return
	}

	// 1. Validate the run exists. 404 before upgrading if not.
	run, err := h.S.GetRun(r.Context(), runID)
	if err != nil {
		var nf store.ErrNotFound
		if errors.As(err, &nf) {
			WriteError(w, http.StatusNotFound, CodeNotFound, "run not found")
			return
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("stream: get run failed")
		WriteError(w, http.StatusInternalServerError, CodeInternal, genericInternalMessage)
		return
	}

	// 2. Require a Postgres pool — non-postgres backends pass nil.
	if h.Pool == nil {
		http.Error(w, "live streaming requires the postgres store backend", http.StatusNotImplemented)
		return
	}

	// 3. Upgrade.
	conn, err := h.Upgrade.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade writes its own error
	}
	defer func() { _ = conn.Close() }()

	// 4. Send initial snapshot: the run + every existing event.
	if err := writeJSON(conn, frame{Type: "run", Data: run}); err != nil {
		return
	}
	events, _ := h.S.ListEventsByRun(r.Context(), runID)
	for _, e := range events {
		if err := writeJSON(conn, frame{Type: "event", Data: e}); err != nil {
			return
		}
	}

	// 5. LISTEN on per-run channel.
	pgConn, err := h.Pool.Acquire(r.Context())
	if err != nil {
		log.Warn().Err(err).Msg("stream: acquire pg conn")
		return
	}
	defer pgConn.Release()

	channel := "saga_event_" + strings.ReplaceAll(runID.String(), "-", "")
	if _, err := pgConn.Exec(r.Context(), "LISTEN "+quoteIdent(channel)); err != nil {
		log.Warn().Err(err).Str("channel", channel).Msg("stream: LISTEN failed")
		return
	}

	// 6. Loop: read NOTIFY, fetch event by ID, send to WS.
	for {
		notify, err := pgConn.Conn().WaitForNotification(r.Context())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn().Err(err).Msg("stream: wait notify")
			return
		}
		eventID, err := uuid.Parse(notify.Payload)
		if err != nil {
			continue
		}
		evt, err := h.S.GetEventByID(r.Context(), eventID)
		if err != nil {
			continue
		}
		if err := writeJSON(conn, frame{Type: "event", Data: evt}); err != nil {
			return
		}
	}
}
