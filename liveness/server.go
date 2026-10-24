// Package liveness serves the WorkerLiveness gRPC service (saga.v1) that
// action workers use to report on the steps the engine dispatched to them.
//
// The stream protocol lives in Server; what a report does lives behind the
// Reporter interface. StoreReporter is the default: it checks the run is
// waiting for the reported step, records the outcome in a store.Store, wakes
// the run on success, and compensates on failure. An application that embeds
// the engine and keeps its own bookkeeping (a verb registry elsewhere, its own
// audit or gating) can pass its own Reporter, usually wrapping StoreReporter.
package liveness

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
	"io"

	golog "github.com/Bugs5382/go-log"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
)

var (
	// ErrRunNotFound is returned by a Reporter for a run it doesn't know. The
	// server answers it with codes.NotFound.
	ErrRunNotFound = errors.New("liveness: run not found")
	// ErrStepNotAwaited is returned by a Reporter when the run isn't waiting
	// for the reported step and attempt (a stale, duplicate or misrouted
	// report). The server answers it with codes.FailedPrecondition; a worker
	// should acknowledge the dispatch and drop it.
	ErrStepNotAwaited = errors.New("liveness: step not awaited")
)

// Reporter takes workers' reports on the steps they were sent.
//
// CheckStep runs when a worker starts a job, before it is acknowledged.
// CompleteStep and FailStep run on the terminal event; implementations
// should check again, since time passes between start and finish. Return an
// error wrapping ErrRunNotFound or ErrStepNotAwaited to refuse a report; any
// other error is answered with codes.Internal and its text is not sent to the
// worker.
type Reporter interface {
	CheckStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int) error
	CompleteStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int, result map[string]any) error
	FailStep(ctx context.Context, runID uuid.UUID, stepID string, attempt int, code, message string, retryable bool) error
}

// Server implements pb.WorkerLivenessServer over a Reporter.
type Server struct {
	pb.UnimplementedWorkerLivenessServer
	reporter Reporter
	log      golog.Logger
}

// NewServer returns a Server that hands reports to r and logs to l. A nil l
// is silent unless a stream's context carries a logger (sagalog.NewContext).
func NewServer(r Reporter, l golog.Logger) *Server {
	return &Server{reporter: r, log: l}
}

// Register attaches srv to a gRPC server.
func Register(gs grpc.ServiceRegistrar, srv *Server) {
	pb.RegisterWorkerLivenessServer(gs, srv)
}

// statusOf maps a Reporter error to the gRPC status the worker receives.
func statusOf(err error) error {
	switch {
	case errors.Is(err, ErrRunNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrStepNotAwaited):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, "liveness: the report could not be recorded")
	}
}

// ExecuteStep is the bidirectional RPC workers use to report on a dispatched
// action. Frame protocol:
//  1. Worker sends StartJob{run_id, step_id, attempt}.
//  2. Server checks the run is waiting for that step and attempt, then
//     replies Acknowledged{}; otherwise it ends the stream with NotFound or
//     FailedPrecondition.
//  3. Worker streams Heartbeat{} messages while executing (optional).
//  4. Worker sends Complete{result_json} (success) OR Error{code, message,
//     retryable} (failure).
//  5. Server processes the terminal message and ends the stream.
//
// Transport errors (network drop, EOF before a terminal message) leave the run
// waiting; redelivery of the dispatch and the worker's idempotency handle the
// retry.
func (s *Server) ExecuteStep(stream pb.WorkerLiveness_ExecuteStepServer) error {
	ctx := stream.Context()
	lg := sagalog.For(ctx, s.log)
	var (
		runID   uuid.UUID
		stepID  string
		attempt int
		started bool
	)
	lg.Debug("liveness: worker stream opened")
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			lg.Debug("liveness: worker stream closed")
			return nil
		}
		if err != nil {
			lg.Error(err, "liveness: recv failed")
			return err
		}
		switch e := ev.Event.(type) {
		case *pb.WorkerEvent_Start:
			if started {
				lg.Warn("liveness: duplicate StartJob; closing stream")
				return status.Error(codes.InvalidArgument, "liveness: duplicate StartJob")
			}
			id, err := uuid.Parse(e.Start.GetRunId())
			if err != nil {
				lg.Warn("liveness: StartJob run id is not a uuid")
				return status.Error(codes.InvalidArgument, "liveness: run_id is not a uuid")
			}
			runID, stepID, attempt, started = id, e.Start.GetStepId(), int(e.Start.GetAttempt()), true
			lg = lg.With(golog.F("run_id", runID.String()), golog.F("step_id", stepID), golog.F("attempt", attempt))
			if err := s.reporter.CheckStep(ctx, runID, stepID, attempt); err != nil {
				lg.Warn("liveness: refused a job the run is not waiting for", golog.F("error", err.Error()))
				return statusOf(err)
			}
			lg.Debug("liveness: worker started job")
			if err := stream.Send(&pb.EngineEvent{Event: &pb.EngineEvent_Ack{Ack: &pb.Acknowledged{}}}); err != nil {
				lg.Error(err, "liveness: send ack failed")
				return err
			}
		case *pb.WorkerEvent_Heartbeat:
			sagalog.Trace(lg, "liveness: heartbeat", golog.F("progress_pct", e.Heartbeat.GetProgressPct()))
		case *pb.WorkerEvent_Complete:
			if !started {
				lg.Warn("liveness: Complete before StartJob; closing stream")
				return status.Error(codes.InvalidArgument, "liveness: Complete before StartJob")
			}
			return s.complete(ctx, lg, runID, stepID, attempt, e.Complete)
		case *pb.WorkerEvent_Error:
			if !started {
				lg.Warn("liveness: Error before StartJob; closing stream")
				return status.Error(codes.InvalidArgument, "liveness: Error before StartJob")
			}
			// The worker's message is passed on but never logged; only the code is.
			lg.Warn("liveness: worker reported action failure", golog.F("code", e.Error.GetCode()),
				golog.F("retryable", e.Error.GetRetryable()))
			if err := s.reporter.FailStep(ctx, runID, stepID, attempt, e.Error.GetCode(), e.Error.GetMessage(), e.Error.GetRetryable()); err != nil {
				lg.Warn("liveness: failure report refused", golog.F("error", err.Error()))
				return statusOf(err)
			}
			lg.Info("liveness: action failed by worker")
			return nil
		}
	}
}

func (s *Server) complete(ctx context.Context, lg golog.Logger, runID uuid.UUID, stepID string, attempt int, c *pb.Complete) error {
	var result map[string]any
	if b := c.GetResultJson(); len(b) > 0 {
		if err := json.Unmarshal(b, &result); err != nil {
			// The raw result is kept on the run but never logged.
			lg.Warn("liveness: result_json decode failed; storing raw result", golog.F("error", err.Error()),
				golog.F("result_bytes", len(b)))
			result = map[string]any{"_raw_result": string(b)}
		}
	}
	if err := s.reporter.CompleteStep(ctx, runID, stepID, attempt, result); err != nil {
		lg.Warn("liveness: completion refused", golog.F("error", err.Error()))
		return statusOf(err)
	}
	lg.Info("liveness: action completed by worker", golog.F("result_keys", len(result)))
	return nil
}
