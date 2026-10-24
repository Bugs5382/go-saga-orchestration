// Package grpc wires the engine-side gRPC server. Workers connect via
// ExecuteStep streams; the server bridges WorkerEvent messages to the
// engine's store hooks (CompleteAction / FailAction) and publishes
// saga.advance to wake paused-on-action sagas.
package grpc

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
	golog "github.com/Bugs5382/go-log"
	"io"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
	"github.com/Bugs5382/go-saga-orchestration/sagalog"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// AdvancePublisher is the minimum surface Server needs to wake sagas
// after a worker completes/fails an action.
type AdvancePublisher interface {
	PublishSagaAdvance(ctx context.Context, runID string) error
}

// Server wires WorkerLiveness.ExecuteStep to the engine's store hooks.
type Server struct {
	pb.UnimplementedWorkerLivenessServer
	S         store.Store
	Publisher AdvancePublisher
	Logger    golog.Logger // optional; nil = silent unless the stream context carries a logger
}

// Register attaches Server to a gRPC server with no logger.
func Register(grpcServer *grpc.Server, s store.Store, pub AdvancePublisher) {
	RegisterWithLogger(grpcServer, s, pub, nil)
}

// RegisterWithLogger attaches Server to a gRPC server, logging to l. A nil l
// is silent unless a stream's context carries a logger.
func RegisterWithLogger(grpcServer *grpc.Server, s store.Store, pub AdvancePublisher, l golog.Logger) {
	pb.RegisterWorkerLivenessServer(grpcServer, &Server{S: s, Publisher: pub, Logger: l})
}

// ExecuteStep is the bidi RPC workers use to report progress on a
// dispatched action. Frame protocol:
//  1. Worker sends StartJob{run_id, step_id, attempt}.
//  2. Server replies Acknowledged{}.
//  3. Worker streams Heartbeat{} messages while executing (optional).
//  4. Worker sends Complete{result_json} (success) OR Error{code, message, retryable} (failure).
//  5. Server processes the terminal message and ends the stream.
//
// Errors at the gRPC layer (network drop, decode failures) leave the
// saga in awaiting-action state — RabbitMQ redelivery + the worker's
// idempotency wrapper handle retries.
func (s *Server) ExecuteStep(stream pb.WorkerLiveness_ExecuteStepServer) error {
	ctx := stream.Context()
	lg := sagalog.For(ctx, s.Logger)
	var (
		runIDStr string
		stepID   string
		attempt  int32
		started  bool
	)
	lg.Debug("grpc: worker stream opened")
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			lg.Debug("grpc: worker stream closed", golog.F("run_id", runIDStr), golog.F("step_id", stepID))
			return nil
		}
		if err != nil {
			lg.Error(err, "grpc: recv", golog.F("run_id", runIDStr), golog.F("step_id", stepID))
			return err
		}
		switch e := ev.Event.(type) {
		case *pb.WorkerEvent_Start:
			if started {
				err := errors.New("grpc: duplicate StartJob")
				lg.Warn("grpc: duplicate StartJob; closing stream", golog.F("run_id", runIDStr), golog.F("step_id", stepID))
				return err
			}
			started = true
			runIDStr = e.Start.RunId
			stepID = e.Start.StepId
			attempt = e.Start.Attempt
			lg = lg.With(golog.F("run_id", runIDStr), golog.F("step_id", stepID), golog.F("attempt", attempt))
			lg.Debug("grpc: worker started job")
			// Acknowledge.
			if err := stream.Send(&pb.EngineEvent{
				Event: &pb.EngineEvent_Ack{Ack: &pb.Acknowledged{}},
			}); err != nil {
				lg.Error(err, "grpc: send ack failed")
				return err
			}
		case *pb.WorkerEvent_Heartbeat:
			sagalog.Trace(lg, "grpc: heartbeat", golog.F("progress_pct", e.Heartbeat.ProgressPct))
			// No state change; could be used for long-action timeout extension.
		case *pb.WorkerEvent_Complete:
			if !started {
				lg.Warn("grpc: complete without start; closing stream")
				return errors.New("grpc: complete without start")
			}
			return s.handleComplete(ctx, lg, runIDStr, int(attempt), e.Complete)
		case *pb.WorkerEvent_Error:
			if !started {
				lg.Warn("grpc: error without start; closing stream")
				return errors.New("grpc: error without start")
			}
			return s.handleError(ctx, lg, runIDStr, int(attempt), e.Error)
		}
	}
}

func (s *Server) handleComplete(ctx context.Context, lg golog.Logger, runIDStr string, attempt int, c *pb.Complete) error {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		lg.Error(err, "grpc: complete: invalid run id")
		return err
	}
	var result map[string]any
	if len(c.ResultJson) > 0 {
		if err := json.Unmarshal(c.ResultJson, &result); err != nil {
			// The raw result is kept on the run but never logged.
			lg.Warn("grpc: result_json decode failed; storing raw result", golog.F("error", err.Error()),
				golog.F("result_bytes", len(c.ResultJson)))
			result = map[string]any{"_raw_result": string(c.ResultJson)}
		}
	}
	if err := s.S.CompleteAction(ctx, runID, attempt, result); err != nil {
		lg.Error(err, "grpc: complete action")
		return err
	}
	lg.Info("grpc: action completed by worker", golog.F("result_keys", len(result)))
	if s.Publisher != nil {
		if err := s.Publisher.PublishSagaAdvance(ctx, runIDStr); err != nil {
			lg.Error(err, "grpc: publish saga.advance")
		}
	}
	return nil
}

func (s *Server) handleError(ctx context.Context, lg golog.Logger, runIDStr string, attempt int, e *pb.Error) error {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		lg.Error(err, "grpc: error report: invalid run id")
		return err
	}
	// The worker's message is stored on the run; only the code is logged.
	lg.Warn("grpc: worker reported action failure", golog.F("code", e.Code), golog.F("retryable", e.Retryable))
	if err := s.S.FailAction(ctx, runID, attempt, e.Code, e.Message, e.Retryable); err != nil {
		lg.Error(err, "grpc: fail action")
		return err
	}
	// FailAction transitioned the run to failed; no advance needed.
	return nil
}
