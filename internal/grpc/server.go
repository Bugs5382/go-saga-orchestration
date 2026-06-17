// Package grpc wires the engine-side gRPC server. Workers connect via
// ExecuteStep streams; the server bridges WorkerEvent messages to the
// engine's store hooks (CompleteAction / FailAction) and publishes
// saga.advance to wake paused-on-action sagas.
package grpc

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
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	pb "github.com/Bugs5382/go-saga-orchestration/proto/livenesspb"
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
}

// Register attaches Server to a gRPC server.
func Register(grpcServer *grpc.Server, s store.Store, pub AdvancePublisher) {
	pb.RegisterWorkerLivenessServer(grpcServer, &Server{S: s, Publisher: pub})
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
	var (
		runIDStr string
		stepID   string
		attempt  int32
		started  bool
	)
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Error().Err(err).Msg("grpc: recv")
			return err
		}
		switch e := ev.Event.(type) {
		case *pb.WorkerEvent_Start:
			if started {
				return errors.New("grpc: duplicate StartJob")
			}
			started = true
			runIDStr = e.Start.RunId
			stepID = e.Start.StepId
			attempt = e.Start.Attempt
			// Acknowledge.
			if err := stream.Send(&pb.EngineEvent{
				Event: &pb.EngineEvent_Ack{Ack: &pb.Acknowledged{}},
			}); err != nil {
				return err
			}
		case *pb.WorkerEvent_Heartbeat:
			log.Debug().Str("run_id", runIDStr).Str("step_id", stepID).
				Int32("progress_pct", e.Heartbeat.ProgressPct).Msg("grpc: heartbeat")
			// No state change; could be used for long-action timeout extension.
		case *pb.WorkerEvent_Complete:
			if !started {
				return errors.New("grpc: complete without start")
			}
			return s.handleComplete(ctx, runIDStr, stepID, int(attempt), e.Complete)
		case *pb.WorkerEvent_Error:
			if !started {
				return errors.New("grpc: error without start")
			}
			return s.handleError(ctx, runIDStr, stepID, int(attempt), e.Error)
		}
	}
}

func (s *Server) handleComplete(ctx context.Context, runIDStr, stepID string, attempt int, c *pb.Complete) error {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		return err
	}
	var result map[string]any
	if len(c.ResultJson) > 0 {
		if err := json.Unmarshal(c.ResultJson, &result); err != nil {
			log.Warn().Err(err).Msg("grpc: result_json decode")
			result = map[string]any{"_raw_result": string(c.ResultJson)}
		}
	}
	if err := s.S.CompleteAction(ctx, runID, attempt, result); err != nil {
		log.Error().Err(err).Msg("grpc: complete action")
		return err
	}
	if s.Publisher != nil {
		if err := s.Publisher.PublishSagaAdvance(ctx, runIDStr); err != nil {
			log.Error().Err(err).Msg("grpc: publish saga.advance")
		}
	}
	return nil
}

func (s *Server) handleError(ctx context.Context, runIDStr, stepID string, attempt int, e *pb.Error) error {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		return err
	}
	if err := s.S.FailAction(ctx, runID, attempt, e.Code, e.Message, e.Retryable); err != nil {
		log.Error().Err(err).Msg("grpc: fail action")
		return err
	}
	// FailAction transitioned the run to failed; no advance needed.
	return nil
}
