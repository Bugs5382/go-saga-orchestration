package saga

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
	"sync"

	"github.com/rs/zerolog"

	"github.com/Bugs5382/go-saga-orchestration/engine"
)

// InProcessPublisher satisfies engine.Publisher (and verbs.ActionDispatchPublisher)
// for embedded use with no message broker. PublishSagaAdvance runs the
// coordinator's Advance in a background goroutine bound to the Saga's context and
// tracked on its WaitGroup, so Saga.Shutdown can cancel and drain in-flight work.
// Action dispatch is unsupported in-process (use a worker / the service mode).
type InProcessPublisher struct {
	coord *engine.Coordinator
	ctx   context.Context // the Saga's derived context; cancelled by Saga.Shutdown
	wg    *sync.WaitGroup // tracks in-flight background advances for draining
	log   zerolog.Logger
}

// PublishSagaAdvance advances the run in a background goroutine.
//
// Semantics for embedders: this is only reached by workflows that spawn child
// runs (parallel / foreach / spawn_saga); linear workflows advance synchronously
// inside Start and never use the publisher. The advance runs on the Saga's
// context (derived from Options.Context) and is registered on the Saga's
// WaitGroup, so Saga.Shutdown cancels it (Advance stops between steps) and waits
// for it to drain. Errors cannot be returned to the caller; set Options.Logger
// to observe them.
func (p *InProcessPublisher) PublishSagaAdvance(_ context.Context, runID string) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.coord.Advance(p.ctx, runID); err != nil {
			p.log.Error().Err(err).Str("run_id", runID).Msg("saga: in-process background advance failed")
		}
	}()
	return nil
}

func (p *InProcessPublisher) PublishActionDispatch(_ context.Context, _ string, _ []byte) error {
	return fmt.Errorf("saga: in-process publisher cannot dispatch actions; run a worker or use the service mode")
}
