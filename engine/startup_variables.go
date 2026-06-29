package engine

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

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/Bugs5382/go-saga-orchestration/store"
)

// StartupVariableProvider supplies per-saga "magic" variables (leading-underscore
// names by convention) that are merged into a run's Variables at start. The
// engine ships no built-in providers; integrators register their own. Best-effort:
// a provider error is logged and skipped so a config glitch never strands a start.
type StartupVariableProvider interface {
	StartupVariables(ctx context.Context, tenantID *uuid.UUID) (map[string]any, error)
}

// InjectStartupVariables runs each provider for the given tenant, merges the
// returned maps, and writes them onto the run via UpdateRunVariables. With no
// providers it is a no-op (no write). Provider errors are logged and skipped.
func InjectStartupVariables(
	ctx context.Context,
	s store.Store,
	runID uuid.UUID,
	tenantID *uuid.UUID,
	log zerolog.Logger,
	providers ...StartupVariableProvider,
) {
	if len(providers) == 0 {
		return
	}
	merge := map[string]any{}
	for _, p := range providers {
		vars, err := p.StartupVariables(ctx, tenantID)
		if err != nil {
			log.Warn().Err(err).Str("run_id", runID.String()).Msg("startup variable provider failed; skipping")
			continue
		}
		for k, v := range vars {
			merge[k] = v
		}
	}
	if len(merge) == 0 {
		return
	}
	if err := s.UpdateRunVariables(ctx, runID, merge); err != nil {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("InjectStartupVariables: write failed")
	}
}
