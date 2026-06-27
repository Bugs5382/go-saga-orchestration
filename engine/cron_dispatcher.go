package engine

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
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Bugs5382/go-saga-orchestration/clock"
	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/licensing"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

// FeatureCronTriggers is the licensing feature flag that gates cron-triggered
// run starts. Tenants without this feature enabled are skipped.
const FeatureCronTriggers = "wf.cron_triggers"

// CronDispatcher polls the store for due cron triggers and starts a new saga
// run for each one it claims. It uses a compare-and-swap on next_fire_at to
// ensure exactly one pod fires per trigger tick. Run it as a goroutine from
// cmd/engine alongside the Timer.
type CronDispatcher struct {
	S         store.Store
	Publisher TimerPublisher
	Clock     clock.Clock
	Tick      time.Duration // default 1s
	Licensing licensing.Resolver
	Providers []StartupVariableProvider
	Logger    zerolog.Logger
}

// Run loops until ctx is cancelled. Each tick it calls fireDue to process all
// cron triggers whose next_fire_at is in the past.
func (d *CronDispatcher) Run(ctx context.Context) error {
	tick := d.Tick
	if tick == 0 {
		tick = time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.Clock.After(tick):
		}
		if err := d.fireDue(ctx); err != nil {
			log.Error().Err(err).Msg("cron dispatcher: fireDue error")
		}
	}
}

// fireDue is one polling pass. It lists due cron triggers, claims each one
// via CAS, checks the tenant's license, and starts a saga run for each
// trigger that this pod wins.
func (d *CronDispatcher) fireDue(ctx context.Context) error {
	now := d.Clock.Now()
	triggers, err := d.S.ListDueCronTriggers(ctx, now, 100)
	if err != nil {
		return err
	}

	logger := d.Logger
	if logger.GetLevel() == zerolog.Disabled {
		logger = log.Logger
	}

	var firstErr error
	for _, tr := range triggers {
		schedStr, _ := tr.Config["schedule"].(string)
		sched, parseErr := ParseSchedule(schedStr)
		if parseErr != nil {
			logger.Warn().Err(parseErr).
				Str("trigger_id", tr.ID.String()).
				Str("schedule", schedStr).
				Msg("cron dispatcher: invalid schedule expression, skipping")
			continue
		}

		next := sched.Next(now)

		won, claimErr := d.S.ClaimCronFire(ctx, tr.ID, *tr.NextFireAt, next)
		if claimErr != nil {
			logger.Error().Err(claimErr).Str("trigger_id", tr.ID.String()).Msg("cron dispatcher: claim error")
			if firstErr == nil {
				firstErr = claimErr
			}
			continue
		}
		if !won {
			continue
		}

		// License gate — do not start a run if the tenant is not entitled.
		enabled, _ := d.Licensing.IsFeatureEnabled(ctx, tr.TenantID, FeatureCronTriggers, nil)
		if !enabled {
			logger.Info().
				Str("trigger_id", tr.ID.String()).
				Msg("cron dispatcher: feature disabled for tenant, skipping run")
			continue
		}

		if err := d.startRun(ctx, tr, logger); err != nil {
			logger.Error().Err(err).Str("trigger_id", tr.ID.String()).Msg("cron dispatcher: start run failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// startRun executes the canonical saga-start sequence for a cron trigger:
// resolve definition → upsert → create run → inject startup variables → publish.
func (d *CronDispatcher) startRun(ctx context.Context, tr domain.SagaTrigger, logger zerolog.Logger) error {
	def, err := d.S.GetPublishedWorkflowByID(ctx, tr.WorkflowID, tr.TenantID)
	if err != nil {
		return err
	}

	trigEntrypoint, _ := tr.Config["entrypoint"].(string)
	startStep, err := def.ResolveEntry(trigEntrypoint)
	if err != nil {
		logger.Error().Err(err).
			Str("trigger_id", tr.ID.String()).
			Str("entrypoint", trigEntrypoint).
			Msg("cron dispatcher: invalid entrypoint, skipping")
		return err
	}

	defRowID, err := d.S.UpsertWorkflowDefinition(ctx, def)
	if err != nil {
		return err
	}

	var inputs map[string]any
	if inp, ok := tr.Config["input"].(map[string]any); ok {
		inputs = inp
	}

	run := domain.NewSagaRun(def.ID, defRowID, tr.TenantID, inputs)
	run.CurrentStep = startStep
	if err := d.S.CreateRun(ctx, run); err != nil {
		return err
	}

	InjectStartupVariables(ctx, d.S, run.ID, tr.TenantID, logger, d.Providers...)

	if d.Publisher != nil {
		if err := d.Publisher.PublishSagaAdvance(ctx, run.ID.String()); err != nil {
			return err
		}
	}

	return nil
}
