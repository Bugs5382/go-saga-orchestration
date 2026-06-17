ALTER TABLE runtime.saga_runs
  ADD COLUMN awaited_action_dispatch TEXT NULL,
  ADD COLUMN current_attempt INT NOT NULL DEFAULT 0;

CREATE INDEX idx_saga_runs_awaited_action ON runtime.saga_runs (awaited_action_dispatch)
  WHERE state = 'paused' AND awaited_action_dispatch IS NOT NULL;
