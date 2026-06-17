ALTER TABLE runtime.saga_runs
  ADD COLUMN parent_step_id   TEXT NULL,
  ADD COLUMN parent_branch_id TEXT NULL,
  ADD COLUMN try_catch_stack  JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX idx_saga_runs_parent ON runtime.saga_runs (parent_run_id)
  WHERE parent_run_id IS NOT NULL;
