DROP INDEX IF EXISTS runtime.idx_saga_runs_parent;
ALTER TABLE runtime.saga_runs
  DROP COLUMN IF EXISTS try_catch_stack,
  DROP COLUMN IF EXISTS parent_branch_id,
  DROP COLUMN IF EXISTS parent_step_id;
