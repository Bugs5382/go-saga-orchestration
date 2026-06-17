DROP INDEX IF EXISTS runtime.idx_saga_runs_awaited_action;
ALTER TABLE runtime.saga_runs
  DROP COLUMN IF EXISTS current_attempt,
  DROP COLUMN IF EXISTS awaited_action_dispatch;
