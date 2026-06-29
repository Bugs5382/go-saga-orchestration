ALTER TABLE runtime.saga_runs
  DROP COLUMN IF EXISTS last_error;
