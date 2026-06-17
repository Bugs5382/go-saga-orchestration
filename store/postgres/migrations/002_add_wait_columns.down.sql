DROP INDEX IF EXISTS runtime.idx_saga_runs_awaited_topic;
DROP INDEX IF EXISTS runtime.idx_saga_runs_wakeup_due;
ALTER TABLE runtime.saga_runs
  DROP COLUMN IF EXISTS awaited_event_headers,
  DROP COLUMN IF EXISTS awaited_event_topic,
  DROP COLUMN IF EXISTS awaited_signal,
  DROP COLUMN IF EXISTS wakeup_at;
