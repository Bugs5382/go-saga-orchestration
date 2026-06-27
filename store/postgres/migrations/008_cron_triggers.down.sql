DROP INDEX IF EXISTS runtime.idx_saga_triggers_cron_due;
ALTER TABLE runtime.saga_triggers
  DROP COLUMN IF EXISTS last_fired_at,
  DROP COLUMN IF EXISTS next_fire_at;
