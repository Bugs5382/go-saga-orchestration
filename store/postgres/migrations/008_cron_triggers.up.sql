ALTER TABLE runtime.saga_triggers
  ADD COLUMN next_fire_at  TIMESTAMPTZ,
  ADD COLUMN last_fired_at TIMESTAMPTZ;

CREATE INDEX idx_saga_triggers_cron_due
  ON runtime.saga_triggers (next_fire_at)
  WHERE trigger_type = 'cron' AND enabled;
