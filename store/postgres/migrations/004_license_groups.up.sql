ALTER TABLE definitions.action_registry
  ADD COLUMN license_group TEXT NOT NULL DEFAULT 'wf.worker_actions_basic';

ALTER TABLE runtime.saga_runs
  ADD COLUMN feature_overrides JSONB NULL;

CREATE INDEX idx_action_registry_license ON definitions.action_registry (license_group);
