DROP INDEX IF EXISTS definitions.idx_action_registry_license;
ALTER TABLE runtime.saga_runs DROP COLUMN IF EXISTS feature_overrides;
ALTER TABLE definitions.action_registry DROP COLUMN IF EXISTS license_group;
