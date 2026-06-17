CREATE TABLE runtime.saga_triggers (
    id              UUID PRIMARY KEY,
    trigger_type    TEXT NOT NULL,
    workflow_id     TEXT NOT NULL,
    version         INT  NOT NULL,
    config          JSONB NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    tenant_id       UUID NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT NOT NULL
);
CREATE INDEX idx_saga_triggers_type_enabled ON runtime.saga_triggers (trigger_type, enabled);
CREATE INDEX idx_saga_triggers_tenant ON runtime.saga_triggers (tenant_id) WHERE tenant_id IS NOT NULL;
