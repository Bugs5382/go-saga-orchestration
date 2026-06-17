CREATE SCHEMA IF NOT EXISTS definitions;
CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE definitions.workflow_definitions (
    id              UUID PRIMARY KEY,
    workflow_id     TEXT NOT NULL,
    version         INT  NOT NULL,
    tenant_id       UUID NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    spec            JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT NOT NULL,
    UNIQUE (workflow_id, version)
);
CREATE INDEX idx_wf_def_workflow_id ON definitions.workflow_definitions (workflow_id);
CREATE INDEX idx_wf_def_tenant ON definitions.workflow_definitions (tenant_id) WHERE tenant_id IS NOT NULL;

CREATE TABLE definitions.rule_definitions (
    id              UUID PRIMARY KEY,
    rule_id         TEXT NOT NULL,
    version         INT  NOT NULL,
    tenant_id       UUID NULL,
    name            TEXT NOT NULL,
    rule_type       TEXT NOT NULL,
    spec            JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT NOT NULL,
    UNIQUE (rule_id, version)
);
CREATE INDEX idx_rule_def_rule_id ON definitions.rule_definitions (rule_id);

CREATE TABLE definitions.action_registry (
    id                  UUID PRIMARY KEY,
    service             TEXT NOT NULL,
    action_name         TEXT NOT NULL,
    version             INT  NOT NULL,
    description         TEXT,
    category            TEXT,
    compensable         BOOLEAN NOT NULL,
    input_schema        JSONB NOT NULL,
    output_schema       JSONB NOT NULL,
    error_codes         TEXT[],
    default_retry       JSONB,
    default_timeout_ms  INT,
    deprecated          BOOLEAN NOT NULL DEFAULT FALSE,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    service_version     TEXT,
    dry_run_supported   BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (service, action_name, version)
);

CREATE TABLE runtime.saga_runs (
    id                       UUID PRIMARY KEY,
    workflow_id              TEXT NOT NULL,
    definition_id            UUID NOT NULL REFERENCES definitions.workflow_definitions(id),
    tenant_id                UUID NULL,
    state                    TEXT NOT NULL,
    current_step             TEXT,
    inputs                   JSONB NOT NULL,
    variables                JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at              TIMESTAMPTZ,
    requires_manual_review   BOOLEAN NOT NULL DEFAULT FALSE,
    trigger_id               UUID,
    parent_run_id            UUID,
    dry_run                  BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_saga_runs_state ON runtime.saga_runs (state);
CREATE INDEX idx_saga_runs_tenant ON runtime.saga_runs (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX idx_saga_runs_last_event ON runtime.saga_runs (last_event_at);

CREATE TABLE audit.saga_run_events (
    id           UUID PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES runtime.saga_runs(id),
    step_id      TEXT,
    attempt      INT NOT NULL DEFAULT 0,
    event_type   TEXT NOT NULL,
    from_state   TEXT,
    to_state     TEXT,
    actor        TEXT NOT NULL,
    metadata     JSONB,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, step_id, attempt, event_type)
);
CREATE INDEX idx_events_run ON audit.saga_run_events (run_id, recorded_at);

CREATE TABLE runtime.saga_dlq_items (
    id            UUID PRIMARY KEY,
    run_id        UUID,
    queue         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    reason        TEXT NOT NULL,
    parked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ,
    resolved_by   TEXT,
    resolution    TEXT
);

CREATE TABLE runtime.saga_trigger_fires (
    id               UUID PRIMARY KEY,
    trigger_id       UUID NOT NULL,
    workflow_id      TEXT NOT NULL,
    fired_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload          JSONB,
    resulting_run_id UUID,
    error            TEXT
);

CREATE TABLE runtime.saga_signals (
    id          UUID PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES runtime.saga_runs(id),
    signal_name TEXT NOT NULL,
    payload     JSONB,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at TIMESTAMPTZ
);

CREATE TABLE runtime.saga_user_tasks (
    id            UUID PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES runtime.saga_runs(id),
    step_id       TEXT NOT NULL,
    assignee      TEXT NOT NULL,
    due_at        TIMESTAMPTZ,
    form_schema   JSONB,
    submitted_at  TIMESTAMPTZ,
    submitted_by  TEXT,
    result        JSONB
);
