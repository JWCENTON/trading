BEGIN;

CREATE TABLE IF NOT EXISTS final_decision_producer_audit_v1 (
    audit_event_id UUID PRIMARY KEY,
    finalized_event_id UUID NOT NULL,
    attempt_id UUID,
    attempt_ordinal INTEGER CHECK (attempt_ordinal IS NULL OR attempt_ordinal > 0),
    event_type TEXT NOT NULL CHECK (event_type IN (
      'FINALIZED',
      'PRODUCER_ATTEMPTED',
      'ACCEPTED',
      'SKIPPED_DISABLED',
      'SKIPPED_KILL_SWITCH',
      'VALIDATION_REJECTED',
      'SERIALIZATION_FAILED',
      'OUTBOX_WRITE_FAILED',
      'IDEMPOTENT_EXISTING',
      'IDEMPOTENCY_CONFLICT'
    )),
    decision_key TEXT NOT NULL CHECK (btrim(decision_key) <> ''),
    deployment_id TEXT NOT NULL CHECK (btrim(deployment_id) <> ''),
    environment TEXT NOT NULL CHECK (btrim(environment) <> ''),
    source_service TEXT NOT NULL CHECK (btrim(source_service) <> ''),
    source_instance TEXT NOT NULL CHECK (btrim(source_instance) <> ''),
    strategy TEXT NOT NULL CHECK (btrim(strategy) <> ''),
    symbol TEXT NOT NULL CHECK (btrim(symbol) <> ''),
    interval TEXT NOT NULL CHECK (btrim(interval) <> ''),
    original_decision_type TEXT NOT NULL CHECK (original_decision_type IN (
      'TRADE_EXECUTED',
      'NO_TRADE',
      'SIGNAL_REJECTED',
      'ENTRY_BLOCKED',
      'ENTRY_SUPPRESSED',
      'PAPER_SIMULATION',
      'SYSTEM_NOT_EVALUATED',
      'TECHNICAL_FAILURE',
      'ACTION_SUPPRESSED'
    )),
    decision_kind TEXT NOT NULL CHECK (decision_kind IN (
      'TRADE','EXIT','HOLD','NO_TRADE','BLOCKED_BY_EXISTING_LOGIC'
    )),
    action TEXT NOT NULL,
    direction TEXT,
    decision_created_at TIMESTAMPTZ NOT NULL,
    finalized_at TIMESTAMPTZ NOT NULL,
    producer_attempted_at TIMESTAMPTZ,
    producer_status TEXT CHECK (producer_status IN (
      'ATTEMPTED',
      'ACCEPTED',
      'SKIPPED_DISABLED',
      'SKIPPED_KILL_SWITCH',
      'VALIDATION_REJECTED',
      'SERIALIZATION_FAILED',
      'OUTBOX_WRITE_FAILED',
      'IDEMPOTENT_EXISTING',
      'IDEMPOTENCY_CONFLICT'
    )),
    skip_reason TEXT,
    error_class TEXT CHECK (error_class IS NULL OR length(error_class) <= 128),
    semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
    outbox_event_id UUID REFERENCES causal_decision_observation_outbox_v1(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_version TEXT NOT NULL DEFAULT 'FINAL_DECISION_PRODUCER_AUDIT_V1',
    event_digest TEXT NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
    CHECK (finalized_at >= decision_created_at),
    CHECK (
      (event_type = 'FINALIZED'
       AND attempt_id IS NULL
       AND attempt_ordinal IS NULL
       AND producer_attempted_at IS NULL
       AND producer_status IS NULL
       AND skip_reason IS NULL
       AND error_class IS NULL
       AND outbox_event_id IS NULL)
      OR
      (event_type IN ('SKIPPED_DISABLED','SKIPPED_KILL_SWITCH')
       AND attempt_id IS NULL
       AND attempt_ordinal IS NULL
       AND producer_attempted_at IS NULL
       AND producer_status = event_type
       AND skip_reason = event_type
       AND error_class IS NULL
       AND outbox_event_id IS NULL)
      OR
      (event_type = 'PRODUCER_ATTEMPTED'
       AND attempt_id IS NOT NULL
       AND attempt_ordinal IS NOT NULL
       AND producer_attempted_at IS NOT NULL
       AND producer_status = 'ATTEMPTED'
       AND skip_reason IS NULL
       AND error_class IS NULL
       AND outbox_event_id IS NULL)
      OR
      (event_type IN ('ACCEPTED','IDEMPOTENT_EXISTING')
       AND attempt_id IS NOT NULL
       AND attempt_ordinal IS NOT NULL
       AND producer_attempted_at IS NOT NULL
       AND producer_status = event_type
       AND skip_reason IS NULL
       AND error_class IS NULL
       AND outbox_event_id IS NOT NULL)
      OR
      (event_type = 'IDEMPOTENCY_CONFLICT'
       AND attempt_id IS NOT NULL
       AND attempt_ordinal IS NOT NULL
       AND producer_attempted_at IS NOT NULL
       AND producer_status = event_type
       AND skip_reason IS NULL
       AND error_class = 'IDEMPOTENCY_CONFLICT'
       AND outbox_event_id IS NOT NULL)
      OR
      (event_type IN ('VALIDATION_REJECTED','SERIALIZATION_FAILED','OUTBOX_WRITE_FAILED')
       AND attempt_id IS NOT NULL
       AND attempt_ordinal IS NOT NULL
       AND producer_attempted_at IS NOT NULL
       AND producer_status = event_type
       AND skip_reason IS NULL
       AND error_class IS NOT NULL
       AND outbox_event_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_final_decision_producer_audit_finalized_v1
  ON final_decision_producer_audit_v1(deployment_id,decision_key)
  WHERE event_type='FINALIZED';

CREATE UNIQUE INDEX IF NOT EXISTS ux_final_decision_producer_audit_attempt_event_v1
  ON final_decision_producer_audit_v1(attempt_id,event_type)
  WHERE attempt_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_created_v1
  ON final_decision_producer_audit_v1(created_at);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_decision_v1
  ON final_decision_producer_audit_v1(deployment_id,decision_key);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_deployment_time_v1
  ON final_decision_producer_audit_v1(deployment_id,finalized_at);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_event_time_v1
  ON final_decision_producer_audit_v1(event_type,created_at);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_source_time_v1
  ON final_decision_producer_audit_v1(source_service,created_at);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_slot_time_v1
  ON final_decision_producer_audit_v1(strategy,symbol,interval,created_at);
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_status_time_v1
  ON final_decision_producer_audit_v1(producer_status,created_at)
  WHERE producer_status IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_final_decision_producer_audit_outbox_v1
  ON final_decision_producer_audit_v1(outbox_event_id)
  WHERE outbox_event_id IS NOT NULL;

CREATE OR REPLACE FUNCTION prevent_final_decision_producer_audit_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'FinalDecision producer audit ledger is immutable and append-only';
END;
$$;

CREATE OR REPLACE TRIGGER final_decision_producer_audit_immutable_v1
BEFORE UPDATE OR DELETE ON final_decision_producer_audit_v1
FOR EACH ROW EXECUTE FUNCTION prevent_final_decision_producer_audit_mutation_v1();

COMMENT ON TABLE final_decision_producer_audit_v1 IS
  'Append-only denominator and producer lifecycle evidence for immutable FinalDecision values.';
COMMENT ON COLUMN final_decision_producer_audit_v1.original_decision_type IS
  'Original FinalDecision type; preserves SYSTEM_NOT_EVALUATED separately from mapped decision_kind.';
COMMENT ON COLUMN final_decision_producer_audit_v1.event_digest IS
  'Bounded canonical lifecycle content digest used to detect conflicting idempotent inserts.';

COMMIT;
