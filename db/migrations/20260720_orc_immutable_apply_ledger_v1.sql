BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS orc_apply_runs_v1 (
    run_id UUID NOT NULL,
    deployment_id TEXT NOT NULL CHECK (deployment_id IN
      ('local-live','local-paper','vps-live','vps-paper')),
    environment TEXT NOT NULL CHECK (environment IN ('trading_live','trading_paper')),
    deployment_identity TEXT NOT NULL,
    writer_service TEXT NOT NULL,
    writer_instance TEXT NOT NULL,
    writer_version TEXT,
    git_sha TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    apply_mode TEXT NOT NULL,
    integration_version TEXT NOT NULL,
    source_view TEXT NOT NULL,
    source_candidate_count INTEGER NOT NULL CHECK (source_candidate_count >= 0),
    candidate_universe_count INTEGER NOT NULL CHECK (candidate_universe_count >= 0),
    slot_decision_count INTEGER NOT NULL CHECK (slot_decision_count >= 0),
    source_excluded_count INTEGER NOT NULL CHECK (source_excluded_count >= 0),
    desired_on_count INTEGER NOT NULL CHECK (desired_on_count >= 0),
    previous_live_on_count INTEGER NOT NULL CHECK (previous_live_on_count >= 0),
    resulting_live_on_count INTEGER NOT NULL CHECK (resulting_live_on_count >= 0),
    touched_on_count INTEGER NOT NULL CHECK (touched_on_count >= 0),
    touched_off_count INTEGER NOT NULL CHECK (touched_off_count >= 0),
    unchanged_on_count INTEGER NOT NULL CHECK (unchanged_on_count >= 0),
    unchanged_off_count INTEGER NOT NULL CHECK (unchanged_off_count >= 0),
    picks_hash TEXT NOT NULL CHECK (picks_hash = '' OR picks_hash ~ '^[0-9a-f]{64}$'),
    transaction_outcome TEXT NOT NULL CHECK (transaction_outcome IN ('COMMITTED','ROLLED_BACK')),
    error_classification TEXT,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    schema_version TEXT NOT NULL DEFAULT 'ORC_APPLY_LEDGER_V1_1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_id, environment, run_id),
    CHECK (completed_at >= started_at),
    CHECK ((deployment_id LIKE '%-live' AND environment='trading_live') OR
           (deployment_id LIKE '%-paper' AND environment='trading_paper')),
    CHECK ((transaction_outcome='COMMITTED' AND error_classification IS NULL) OR
           (transaction_outcome='ROLLED_BACK' AND error_classification IS NOT NULL)),
    CHECK (source_excluded_count = source_candidate_count - candidate_universe_count),
    CHECK (transaction_outcome <> 'COMMITTED' OR
           candidate_universe_count = slot_decision_count)
);

CREATE TABLE IF NOT EXISTS orc_apply_slot_decisions_v1 (
    run_id UUID NOT NULL,
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    slot_key TEXT NOT NULL,
    previous_live_orders_enabled BOOLEAN NOT NULL,
    want_on BOOLEAN NOT NULL,
    resulting_live_orders_enabled BOOLEAN NOT NULL,
    transition_type TEXT NOT NULL CHECK (transition_type IN
      ('ENABLED','DISABLED','RETAINED_ON','RETAINED_OFF')),
    touched BOOLEAN NOT NULL,
    control_mode TEXT NOT NULL,
    control_source TEXT NOT NULL,
    decision_reason TEXT NOT NULL,
    pick_source TEXT,
    v63_eligible BOOLEAN,
    v63_picked BOOLEAN,
    v63_reason TEXT,
    v63_score NUMERIC,
    v63_rank BIGINT,
    trades_3d NUMERIC,
    net_pnl_3d NUMERIC,
    profit_factor_3d NUMERIC,
    gate_fresh BOOLEAN,
    hysteresis_regime TEXT,
    hysteresis_confidence NUMERIC,
    hysteresis_reason TEXT,
    hysteresis_holding_previous BOOLEAN,
    v7_ready BOOLEAN,
    v7_readiness_reason TEXT,
    v7_reason TEXT,
    v7_rank BIGINT,
    runs_15m BIGINT,
    buy_decisions_15m BIGINT,
    signals_15m BIGINT,
    hard_blocks_15m BIGINT,
    mme_avoid BOOLEAN,
    mme_remaining_score NUMERIC,
    mme_exhaustion_risk NUMERIC,
    mme_status TEXT,
    mme_hint TEXT,
    mme_readiness_score NUMERIC,
    mme_sequence_type TEXT,
    mme_sequence_stage TEXT,
    mme_sequence_quality NUMERIC,
    mme_late_entry_risk NUMERIC,
    mme_context_status TEXT,
    context_v2_ready_now BOOLEAN,
    included_in_pick_set BOOLEAN NOT NULL,
    source_refreshed_at TIMESTAMPTZ,
    slot_snapshot_hash TEXT NOT NULL CHECK (slot_snapshot_hash ~ '^[0-9a-f]{64}$'),
    snapshot_json JSONB NOT NULL,
    writer_service TEXT NOT NULL,
    writer_instance TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version TEXT NOT NULL DEFAULT 'ORC_APPLY_LEDGER_V1',
    PRIMARY KEY (deployment_id, environment, run_id, slot_key),
    FOREIGN KEY (deployment_id, environment, run_id)
      REFERENCES orc_apply_runs_v1(deployment_id, environment, run_id)
      DEFERRABLE INITIALLY DEFERRED,
    CHECK (slot_key = upper(symbol)||'|'||interval||'|'||upper(strategy)),
    CHECK (resulting_live_orders_enabled = want_on),
    CHECK ((transition_type='ENABLED' AND NOT previous_live_orders_enabled AND want_on) OR
           (transition_type='DISABLED' AND previous_live_orders_enabled AND NOT want_on) OR
           (transition_type='RETAINED_ON' AND previous_live_orders_enabled AND want_on) OR
           (transition_type='RETAINED_OFF' AND NOT previous_live_orders_enabled AND NOT want_on))
);

CREATE INDEX IF NOT EXISTS ix_orc_apply_runs_recent_v1
  ON orc_apply_runs_v1(deployment_id,environment,completed_at DESC);
CREATE INDEX IF NOT EXISTS ix_orc_apply_slot_history_v1
  ON orc_apply_slot_decisions_v1(deployment_id,environment,slot_key,recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_orc_apply_slot_transition_v1
  ON orc_apply_slot_decisions_v1(deployment_id,environment,transition_type,recorded_at DESC);

CREATE OR REPLACE FUNCTION prevent_orc_apply_ledger_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ORC apply ledger is immutable and append-only';
END;
$$;

DROP TRIGGER IF EXISTS orc_apply_runs_immutable_v1 ON orc_apply_runs_v1;
CREATE TRIGGER orc_apply_runs_immutable_v1
BEFORE UPDATE OR DELETE ON orc_apply_runs_v1
FOR EACH ROW EXECUTE FUNCTION prevent_orc_apply_ledger_mutation_v1();

DROP TRIGGER IF EXISTS orc_apply_slots_immutable_v1 ON orc_apply_slot_decisions_v1;
CREATE TRIGGER orc_apply_slots_immutable_v1
BEFORE UPDATE OR DELETE ON orc_apply_slot_decisions_v1
FOR EACH ROW EXECUTE FUNCTION prevent_orc_apply_ledger_mutation_v1();

COMMIT;
