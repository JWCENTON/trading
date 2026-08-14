BEGIN;

DO $$
BEGIN
    IF to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.simulated_orders') IS NULL
       OR to_regclass('public.simulated_execution_fills_v1') IS NULL
       OR to_regclass('public.positions') IS NULL
       OR to_regclass('public.decision_replay_v1') IS NULL
       OR to_regclass('public.learning_feature_warehouse_v1') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION 'ENTRY_OPPORTUNITY_EVIDENCE_V1_PREREQUISITE_MISSING';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS public.entry_opportunity_evidence_v1 (
    snapshot_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL UNIQUE
        REFERENCES public.decision_registry_v1(decision_id) ON DELETE RESTRICT,
    decision_key TEXT,
    decision_created_at TIMESTAMPTZ NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    source_revision TEXT,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,

    market_regime TEXT,
    regime_confidence NUMERIC,
    hysteresis_regime TEXT,
    compatibility_status TEXT,
    compatibility_score NUMERIC,
    market_availability_status TEXT NOT NULL,
    market_context JSONB NOT NULL DEFAULT '{}'::jsonb,

    signal_action TEXT,
    signal_reason TEXT,
    strategy_availability_status TEXT NOT NULL,
    strategy_features JSONB NOT NULL DEFAULT '{}'::jsonb,

    realtime_availability_status TEXT NOT NULL,
    realtime_context JSONB,
    mme_availability_status TEXT NOT NULL,
    mme_context JSONB,
    slot_brain_availability_status TEXT NOT NULL,
    slot_brain_context JSONB,
    orc_availability_status TEXT NOT NULL,
    orc_context JSONB,

    planned_entry_notional NUMERIC NOT NULL,
    fee_rate_entry_assumption NUMERIC NOT NULL,
    fee_rate_exit_assumption NUMERIC NOT NULL,
    expected_round_trip_fee_usdc NUMERIC NOT NULL,
    expected_round_trip_fee_pct NUMERIC NOT NULL,
    break_even_move_pct NUMERIC NOT NULL,
    fee_model_version TEXT NOT NULL,
    fee_config_source TEXT NOT NULL,
    spread_pct NUMERIC,
    execution_quality_status TEXT NOT NULL,
    execution_quality_context JSONB,

    expected_move_pct NUMERIC,
    expected_move_model_version TEXT,
    evidence_payload_hash TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'ENTRY_OPPORTUNITY_EVIDENCE_V1',
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT entry_opportunity_environment_ck CHECK (
        environment IN ('trading_paper','trading_live')
    ),
    CONSTRAINT entry_opportunity_fee_rates_ck CHECK (
        fee_rate_entry_assumption >= 0 AND fee_rate_entry_assumption <= 0.10
        AND fee_rate_exit_assumption >= 0 AND fee_rate_exit_assumption <= 0.10
    ),
    CONSTRAINT entry_opportunity_nonnegative_economics_ck CHECK (
        planned_entry_notional >= 0
        AND expected_round_trip_fee_usdc >= 0
        AND expected_round_trip_fee_pct >= 0
        AND break_even_move_pct >= 0
    ),
    CONSTRAINT entry_opportunity_expected_move_v1_ck CHECK (
        expected_move_pct IS NULL
        AND expected_move_model_version IS NULL
    ),
    CONSTRAINT entry_opportunity_hash_ck CHECK (
        evidence_payload_hash ~ '^[0-9a-f]{64}$'
    )
);

COMMENT ON TABLE public.entry_opportunity_evidence_v1 IS
    'Immutable shadow-only entry-time state. It contains no future outcome fields and cannot change trading action.';
COMMENT ON COLUMN public.entry_opportunity_evidence_v1.expected_move_pct IS
    'Reserved contract field. V1 deliberately implements no expected-move model.';

CREATE TABLE IF NOT EXISTS public.entry_opportunity_evidence_audit_v1 (
    audit_id BIGSERIAL PRIMARY KEY,
    decision_id UUID,
    snapshot_id UUID,
    environment TEXT,
    deployment_id TEXT,
    event_type TEXT NOT NULL,
    status_reason TEXT,
    error_class TEXT,
    source_service TEXT NOT NULL DEFAULT 'paper_execution_writer',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT entry_opportunity_audit_event_ck CHECK (
        event_type IN ('CAPTURED','IDEMPOTENT_EXISTING','ENTRY_OPPORTUNITY_EVIDENCE_MISSING')
    )
);

CREATE INDEX IF NOT EXISTS ix_entry_opportunity_audit_decision_v1
    ON public.entry_opportunity_evidence_audit_v1(decision_id,created_at DESC);

ALTER TABLE public.decision_registry_v1
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID,
    ADD COLUMN IF NOT EXISTS entry_opportunity_evidence_status TEXT,
    ADD COLUMN IF NOT EXISTS entry_opportunity_evidence_reason TEXT;

ALTER TABLE public.simulated_orders
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID;
ALTER TABLE public.simulated_execution_fills_v1
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID;
ALTER TABLE public.positions
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID;
ALTER TABLE public.decision_replay_v1
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID;
ALTER TABLE public.learning_feature_warehouse_v1
    ADD COLUMN IF NOT EXISTS entry_opportunity_snapshot_id UUID;

DO $$
DECLARE
    item RECORD;
BEGIN
    FOR item IN
        SELECT * FROM (VALUES
            ('decision_registry_v1','fk_decision_registry_entry_opportunity_v1'),
            ('simulated_orders','fk_simulated_orders_entry_opportunity_v1'),
            ('simulated_execution_fills_v1','fk_simulated_fills_entry_opportunity_v1'),
            ('positions','fk_positions_entry_opportunity_v1'),
            ('decision_replay_v1','fk_decision_replay_entry_opportunity_v1'),
            ('learning_feature_warehouse_v1','fk_learning_warehouse_entry_opportunity_v1')
        ) AS valueset(table_name,constraint_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
             WHERE conname=item.constraint_name
               AND conrelid=('public.'||item.table_name)::regclass
        ) THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD CONSTRAINT %I FOREIGN KEY(entry_opportunity_snapshot_id) REFERENCES public.entry_opportunity_evidence_v1(snapshot_id) ON DELETE RESTRICT',
                item.table_name,item.constraint_name
            );
        END IF;
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION public.guard_entry_opportunity_snapshot_immutable_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ENTRY_OPPORTUNITY_SNAPSHOT_IMMUTABLE';
END
$$;

DROP TRIGGER IF EXISTS trg_entry_opportunity_snapshot_immutable_v1
    ON public.entry_opportunity_evidence_v1;
CREATE TRIGGER trg_entry_opportunity_snapshot_immutable_v1
BEFORE UPDATE OR DELETE ON public.entry_opportunity_evidence_v1
FOR EACH ROW EXECUTE FUNCTION public.guard_entry_opportunity_snapshot_immutable_v1();

DROP TRIGGER IF EXISTS trg_entry_opportunity_audit_append_only_v1
    ON public.entry_opportunity_evidence_audit_v1;
CREATE TRIGGER trg_entry_opportunity_audit_append_only_v1
BEFORE UPDATE OR DELETE ON public.entry_opportunity_evidence_audit_v1
FOR EACH ROW EXECUTE FUNCTION public.guard_entry_opportunity_snapshot_immutable_v1();

CREATE OR REPLACE FUNCTION public.guard_entry_opportunity_reference_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.entry_opportunity_snapshot_id IS NOT NULL
       AND NEW.entry_opportunity_snapshot_id IS DISTINCT FROM OLD.entry_opportunity_snapshot_id THEN
        RAISE EXCEPTION 'ENTRY_OPPORTUNITY_REFERENCE_IMMUTABLE';
    END IF;
    RETURN NEW;
END
$$;

DO $$
DECLARE
    item RECORD;
BEGIN
    FOR item IN
        SELECT * FROM (VALUES
            ('decision_registry_v1','trg_decision_registry_entry_opportunity_ref_v1'),
            ('simulated_orders','trg_simulated_orders_entry_opportunity_ref_v1'),
            ('simulated_execution_fills_v1','trg_simulated_fills_entry_opportunity_ref_v1'),
            ('positions','trg_positions_entry_opportunity_ref_v1'),
            ('decision_replay_v1','trg_decision_replay_entry_opportunity_ref_v1'),
            ('learning_feature_warehouse_v1','trg_learning_warehouse_entry_opportunity_ref_v1')
        ) AS valueset(table_name,trigger_name)
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.%I',item.trigger_name,item.table_name);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OF entry_opportunity_snapshot_id ON public.%I FOR EACH ROW EXECUTE FUNCTION public.guard_entry_opportunity_reference_v1()',
            item.trigger_name,item.table_name
        );
    END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION public.propagate_entry_opportunity_reference_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    resolved_snapshot_id UUID;
BEGIN
    IF NEW.entry_opportunity_snapshot_id IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.position_id IS NOT NULL THEN
        SELECT p.entry_opportunity_snapshot_id
          INTO resolved_snapshot_id
          FROM public.positions p
         WHERE p.id=NEW.position_id;
    END IF;
    IF resolved_snapshot_id IS NULL AND NEW.decision_key IS NOT NULL THEN
        SELECT d.entry_opportunity_snapshot_id
          INTO resolved_snapshot_id
          FROM public.decision_registry_v1 d
         WHERE d.legacy_decision_key=NEW.decision_key
         ORDER BY d.decision_timestamp DESC
         LIMIT 1;
    END IF;
    NEW.entry_opportunity_snapshot_id := resolved_snapshot_id;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_decision_replay_entry_opportunity_propagate_v1
    ON public.decision_replay_v1;
CREATE TRIGGER trg_decision_replay_entry_opportunity_propagate_v1
BEFORE INSERT OR UPDATE OF position_id,decision_key ON public.decision_replay_v1
FOR EACH ROW EXECUTE FUNCTION public.propagate_entry_opportunity_reference_v1();

DROP TRIGGER IF EXISTS trg_learning_warehouse_entry_opportunity_propagate_v1
    ON public.learning_feature_warehouse_v1;
CREATE TRIGGER trg_learning_warehouse_entry_opportunity_propagate_v1
BEFORE INSERT OR UPDATE OF position_id,decision_key ON public.learning_feature_warehouse_v1
FOR EACH ROW EXECUTE FUNCTION public.propagate_entry_opportunity_reference_v1();

CREATE OR REPLACE VIEW public.v_entry_opportunity_outcome_labels_v1 AS
SELECT
    snapshot.snapshot_id,
    snapshot.decision_id,
    position_row.id AS position_id,
    outcome.outcome_status,
    outcome.mfe_pct AS actual_mfe_pct,
    outcome.mae_pct AS actual_mae_pct,
    outcome.net_pnl_usdc AS actual_net_pnl_usdc,
    outcome.mfe_pct-snapshot.break_even_move_pct AS mfe_minus_entry_break_even,
    CASE
        WHEN outcome.outcome_status<>'COMPLETE' THEN NULL
        WHEN outcome.net_pnl_usdc>0 THEN 'ACTUAL_NET_WIN'
        WHEN outcome.mfe_pct>=snapshot.break_even_move_pct THEN 'BREAK_EVEN_REACHABLE'
        ELSE 'ECONOMICALLY_UNVIABLE'
    END AS economic_viability_label
FROM public.entry_opportunity_evidence_v1 snapshot
LEFT JOIN public.positions position_row
  ON position_row.entry_opportunity_snapshot_id=snapshot.snapshot_id
LEFT JOIN public.decision_outcomes_v1 outcome
  ON outcome.position_id=position_row.id
 AND outcome.outcome_type='ACTUAL_TRADE';

COMMENT ON VIEW public.v_entry_opportunity_outcome_labels_v1 IS
    'Post-outcome diagnostic labels. This view is intentionally separate from immutable entry-time evidence.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260814_entry_opportunity_evidence_v1.sql',
    COALESCE(NULLIF(current_setting('waltrade.migration_checksum',true),''),repeat('0',64)),
    'PAPER','LOCAL',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    COALESCE(NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)),
    'ENTRY_OPPORTUNITY_EVIDENCE_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
     WHERE migration_id='20260814_entry_opportunity_evidence_v1.sql'
);

COMMIT;
