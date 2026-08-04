-- WALTRADE LEARNING CANONICAL FINANCIAL PAYLOAD V1
-- Future Learning generations source both eligibility and financial values
-- from the same-position canonical Financial Truth contract.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $prerequisites$
BEGIN
    IF to_regclass('public.canonical_financial_truth_v1') IS NULL
       OR to_regclass('public.v_learning_eligible_closed_positions_v1') IS NULL
       OR to_regclass('public.learning_canonical_source_snapshots_v2') IS NULL
       OR to_regclass('public.learning_canonical_source_snapshot_rows_v2') IS NULL
       OR to_regprocedure(
           'public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
       ) IS NULL
       OR to_regprocedure(
           'public.learning_canonical_evidence_universe_live_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
       ) IS NULL
       OR to_regprocedure(
           'public.learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
       ) IS NULL
       OR to_regprocedure(
           'public.capture_learning_canonical_source_snapshot_v2(bigint)'
       ) IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_FINANCIAL_PAYLOAD_V1_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

ALTER TABLE public.learning_canonical_source_snapshots_v2
    ADD COLUMN IF NOT EXISTS financial_payload_contract_version TEXT;

ALTER TABLE public.learning_canonical_source_snapshot_rows_v2
    ADD COLUMN IF NOT EXISTS net_pnl_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS entry_notional_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS exit_notional_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS entry_fee_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS exit_fee_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS financial_truth_status TEXT,
    ADD COLUMN IF NOT EXISTS financial_truth_calculation_version TEXT,
    ADD COLUMN IF NOT EXISTS financial_truth_source_authority TEXT,
    ADD COLUMN IF NOT EXISTS financial_payload_contract_version TEXT;

COMMENT ON COLUMN public.learning_canonical_source_snapshots_v2.financial_payload_contract_version IS
    'NULL denotes an immutable pre-canonical-FT snapshot; new snapshots use CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1.';
COMMENT ON COLUMN public.learning_canonical_source_snapshot_rows_v2.financial_payload_contract_version IS
    'NULL denotes an immutable pre-canonical-FT row; new rows use CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1.';

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_learning_canonical_snapshot_rows_v2_canonical_position
ON public.learning_canonical_source_snapshot_rows_v2(snapshot_token, position_id)
WHERE financial_payload_contract_version =
      'CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1';

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_live_v2(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ,
    net_pnl_usdc NUMERIC, entry_notional_usdc NUMERIC,
    exit_notional_usdc NUMERIC, entry_fee_usdc NUMERIC,
    exit_fee_usdc NUMERIC, financial_truth_status TEXT,
    financial_truth_calculation_version TEXT,
    financial_truth_source_authority TEXT,
    financial_payload_contract_version TEXT
)
LANGUAGE SQL
STABLE
AS $function$
    SELECT
        source.environment, source.symbol, source.interval, source.strategy,
        source.decision_key, source.decision_id, source.position_id,
        source.entry_time, source.exit_time, source.outcome_timestamp,
        financial_truth.authoritative_net_pnl,
        financial_truth.authoritative_gross_pnl,
        financial_truth.authoritative_fees_usdc,
        source.mfe_pct, source.mae_pct, source.regime_identity,
        source.regime_context, source.source_refreshed_at,
        source.has_full_context, source.has_avoid_review,
        source.has_entry_quality_review, source.has_positive_confirmation,
        source.eligibility_reason, source.registry_available_at,
        source.outcome_available_at,
        financial_truth.authoritative_net_pnl,
        financial_truth.authoritative_entry_notional,
        financial_truth.authoritative_exit_notional,
        financial_truth.authoritative_entry_fees_usdc,
        financial_truth.authoritative_exit_fees_usdc,
        financial_truth.financial_truth_status,
        financial_truth.calculation_version,
        financial_truth.source_authority,
        'CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1'::TEXT
    FROM public.learning_canonical_evidence_universe_pre_ft_quarantine_v1(
        p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
    ) source
    JOIN public.v_learning_eligible_closed_positions_v1 eligible_position
      ON eligible_position.id = source.position_id
    JOIN public.canonical_financial_truth_v1 financial_truth
      ON financial_truth.position_id = eligible_position.id
     AND financial_truth.financial_truth_status = 'COMPLETE'
    ORDER BY source.decision_key
$function$;

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_live_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
AS $function$
    SELECT
        source.environment, source.symbol, source.interval, source.strategy,
        source.decision_key, source.decision_id, source.position_id,
        source.entry_time, source.exit_time, source.outcome_timestamp,
        source.realized_pnl_usdc, source.gross_pnl_usdc, source.fees_usdc,
        source.mfe_pct, source.mae_pct, source.regime_identity,
        source.regime_context, source.source_refreshed_at,
        source.has_full_context, source.has_avoid_review,
        source.has_entry_quality_review, source.has_positive_confirmation,
        source.eligibility_reason, source.registry_available_at,
        source.outcome_available_at
    FROM public.learning_canonical_evidence_universe_live_v2(
        p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
    ) source
    ORDER BY source.decision_key
$function$;

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_v2(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ,
    net_pnl_usdc NUMERIC, entry_notional_usdc NUMERIC,
    exit_notional_usdc NUMERIC, entry_fee_usdc NUMERIC,
    exit_fee_usdc NUMERIC, financial_truth_status TEXT,
    financial_truth_calculation_version TEXT,
    financial_truth_source_authority TEXT,
    financial_payload_contract_version TEXT
)
LANGUAGE plpgsql
STABLE
AS $function$
DECLARE
    v_token_text TEXT := current_setting(
        'waltrade.learning_source_snapshot_token', true
    );
    v_token UUID;
    v_header public.learning_canonical_source_snapshots_v2%ROWTYPE;
BEGIN
    IF v_token_text IS NULL OR v_token_text = '' THEN
        RETURN QUERY
        SELECT *
        FROM public.learning_canonical_evidence_universe_live_v2(
            p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
        );
        RETURN;
    END IF;

    v_token := v_token_text::UUID;
    SELECT * INTO STRICT v_header
    FROM public.learning_canonical_source_snapshots_v2
    WHERE snapshot_token = v_token
      AND snapshot_status = 'COMPLETE';
    IF p_environment <> v_header.source_environment THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_CONTEXT_MISMATCH snapshot=% expected=% actual=%',
            v_token, v_header.source_environment, p_environment;
    END IF;

    RETURN QUERY
    SELECT
        row.environment, row.symbol, row.interval, row.strategy,
        row.decision_key, row.decision_id, row.position_id, row.entry_time,
        row.exit_time, row.outcome_timestamp, row.realized_pnl_usdc,
        row.gross_pnl_usdc, row.fees_usdc, row.mfe_pct, row.mae_pct,
        row.regime_identity, row.regime_context, row.source_refreshed_at,
        row.has_full_context, row.has_avoid_review,
        row.has_entry_quality_review, row.has_positive_confirmation,
        row.eligibility_reason, row.registry_available_at,
        row.outcome_available_at, row.net_pnl_usdc,
        row.entry_notional_usdc, row.exit_notional_usdc,
        row.entry_fee_usdc, row.exit_fee_usdc, row.financial_truth_status,
        row.financial_truth_calculation_version,
        row.financial_truth_source_authority,
        COALESCE(
            row.financial_payload_contract_version,
            'LEGACY_WAREHOUSE_PAYLOAD_V0'
        )
    FROM public.learning_canonical_source_snapshot_rows_v2 row
    WHERE row.snapshot_token = v_token
    ORDER BY row.ordinal;
END;
$function$;

CREATE OR REPLACE FUNCTION public.learning_canonical_evidence_universe_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE (
    environment TEXT, symbol TEXT, "interval" TEXT, strategy TEXT,
    decision_key TEXT, decision_id UUID, position_id BIGINT,
    entry_time TIMESTAMPTZ, exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ, realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC, fees_usdc NUMERIC, mfe_pct NUMERIC,
    mae_pct NUMERIC, regime_identity TEXT, regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ, has_full_context BOOLEAN,
    has_avoid_review BOOLEAN, has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN, eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ, outcome_available_at TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
AS $function$
    SELECT
        source.environment, source.symbol, source.interval, source.strategy,
        source.decision_key, source.decision_id, source.position_id,
        source.entry_time, source.exit_time, source.outcome_timestamp,
        source.realized_pnl_usdc, source.gross_pnl_usdc, source.fees_usdc,
        source.mfe_pct, source.mae_pct, source.regime_identity,
        source.regime_context, source.source_refreshed_at,
        source.has_full_context, source.has_avoid_review,
        source.has_entry_quality_review, source.has_positive_confirmation,
        source.eligibility_reason, source.registry_available_at,
        source.outcome_available_at
    FROM public.learning_canonical_evidence_universe_v2(
        p_environment, p_sample_from, p_sample_to, p_evidence_cutoff_at
    ) source
    ORDER BY source.decision_key
$function$;

CREATE OR REPLACE FUNCTION public.prevent_learning_frozen_source_mutation_v2()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_TABLE_NAME = 'learning_canonical_source_snapshots_v2' THEN
        IF TG_OP = 'UPDATE'
           AND OLD.snapshot_status = 'BUILDING'
           AND NEW.snapshot_status = 'COMPLETE'
           AND NEW.snapshot_token = OLD.snapshot_token
           AND NEW.feedback_run_id = OLD.feedback_run_id
           AND NEW.deployment_instance_id = OLD.deployment_instance_id
           AND NEW.environment = OLD.environment
           AND NEW.deployment_id = OLD.deployment_id
           AND NEW.source_environment = OLD.source_environment
           AND NEW.evidence_window_start = OLD.evidence_window_start
           AND NEW.evidence_window_end = OLD.evidence_window_end
           AND NEW.evidence_cutoff_at = OLD.evidence_cutoff_at
           AND NEW.source_snapshot_at = OLD.source_snapshot_at
           AND NEW.created_at = OLD.created_at
           AND NEW.financial_payload_contract_version
               IS NOT DISTINCT FROM OLD.financial_payload_contract_version
        THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION
        'LEARNING_FROZEN_SOURCE_IMMUTABLE table=% operation=%',
        TG_TABLE_NAME, TG_OP;
END;
$function$;

CREATE OR REPLACE FUNCTION public.capture_learning_canonical_source_snapshot_v2(
    p_feedback_run_id BIGINT
)
RETURNS UUID
LANGUAGE plpgsql
AS $function$
DECLARE
    v_run public.learning_feedback_refresh_runs_v1%ROWTYPE;
    v_identity RECORD;
    v_token UUID;
    v_existing public.learning_canonical_source_snapshots_v2%ROWTYPE;
    v_source_environment TEXT;
    v_cutoff TIMESTAMPTZ;
    v_window_start TIMESTAMPTZ;
    v_source_snapshot_at TIMESTAMPTZ := clock_timestamp();
    v_source_count INTEGER;
    v_eligible_count INTEGER;
    v_snapshot_hash TEXT;
BEGIN
    SELECT * INTO STRICT v_run
    FROM public.learning_feedback_refresh_runs_v1
    WHERE id = p_feedback_run_id AND status IN ('RUNNING', 'OK');
    SELECT * INTO STRICT v_identity
    FROM public.learning_evidence_runtime_identity_v1();
    v_source_environment := CASE v_identity.environment
        WHEN 'live' THEN 'trading_live'
        WHEN 'paper' THEN 'trading_paper'
    END;
    IF v_run.environment <> v_source_environment THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_DATABASE_IDENTITY_MISMATCH run=% runtime=%',
            v_run.environment, v_source_environment;
    END IF;
    v_cutoff := COALESCE(v_run.started_at, v_run.requested_at);
    v_window_start := v_cutoff - make_interval(days => v_run.window_days);

    SELECT * INTO v_existing
    FROM public.learning_canonical_source_snapshots_v2
    WHERE feedback_run_id = p_feedback_run_id;
    IF FOUND THEN
        IF v_existing.snapshot_status <> 'COMPLETE'
           OR v_existing.deployment_instance_id <> v_identity.deployment_instance_id
           OR v_existing.environment <> v_identity.environment
           OR v_existing.deployment_id <> v_identity.deployment_id
           OR v_existing.source_environment <> v_source_environment
           OR v_existing.evidence_window_start <> v_window_start
           OR v_existing.evidence_window_end <> v_cutoff
           OR v_existing.evidence_cutoff_at <> v_cutoff
           OR v_existing.source_row_count <> (
                SELECT count(*)
                FROM public.learning_canonical_source_snapshot_rows_v2
                WHERE snapshot_token = v_existing.snapshot_token
           )
           OR v_existing.snapshot_hash <> (
                SELECT encode(digest(COALESCE(string_agg(
                    row_hash, E'\n' ORDER BY ordinal), ''), 'sha256'), 'hex')
                FROM public.learning_canonical_source_snapshot_rows_v2
                WHERE snapshot_token = v_existing.snapshot_token
           )
        THEN
            RAISE EXCEPTION
                'LEARNING_FROZEN_SOURCE_IDEMPOTENCY_CONFLICT run=%',
                p_feedback_run_id;
        END IF;
        PERFORM set_config(
            'waltrade.learning_source_snapshot_token',
            v_existing.snapshot_token::TEXT, true
        );
        RETURN v_existing.snapshot_token;
    END IF;
    IF v_run.status <> 'RUNNING' THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_NEW_SNAPSHOT_REQUIRES_RUNNING run=% status=%',
            p_feedback_run_id, v_run.status;
    END IF;

    v_token := gen_random_uuid();
    INSERT INTO public.learning_canonical_source_snapshots_v2 (
        snapshot_token, feedback_run_id, deployment_instance_id, environment,
        deployment_id, source_environment, evidence_window_start,
        evidence_window_end, evidence_cutoff_at, source_snapshot_at,
        snapshot_status, financial_payload_contract_version
    ) VALUES (
        v_token, p_feedback_run_id, v_identity.deployment_instance_id,
        v_identity.environment, v_identity.deployment_id,
        v_source_environment, v_window_start, v_cutoff, v_cutoff,
        v_source_snapshot_at, 'BUILDING',
        'CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1'
    );

    INSERT INTO public.learning_canonical_source_snapshot_rows_v2 (
        snapshot_token, ordinal, environment, symbol, interval, strategy,
        decision_key, decision_id, position_id, entry_time, exit_time,
        outcome_timestamp, realized_pnl_usdc, gross_pnl_usdc, fees_usdc,
        mfe_pct, mae_pct, regime_identity, regime_context,
        source_refreshed_at, has_full_context, has_avoid_review,
        has_entry_quality_review, has_positive_confirmation,
        eligibility_reason, registry_available_at, outcome_available_at,
        net_pnl_usdc, entry_notional_usdc, exit_notional_usdc,
        entry_fee_usdc, exit_fee_usdc, financial_truth_status,
        financial_truth_calculation_version,
        financial_truth_source_authority,
        financial_payload_contract_version, row_hash
    )
    SELECT
        v_token, row_number() OVER (ORDER BY source.decision_key),
        source.environment, source.symbol, source.interval, source.strategy,
        source.decision_key, source.decision_id, source.position_id,
        source.entry_time, source.exit_time, source.outcome_timestamp,
        source.realized_pnl_usdc, source.gross_pnl_usdc, source.fees_usdc,
        source.mfe_pct, source.mae_pct, source.regime_identity,
        source.regime_context, source.source_refreshed_at,
        source.has_full_context, source.has_avoid_review,
        source.has_entry_quality_review, source.has_positive_confirmation,
        source.eligibility_reason, source.registry_available_at,
        source.outcome_available_at, source.net_pnl_usdc,
        source.entry_notional_usdc, source.exit_notional_usdc,
        source.entry_fee_usdc, source.exit_fee_usdc,
        source.financial_truth_status,
        source.financial_truth_calculation_version,
        source.financial_truth_source_authority,
        source.financial_payload_contract_version,
        encode(digest(jsonb_build_array(
            source.environment, source.symbol, source.interval,
            source.strategy, source.decision_key, source.decision_id,
            source.position_id, source.entry_time, source.exit_time,
            source.outcome_timestamp, source.realized_pnl_usdc,
            source.gross_pnl_usdc, source.fees_usdc, source.mfe_pct,
            source.mae_pct, source.regime_identity, source.regime_context,
            source.source_refreshed_at, source.has_full_context,
            source.has_avoid_review, source.has_entry_quality_review,
            source.has_positive_confirmation, source.eligibility_reason,
            source.registry_available_at, source.outcome_available_at,
            source.net_pnl_usdc, source.entry_notional_usdc,
            source.exit_notional_usdc, source.entry_fee_usdc,
            source.exit_fee_usdc, source.financial_truth_status,
            source.financial_truth_calculation_version,
            source.financial_truth_source_authority,
            source.financial_payload_contract_version
        )::TEXT, 'sha256'), 'hex')
    FROM public.learning_canonical_evidence_universe_live_v2(
        v_source_environment, v_window_start, v_cutoff, v_cutoff
    ) source
    ORDER BY source.decision_key;
    GET DIAGNOSTICS v_source_count = ROW_COUNT;

    SELECT count(*) FILTER (WHERE eligibility_reason = 'ELIGIBLE'),
           encode(digest(COALESCE(string_agg(
               row_hash, E'\n' ORDER BY ordinal), ''), 'sha256'), 'hex')
    INTO v_eligible_count, v_snapshot_hash
    FROM public.learning_canonical_source_snapshot_rows_v2
    WHERE snapshot_token = v_token;

    UPDATE public.learning_canonical_source_snapshots_v2
    SET snapshot_status = 'COMPLETE',
        source_row_count = v_source_count,
        eligible_row_count = v_eligible_count,
        snapshot_hash = v_snapshot_hash,
        completed_at = clock_timestamp()
    WHERE snapshot_token = v_token AND snapshot_status = 'BUILDING';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_FINALIZE_CONFLICT snapshot=%', v_token;
    END IF;
    PERFORM set_config(
        'waltrade.learning_source_snapshot_token', v_token::TEXT, true
    );
    RETURN v_token;
END;
$function$;

DO $postconditions$
DECLARE
    v_live TEXT := pg_get_functiondef(
        'public.learning_canonical_evidence_universe_live_v2(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'::regprocedure
    );
    v_capture TEXT := pg_get_functiondef(
        'public.capture_learning_canonical_source_snapshot_v2(bigint)'::regprocedure
    );
BEGIN
    IF position(
           'financial_truth.position_id = eligible_position.id' IN v_live
       ) = 0
       OR position('authoritative_net_pnl' IN v_live) = 0
       OR position('authoritative_fees_usdc' IN v_live) = 0
       OR position('learning_canonical_evidence_universe_live_v2' IN v_capture) = 0
       OR position('CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1' IN v_capture) = 0
       OR position('source.financial_truth_source_authority' IN v_capture) = 0
    THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_FINANCIAL_PAYLOAD_V1_POSTCONDITION_FAILED';
    END IF;
END;
$postconditions$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id, checksum_sha256, environment, deployment_id, database_name,
    applied_by, status, success, execution_duration_ms, git_sha,
    schema_baseline_version
)
SELECT
    '20260804_learning_canonical_financial_payload_v1.sql',
    'a074cabdc55f36861964807a692f8644063577c63abe0e56c361474eafcc81d5',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'LEARNING_CANONICAL_FINANCIAL_PAYLOAD_V1', current_database(),
    'operator-migration', 'APPLIED', TRUE, 0,
    'dddddeccbd0f688d2d846a373c55f466a051f2b7',
    'LEARNING_CANONICAL_FINANCIAL_PAYLOAD_V1'
WHERE NOT EXISTS (
    SELECT 1
    FROM public.schema_migration_ledger_v1
    WHERE migration_id =
          '20260804_learning_canonical_financial_payload_v1.sql'
);

COMMIT;
