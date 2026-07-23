BEGIN;

DO $prerequisites$
BEGIN
    IF to_regprocedure(
        'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)'
    ) IS NULL
       OR to_regclass(
            'public.learning_canonical_evidence_selection_v1'
       ) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_MANIFEST_PREREQUISITE_MISSING: canonical source universe migration';
    END IF;
    IF to_regprocedure(
        'refresh_learning_feedback_engine_v1(integer,integer,integer)'
    ) IS NULL
       OR to_regclass('public.learning_proposal_validation_runs_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_EVIDENCE_MANIFEST_PREREQUISITE_MISSING: feedback/validation contract';
    END IF;
END;
$prerequisites$;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION learning_evidence_runtime_identity_v1()
RETURNS TABLE(deployment_instance_id TEXT, environment TEXT, deployment_id TEXT)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_instance TEXT := current_setting('waltrade.deployment_instance_id', true);
    v_environment TEXT := current_setting('waltrade.environment', true);
BEGIN
    IF v_environment NOT IN ('live','paper')
       OR v_instance IS NULL OR length(v_instance) NOT BETWEEN 1 AND 63
       OR v_instance !~ '^[a-z0-9]+(?:-[a-z0-9]+)*$'
       OR v_instance LIKE '%-live' OR v_instance LIKE '%-paper'
    THEN
        RAISE EXCEPTION 'INVALID_LEARNING_EVIDENCE_RUNTIME_IDENTITY instance_id=% environment=%',
            COALESCE(v_instance, '<missing>'), COALESCE(v_environment, '<missing>');
    END IF;
    RETURN QUERY SELECT v_instance, v_environment, v_instance || '-' || v_environment;
END;
$$;

-- V1.2 is the historical semantic authority for per-slot profit factor.
-- Repeating the helper here gives existing installations the same calculation
-- used by fresh installs, without updating historical feedback statistics.
CREATE OR REPLACE FUNCTION learning_canonical_profit_factor_v1(
    p_decisions INTEGER,
    p_pnl_coverage_count INTEGER,
    p_gross_profit_usdc NUMERIC,
    p_gross_loss_usdc NUMERIC
)
RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT ROUND(CASE
        WHEN COALESCE(p_decisions, 0) = 0 THEN NULL::NUMERIC
        WHEN COALESCE(p_pnl_coverage_count, 0) = 0 THEN NULL::NUMERIC
        WHEN COALESCE(ABS(p_gross_loss_usdc), 0) = 0
             AND COALESCE(p_gross_profit_usdc, 0) > 0
            THEN 999::NUMERIC
        WHEN COALESCE(ABS(p_gross_loss_usdc), 0) = 0
            THEN 0::NUMERIC
        ELSE
            COALESCE(p_gross_profit_usdc, 0)
            / ABS(p_gross_loss_usdc)
    END, 12)
$$;

CREATE TABLE IF NOT EXISTS learning_evidence_manifests_v1 (
    evidence_manifest_id UUID PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    deployment_instance_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    feedback_run_id BIGINT NOT NULL REFERENCES learning_feedback_refresh_runs_v1(id),
    validation_run_id BIGINT REFERENCES learning_proposal_validation_runs_v1(id),
    shadow_recommendation_id TEXT,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    proposal_action TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    manifest_status TEXT NOT NULL CHECK (manifest_status IN ('BUILDING','COMPLETE','LEGACY_AGGREGATE_ONLY')),
    construction_token UUID UNIQUE,
    exact_membership_available BOOLEAN NOT NULL,
    evidence_window_start TIMESTAMPTZ,
    evidence_window_end TIMESTAMPTZ,
    source_snapshot_at TIMESTAMPTZ,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    evidence_decision_count INTEGER NOT NULL CHECK (evidence_decision_count >= 0),
    manifest_hash TEXT NOT NULL,
    aggregate_hash TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    validation_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (deployment_id, environment, feedback_run_id, symbol, interval, strategy, window_days),
    UNIQUE (feedback_run_id, symbol, interval, strategy, window_days),
    CHECK (deployment_id = deployment_instance_id || '-' || environment),
    CHECK ((manifest_status IN ('BUILDING','COMPLETE') AND exact_membership_available
            AND construction_token IS NOT NULL AND source_snapshot_at IS NOT NULL)
        OR (manifest_status = 'LEGACY_AGGREGATE_ONLY' AND NOT exact_membership_available
            AND construction_token IS NULL AND source_snapshot_at IS NULL))
);

CREATE TABLE IF NOT EXISTS learning_evidence_membership_v1 (
    evidence_manifest_id UUID NOT NULL REFERENCES learning_evidence_manifests_v1(evidence_manifest_id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    decision_key TEXT NOT NULL,
    decision_id UUID,
    position_id BIGINT,
    entry_timestamp TIMESTAMPTZ,
    exit_timestamp TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,
    realized_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    regime_identity TEXT,
    regime_context JSONB,
    source_table TEXT NOT NULL,
    source_version TEXT NOT NULL,
    pnl_available BOOLEAN NOT NULL,
    fees_available BOOLEAN NOT NULL,
    mfe_available BOOLEAN NOT NULL,
    mae_available BOOLEAN NOT NULL,
    regime_available BOOLEAN NOT NULL,
    row_fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (evidence_manifest_id, ordinal),
    UNIQUE (evidence_manifest_id, decision_key)
);

CREATE TABLE IF NOT EXISTS learning_evidence_aggregates_v1 (
    evidence_manifest_id UUID PRIMARY KEY REFERENCES learning_evidence_manifests_v1(evidence_manifest_id),
    decisions INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    breakeven INTEGER NOT NULL,
    gross_profit_usdc NUMERIC,
    gross_loss_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    expectancy_usdc NUMERIC,
    profit_factor NUMERIC,
    win_rate_pct NUMERIC,
    fees_usdc NUMERIC,
    max_drawdown_usdc NUMERIC,
    mfe_average_pct NUMERIC,
    mfe_max_pct NUMERIC,
    mae_average_pct NUMERIC,
    mae_min_pct NUMERIC,
    regime_distribution JSONB NOT NULL DEFAULT '{}'::jsonb,
    pnl_coverage_count INTEGER NOT NULL,
    fees_coverage_count INTEGER NOT NULL,
    mfe_coverage_count INTEGER NOT NULL,
    mae_coverage_count INTEGER NOT NULL,
    regime_coverage_count INTEGER NOT NULL,
    missing_pnl_count INTEGER NOT NULL,
    missing_fees_count INTEGER NOT NULL,
    missing_mfe_count INTEGER NOT NULL,
    missing_mae_count INTEGER NOT NULL,
    missing_regime_count INTEGER NOT NULL,
    aggregate_payload JSONB NOT NULL,
    aggregate_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION require_manifest_header_construction_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token UUID;
    v_api_token UUID;
    v_instance TEXT;
    v_environment TEXT;
    v_deployment TEXT;
BEGIN
    SELECT deployment_instance_id,environment,deployment_id
      INTO v_instance,v_environment,v_deployment
      FROM learning_evidence_runtime_identity_v1();
    IF NEW.deployment_instance_id <> v_instance
       OR NEW.environment <> v_environment
       OR NEW.deployment_id <> v_deployment
    THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_HEADER_CONTEXT_MISMATCH';
    END IF;
    IF NEW.manifest_status = 'LEGACY_AGGREGATE_ONLY' THEN
        IF NEW.construction_token IS NOT NULL OR NEW.exact_membership_available THEN
            RAISE EXCEPTION 'INVALID_LEGACY_LEARNING_MANIFEST';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.manifest_status <> 'BUILDING' THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_MUST_START_BUILDING';
    END IF;
    BEGIN
        v_token := nullif(current_setting(
            'waltrade.learning_manifest_construction_token', true), '')::UUID;
        v_api_token := nullif(current_setting(
            'waltrade.learning_manifest_capture_api_token', true), '')::UUID;
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'INVALID_LEARNING_MANIFEST_CONSTRUCTION_TOKEN';
    END;
    IF v_token IS NULL OR v_api_token IS NULL OR v_api_token <> v_token
       OR v_token <> NEW.construction_token
    THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_HEADER_CAPABILITY_REQUIRED';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS learning_evidence_manifest_construction_v1 ON learning_evidence_manifests_v1;
CREATE TRIGGER learning_evidence_manifest_construction_v1 BEFORE INSERT ON learning_evidence_manifests_v1
FOR EACH ROW EXECUTE FUNCTION require_manifest_header_construction_v1();

CREATE OR REPLACE FUNCTION prevent_learning_evidence_manifest_mutation_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token UUID;
BEGIN
    IF TG_TABLE_NAME = 'learning_evidence_manifests_v1' AND TG_OP = 'UPDATE' THEN
      IF OLD.manifest_status = 'BUILDING'
         AND NEW.manifest_status = 'COMPLETE'
         AND (to_jsonb(OLD) - 'manifest_status') = (to_jsonb(NEW) - 'manifest_status')
      THEN
        BEGIN
            v_token := nullif(current_setting(
                'waltrade.learning_manifest_construction_token', true), '')::UUID;
        EXCEPTION WHEN invalid_text_representation THEN
            RAISE EXCEPTION 'INVALID_LEARNING_MANIFEST_CONSTRUCTION_TOKEN';
        END;
        IF v_token IS NULL OR v_token <> OLD.construction_token THEN
            RAISE EXCEPTION 'LEARNING_MANIFEST_FINALIZATION_CAPABILITY_REQUIRED';
        END IF;
        RETURN NEW;
      END IF;
    END IF;
    RAISE EXCEPTION 'learning evidence manifest is immutable and append-only';
END;
$$;

DROP TRIGGER IF EXISTS learning_evidence_manifest_immutable_v1 ON learning_evidence_manifests_v1;
CREATE TRIGGER learning_evidence_manifest_immutable_v1 BEFORE UPDATE OR DELETE ON learning_evidence_manifests_v1
FOR EACH ROW EXECUTE FUNCTION prevent_learning_evidence_manifest_mutation_v1();
DROP TRIGGER IF EXISTS learning_evidence_membership_immutable_v1 ON learning_evidence_membership_v1;
CREATE TRIGGER learning_evidence_membership_immutable_v1 BEFORE UPDATE OR DELETE ON learning_evidence_membership_v1
FOR EACH ROW EXECUTE FUNCTION prevent_learning_evidence_manifest_mutation_v1();
DROP TRIGGER IF EXISTS learning_evidence_aggregate_immutable_v1 ON learning_evidence_aggregates_v1;
CREATE TRIGGER learning_evidence_aggregate_immutable_v1 BEFORE UPDATE OR DELETE ON learning_evidence_aggregates_v1
FOR EACH ROW EXECUTE FUNCTION prevent_learning_evidence_manifest_mutation_v1();

CREATE OR REPLACE FUNCTION require_manifest_construction_transaction_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token UUID;
    v_header RECORD;
    v_instance TEXT;
    v_environment TEXT;
    v_deployment TEXT;
    v_registry_environment TEXT;
BEGIN
    BEGIN
        v_token := nullif(current_setting(
            'waltrade.learning_manifest_construction_token', true), '')::UUID;
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'INVALID_LEARNING_MANIFEST_CONSTRUCTION_TOKEN';
    END;
    IF v_token IS NULL THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_CONSTRUCTION_TOKEN_REQUIRED';
    END IF;
    SELECT deployment_instance_id,environment,deployment_id
      INTO v_instance,v_environment,v_deployment
      FROM learning_evidence_runtime_identity_v1();
    SELECT * INTO v_header FROM learning_evidence_manifests_v1
     WHERE evidence_manifest_id = NEW.evidence_manifest_id;
    IF NOT FOUND OR v_header.manifest_status <> 'BUILDING'
       OR v_header.construction_token <> v_token
       OR v_header.deployment_instance_id <> v_instance
       OR v_header.environment <> v_environment
       OR v_header.deployment_id <> v_deployment
    THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_CONSTRUCTION_CAPABILITY_MISMATCH';
    END IF;
    IF TG_TABLE_NAME = 'learning_evidence_membership_v1' THEN
        IF NEW.entry_timestamp > v_header.evidence_cutoff_at
           OR NEW.exit_timestamp > v_header.evidence_cutoff_at
           OR NEW.outcome_timestamp > v_header.evidence_cutoff_at
        THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_AFTER_CUTOFF manifest=% decision=%',
                NEW.evidence_manifest_id,NEW.decision_key;
        END IF;
        v_registry_environment := CASE v_environment
            WHEN 'live' THEN 'trading_live' WHEN 'paper' THEN 'trading_paper'
        END;
        IF NEW.decision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM decision_registry_v1 r
             WHERE r.decision_id=NEW.decision_id
               AND r.environment=v_registry_environment
        ) THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_CROSS_DEPLOYMENT_DECISION manifest=% decision=%',
                NEW.evidence_manifest_id,NEW.decision_key;
        END IF;
        IF NEW.pnl_available <> (NEW.realized_pnl_usdc IS NOT NULL)
           OR NEW.fees_available <> (NEW.fees_usdc IS NOT NULL)
           OR NEW.mfe_available <> (NEW.mfe_pct IS NOT NULL)
           OR NEW.mae_available <> (NEW.mae_pct IS NOT NULL)
           OR NEW.regime_available <> (NEW.regime_identity IS NOT NULL)
        THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_COVERAGE_MISMATCH manifest=% decision=%',
                NEW.evidence_manifest_id,NEW.decision_key;
        END IF;
        IF NEW.row_fingerprint <> encode(digest(jsonb_build_object(
             'decision_key',NEW.decision_key,'decision_id',NEW.decision_id,
             'position_id',NEW.position_id,'entry_timestamp',NEW.entry_timestamp,
             'exit_timestamp',NEW.exit_timestamp,'outcome_timestamp',NEW.outcome_timestamp,
             'realized_pnl_usdc',NEW.realized_pnl_usdc,'fees_usdc',NEW.fees_usdc,
             'gross_pnl_usdc',NEW.gross_pnl_usdc,'mfe_pct',NEW.mfe_pct,
             'mae_pct',NEW.mae_pct,'regime_identity',NEW.regime_identity)::text,'sha256'),'hex')
        THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_ROW_FINGERPRINT_MISMATCH manifest=% decision=%',
                NEW.evidence_manifest_id,NEW.decision_key;
        END IF;
    ELSIF TG_TABLE_NAME = 'learning_evidence_aggregates_v1' THEN
        IF NEW.aggregate_hash <> encode(digest(NEW.aggregate_payload::text,'sha256'),'hex') THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_AGGREGATE_HASH_MISMATCH manifest=%',
                NEW.evidence_manifest_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS learning_evidence_membership_same_tx_v1 ON learning_evidence_membership_v1;
CREATE TRIGGER learning_evidence_membership_same_tx_v1 BEFORE INSERT ON learning_evidence_membership_v1
FOR EACH ROW EXECUTE FUNCTION require_manifest_construction_transaction_v1();
DROP TRIGGER IF EXISTS learning_evidence_aggregate_same_tx_v1 ON learning_evidence_aggregates_v1;
CREATE TRIGGER learning_evidence_aggregate_same_tx_v1 BEFORE INSERT ON learning_evidence_aggregates_v1
FOR EACH ROW EXECUTE FUNCTION require_manifest_construction_transaction_v1();

CREATE OR REPLACE FUNCTION validate_complete_learning_evidence_manifest_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_status TEXT;
    v_expected_count INTEGER;
    v_expected_manifest_hash TEXT;
    v_expected_aggregate_hash TEXT;
    v_children INTEGER;
    v_aggregates INTEGER;
    v_manifest_hash TEXT;
    v_aggregate_hash TEXT;
BEGIN
    SELECT manifest_status,evidence_decision_count,manifest_hash,aggregate_hash
      INTO v_status,v_expected_count,v_expected_manifest_hash,v_expected_aggregate_hash
      FROM learning_evidence_manifests_v1
     WHERE evidence_manifest_id=NEW.evidence_manifest_id;
    IF v_status = 'LEGACY_AGGREGATE_ONLY' THEN RETURN NULL; END IF;
    IF v_status <> 'COMPLETE' THEN
        RAISE EXCEPTION 'UNFINALIZED_LEARNING_EVIDENCE_MANIFEST id=% status=%',
            NEW.evidence_manifest_id,v_status;
    END IF;
    SELECT count(*),encode(digest(COALESCE(string_agg(
               jsonb_build_array(decision_key,decision_id)::text,E'\n' ORDER BY decision_key),''),'sha256'),'hex')
      INTO v_children,v_manifest_hash FROM learning_evidence_membership_v1
     WHERE evidence_manifest_id=NEW.evidence_manifest_id;
    SELECT count(*),min(aggregate_hash)
      INTO v_aggregates,v_aggregate_hash FROM learning_evidence_aggregates_v1
     WHERE evidence_manifest_id=NEW.evidence_manifest_id;
    IF v_children <> v_expected_count OR v_aggregates <> 1
       OR v_manifest_hash IS DISTINCT FROM v_expected_manifest_hash
       OR v_aggregate_hash IS DISTINCT FROM v_expected_aggregate_hash
    THEN
        RAISE EXCEPTION 'INCOMPLETE_LEARNING_EVIDENCE_MANIFEST id=% header_count=% child_count=% aggregate_count=% manifest_match=% aggregate_match=%',
          NEW.evidence_manifest_id,v_expected_count,v_children,v_aggregates,
          v_manifest_hash IS NOT DISTINCT FROM v_expected_manifest_hash,
          v_aggregate_hash IS NOT DISTINCT FROM v_expected_aggregate_hash;
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS learning_evidence_complete_deferred_v1 ON learning_evidence_manifests_v1;
CREATE CONSTRAINT TRIGGER learning_evidence_complete_deferred_v1
AFTER INSERT ON learning_evidence_manifests_v1 DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_complete_learning_evidence_manifest_v1();

CREATE OR REPLACE FUNCTION finalize_learning_evidence_manifest_v1(p_manifest_id UUID)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_header RECORD;
    v_token UUID;
    v_children INTEGER;
    v_aggregates INTEGER;
    v_manifest_hash TEXT;
    v_aggregate_hash TEXT;
BEGIN
    BEGIN
        v_token := nullif(current_setting(
            'waltrade.learning_manifest_construction_token', true), '')::UUID;
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'INVALID_LEARNING_MANIFEST_CONSTRUCTION_TOKEN';
    END;
    SELECT * INTO v_header FROM learning_evidence_manifests_v1
     WHERE evidence_manifest_id=p_manifest_id FOR UPDATE;
    IF NOT FOUND OR v_header.manifest_status <> 'BUILDING'
       OR v_token IS NULL OR v_token <> v_header.construction_token
    THEN
        RAISE EXCEPTION 'LEARNING_MANIFEST_FINALIZATION_CAPABILITY_REQUIRED';
    END IF;
    SELECT count(*),encode(digest(COALESCE(string_agg(
               jsonb_build_array(decision_key,decision_id)::text,E'\n' ORDER BY decision_key),''),'sha256'),'hex')
      INTO v_children,v_manifest_hash FROM learning_evidence_membership_v1
     WHERE evidence_manifest_id=p_manifest_id;
    SELECT count(*),min(aggregate_hash)
      INTO v_aggregates,v_aggregate_hash FROM learning_evidence_aggregates_v1
     WHERE evidence_manifest_id=p_manifest_id;
    IF v_children <> v_header.evidence_decision_count OR v_aggregates <> 1
       OR v_manifest_hash IS DISTINCT FROM v_header.manifest_hash
       OR v_aggregate_hash IS DISTINCT FROM v_header.aggregate_hash
    THEN
        RAISE EXCEPTION 'INCOMPLETE_LEARNING_EVIDENCE_MANIFEST id=% header_count=% child_count=% aggregate_count=% manifest_match=% aggregate_match=%',
          p_manifest_id,v_header.evidence_decision_count,v_children,v_aggregates,
          v_manifest_hash IS NOT DISTINCT FROM v_header.manifest_hash,
          v_aggregate_hash IS NOT DISTINCT FROM v_header.aggregate_hash;
    END IF;
    UPDATE learning_evidence_manifests_v1 SET manifest_status='COMPLETE'
     WHERE evidence_manifest_id=p_manifest_id;
END;
$$;

CREATE OR REPLACE FUNCTION capture_learning_evidence_manifests_v1(p_feedback_run_id BIGINT)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_run RECORD;
    v_observation RECORD;
    v_existing RECORD;
    v_manifest_id UUID;
    v_construction_token UUID;
    v_manifest_hash TEXT;
    v_aggregate_hash TEXT;
    v_aggregate JSONB;
    v_selection JSONB;
    v_source_count INTEGER;
    v_inserted_count INTEGER;
    v_created INTEGER := 0;
    v_deployment_id TEXT;
    v_deployment_instance_id TEXT;
    v_environment TEXT;
    v_registry_deployment_id TEXT;
    v_registry_environment TEXT;
    v_evidence_cutoff_at TIMESTAMPTZ;
BEGIN
    SELECT r.*, vr.id AS validation_run_id
      INTO v_run
      FROM learning_feedback_refresh_runs_v1 r
      LEFT JOIN learning_proposal_validation_runs_v1 vr ON vr.refresh_run_id = r.id
     WHERE r.id = p_feedback_run_id AND r.status = 'OK';
    IF NOT FOUND THEN RAISE EXCEPTION 'feedback run % is missing or not OK', p_feedback_run_id; END IF;
    v_evidence_cutoff_at := COALESCE(v_run.started_at,v_run.requested_at);
    SELECT deployment_instance_id,environment,deployment_id
      INTO v_deployment_instance_id,v_environment,v_deployment_id
      FROM learning_evidence_runtime_identity_v1();
    v_registry_environment := CASE v_environment
        WHEN 'live' THEN 'trading_live' WHEN 'paper' THEN 'trading_paper'
    END;
    IF v_run.environment <> v_registry_environment THEN
        RAISE EXCEPTION 'LEARNING_EVIDENCE_RUNTIME_DATABASE_MISMATCH runtime=% source=%',
            v_environment, v_run.environment;
    END IF;
    SELECT CASE WHEN count(DISTINCT deployment_id)=1 THEN min(deployment_id) END
      INTO v_registry_deployment_id FROM decision_registry_v1
     WHERE environment=v_registry_environment;
    IF v_registry_deployment_id IS NULL THEN
        RAISE EXCEPTION 'AMBIGUOUS_OR_MISSING_LEGACY_REGISTRY_PROVENANCE environment=%',
            v_registry_environment;
    END IF;

    FOR v_observation IN
        SELECT o.*, s.sample_from, s.sample_to, s.wins AS source_wins,
               s.losses AS source_losses, s.breakeven AS source_breakeven,
               s.gross_profit_usdc AS source_gross_profit_usdc,
               s.gross_loss_usdc AS source_gross_loss_usdc,
               s.net_pnl_usdc AS source_net_pnl_usdc,
               s.profit_factor AS source_profit_factor,
               s.expectancy_usdc AS source_expectancy_usdc,
               s.win_rate_pct AS source_win_rate_pct
          FROM learning_proposal_observations_v1 o
          JOIN learning_slot_statistics_v1 s USING (environment, symbol, interval, strategy, window_days)
         WHERE o.refresh_run_id = p_feedback_run_id
         ORDER BY o.symbol, o.interval, o.strategy, o.window_days
    LOOP
        IF v_observation.environment <> v_registry_environment THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_OBSERVATION_CONTEXT_MISMATCH runtime=% observation=%',
                v_registry_environment,v_observation.environment;
        END IF;
        WITH normalized AS (
            SELECT u.decision_key, u.decision_id, u.position_id,
                   u.entry_time, u.exit_time, u.outcome_timestamp,
                   u.realized_pnl_usdc, u.fees_usdc, u.gross_pnl_usdc,
                   u.mfe_pct, u.mae_pct, u.regime_identity, u.regime_context,
                   'learning_canonical_evidence_universe_v1'::TEXT AS source_table,
                   'CANONICAL_DECISION_SOURCE_UNIVERSE_V1'::TEXT AS source_version
              FROM learning_canonical_evidence_universe_v1(
                   v_observation.environment, v_observation.sample_from,
                   v_observation.sample_to, v_evidence_cutoff_at) u
             WHERE u.symbol = v_observation.symbol
               AND u.interval = v_observation.interval
               AND u.strategy = v_observation.strategy
               AND u.eligibility_reason = 'ELIGIBLE'
        ), running AS (
            SELECT n.*, SUM(realized_pnl_usdc) OVER (
                ORDER BY outcome_timestamp NULLS LAST, decision_key
            ) AS equity FROM normalized n
        ), enriched AS (
            SELECT r.*, MAX(equity) OVER (
                ORDER BY outcome_timestamp NULLS LAST, decision_key
            ) - equity AS drawdown FROM running r
        ), regimes AS (
            SELECT COALESCE(jsonb_object_agg(regime_identity, count_value ORDER BY regime_identity),'{}'::jsonb) value
              FROM (SELECT regime_identity, count(*) count_value FROM enriched WHERE regime_identity IS NOT NULL GROUP BY regime_identity) x
        ), agg AS (
            SELECT count(*)::INTEGER decisions,
                   count(*) FILTER (WHERE realized_pnl_usdc>0)::INTEGER wins,
                   count(*) FILTER (WHERE realized_pnl_usdc<0)::INTEGER losses,
                   count(*) FILTER (WHERE realized_pnl_usdc=0)::INTEGER breakeven,
                   sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0) gross_profit,
                   sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0) gross_loss,
                   sum(realized_pnl_usdc) net_pnl, avg(realized_pnl_usdc) expectancy,
                   learning_canonical_profit_factor_v1(
                     count(*)::INTEGER,
                     count(realized_pnl_usdc)::INTEGER,
                     sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0),
                     sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0)
                   ) profit_factor,
                   100.0*count(*) FILTER (WHERE realized_pnl_usdc>0)/NULLIF(count(*),0) win_rate,
                   sum(fees_usdc) fees, max(drawdown) max_drawdown, avg(mfe_pct) mfe_avg,
                   max(mfe_pct) mfe_max, avg(mae_pct) mae_avg, min(mae_pct) mae_min,
                   count(realized_pnl_usdc)::INTEGER pnl_cov, count(fees_usdc)::INTEGER fees_cov,
                   count(mfe_pct)::INTEGER mfe_cov, count(mae_pct)::INTEGER mae_cov,
                   count(regime_identity)::INTEGER regime_cov
              FROM enriched
        )
        SELECT a.decisions,
               encode(digest(COALESCE((SELECT string_agg(jsonb_build_array(decision_key,decision_id)::text,E'\n' ORDER BY decision_key) FROM normalized),''),'sha256'),'hex'),
               jsonb_build_object('decisions',a.decisions,'wins',a.wins,'losses',a.losses,'breakeven',a.breakeven,
                 'gross_profit_usdc',a.gross_profit,'gross_loss_usdc',a.gross_loss,'net_pnl_usdc',a.net_pnl,
                 'expectancy_usdc',a.expectancy,'profit_factor',a.profit_factor,'win_rate_pct',a.win_rate,
                 'fees_usdc',a.fees,'max_drawdown_usdc',a.max_drawdown,'mfe_average_pct',a.mfe_avg,
                 'mfe_max_pct',a.mfe_max,'mae_average_pct',a.mae_avg,'mae_min_pct',a.mae_min,
                 'regime_distribution',regimes.value,'pnl_coverage_count',a.pnl_cov,'fees_coverage_count',a.fees_cov,
                 'mfe_coverage_count',a.mfe_cov,'mae_coverage_count',a.mae_cov,'regime_coverage_count',a.regime_cov,
                 'missing_pnl_count',a.decisions-a.pnl_cov,'missing_fees_count',a.decisions-a.fees_cov,
                 'missing_mfe_count',a.decisions-a.mfe_cov,'missing_mae_count',a.decisions-a.mae_cov,
                 'missing_regime_count',a.decisions-a.regime_cov)
          INTO v_source_count, v_manifest_hash, v_aggregate
          FROM agg a CROSS JOIN regimes;

        SELECT jsonb_build_object(
                   'source_candidates', count(*),
                   'canonical_eligible', count(*) FILTER (
                       WHERE eligibility_reason='ELIGIBLE'),
                   'excluded_missing_registry', count(*) FILTER (
                       WHERE eligibility_reason='EXCLUDED_MISSING_REGISTRY'),
                   'excluded_missing_outcome', count(*) FILTER (
                       WHERE eligibility_reason='EXCLUDED_MISSING_OUTCOME'),
                   'excluded_conflicting_identity', count(*) FILTER (
                       WHERE eligibility_reason IN (
                           'EXCLUDED_CONFLICTING_IDENTITY',
                           'EXCLUDED_CROSS_DEPLOYMENT')),
                   'excluded_conflicting_outcome', count(*) FILTER (
                       WHERE eligibility_reason IN (
                           'EXCLUDED_CONFLICTING_OUTCOME',
                           'EXCLUDED_CONFLICTING_PNL')),
                   'excluded_post_cutoff', count(*) FILTER (
                       WHERE eligibility_reason='EXCLUDED_POST_CUTOFF'),
                   'excluded_chronology', count(*) FILTER (
                       WHERE eligibility_reason='EXCLUDED_CHRONOLOGY'),
                   'excluded_other_reason', count(*) FILTER (
                       WHERE eligibility_reason NOT IN (
                           'ELIGIBLE','EXCLUDED_MISSING_REGISTRY',
                           'EXCLUDED_MISSING_OUTCOME',
                           'EXCLUDED_CONFLICTING_IDENTITY',
                           'EXCLUDED_CROSS_DEPLOYMENT',
                           'EXCLUDED_CONFLICTING_OUTCOME',
                           'EXCLUDED_CONFLICTING_PNL',
                           'EXCLUDED_POST_CUTOFF',
                           'EXCLUDED_CHRONOLOGY')),
                   'source_universe_hash', encode(digest(COALESCE(
                       string_agg(jsonb_build_array(
                           decision_key, eligibility_reason,
                           decision_id)::TEXT, E'\n' ORDER BY decision_key),
                       ''), 'sha256'), 'hex'))
          INTO v_selection
          FROM learning_canonical_evidence_universe_v1(
               v_observation.environment, v_observation.sample_from,
               v_observation.sample_to, v_evidence_cutoff_at)
         WHERE symbol = v_observation.symbol
           AND interval = v_observation.interval
           AND strategy = v_observation.strategy;

        INSERT INTO learning_canonical_evidence_selection_v1 (
            feedback_run_id, environment, symbol, interval, strategy,
            window_days, evidence_cutoff_at, source_candidate_count,
            canonical_eligible_count, excluded_missing_registry,
            excluded_missing_outcome, excluded_conflicting_identity,
            excluded_conflicting_outcome, excluded_post_cutoff,
            excluded_chronology, excluded_other_reason, source_universe_hash
        ) VALUES (
            p_feedback_run_id, v_observation.environment,
            v_observation.symbol, v_observation.interval,
            v_observation.strategy, v_observation.window_days,
            v_evidence_cutoff_at,
            (v_selection->>'source_candidates')::INTEGER,
            (v_selection->>'canonical_eligible')::INTEGER,
            (v_selection->>'excluded_missing_registry')::INTEGER,
            (v_selection->>'excluded_missing_outcome')::INTEGER,
            (v_selection->>'excluded_conflicting_identity')::INTEGER,
            (v_selection->>'excluded_conflicting_outcome')::INTEGER,
            (v_selection->>'excluded_post_cutoff')::INTEGER,
            (v_selection->>'excluded_chronology')::INTEGER,
            (v_selection->>'excluded_other_reason')::INTEGER,
            v_selection->>'source_universe_hash'
        ) ON CONFLICT DO NOTHING;

        IF NOT EXISTS (
            SELECT 1 FROM learning_canonical_evidence_selection_v1 t
             WHERE t.feedback_run_id=p_feedback_run_id
               AND t.symbol=v_observation.symbol
               AND t.interval=v_observation.interval
               AND t.strategy=v_observation.strategy
               AND t.window_days=v_observation.window_days
               AND t.source_universe_hash=
                   v_selection->>'source_universe_hash'
               AND t.source_candidate_count=
                   (v_selection->>'source_candidates')::INTEGER
               AND t.canonical_eligible_count=
                   (v_selection->>'canonical_eligible')::INTEGER
        ) THEN
            RAISE EXCEPTION
                'LEARNING_CANONICAL_SOURCE_UNIVERSE_CONFLICT run=% slot=%/%/%',
                p_feedback_run_id, v_observation.symbol,
                v_observation.interval, v_observation.strategy;
        END IF;

        IF v_source_count <> v_observation.evidence_decisions THEN
            IF EXISTS (
                SELECT 1 FROM learning_evidence_manifests_v1 m
                 WHERE m.deployment_id=v_deployment_id
                   AND m.environment=v_environment
                   AND m.feedback_run_id=p_feedback_run_id
                   AND m.symbol=v_observation.symbol
                   AND m.interval=v_observation.interval
                   AND m.strategy=v_observation.strategy
                   AND m.window_days=v_observation.window_days
            ) THEN
                RAISE EXCEPTION 'LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT run=% slot=%/%/%',
                    p_feedback_run_id,v_observation.symbol,v_observation.interval,
                    v_observation.strategy;
            END IF;
            RAISE EXCEPTION 'LEARNING_EVIDENCE_COUNT_MISMATCH run=% slot=%/%/% expected=% actual=%',
                p_feedback_run_id,v_observation.symbol,v_observation.interval,v_observation.strategy,
                v_observation.evidence_decisions,v_source_count;
        END IF;
        IF (v_aggregate->>'wins')::INTEGER <> v_observation.source_wins
           OR (v_aggregate->>'losses')::INTEGER <> v_observation.source_losses
           OR (v_aggregate->>'breakeven')::INTEGER <> v_observation.source_breakeven
           OR (v_aggregate->>'gross_profit_usdc')::NUMERIC
                IS DISTINCT FROM v_observation.source_gross_profit_usdc
           OR (v_aggregate->>'gross_loss_usdc')::NUMERIC
                IS DISTINCT FROM v_observation.source_gross_loss_usdc
           OR (v_aggregate->>'net_pnl_usdc')::NUMERIC
                IS DISTINCT FROM v_observation.source_net_pnl_usdc
           OR round((v_aggregate->>'profit_factor')::NUMERIC,12)
                IS DISTINCT FROM round(v_observation.source_profit_factor,12)
           OR round((v_aggregate->>'expectancy_usdc')::NUMERIC,12)
                IS DISTINCT FROM round(v_observation.source_expectancy_usdc,12)
           OR round((v_aggregate->>'win_rate_pct')::NUMERIC,4)
                IS DISTINCT FROM v_observation.source_win_rate_pct
        THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_AGGREGATE_PARITY_MISMATCH run=% slot=%/%/% aggregate=% source=%',
                p_feedback_run_id,v_observation.symbol,v_observation.interval,
                v_observation.strategy,v_aggregate,
                jsonb_build_object('wins',v_observation.source_wins,
                  'losses',v_observation.source_losses,
                  'breakeven',v_observation.source_breakeven,
                  'gross_profit_usdc',v_observation.source_gross_profit_usdc,
                  'gross_loss_usdc',v_observation.source_gross_loss_usdc,
                  'net_pnl_usdc',v_observation.source_net_pnl_usdc,
                  'profit_factor',v_observation.source_profit_factor,
                  'expectancy_usdc',v_observation.source_expectancy_usdc,
                  'win_rate_pct',v_observation.source_win_rate_pct);
        END IF;
        v_aggregate_hash := encode(digest(v_aggregate::text,'sha256'),'hex');

        SELECT m.* INTO v_existing FROM learning_evidence_manifests_v1 m
         WHERE m.deployment_id=v_deployment_id AND m.environment=v_environment
           AND m.feedback_run_id=p_feedback_run_id
           AND m.validation_run_id IS NOT DISTINCT FROM v_run.validation_run_id
           AND m.symbol=v_observation.symbol AND m.interval=v_observation.interval
           AND m.strategy=v_observation.strategy AND m.window_days=v_observation.window_days;
        IF FOUND THEN
            IF v_existing.manifest_status='BUILDING' THEN
                RAISE EXCEPTION 'STALE_BUILDING_LEARNING_EVIDENCE_MANIFEST id=%',
                    v_existing.evidence_manifest_id;
            END IF;
            IF v_existing.manifest_status<>'COMPLETE'
               OR NOT v_existing.exact_membership_available
               OR v_existing.evidence_window_start IS DISTINCT FROM v_observation.sample_from
               OR v_existing.evidence_window_end IS DISTINCT FROM v_observation.sample_to
               OR v_existing.source_snapshot_at IS DISTINCT FROM v_evidence_cutoff_at
               OR v_existing.evidence_cutoff_at IS DISTINCT FROM v_evidence_cutoff_at
               OR v_existing.evidence_decision_count<>v_source_count
               OR v_existing.manifest_hash<>v_manifest_hash
               OR v_existing.aggregate_hash<>v_aggregate_hash
               OR (SELECT count(*) FROM learning_evidence_membership_v1 c
                    WHERE c.evidence_manifest_id=v_existing.evidence_manifest_id)<>v_source_count
               OR (SELECT count(*) FROM learning_evidence_aggregates_v1 a
                    WHERE a.evidence_manifest_id=v_existing.evidence_manifest_id)<>1
               OR (SELECT encode(digest(COALESCE(string_agg(
                      jsonb_build_array(c.decision_key,c.decision_id)::text,E'\n'
                      ORDER BY c.decision_key),''),'sha256'),'hex')
                     FROM learning_evidence_membership_v1 c
                    WHERE c.evidence_manifest_id=v_existing.evidence_manifest_id)<>v_manifest_hash
               OR (SELECT a.aggregate_hash FROM learning_evidence_aggregates_v1 a
                    WHERE a.evidence_manifest_id=v_existing.evidence_manifest_id)<>v_aggregate_hash
            THEN
                RAISE EXCEPTION 'LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT id=%',
                    v_existing.evidence_manifest_id;
            END IF;
            CONTINUE;
        END IF;

        v_manifest_id := gen_random_uuid();
        v_construction_token := gen_random_uuid();
        PERFORM set_config('waltrade.learning_manifest_construction_token',
            v_construction_token::TEXT,true);
        PERFORM set_config('waltrade.learning_manifest_capture_api_token',
            v_construction_token::TEXT,true);

        INSERT INTO learning_evidence_manifests_v1
          (evidence_manifest_id,deployment_id,deployment_instance_id,environment,
           feedback_run_id,validation_run_id,shadow_recommendation_id,symbol,interval,
           strategy,window_days,proposal_action,validation_status,manifest_status,
           construction_token,exact_membership_available,evidence_window_start,
           evidence_window_end,source_snapshot_at,evidence_cutoff_at,
           evidence_decision_count,manifest_hash,
           aggregate_hash,engine_version,validation_version,created_at)
        VALUES (v_manifest_id,v_deployment_id,v_deployment_instance_id,v_environment,
            p_feedback_run_id,v_run.validation_run_id,NULL,v_observation.symbol,
            v_observation.interval,v_observation.strategy,v_observation.window_days,
            v_observation.proposal_action,'OBSERVED','BUILDING',v_construction_token,true,
            v_observation.sample_from,v_observation.sample_to,
            v_evidence_cutoff_at,v_evidence_cutoff_at,
            v_source_count,v_manifest_hash,v_aggregate_hash,v_run.engine_version,
            'LEARNING_FEEDBACK_VALIDATION_V1_3',clock_timestamp());

        WITH normalized AS (
            SELECT u.decision_key,u.decision_id,u.position_id,u.entry_time,
                   u.exit_time,u.outcome_timestamp,u.realized_pnl_usdc,
                   u.fees_usdc,u.gross_pnl_usdc,u.mfe_pct,u.mae_pct,
                   u.regime_identity AS market_regime
              FROM learning_canonical_evidence_universe_v1(
                   v_observation.environment, v_observation.sample_from,
                   v_observation.sample_to, v_evidence_cutoff_at) u
             WHERE u.symbol=v_observation.symbol
               AND u.interval=v_observation.interval
               AND u.strategy=v_observation.strategy
               AND u.eligibility_reason='ELIGIBLE'
        )
        INSERT INTO learning_evidence_membership_v1
        SELECT v_manifest_id,row_number() OVER(ORDER BY decision_key),decision_key,decision_id,position_id,
               entry_time,exit_time,outcome_timestamp,realized_pnl_usdc,fees_usdc,gross_pnl_usdc,mfe_pct,mae_pct,
               market_regime,CASE WHEN market_regime IS NULL THEN NULL ELSE jsonb_build_object('market_regime',market_regime) END,
               'learning_canonical_evidence_universe_v1','CANONICAL_DECISION_SOURCE_UNIVERSE_V1',realized_pnl_usdc IS NOT NULL,
               fees_usdc IS NOT NULL,mfe_pct IS NOT NULL,mae_pct IS NOT NULL,market_regime IS NOT NULL,
               encode(digest(jsonb_build_object('decision_key',decision_key,'decision_id',decision_id,'position_id',position_id,
                 'entry_timestamp',entry_time,'exit_timestamp',exit_time,'outcome_timestamp',outcome_timestamp,
                 'realized_pnl_usdc',realized_pnl_usdc,'fees_usdc',fees_usdc,'gross_pnl_usdc',gross_pnl_usdc,
                 'mfe_pct',mfe_pct,'mae_pct',mae_pct,'regime_identity',market_regime)::text,'sha256'),'hex'),clock_timestamp()
          FROM normalized ORDER BY decision_key;
        GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
        IF v_inserted_count <> v_source_count THEN
            RAISE EXCEPTION 'LEARNING_EVIDENCE_CHILD_COUNT_MISMATCH expected=% inserted=%',v_source_count,v_inserted_count;
        END IF;

        INSERT INTO learning_evidence_aggregates_v1
        SELECT v_manifest_id,(v_aggregate->>'decisions')::int,(v_aggregate->>'wins')::int,
          (v_aggregate->>'losses')::int,(v_aggregate->>'breakeven')::int,
          (v_aggregate->>'gross_profit_usdc')::numeric,(v_aggregate->>'gross_loss_usdc')::numeric,
          (v_aggregate->>'net_pnl_usdc')::numeric,(v_aggregate->>'expectancy_usdc')::numeric,
          (v_aggregate->>'profit_factor')::numeric,(v_aggregate->>'win_rate_pct')::numeric,
          (v_aggregate->>'fees_usdc')::numeric,(v_aggregate->>'max_drawdown_usdc')::numeric,
          (v_aggregate->>'mfe_average_pct')::numeric,(v_aggregate->>'mfe_max_pct')::numeric,
          (v_aggregate->>'mae_average_pct')::numeric,(v_aggregate->>'mae_min_pct')::numeric,
          v_aggregate->'regime_distribution',(v_aggregate->>'pnl_coverage_count')::int,
          (v_aggregate->>'fees_coverage_count')::int,(v_aggregate->>'mfe_coverage_count')::int,
          (v_aggregate->>'mae_coverage_count')::int,(v_aggregate->>'regime_coverage_count')::int,
          (v_aggregate->>'missing_pnl_count')::int,(v_aggregate->>'missing_fees_count')::int,
          (v_aggregate->>'missing_mfe_count')::int,(v_aggregate->>'missing_mae_count')::int,
          (v_aggregate->>'missing_regime_count')::int,v_aggregate,v_aggregate_hash,clock_timestamp();
        PERFORM finalize_learning_evidence_manifest_v1(v_manifest_id);
        PERFORM set_config('waltrade.learning_manifest_construction_token','',true);
        PERFORM set_config('waltrade.learning_manifest_capture_api_token','',true);
        v_created := v_created + 1;
    END LOOP;
    RETURN jsonb_build_object('status','ok','feedback_run_id',p_feedback_run_id,'manifests_created',v_created);
END;
$$;

CREATE OR REPLACE FUNCTION trigger_capture_learning_evidence_manifests_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status='OK' AND OLD.status IS DISTINCT FROM 'OK' THEN
        PERFORM capture_learning_evidence_manifests_v1(NEW.id);
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_zz_learning_evidence_manifest_v1 ON learning_feedback_refresh_runs_v1;
CREATE TRIGGER trg_zz_learning_evidence_manifest_v1 AFTER UPDATE OF status ON learning_feedback_refresh_runs_v1
FOR EACH ROW EXECUTE FUNCTION trigger_capture_learning_evidence_manifests_v1();

CREATE OR REPLACE FUNCTION require_complete_learning_evidence_manifest_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_deployment_id TEXT;
    v_deployment_instance_id TEXT;
    v_environment TEXT;
    v_source_environment TEXT;
BEGIN
    SELECT deployment_instance_id,environment,deployment_id
      INTO STRICT v_deployment_instance_id,v_environment,v_deployment_id
      FROM learning_evidence_runtime_identity_v1();
    v_source_environment := CASE v_environment
        WHEN 'live' THEN 'trading_live' WHEN 'paper' THEN 'trading_paper'
    END;
    IF NEW.environment <> v_source_environment THEN
        RAISE EXCEPTION 'LEARNING_EVIDENCE_PUBLISHER_CONTEXT_MISMATCH runtime=% source=%',
            v_environment,NEW.environment;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM learning_evidence_manifests_v1 m
        WHERE m.feedback_run_id=NEW.source_refresh_run_id
          AND m.deployment_id=v_deployment_id AND m.environment=v_environment
          AND m.symbol=NEW.symbol AND m.interval=NEW.interval AND m.strategy=NEW.strategy
          AND m.manifest_status='COMPLETE' AND m.exact_membership_available
          AND (SELECT count(*) FROM learning_evidence_membership_v1 c
                WHERE c.evidence_manifest_id=m.evidence_manifest_id)=m.evidence_decision_count
          AND EXISTS (SELECT 1 FROM learning_evidence_aggregates_v1 a
                WHERE a.evidence_manifest_id=m.evidence_manifest_id AND a.aggregate_hash=m.aggregate_hash)) THEN
        RAISE EXCEPTION 'COMPLETE_LEARNING_EVIDENCE_MANIFEST_REQUIRED run=% slot=%/%/%',
          NEW.source_refresh_run_id,NEW.symbol,NEW.interval,NEW.strategy;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_learning_shadow_manifest_required_v1 ON learning_shadow_confidence_proposals_v1;
CREATE TRIGGER trg_learning_shadow_manifest_required_v1 BEFORE INSERT OR UPDATE ON learning_shadow_confidence_proposals_v1
FOR EACH ROW EXECUTE FUNCTION require_complete_learning_evidence_manifest_v1();

-- Preserve historical aggregates explicitly; never infer their missing membership.
WITH runtime_identity AS (
    SELECT deployment_instance_id,environment,deployment_id
      FROM learning_evidence_runtime_identity_v1()
)
INSERT INTO learning_evidence_manifests_v1
  (evidence_manifest_id,deployment_id,deployment_instance_id,environment,
   feedback_run_id,validation_run_id,shadow_recommendation_id,symbol,interval,
   strategy,window_days,proposal_action,validation_status,manifest_status,
   construction_token,exact_membership_available,evidence_window_start,
   evidence_window_end,source_snapshot_at,evidence_cutoff_at,
   evidence_decision_count,manifest_hash,
   aggregate_hash,engine_version,validation_version,created_at)
SELECT gen_random_uuid(),i.deployment_id,i.deployment_instance_id,i.environment,
       o.refresh_run_id,vr.id,NULL,o.symbol,o.interval,o.strategy,o.window_days,
       o.proposal_action,'OBSERVED','LEGACY_AGGREGATE_ONLY',NULL,false,
       NULL,NULL,NULL,COALESCE(r.finished_at,o.observed_at),o.evidence_decisions,
       encode(digest('LEGACY_AGGREGATE_ONLY|'||o.refresh_run_id||'|'||o.proposal_key,'sha256'),'hex'),
       encode(digest(jsonb_build_object('evidence_decisions',o.evidence_decisions,'net_pnl_usdc',o.evidence_net_pnl_usdc,
         'profit_factor',o.evidence_profit_factor,'win_rate_pct',o.evidence_win_rate_pct,
         'context_coverage_pct',o.evidence_context_coverage_pct)::text,'sha256'),'hex'),
       r.engine_version,'LEARNING_FEEDBACK_VALIDATION_V1_3',o.observed_at
  FROM learning_proposal_observations_v1 o
  JOIN learning_feedback_refresh_runs_v1 r ON r.id=o.refresh_run_id
  LEFT JOIN learning_proposal_validation_runs_v1 vr ON vr.refresh_run_id=o.refresh_run_id
  CROSS JOIN runtime_identity i
 WHERE o.environment = CASE i.environment
        WHEN 'live' THEN 'trading_live' WHEN 'paper' THEN 'trading_paper' END
ON CONFLICT (deployment_id,environment,feedback_run_id,symbol,interval,strategy,window_days) DO NOTHING;

COMMIT;
