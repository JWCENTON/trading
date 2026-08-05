from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2 import errors


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT
    / "db/migrations/20260805_learning_evidence_aggregate_numeric_parity_v1.sql"
).read_text()


BOOTSTRAP = r"""
CREATE EXTENSION pgcrypto;

CREATE TABLE learning_slot_statistics_v1(
    feedback_run_id BIGINT PRIMARY KEY,
    gross_profit_usdc NUMERIC(28,12),
    gross_loss_usdc NUMERIC(28,12),
    net_pnl_usdc NUMERIC(28,12),
    profit_factor NUMERIC(28,12),
    expectancy_usdc NUMERIC(28,12)
);
CREATE TABLE learning_evidence_aggregates_v1(id BIGINT PRIMARY KEY);
CREATE TABLE learning_numeric_parity_fixture_v1(
    ordinal INTEGER PRIMARY KEY,
    realized_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    regime_identity TEXT
);
CREATE TABLE learning_numeric_parity_results_v1(
    feedback_run_id BIGINT PRIMARY KEY,
    aggregate_payload JSONB NOT NULL,
    aggregate_hash TEXT NOT NULL
);
CREATE TABLE schema_migration_ledger_v1(
    ledger_id BIGSERIAL PRIMARY KEY,
    migration_id TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    applied_by TEXT NOT NULL,
    status TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    execution_duration_ms BIGINT NOT NULL,
    git_sha TEXT NOT NULL,
    error_summary TEXT,
    schema_baseline_version TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION capture_learning_evidence_manifests_v1(
    p_feedback_run_id BIGINT
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_observation RECORD;
    v_source_count INTEGER;
    v_manifest_hash TEXT;
    v_aggregate JSONB;
    v_aggregate_hash TEXT;
BEGIN
    SELECT 1 AS source_wins, 1 AS source_losses, 0 AS source_breakeven,
           gross_profit_usdc AS source_gross_profit_usdc,
           gross_loss_usdc AS source_gross_loss_usdc,
           net_pnl_usdc AS source_net_pnl_usdc,
           profit_factor AS source_profit_factor,
           expectancy_usdc AS source_expectancy_usdc,
           50.0000::NUMERIC AS source_win_rate_pct
      INTO STRICT v_observation
      FROM learning_slot_statistics_v1
     WHERE feedback_run_id = p_feedback_run_id;

    WITH normalized AS (
        SELECT ordinal::TEXT AS decision_key, realized_pnl_usdc, fees_usdc,
               mfe_pct, mae_pct, regime_identity
          FROM learning_numeric_parity_fixture_v1
    ), running AS (
        SELECT n.*, SUM(realized_pnl_usdc) OVER (ORDER BY decision_key) AS equity
          FROM normalized n
    ), enriched AS (
        SELECT r.*, MAX(equity) OVER (ORDER BY decision_key) - equity AS drawdown
          FROM running r
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
               round(sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0)
                     / abs(sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0)),12) profit_factor,
               100.0*count(*) FILTER (WHERE realized_pnl_usdc>0)/NULLIF(count(*),0) win_rate,
                   sum(fees_usdc) fees, max(drawdown) max_drawdown, avg(mfe_pct) mfe_avg,
               max(mfe_pct) mfe_max, avg(mae_pct) mae_avg, min(mae_pct) mae_min,
               count(realized_pnl_usdc)::INTEGER pnl_cov, count(fees_usdc)::INTEGER fees_cov,
               count(mfe_pct)::INTEGER mfe_cov, count(mae_pct)::INTEGER mae_cov,
               count(regime_identity)::INTEGER regime_cov
          FROM enriched
    )
    SELECT a.decisions, repeat('0',64),
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
            p_feedback_run_id,'BTCUSDC','1m','BBRANGE',v_aggregate,
            jsonb_build_object('net_pnl_usdc',v_observation.source_net_pnl_usdc);
    END IF;
    v_aggregate_hash := encode(digest(v_aggregate::text,'sha256'),'hex');
    INSERT INTO learning_numeric_parity_results_v1
    VALUES (p_feedback_run_id,v_aggregate,v_aggregate_hash);
    RETURN jsonb_build_object('status','ok','aggregate_hash',v_aggregate_hash);
END;
$$;

INSERT INTO learning_numeric_parity_fixture_v1 VALUES
    (1, 0.4130977548676, 0.0000000000004, 1, -0.2, 'RANGE'),
    (2,-0.175934918873280,0.000000000000280,2,-0.1,'RANGE');
INSERT INTO learning_slot_statistics_v1 VALUES (
    347,0.413097754868,-0.175934918873,0.237162835994,
    round(0.4130977548676 / 0.175934918873280,12),
    round(0.237162835994320 / 2,12)
);
"""


@pytest.fixture(scope="module")
def parity_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_learning_aggregate_numeric_parity_v1"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(BOOTSTRAP)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def rollback_case(parity_db):
    yield
    parity_db.rollback()


def test_exact_vps_regression_normalizes_and_hashes_identically(parity_db):
    with parity_db.cursor() as cur:
        cur.execute("SELECT capture_learning_evidence_manifests_v1(347)")
        assert cur.fetchone()[0]["status"] == "ok"
        cur.execute(
            """
            SELECT aggregate_payload, aggregate_hash,
                   encode(digest((aggregate_payload || jsonb_build_object(
                     'gross_profit_usdc',learning_financial_normalize_v1(s.gross_profit_usdc),
                     'gross_loss_usdc',learning_financial_normalize_v1(s.gross_loss_usdc),
                     'net_pnl_usdc',learning_financial_normalize_v1(s.net_pnl_usdc)
                   ))::text,'sha256'),'hex') AS source_hash
              FROM learning_numeric_parity_results_v1 r
              JOIN learning_slot_statistics_v1 s USING(feedback_run_id)
             WHERE feedback_run_id=347
            """
        )
        payload, aggregate_hash, source_hash = cur.fetchone()
        assert payload["financial_normalization_contract"] == (
            "LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1"
        )
        assert Decimal(str(payload["gross_profit_usdc"])) == Decimal("0.413097754868")
        assert Decimal(str(payload["gross_loss_usdc"])) == Decimal("-0.175934918873")
        assert Decimal(str(payload["net_pnl_usdc"])) == Decimal("0.237162835994")
        assert aggregate_hash == source_hash


def test_real_delta_after_12_places_remains_fail_closed(parity_db):
    with parity_db.cursor() as cur:
        cur.execute(
            "UPDATE learning_slot_statistics_v1 "
            "SET net_pnl_usdc=0.237162835995 WHERE feedback_run_id=347"
        )
        with pytest.raises(errors.RaiseException) as caught:
            cur.execute("SELECT capture_learning_evidence_manifests_v1(347)")
        assert "LEARNING_EVIDENCE_AGGREGATE_PARITY_MISMATCH" in str(caught.value)
    parity_db.rollback()
    with parity_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM learning_numeric_parity_results_v1")
        assert cur.fetchone()[0] == 0


def test_normalizer_is_null_safe_and_migration_ledger_is_exactly_once(parity_db):
    with parity_db.cursor() as cur:
        cur.execute(
            "SELECT learning_financial_normalize_v1(NULL::numeric), "
            "learning_financial_normalize_v1(1.0000000000005), "
            "learning_financial_normalize_v1(-1.0000000000005)"
        )
        assert cur.fetchone() == (
            None,
            Decimal("1.000000000001"),
            Decimal("-1.000000000001"),
        )
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 "
            "WHERE migration_id=%s",
            ("20260805_learning_evidence_aggregate_numeric_parity_v1.sql",),
        )
        assert cur.fetchone()[0] == 1
