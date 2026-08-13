from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT
    / "db/migrations/20260813_decision_outcome_canonical_financial_truth_source_v1.sql"
).read_text()
REPROJECTION = (
    ROOT / "tools/reproject_decision_outcomes_from_canonical_ft_v1.sql"
).read_text()


SCHEMA = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE public.positions(
    id BIGINT PRIMARY KEY,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ,
    status TEXT NOT NULL,
    qty NUMERIC,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    exit_reason TEXT
);

CREATE TABLE public.exit_trace_v1(
    position_id BIGINT PRIMARY KEY,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    giveback_pct NUMERIC
);

CREATE TABLE public.canonical_financial_truth_v1(
    position_id BIGINT PRIMARY KEY REFERENCES public.positions(id),
    financial_truth_status TEXT NOT NULL,
    authoritative_gross_pnl NUMERIC,
    authoritative_entry_fees_usdc NUMERIC,
    authoritative_exit_fees_usdc NUMERIC,
    authoritative_fees_usdc NUMERIC,
    authoritative_net_pnl NUMERIC
);

CREATE TABLE public.decision_registry_v1(
    decision_id UUID PRIMARY KEY,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    position_id BIGINT REFERENCES public.positions(id),
    decision_time TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.decision_outcomes_v1(
    outcome_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL REFERENCES public.decision_registry_v1(decision_id),
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    outcome_type TEXT NOT NULL,
    position_id BIGINT NOT NULL REFERENCES public.positions(id),
    source_table TEXT NOT NULL,
    source_id TEXT NOT NULL,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    giveback_pct NUMERIC,
    outcome_status TEXT NOT NULL,
    outcome_reason TEXT,
    evidence JSONB NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    UNIQUE(decision_id,outcome_type)
);

CREATE TABLE public.schema_migration_ledger_v1(
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

CREATE OR REPLACE FUNCTION public.refresh_decision_identity_outcome_v1(
    p_lookback_hours INTEGER,
    p_environment TEXT,
    p_deployment_id TEXT,
    p_run_id UUID DEFAULT gen_random_uuid()
)
RETURNS JSONB
LANGUAGE plpgsql
AS $function$
DECLARE
    v_count INTEGER;
BEGIN
    WITH source_outcomes AS (
        SELECT
            d.decision_id,
            d.deployment_id,
            d.environment,
            p.id AS position_id,
            p.id::TEXT AS source_id,
            p.exit_time,
            p.exit_reason,
            p.gross_pnl_usdc,
            p.fees_usdc,
            p.net_pnl_usdc,
            e.mfe_pct,
            e.mae_pct,
            e.giveback_pct,
            CASE WHEN e.position_id IS NULL THEN 'POSITIONS_ONLY'
                 ELSE 'POSITIONS_PLUS_EXIT_TRACE' END AS path_source
        FROM decision_registry_v1 d
        JOIN positions p ON p.id = d.position_id
        LEFT JOIN exit_trace_v1 e ON e.position_id = p.id
        WHERE d.environment = p_environment
          AND d.deployment_id = p_deployment_id
          AND d.decision_type = 'TRADE_EXECUTED'
          AND p.status = 'CLOSED'
          AND p.entry_time >= clock_timestamp()
              - make_interval(hours => p_lookback_hours)
    ), upserted AS (
        INSERT INTO decision_outcomes_v1 (
            outcome_id, decision_id, deployment_id, environment,
            outcome_type, position_id, source_table, source_id,
            gross_pnl_usdc, fees_usdc, net_pnl_usdc,
            mfe_pct, mae_pct, giveback_pct,
            outcome_status, outcome_reason, evidence,
            calculated_at, refreshed_at
        )
        SELECT
            gen_random_uuid(), s.decision_id, s.deployment_id, s.environment,
            'ACTUAL_TRADE', s.position_id, 'positions', s.source_id,
            s.gross_pnl_usdc, s.fees_usdc, s.net_pnl_usdc,
            s.mfe_pct, s.mae_pct, s.giveback_pct,
            CASE WHEN s.net_pnl_usdc IS NULL THEN 'PARTIAL' ELSE 'COMPLETE' END,
            CASE WHEN s.net_pnl_usdc IS NULL
                THEN 'Closed position has incomplete net PnL'
                ELSE s.exit_reason END,
            jsonb_build_object(
                'exit_time', s.exit_time,
                'exit_reason', s.exit_reason,
                'path_source', CASE WHEN s.mfe_pct IS NULL AND s.mae_pct IS NULL
                    THEN 'missing' ELSE 'exit_trace_v1' END
            ),
            clock_timestamp(), clock_timestamp()
        FROM source_outcomes s
        ON CONFLICT (decision_id,outcome_type) DO UPDATE SET
            gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
            fees_usdc = EXCLUDED.fees_usdc,
            net_pnl_usdc = EXCLUDED.net_pnl_usdc,
            outcome_status = EXCLUDED.outcome_status,
            outcome_reason = EXCLUDED.outcome_reason,
            evidence = EXCLUDED.evidence,
            refreshed_at = clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) INTO v_count FROM upserted;

    RETURN jsonb_build_object('upserted',v_count,'run_id',p_run_id);
END;
$function$;
"""


@pytest.fixture(scope="module")
def outcome_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_decision_outcome_ft_source"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def rollback_case(outcome_db):
    yield
    outcome_db.rollback()


def _position(cur, position_id: int):
    cur.execute(
        """
        INSERT INTO positions(
            id,entry_time,exit_time,status,qty,
            gross_pnl_usdc,fees_usdc,net_pnl_usdc,exit_reason
        ) VALUES (%s,now()-interval '1 hour',now(),'CLOSED',0,0,0,0,'TEST_EXIT')
        """,
        (position_id,),
    )
    cur.execute(
        """
        INSERT INTO decision_registry_v1(
            decision_id,environment,deployment_id,decision_type,
            position_id,decision_time
        ) VALUES (gen_random_uuid(),'trading_paper','LOCAL','TRADE_EXECUTED',%s,now())
        """,
        (position_id,),
    )


def _ft(cur, position_id: int, status: str, gross, fees, net):
    cur.execute(
        """
        INSERT INTO canonical_financial_truth_v1(
            position_id,financial_truth_status,authoritative_gross_pnl,
            authoritative_entry_fees_usdc,authoritative_exit_fees_usdc,
            authoritative_fees_usdc,authoritative_net_pnl
        ) VALUES (%s,%s,%s,%s/2,%s/2,%s,%s)
        """,
        (position_id, status, gross, fees, fees, fees, net),
    )


def _refresh(cur):
    cur.execute(
        "SELECT refresh_decision_identity_outcome_v1(24,'trading_paper','LOCAL')"
    )


def _outcome(cur, position_id: int):
    cur.execute(
        """
        SELECT gross_pnl_usdc,fees_usdc,net_pnl_usdc,
               outcome_status,evidence->>'economics_source',
               evidence->>'financial_truth_status'
        FROM decision_outcomes_v1
        WHERE position_id=%s
        """,
        (position_id,),
    )
    return cur.fetchone()


def test_qty_zero_legacy_zeros_project_exact_positive_and_loss_canonical_ft(
    outcome_db,
):
    with outcome_db.cursor() as cur:
        for position_id in (1, 2, 3):
            _position(cur, position_id)
        _ft(cur, 1, "COMPLETE", Decimal("1.25"), Decimal("0.25"), Decimal("1.00"))
        _ft(cur, 2, "COMPLETE", Decimal("2.00"), Decimal("0.20"), Decimal("1.80"))
        _ft(cur, 3, "COMPLETE", Decimal("-0.80"), Decimal("0.20"), Decimal("-1.00"))

        _refresh(cur)

        assert _outcome(cur, 1) == (
            Decimal("1.25"), Decimal("0.25"), Decimal("1.00"),
            "COMPLETE", "CANONICAL_FINANCIAL_TRUTH_V1", "COMPLETE",
        )
        assert _outcome(cur, 2)[:3] == (
            Decimal("2.00"), Decimal("0.20"), Decimal("1.80"),
        )
        assert _outcome(cur, 3)[:3] == (
            Decimal("-0.80"), Decimal("0.20"), Decimal("-1.00"),
        )


def test_incomplete_or_absent_ft_never_fabricates_authoritative_zero(outcome_db):
    with outcome_db.cursor() as cur:
        _position(cur, 4)
        _position(cur, 5)
        _ft(cur, 4, "INCOMPLETE", Decimal("9"), Decimal("1"), Decimal("8"))

        _refresh(cur)

        assert _outcome(cur, 4) == (
            None, None, None, "PARTIAL", "UNRESOLVED", "INCOMPLETE",
        )
        assert _outcome(cur, 5) == (
            None, None, None, "PARTIAL", "UNRESOLVED", "ABSENT",
        )


def test_refresh_is_idempotent_for_canonical_economics(outcome_db):
    with outcome_db.cursor() as cur:
        _position(cur, 6)
        _ft(cur, 6, "COMPLETE", Decimal("3.50"), Decimal("0.50"), Decimal("3.00"))

        _refresh(cur)
        first = _outcome(cur, 6)
        _refresh(cur)
        second = _outcome(cur, 6)
        cur.execute(
            "SELECT count(*) FROM decision_outcomes_v1 WHERE position_id=6"
        )

        assert first == second
        assert cur.fetchone()[0] == 1


def test_patch_and_reprojection_preserve_protected_economics_tables():
    assert "p.gross_pnl_usdc" not in MIGRATION.split("v_new_fragment :=", 1)[1].split(
        "v_old_fragment :=", 1
    )[0]
    assert "UPDATE public.positions" not in MIGRATION
    assert "UPDATE public.canonical_financial_truth_v1" not in MIGRATION
    assert "UPDATE positions" not in REPROJECTION
    assert "UPDATE canonical_financial_truth_v1" not in REPROJECTION
    assert "financial_truth.financial_truth_status = 'COMPLETE'" in REPROJECTION
    assert "outcome.environment = 'trading_paper'" in REPROJECTION
    assert "outcome.deployment_id = 'LOCAL'" in REPROJECTION
    assert "outcome.environment = 'trading_live'" not in REPROJECTION
    assert "target_position_ids" in REPROJECTION
    assert "v_target_count > 1000" in REPROJECTION
