from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
import pytest


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT
    / "db/migrations/20260808_learning_canonical_registry_resolution_v1.sql"
).read_text()

FORWARD_ID = UUID("10000000-0000-4000-8000-000000000001")
LEGACY_ID = UUID("20000000-0000-4000-8000-000000000002")
NOW = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
CUTOFF = NOW - timedelta(hours=1)


SCHEMA = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE schema_migration_ledger_v1(
    migration_id TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    applied_by TEXT NOT NULL,
    status TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    execution_duration_ms BIGINT NOT NULL,
    git_sha TEXT,
    schema_baseline_version TEXT NOT NULL
);

CREATE TABLE v_decision_intelligence_v1(
    environment TEXT,
    symbol TEXT,
    interval TEXT,
    strategy TEXT,
    decision_key TEXT,
    net_pnl_usdc NUMERIC,
    recommendation_type TEXT,
    recommendation_action TEXT,
    missing_context_count INTEGER,
    refreshed_at TIMESTAMPTZ,
    decision_lifecycle_status TEXT,
    has_pnl BOOLEAN,
    created_at TIMESTAMPTZ
);

CREATE TABLE learning_feature_warehouse_v1(
    id BIGINT PRIMARY KEY,
    environment TEXT,
    decision_key TEXT,
    position_id BIGINT,
    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    net_pnl_usdc NUMERIC
);

CREATE TABLE decision_registry_v1(
    decision_id UUID PRIMARY KEY,
    legacy_decision_key TEXT,
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    decision_source TEXT NOT NULL,
    engine_version TEXT,
    position_id BIGINT,
    market_regime TEXT,
    ingested_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE decision_outcomes_v1(
    outcome_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL REFERENCES decision_registry_v1(decision_id),
    outcome_type TEXT NOT NULL,
    position_id BIGINT,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    outcome_status TEXT,
    calculated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ
);

CREATE FUNCTION learning_canonical_evidence_universe_pre_ft_quarantine_v1(
    p_environment TEXT,
    p_sample_from TIMESTAMPTZ,
    p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE(
    environment TEXT,symbol TEXT,"interval" TEXT,strategy TEXT,
    decision_key TEXT,decision_id UUID,position_id BIGINT,
    entry_time TIMESTAMPTZ,exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,mfe_pct NUMERIC,
    mae_pct NUMERIC,regime_identity TEXT,regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ,has_full_context BOOLEAN,
    has_avoid_review BOOLEAN,has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN,eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ,outcome_available_at TIMESTAMPTZ
) LANGUAGE SQL STABLE AS $$
    SELECT NULL::TEXT,NULL::TEXT,NULL::TEXT,NULL::TEXT,NULL::TEXT,NULL::UUID,
           NULL::BIGINT,NULL::TIMESTAMPTZ,NULL::TIMESTAMPTZ,NULL::TIMESTAMPTZ,
           NULL::NUMERIC,NULL::NUMERIC,NULL::NUMERIC,NULL::NUMERIC,NULL::NUMERIC,
           NULL::TEXT,NULL::JSONB,NULL::TIMESTAMPTZ,NULL::BOOLEAN,NULL::BOOLEAN,
           NULL::BOOLEAN,NULL::BOOLEAN,NULL::TEXT,NULL::TIMESTAMPTZ,
           NULL::TIMESTAMPTZ
    WHERE false
$$;
"""


@pytest.fixture()
def registry_db(disposable_postgres_v16):
    database = "waltrade_baseline_test_learning_registry_resolution_v1"
    try:
        disposable_postgres_v16.create_database(database)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=disposable_postgres_v16.port,
        dbname=database,
        user=disposable_postgres_v16.user,
        password=disposable_postgres_v16.password,
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture()
def rollback_case(registry_db):
    yield registry_db
    registry_db.rollback()


def _candidate(cur, *, key: str, position_id: int) -> None:
    cur.execute(
        """
        INSERT INTO v_decision_intelligence_v1 VALUES (
          'trading_paper','BTCUSDC','1m','BBRANGE',%s,1.10,
          'ENTRY_REVIEW','CONFIRM',0,%s,'CLOSED',true,%s
        )
        """,
        (key, CUTOFF - timedelta(minutes=5), CUTOFF - timedelta(hours=3)),
    )
    cur.execute(
        """
        INSERT INTO learning_feature_warehouse_v1 VALUES (
          %s,'trading_paper',%s,%s,%s,%s,%s,1.10
        )
        """,
        (
            position_id,
            key,
            position_id,
            CUTOFF - timedelta(hours=3),
            CUTOFF - timedelta(hours=2),
            CUTOFF - timedelta(hours=3),
        ),
    )


def _registry(
    cur,
    *,
    decision_id: UUID,
    position_id: int,
    ingested_at: datetime,
    legacy_key: str | None = None,
    forward: bool,
) -> None:
    cur.execute(
        """
        INSERT INTO decision_registry_v1 VALUES (
          %s,%s,'LOCAL','trading_paper',%s,%s,%s,%s,'TRENDING',%s
        )
        """,
        (
            str(decision_id),
            legacy_key,
            "ENTRY_DECISION" if forward else "TRADE_EXECUTED",
            (
                "FINAL_DECISION_EXECUTION_EPILOG"
                if forward
                else "POSITION"
            ),
            "FORWARD_DECISION_REGISTRY_CONTINUITY_V1" if forward else "LEGACY",
            position_id,
            ingested_at,
        ),
    )


def _outcome(cur, *, decision_id: UUID, position_id: int) -> None:
    cur.execute(
        """
        INSERT INTO decision_outcomes_v1 VALUES (
          gen_random_uuid(),%s,'ACTUAL_TRADE',%s,1.25,0.15,1.10,
          2.0,-1.0,'COMPLETE',%s,%s
        )
        """,
        (
            str(decision_id),
            position_id,
            CUTOFF - timedelta(hours=1),
            CUTOFF - timedelta(hours=1),
        ),
    )


def _resolution(cur, key: str) -> tuple[str | None, str]:
    cur.execute(
        """
        SELECT decision_id,eligibility_reason
        FROM learning_canonical_evidence_universe_pre_ft_quarantine_v1(
          'trading_paper',%s,%s,%s
        )
        WHERE decision_key=%s
        """,
        (NOW - timedelta(days=1), NOW, CUTOFF, key),
    )
    return cur.fetchone()


def test_forward_entry_decision_by_warehouse_position_is_eligible(rollback_case):
    with rollback_case.cursor() as cur:
        _candidate(cur, key="candidate-forward", position_id=101)
        _registry(
            cur,
            decision_id=FORWARD_ID,
            position_id=101,
            ingested_at=CUTOFF - timedelta(minutes=30),
            forward=True,
        )
        _outcome(cur, decision_id=FORWARD_ID, position_id=101)
        assert _resolution(cur, "candidate-forward") == (
            str(FORWARD_ID),
            "ELIGIBLE",
        )


def test_legacy_trade_executed_mapping_remains_eligible(rollback_case):
    with rollback_case.cursor() as cur:
        _candidate(cur, key="candidate-legacy", position_id=102)
        _registry(
            cur,
            decision_id=LEGACY_ID,
            position_id=102,
            ingested_at=CUTOFF - timedelta(minutes=30),
            legacy_key="candidate-legacy",
            forward=False,
        )
        _outcome(cur, decision_id=LEGACY_ID, position_id=102)
        assert _resolution(cur, "candidate-legacy") == (
            str(LEGACY_ID),
            "ELIGIBLE",
        )


def test_forward_wins_over_legacy_without_identity_conflict(rollback_case):
    with rollback_case.cursor() as cur:
        _candidate(cur, key="candidate-both", position_id=103)
        _registry(
            cur,
            decision_id=LEGACY_ID,
            position_id=103,
            ingested_at=CUTOFF - timedelta(minutes=40),
            legacy_key="candidate-both",
            forward=False,
        )
        _registry(
            cur,
            decision_id=FORWARD_ID,
            position_id=103,
            ingested_at=CUTOFF - timedelta(minutes=20),
            forward=True,
        )
        _outcome(cur, decision_id=LEGACY_ID, position_id=103)
        _outcome(cur, decision_id=FORWARD_ID, position_id=103)
        assert _resolution(cur, "candidate-both") == (
            str(FORWARD_ID),
            "ELIGIBLE",
        )


def test_post_cutoff_forward_registry_is_excluded(rollback_case):
    with rollback_case.cursor() as cur:
        _candidate(cur, key="candidate-late", position_id=104)
        _registry(
            cur,
            decision_id=FORWARD_ID,
            position_id=104,
            ingested_at=CUTOFF + timedelta(seconds=1),
            forward=True,
        )
        _outcome(cur, decision_id=FORWARD_ID, position_id=104)
        assert _resolution(cur, "candidate-late") == (
            None,
            "EXCLUDED_POST_CUTOFF",
        )


def test_wrong_position_forward_registry_does_not_match(rollback_case):
    with rollback_case.cursor() as cur:
        _candidate(cur, key="candidate-wrong-position", position_id=105)
        _registry(
            cur,
            decision_id=FORWARD_ID,
            position_id=999,
            ingested_at=CUTOFF - timedelta(minutes=30),
            forward=True,
        )
        _outcome(cur, decision_id=FORWARD_ID, position_id=999)
        assert _resolution(cur, "candidate-wrong-position") == (
            None,
            "EXCLUDED_MISSING_REGISTRY",
        )


def test_resolution_migration_is_idempotent_and_tiered(registry_db):
    with registry_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 "
            "WHERE migration_id="
            "'20260808_learning_canonical_registry_resolution_v1.sql'"
        )
        assert cur.fetchone()[0] == 1
    assert "forward_registry AS" in MIGRATION
    assert "legacy_registry AS" in MIGRATION
    assert "WHERE NOT EXISTS" in MIGRATION
    assert "UNION ALL" in MIGRATION
    assert " OR r.legacy_decision_key" not in MIGRATION
