"""Disposable PostgreSQL contract tests for the producer audit ledger."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest


TEST_DSN = os.getenv("WALTRADE_TEST_PG_DSN", "").strip()
pytestmark = pytest.mark.skipif(
    not TEST_DSN,
    reason="WALTRADE_TEST_PG_DSN is not set; real disposable PostgreSQL required",
)

import psycopg2  # noqa: E402
from psycopg2.extensions import parse_dsn  # noqa: E402

from common.decision_contract import (  # noqa: E402
    DecisionReason,
    EvaluationContext,
    FinalDecision,
)
from common.final_decision_producer_audit import (  # noqa: E402
    AuditDecisionContext,
    AuditIdentity,
    FinalDecisionProducerAuditLedger,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260724_final_decision_producer_audit_ledger_v1.sql"
).read_text()
TRANSPORT_MIGRATION = (
    ROOT / "db/migrations/20260717_causal_decision_observation_transport_v1.sql"
).read_text()


def _guarded_connect():
    database = str(parse_dsn(TEST_DSN).get("dbname") or "")
    if not database.lower().endswith("_test"):
        raise RuntimeError("WALTRADE_TEST_PG_DSN database must end in _test")
    conn = psycopg2.connect(TEST_DSN, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if str(cur.fetchone()[0]) != database:
                raise RuntimeError("unexpected disposable PostgreSQL database")
            cur.execute(
                "SELECT value FROM automation_kv "
                "WHERE key='waltrade_disposable_test_db'"
            )
            marker = cur.fetchone()
            if marker is None or str(marker[0]).lower() != "true":
                raise RuntimeError(
                    "waltrade_disposable_test_db=true marker is required"
                )
            cur.execute("SET lock_timeout='2s'")
            cur.execute("SET statement_timeout='10s'")
        return conn
    except Exception:
        conn.close()
        raise


def _apply_migration():
    conn = _guarded_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(TRANSPORT_MIGRATION)
            cur.execute(MIGRATION)
        conn.commit()
    finally:
        conn.close()


def _decision(deployment):
    now = datetime.now(timezone.utc)
    return FinalDecision.no_trade(
        EvaluationContext(
            deployment_id=deployment,
            environment="trading_paper",
            symbol="PGTESTUSDC",
            interval="1m",
            strategy="RSI",
            candle_open_time=now,
            evaluation_started_at=now,
            engine_name="postgres-test",
            paper_mode=True,
        ),
        DecisionReason.NO_SIGNAL,
        finished_at=now,
    )


def _context(deployment, decision_key, token):
    item = _decision(deployment)
    base = AuditDecisionContext.from_decision(
        item,
        decision_key=decision_key,
        source_service=f"postgres-ledger-test-{token}",
        source_instance="disposable-postgres",
    )
    return replace(
        base,
        identity=AuditIdentity.build(deployment, decision_key),
    )


def test_real_postgresql_migration_identity_constraints_and_equations():
    _apply_migration()
    _apply_migration()
    token = uuid.uuid4().hex
    shared_key = f"pg-ledger-shared-{token}"
    attempted_at = datetime.now(timezone.utc)

    local = FinalDecisionProducerAuditLedger(
        _guarded_connect,
        _context("local-paper", shared_key, token),
    )
    assert local.append("FINALIZED")
    assert local.append("FINALIZED")
    assert local.append("PRODUCER_ATTEMPTED", attempted_at=attempted_at)
    assert local.append(
        "OUTBOX_WRITE_FAILED",
        attempted_at=attempted_at,
        error_class="SyntheticPostgresTestFailure",
    )
    local.close()

    vps = FinalDecisionProducerAuditLedger(
        _guarded_connect,
        _context("vps-paper", shared_key, token),
    )
    assert vps.append("FINALIZED")
    assert vps.append(
        "SKIPPED_DISABLED",
        skip_reason="SKIPPED_DISABLED",
    )
    vps.close()

    conn = _guarded_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  count(*) FILTER (WHERE event_type='FINALIZED'),
                  count(*) FILTER (WHERE event_type='PRODUCER_ATTEMPTED'),
                  count(*) FILTER (WHERE event_type='SKIPPED_DISABLED'),
                  count(*) FILTER (WHERE event_type='OUTBOX_WRITE_FAILED')
                FROM final_decision_producer_audit_v1
                WHERE source_service=%s
                """,
                (f"postgres-ledger-test-{token}",),
            )
            assert cur.fetchone() == (2, 1, 1, 1)

            cur.execute(
                """
                SELECT count(DISTINCT deployment_id),count(DISTINCT decision_key)
                FROM final_decision_producer_audit_v1
                WHERE source_service=%s AND event_type='FINALIZED'
                """,
                (f"postgres-ledger-test-{token}",),
            )
            assert cur.fetchone() == (2, 1)

            cur.execute(
                """
                SELECT avg(pg_column_size(a))
                FROM final_decision_producer_audit_v1 a
                WHERE source_service=%s
                """,
                (f"postgres-ledger-test-{token}",),
            )
            average_row_bytes = float(cur.fetchone()[0])
            assert 200 <= average_row_bytes < 2048

            with pytest.raises(psycopg2.Error):
                cur.execute(
                    "UPDATE final_decision_producer_audit_v1 "
                    "SET action='MUTATED' WHERE source_service=%s",
                    (f"postgres-ledger-test-{token}",),
                )
        conn.rollback()

        with conn.cursor() as cur:
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    "DELETE FROM final_decision_producer_audit_v1 "
                    "WHERE source_service=%s",
                    (f"postgres-ledger-test-{token}",),
                )
        conn.rollback()
    finally:
        conn.close()
