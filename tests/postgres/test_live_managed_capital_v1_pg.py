"""Disposable PostgreSQL gates for LIVE managed-capital persistence."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.live_managed_capital import (
    LiveManagedCapitalReadContext,
    RawOkxAccountSnapshot,
    RawOkxBalance,
    activate_live_managed_capital_baseline,
    build_live_baseline_plan,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260820_live_managed_capital_authority_v1.sql"
).read_text()


def _apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def test_migration_is_idempotent_empty_and_append_only(disposable_postgres_v16):
    name = "waltrade_baseline_test_live_capital_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn)
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM live_managed_capital_baseline_v1),"
                "(SELECT count(*) FROM owner_capital_flow_v1),"
                "(SELECT count(*) FROM live_managed_equity_observation_v1)"
            )
            assert cur.fetchone() == (0, 0, 0)
            cur.execute(
                "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND "
                "tgname IN ('trg_live_managed_capital_baseline_v1_append_only',"
                "'trg_owner_capital_flow_v1_append_only',"
                "'trg_live_managed_equity_observation_v1_append_only')"
            )
            assert cur.fetchone()[0] == 3
    finally:
        conn.close()


def test_exact_artifact_apply_timestamps_duplicate_and_append_only(disposable_postgres_v16):
    name = "waltrade_baseline_test_artifact_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    identity = "a" * 64
    revision = "6" * 40
    plan_created_at = datetime(2026, 8, 20, 22, 20, tzinfo=timezone.utc)
    accepted_at = plan_created_at - timedelta(minutes=3)
    context = LiveManagedCapitalReadContext(
        snapshot=RawOkxAccountSnapshot(identity, plan_created_at, (
            RawOkxBalance(
                "USDC", Decimal("205.1128544594105"),
                Decimal("205.1128544594105"), Decimal("0"), Decimal("0"), {},
            ),
        )), marks={}, inventory_quantities={}, inventory_limits={},
    )
    artifact = build_live_baseline_plan(
        context, deployment_id="local-live", plan_created_at=plan_created_at,
        accepted_at_candidate=accepted_at, runtime_revision=revision,
    )
    try:
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT clock_timestamp()")
            before_apply = cur.fetchone()[0]
            baseline_id = activate_live_managed_capital_baseline(
                cur, artifact=artifact,
                expected_fingerprint=artifact["artifact_fingerprint"],
                approved_by="Product Owner",
                approval_reference={"approval": "YES"},
                fresh_environment="LIVE", fresh_deployment_id="local-live",
                fresh_account_identity_fingerprint=identity,
                fresh_runtime_revision=revision,
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT accepted_at,created_at,baseline_managed_equity,"
                "valuation_snapshot,raw_balance_snapshot FROM "
                "live_managed_capital_baseline_v1 WHERE baseline_id=%s",
                (baseline_id,),
            )
            row = cur.fetchone()
            assert row[0] == accepted_at
            assert row[1] >= before_apply
            assert row[0] != row[1]
            assert row[2] == Decimal("205.112854459410500000")
            assert row[3] == artifact["valuation_snapshot"]
            assert row[4] == artifact["raw_balance_snapshot"]

            with pytest.raises(ValueError, match="ALREADY_ACCEPTED"):
                activate_live_managed_capital_baseline(
                    cur, artifact=artifact,
                    expected_fingerprint=artifact["artifact_fingerprint"],
                    approved_by="Product Owner",
                    approval_reference={"approval": "YES"},
                    fresh_environment="LIVE", fresh_deployment_id="local-live",
                    fresh_account_identity_fingerprint=identity,
                    fresh_runtime_revision=revision,
                )
        conn.rollback()

        with conn.cursor() as cur:
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE live_managed_capital_baseline_v1 "
                    "SET approved_by='changed' WHERE baseline_id=%s", (baseline_id,),
                )
        conn.rollback()
    finally:
        conn.close()
