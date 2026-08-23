"""Disposable PostgreSQL gates for LIVE drawdown history authority V1."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.live_drawdown_history import (
    CONTRACT_VERSION,
    ObservationCandidate,
    _fingerprint,
    persist_observation_candidate,
    read_live_drawdown_history,
    reemit_late_event_history,
)
from common.live_managed_capital import record_owner_capital_flow


ROOT = Path(__file__).resolve().parents[2]
BASE = (ROOT / "db/migrations/20260820_live_managed_capital_authority_v1.sql").read_text()
FLOW = (ROOT / "db/migrations/20260823_owner_capital_flow_canonical_sync_authority_v1.sql").read_text()
HISTORY = (ROOT / "db/migrations/20260823_live_drawdown_history_authority_v1.sql").read_text()
IDENTITY = "a" * 64
ACTIVATION = "b" * 64
REVISION = "c" * 40
BASELINE_AT = datetime(2026, 8, 20, 22, 41, 3, 623066, tzinfo=timezone.utc)
NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
D = Decimal


def database(disposable_postgres_v16, prefix):
    name = prefix + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(BASE)
        cur.execute(FLOW)
        cur.execute(HISTORY)
    conn.commit()
    return conn


def baseline(conn):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO live_managed_capital_baseline_v1(
                 environment,deployment_id,contract_version,
                 account_identity_fingerprint,account_scope,accepted_at,
                 managed_asset_scope,raw_balance_snapshot,valuation_snapshot,
                 baseline_managed_equity,raw_okx_usdc_avail_bal,
                 available_capital,available_capital_status,reserved_capital,
                 reserved_capital_status,ownership_reconciliation_status,
                 runtime_revision,approved_by,approval_reference,
                 activation_fingerprint
               ) VALUES (
                 'LIVE','local-live','LIVE_MANAGED_CAPITAL_AUTHORITY_V1',%s,
                 'DEDICATED_WALTRADE_MANAGED_ACCOUNT',%s,%s::jsonb,%s::jsonb,
                 %s::jsonb,100,100,NULL,'INCOMPLETE',NULL,
                 'NOT_YET_CANONICAL','CANONICAL',%s,'Product Owner',%s::jsonb,%s
               ) RETURNING baseline_id""",
            (
                IDENTITY, BASELINE_AT,
                json.dumps(["USDC", "BTC", "ETH", "SOL", "BNB"]),
                json.dumps({"USDC": {"total_balance": "100"}}),
                json.dumps({"USDC": {"price": "1"}}), REVISION,
                json.dumps({"approval": "YES"}), ACTIVATION,
            ),
        )
        value = int(cur.fetchone()[0])
    conn.commit()
    return value


def sync_run(conn, *, cutoff, status="CANONICAL", fingerprint="d" * 64):
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO owner_capital_flow_sync_run_v1(
                 run_id,environment,deployment_id,account_identity_fingerprint,
                 source,contract_version,range_from,source_cutoff,overlap_from,
                 sync_through,source_endpoint,terminal_cursor,last_source_event_id,
                 page_count,source_event_count,canonical_event_count,late_event_count,
                 started_at,completed_at,producer_identity,git_revision,
                 source_fingerprint,status,error_code,evidence
               ) VALUES (
                 %s,'LIVE','local-live',%s,'TRADING_ACCOUNT_BILLS',
                 'OWNER_CAPITAL_FLOW_CANONICAL_SYNC_AUTHORITY_V1',%s,%s,%s,%s,
                 '/api/v5/account/bills',NULL,NULL,1,0,0,0,%s,%s,'test',%s,%s,%s,%s,%s::jsonb
               )""",
            (
                run_id, IDENTITY, BASELINE_AT, cutoff, BASELINE_AT,
                cutoff if status == "CANONICAL" else None,
                cutoff, cutoff, REVISION, fingerprint, status,
                None if status == "CANONICAL" else "UNRESOLVED_LATE_EVENT",
                json.dumps({"pages_exhausted": True}),
            ),
        )
    conn.commit()
    return run_id


def candidate(baseline_id, *, observed_at=NOW, managed="100", suffix="base"):
    portfolio = {"authority": "PORTFOLIO_STATE_V1", "suffix": suffix}
    capital = {"authority": "LIVE_MANAGED_CAPITAL_AUTHORITY_V1", "suffix": suffix}
    portfolio_fp = _fingerprint(portfolio)
    capital_fp = _fingerprint(capital)
    identity = _fingerprint({"baseline": baseline_id, "suffix": suffix})
    return ObservationCandidate(
        baseline_id, "local-live", IDENTITY, ACTIVATION, observed_at,
        observed_at.replace(minute=(observed_at.minute // 15) * 15, second=0, microsecond=0),
        "CADENCE_15M", f"bucket:{suffix}", identity, D(managed), D("1"),
        D("2"), D("10"), D("3"), D("87"), portfolio, capital,
        portfolio_fp, capital_fp, "test", REVISION,
    )


def test_migration_idempotent_additive_empty_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_drawdown_schema_")
    try:
        with conn.cursor() as cur:
            cur.execute(HISTORY)
            cur.execute(
                """SELECT count(*),to_regclass('v_live_drawdown_history_observation_v1')
                   FROM live_managed_equity_observation_v1"""
            )
            assert cur.fetchone() == (0, "v_live_drawdown_history_observation_v1")
            cur.execute(
                """SELECT count(*) FROM pg_trigger
                   WHERE tgname='trg_live_managed_equity_observation_v1_append_only'
                     AND NOT tgisinternal"""
            )
            assert cur.fetchone()[0] == 1
        conn.commit()
    finally:
        conn.close()


def test_watermark_gate_flow_neutrality_idempotency_and_source_conflict(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_drawdown_persist_")
    try:
        baseline_id = baseline(conn)
        sync_run(conn, cutoff=NOW - timedelta(minutes=1))
        item = candidate(baseline_id, managed="110")
        with conn.cursor() as cur:
            assert persist_observation_candidate(cur, item).status == "INCOMPLETE_CAPITAL_FLOW"
        conn.rollback()
        with conn.cursor() as cur:
            record_owner_capital_flow(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                source_event_identity="OKX:TRADING_BILL:1", asset="USDC",
                quantity=D("10"), value_usdc=D("10"),
                event_at=NOW - timedelta(minutes=2), event_type="TRANSFER_IN",
                source="TRADING_ACCOUNT_BILLS",
                raw_provenance_reference={"billId": "1"},
                valuation_provenance={"authority": "balChg"},
            )
        conn.commit()
        sync_run(conn, cutoff=NOW, fingerprint="e" * 64)
        with conn.cursor() as cur:
            first = persist_observation_candidate(cur, item)
            second = persist_observation_candidate(cur, item)
            assert first.status == second.status == "CANONICAL"
            assert first.observation_id == second.observation_id
            cur.execute(
                """SELECT raw_managed_equity,cumulative_flow_in_usdc,
                          flow_adjusted_equity
                   FROM live_managed_equity_observation_v1"""
            )
            assert cur.fetchone() == (D("110"), D("10"), D("100"))
            conflict = replace(item, portfolio_state_fingerprint="f" * 64)
            assert persist_observation_candidate(cur, conflict).status == "SOURCE_FINGERPRINT_MISMATCH"
        conn.commit()
        sync_run(conn, cutoff=NOW + timedelta(minutes=1), fingerprint="8" * 64)
        with conn.cursor() as cur:
            replay_after_newer_watermark = persist_observation_candidate(cur, item)
            assert replay_after_newer_watermark.status == "CANONICAL"
            assert replay_after_newer_watermark.observation_id == first.observation_id
        conn.commit()
        with conn.cursor() as cur:
            record_owner_capital_flow(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                source_event_identity="OKX:TRADING_BILL:2", asset="USDC",
                quantity=D("4"), value_usdc=D("4"),
                event_at=NOW + timedelta(minutes=10), event_type="TRANSFER_OUT",
                source="TRADING_ACCOUNT_BILLS",
                raw_provenance_reference={"billId": "2"},
                valuation_provenance={"authority": "balChg"},
            )
        conn.commit()
        later = NOW + timedelta(minutes=15)
        sync_run(conn, cutoff=later, fingerprint="9" * 64)
        with conn.cursor() as cur:
            outbound = persist_observation_candidate(
                cur, candidate(
                    baseline_id, observed_at=later, managed="106", suffix="out"
                ),
            )
            assert outbound.status == "CANONICAL"
            cur.execute(
                """SELECT cumulative_flow_in_usdc,cumulative_flow_out_usdc,
                          flow_adjusted_equity
                   FROM live_managed_equity_observation_v1
                   WHERE observation_id=%s""",
                (outbound.observation_id,),
            )
            assert cur.fetchone() == (D("10"), D("4"), D("100"))
        conn.commit()
    finally:
        conn.close()


def test_late_event_invalidates_then_append_only_reemission_restores(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_drawdown_late_")
    try:
        baseline_id = baseline(conn)
        canonical_run = sync_run(conn, cutoff=NOW, fingerprint="1" * 64)
        item = candidate(baseline_id)
        with conn.cursor() as cur:
            assert persist_observation_candidate(cur, item).status == "CANONICAL"
        conn.commit()
        with conn.cursor() as cur:
            record_owner_capital_flow(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                source_event_identity="OKX:TRADING_BILL:LATE", asset="USDC",
                quantity=D("7.5"), value_usdc=D("7.5"),
                event_at=NOW - timedelta(minutes=10), event_type="TRANSFER_IN",
                source="TRADING_ACCOUNT_BILLS",
                raw_provenance_reference={"billId": "LATE"},
                valuation_provenance={"authority": "balChg"},
            )
        conn.commit()
        late_run = sync_run(
            conn, cutoff=NOW + timedelta(minutes=5),
            status="LATE_EVENT_RECONCILIATION_REQUIRED", fingerprint="2" * 64,
        )
        reconciliation_key = "3" * 64
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO owner_capital_flow_reconciliation_v1(
                     reconciliation_key,environment,deployment_id,
                     account_identity_fingerprint,source,source_event_identity,
                     event_at,prior_sync_through,affected_from,state,
                     source_run_id,evidence
                   ) VALUES (%s,'LIVE','local-live',%s,'TRADING_ACCOUNT_BILLS',
                     'OKX:TRADING_BILL:LATE',%s,%s,%s,'REQUIRED',%s,%s::jsonb)""",
                (
                    reconciliation_key, IDENTITY, NOW - timedelta(minutes=10),
                    NOW, NOW - timedelta(minutes=10), late_run,
                    json.dumps({"reason": "late"}),
                ),
            )
        conn.commit()
        with conn.cursor() as cur:
            invalid = read_live_drawdown_history(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, as_of=NOW,
            )
            assert invalid.history_status == "INCOMPLETE_CAPITAL_FLOW"
            assert reemit_late_event_history(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
            ) == 1
        conn.commit()
        sync_run(
            conn, cutoff=NOW + timedelta(minutes=10),
            status="CANONICAL", fingerprint="4" * 64,
        )
        with conn.cursor() as cur:
            restored = read_live_drawdown_history(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                as_of=NOW + timedelta(minutes=10),
            )
            assert restored.history_status == "CANONICAL"
            assert restored.current_managed_equity == D("100")
            assert restored.current_flow_adjusted_equity == D("92.5")
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE owner_capital_flow_reconciliation_v1 SET state='REQUIRED'"
                )
        conn.rollback()
    finally:
        conn.close()
