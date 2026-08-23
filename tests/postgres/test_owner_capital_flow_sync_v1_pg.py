"""Disposable PostgreSQL gates for owner-flow canonical sync authority V1."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.owner_capital_flow_sync import (
    load_owner_flow_history_authority,
    record_reconciliation_resolution,
    synchronize_owner_capital_flows,
)


ROOT = Path(__file__).resolve().parents[2]
BASE_MIGRATION = (
    ROOT / "db/migrations/20260820_live_managed_capital_authority_v1.sql"
).read_text()
SYNC_MIGRATION = (
    ROOT
    / "db/migrations/20260823_owner_capital_flow_canonical_sync_authority_v1.sql"
).read_text()
IDENTITY = "a" * 64
REVISION = "6" * 40
BASELINE_AT = datetime(2026, 8, 20, 22, 41, 3, 623066, tzinfo=timezone.utc)
NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def _apply(conn):
    with conn.cursor() as cur:
        cur.execute(BASE_MIGRATION)
        cur.execute(SYNC_MIGRATION)
    conn.commit()


def _baseline(conn):
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
                 %s::jsonb,205,205,NULL,'INCOMPLETE',NULL,
                 'NOT_YET_CANONICAL','CANONICAL',%s,'Product Owner',%s::jsonb,%s
               ) RETURNING baseline_id""",
            (
                IDENTITY, BASELINE_AT,
                json.dumps(["USDC", "BTC", "ETH", "SOL", "BNB"]),
                json.dumps({"USDC": {"total_balance": "205"}}),
                json.dumps({"USDC": {"price": "1"}}),
                REVISION, json.dumps({"approval": "YES"}), "b" * 64,
            ),
        )
        baseline_id = int(cur.fetchone()[0])
    conn.commit()
    return baseline_id


def _bill(
    bill_id: str, *, at: datetime, subtype="11", source_from="6",
    source_to="18", change="10", asset="USDC",
):
    return {
        "billId": bill_id,
        "type": "1",
        "subType": subtype,
        "from": source_from,
        "to": source_to,
        "ccy": asset,
        "balChg": change,
        "ts": str(int(at.timestamp() * 1000)),
    }


class Client:
    def __init__(self, rows=(), *, identity=IDENTITY):
        self.rows = list(rows)
        self.identity = identity
        self.calls = []

    def get_account_identity(self, refresh=False):
        return SimpleNamespace(fingerprint=self.identity, scope="MAIN"), "FRESH"

    def get_account_bills_page(self, **kwargs):
        self.calls.append(kwargs)
        return {"code": "0", "data": list(self.rows)}


def _database(disposable_postgres_v16, prefix):
    name = prefix + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    _apply(conn)
    return conn


def test_migration_idempotent_empty_append_only_and_contract_views(
    disposable_postgres_v16,
):
    conn = _database(
        disposable_postgres_v16, "waltrade_baseline_test_owner_flow_schema_"
    )
    try:
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                     (SELECT count(*) FROM owner_capital_flow_sync_run_v1),
                     (SELECT count(*) FROM owner_capital_flow_reconciliation_v1),
                     to_regclass('v_owner_capital_flow_sync_authority_v1'),
                     to_regclass('v_live_managed_equity_observation_flow_authority_v1')"""
            )
            assert cur.fetchone() == (
                0, 0, "v_owner_capital_flow_sync_authority_v1",
                "v_live_managed_equity_observation_flow_authority_v1",
            )
            cur.execute(
                """SELECT count(*) FROM pg_trigger
                   WHERE NOT tgisinternal AND tgname IN (
                     'trg_owner_capital_flow_sync_run_v1_append_only',
                     'trg_owner_capital_flow_reconciliation_v1_append_only'
                   )"""
            )
            assert cur.fetchone()[0] == 2
    finally:
        conn.close()


def test_complete_sync_replay_exact_totals_and_stale_observation_gate(
    disposable_postgres_v16,
):
    conn = _database(
        disposable_postgres_v16, "waltrade_baseline_test_owner_flow_sync_"
    )
    try:
        _baseline(conn)
        rows = [
            _bill(
                "101", at=BASELINE_AT + timedelta(hours=1),
                change="10.000000000000000001",
            ),
            _bill(
                "102", at=BASELINE_AT + timedelta(hours=2), subtype="12",
                source_from="18", source_to="6",
                change="-3.000000000000000009",
            ),
        ]
        # OKX pages are newest first.
        client = Client(list(reversed(rows)))
        first = synchronize_owner_capital_flows(
            conn, exchange_client=client, deployment_id="local-live",
            observed_at=NOW, git_revision=REVISION,
        )
        second = synchronize_owner_capital_flows(
            conn, exchange_client=client, deployment_id="local-live",
            observed_at=NOW + timedelta(minutes=10), git_revision=REVISION,
        )
        assert first.status == second.status == "CANONICAL"
        assert first.sync_through == NOW - timedelta(minutes=5)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT event_type,quantity,value_usdc,source_event_identity
                   FROM owner_capital_flow_v1 ORDER BY event_at"""
            )
            assert cur.fetchall() == [
                (
                    "TRANSFER_IN", Decimal("10.000000000000000001"),
                    Decimal("10.000000000000000001"),
                    "OKX:TRADING_BILL:101",
                ),
                (
                    "TRANSFER_OUT", Decimal("3.000000000000000009"),
                    Decimal("3.000000000000000009"),
                    "OKX:TRADING_BILL:102",
                ),
            ]
            authority = load_owner_flow_history_authority(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                as_of=BASELINE_AT + timedelta(hours=3),
            )
            stale = load_owner_flow_history_authority(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY,
                as_of=NOW + timedelta(minutes=6),
            )
        assert authority.flow_history_status == "CANONICAL"
        assert authority.cumulative_flow_in == Decimal("10.000000000000000001")
        assert authority.cumulative_flow_out == Decimal("3.000000000000000009")
        assert stale.flow_history_status == "STALE_SYNC"
        assert stale.cumulative_flow_in is None
        assert stale.cumulative_flow_out is None
    finally:
        conn.close()


@pytest.mark.parametrize(
    "client,expected_status,expected_error",
    [
        (
            Client([_bill("201", at=BASELINE_AT + timedelta(hours=1), asset="BTC")]),
            "UNSUPPORTED_ASSET", "UNSUPPORTED_ASSET:BTC",
        ),
        (Client([], identity="c" * 64), "ACCOUNT_IDENTITY_MISMATCH", "IDENTITY"),
    ],
)
def test_unsupported_asset_and_account_mismatch_do_not_advance_watermark(
    disposable_postgres_v16, client, expected_status, expected_error,
):
    conn = _database(
        disposable_postgres_v16, "waltrade_baseline_test_owner_flow_fail_"
    )
    try:
        _baseline(conn)
        result = synchronize_owner_capital_flows(
            conn, exchange_client=client, deployment_id="local-live",
            observed_at=NOW, git_revision=REVISION,
        )
        assert result.status == expected_status
        assert expected_error in result.error_code
        assert result.sync_through is None
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM owner_capital_flow_v1")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT status,sync_through FROM v_owner_capital_flow_sync_authority_v1"
            )
            assert cur.fetchone() == (expected_status, None)
    finally:
        conn.close()


def test_late_event_append_only_invalidates_then_resolution_restores_projection(
    disposable_postgres_v16,
):
    conn = _database(
        disposable_postgres_v16, "waltrade_baseline_test_owner_flow_late_"
    )
    try:
        baseline_id = _baseline(conn)
        first = synchronize_owner_capital_flows(
            conn, exchange_client=Client([]), deployment_id="local-live",
            observed_at=NOW, git_revision=REVISION,
        )
        assert first.status == "CANONICAL"

        late_at = NOW - timedelta(hours=1)
        late = synchronize_owner_capital_flows(
            conn,
            exchange_client=Client([_bill("301", at=late_at, change="7.5")]),
            deployment_id="local-live",
            observed_at=NOW + timedelta(minutes=10),
            git_revision=REVISION,
        )
        assert late.status == "LATE_EVENT_RECONCILIATION_REQUIRED"
        assert late.late_event_count == 1
        assert late.sync_through is None

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO live_managed_equity_observation_v1(
                     baseline_id,deployment_id,observed_at,raw_managed_equity,
                     cumulative_flow_in_usdc,cumulative_flow_out_usdc,
                     flow_adjusted_equity,evidence_fingerprint,evidence_status
                   ) VALUES (%s,'local-live',%s,210,0,0,210,%s,'COMPLETE')""",
                (baseline_id, NOW, "d" * 64),
            )
            cur.execute(
                """SELECT reconciliation_key,state
                   FROM v_owner_capital_flow_reconciliation_current_v1"""
            )
            reconciliation_key, state = cur.fetchone()
            assert state == "REQUIRED"
            cur.execute(
                """SELECT flow_history_status
                   FROM v_live_managed_equity_observation_flow_authority_v1"""
            )
            assert cur.fetchone()[0] == "LATE_EVENT_RECONCILIATION_REQUIRED"
            resolution_id = record_reconciliation_resolution(
                cur,
                reconciliation_key=reconciliation_key,
                source_run_id=late.run_id,
                evidence={
                    "drawdown_recomputed": True,
                    "affected_observations_reemitted": ["d" * 64],
                },
            )
            assert resolution_id > 0
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state FROM v_owner_capital_flow_reconciliation_current_v1"
            )
            assert cur.fetchone()[0] == "RESOLVED"
            cur.execute(
                "SELECT flow_history_status FROM v_live_managed_equity_observation_flow_authority_v1"
            )
            assert cur.fetchone()[0] == "LATE_EVENT_RECONCILIATION_REQUIRED"
        restored = synchronize_owner_capital_flows(
            conn,
            exchange_client=Client([_bill("301", at=late_at, change="7.5")]),
            deployment_id="local-live",
            observed_at=NOW + timedelta(minutes=20),
            git_revision=REVISION,
        )
        assert restored.status == "CANONICAL"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT flow_history_status FROM v_live_managed_equity_observation_flow_authority_v1"
            )
            assert cur.fetchone()[0] == "COMPLETE"
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE owner_capital_flow_reconciliation_v1 SET state='REQUIRED'"
                )
        conn.rollback()
    finally:
        conn.close()


def test_existing_exchange_identity_with_different_economics_blocks_watermark(
    disposable_postgres_v16,
):
    conn = _database(
        disposable_postgres_v16, "waltrade_baseline_test_owner_flow_conflict_"
    )
    try:
        _baseline(conn)
        event_at = BASELINE_AT + timedelta(hours=1)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO owner_capital_flow_v1(
                     environment,deployment_id,account_identity_fingerprint,
                     source_event_identity,asset,quantity,value_usdc,event_at,
                     event_type,source,evidence_status,
                     raw_provenance_reference,valuation_provenance
                   ) VALUES (
                     'LIVE','local-live',%s,'OKX:TRADING_BILL:401','USDC',9,9,%s,
                     'TRANSFER_IN','TRADING_ACCOUNT_BILLS','COMPLETE',%s::jsonb,%s::jsonb
                   )""",
                (
                    IDENTITY, event_at,
                    json.dumps({"billId": "401", "balChg": "9"}),
                    json.dumps({"authority": "test-conflict"}),
                ),
            )
        conn.commit()
        result = synchronize_owner_capital_flows(
            conn,
            exchange_client=Client([_bill("401", at=event_at, change="10")]),
            deployment_id="local-live",
            observed_at=NOW,
            git_revision=REVISION,
        )
        assert result.status == "PARTIAL_SYNC"
        assert result.error_code == "OWNER_CAPITAL_FLOW_IDEMPOTENCY_CONFLICT"
        assert result.sync_through is None
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status,error_code,sync_through "
                "FROM v_owner_capital_flow_sync_authority_v1"
            )
            assert cur.fetchone() == (
                "PARTIAL_SYNC", "OWNER_CAPITAL_FLOW_IDEMPOTENCY_CONFLICT", None,
            )
            cur.execute(
                "SELECT quantity FROM owner_capital_flow_v1 "
                "WHERE source_event_identity='OKX:TRADING_BILL:401'"
            )
            assert cur.fetchone()[0] == Decimal("9.000000000000000000")
    finally:
        conn.close()
