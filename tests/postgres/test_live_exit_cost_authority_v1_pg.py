from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest

from common.live_exit_cost import (
    capture_okx_exit_cost_snapshot_cursor,
    link_latest_exit_cost_snapshot_cursor,
    load_live_exit_cost_links_cursor,
)
from common.position_risk_boundary import (
    accept_boundary_policy_cursor,
    activate_live_boundary_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
BOUNDARY_MIGRATION = (ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql").read_text()
MIGRATION = (ROOT / "db/migrations/20260821_z_live_exit_cost_authority_v1.sql").read_text()
NOW = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
IDENTITY = "a" * 64


class FeeClient:
    def __init__(self, taker="-0.0008"):
        self.taker = taker

    def get_trade_fee(self, *, symbol, instrument_type):
        return {"code": "0", "data": [{
            "instType": instrument_type, "taker": self.taker,
            "maker": "-0.0005", "level": "Lv1", "ruleType": "normal",
            "ts": "1787313600000",
        }]}


def database(disposable_postgres_v16):
    name = "waltrade_baseline_test_live_exit_cost_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    return disposable_postgres_v16.connect(name)


def setup(conn):
    with conn.cursor() as cur:
        cur.execute(BOUNDARY_MIGRATION)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
        cur.execute("CREATE TABLE positions(id bigint PRIMARY KEY,symbol text NOT NULL)")
        cur.execute("INSERT INTO positions VALUES (501,'BTCUSDC'),(502,'ETHUSDC')")
    conn.commit()


def accept_and_activate(cur, *, position_id=501, symbol="BTCUSDC", at=NOW):
    status, boundary_id = accept_boundary_policy_cursor(
        cur, environment="LIVE", deployment_id="local-live",
        account_identity_fingerprint=IDENTITY, reservation_id=uuid.uuid4(),
        decision_id=f"decision-{position_id}", intent_id=f"intent-{position_id}",
        order_identity=f"order-{position_id}", symbol=symbol, strategy="RSI",
        interval="1m", effective_at=at, source_authority="TEST_COMMITMENT",
        provenance={"test": True}, boundary_distance_pct=Decimal("0.8"),
    )
    assert status == "INSERTED"
    assert activate_live_boundary_cursor(
        cur, intent_id=f"intent-{position_id}", position_id=position_id,
        canonical_entry_basis=Decimal("100"), effective_at=at,
    ) == "INSERTED"
    return boundary_id


def test_snapshot_link_idempotency_freeze_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        setup(conn)
        with conn.cursor() as cur:
            first_status, first = capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient(), deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, symbol="BTCUSDC",
                observed_at=NOW - timedelta(minutes=1),
            )
            assert first_status == "INSERTED"
            assert capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient(), deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, symbol="BTCUSDC",
                observed_at=NOW - timedelta(minutes=1),
            )[0] == "IDEMPOTENT"
            boundary_id = accept_and_activate(cur)
            cur.execute(
                "SELECT exit_cost_snapshot_id FROM live_position_exit_cost_link_v1 "
                "WHERE position_id=501"
            )
            assert uuid.UUID(str(cur.fetchone()[0])) == first.exit_cost_snapshot_id

            _, later = capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient("-0.0012"),
                deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, symbol="BTCUSDC",
                observed_at=NOW + timedelta(minutes=1),
            )
            assert link_latest_exit_cost_snapshot_cursor(
                cur, position_id=501, boundary_id=boundary_id,
                deployment_id="local-live", account_identity_fingerprint=IDENTITY,
                symbol="BTCUSDC", effective_at=NOW + timedelta(minutes=2),
            ) == "ALREADY_FROZEN"
            cur.execute(
                "SELECT exit_cost_snapshot_id FROM live_position_exit_cost_link_v1 "
                "WHERE position_id=501"
            )
            assert uuid.UUID(str(cur.fetchone()[0])) == first.exit_cost_snapshot_id
            assert later.exit_cost_snapshot_id != first.exit_cost_snapshot_id

            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE live_exit_cost_snapshot_v1 SET canonical_fee_rate=0 "
                    "WHERE exit_cost_snapshot_id=%s", (str(first.exit_cost_snapshot_id),)
                )
        conn.rollback()
    finally:
        conn.close()


def test_scope_fencing_stale_and_missing_fail_closed(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        setup(conn)
        with conn.cursor() as cur:
            capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient(), deployment_id="local-live",
                account_identity_fingerprint="b" * 64, symbol="BTCUSDC",
                observed_at=NOW - timedelta(minutes=1),
            )
            capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient(), deployment_id="vps-live",
                account_identity_fingerprint=IDENTITY, symbol="BTCUSDC",
                observed_at=NOW - timedelta(minutes=1),
            )
            _, stale = capture_okx_exit_cost_snapshot_cursor(
                cur, exchange_client=FeeClient(), deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, symbol="ETHUSDC",
                observed_at=NOW - timedelta(hours=23),
            )
            boundary_id = accept_and_activate(
                cur, position_id=502, symbol="ETHUSDC", at=NOW - timedelta(hours=22)
            )
            loaded = load_live_exit_cost_links_cursor(
                cur, deployment_id="local-live",
                account_identity_fingerprint=IDENTITY, as_of=NOW + timedelta(hours=2),
            )
            assert loaded[502] == (None, "STALE_FEE_EVIDENCE", stale.contract_version)
            btc_boundary_id = accept_and_activate(cur, position_id=501)
            cur.execute(
                "SELECT count(*) FROM live_position_exit_cost_link_v1 "
                "WHERE position_id=501"
            )
            assert cur.fetchone() == (0,)
            with pytest.raises(Exception, match="LIVE_EXIT_COST_SCOPE_MISMATCH"):
                link_latest_exit_cost_snapshot_cursor(
                    cur, position_id=501, boundary_id=btc_boundary_id,
                    deployment_id="vps-live",
                    account_identity_fingerprint=IDENTITY,
                    symbol="BTCUSDC", effective_at=NOW,
                )
        conn.rollback()
    finally:
        conn.close()
