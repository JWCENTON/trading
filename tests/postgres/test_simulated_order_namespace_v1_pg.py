from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from common.simulated_execution_evidence import (
    SimulatedOrderWriteBlocked,
    create_simulated_order_cursor,
)
from common.simulated_order_namespace import (
    ADMINISTRATIVE_ORDER_CLASS,
    CONTRACT_CHECKSUM,
    MIGRATION_ID,
    NAMESPACE_SCHEMA_VERSION,
    detect_simulated_order_namespace,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations" / MIGRATION_ID).read_text()
LEDGER = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()

LEGACY_SCHEMA = """
CREATE TABLE positions(id INTEGER PRIMARY KEY);
CREATE TABLE simulated_orders(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  symbol TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  strategy TEXT NOT NULL,
  side TEXT NOT NULL,
  price NUMERIC NOT NULL,
  quantity_btc NUMERIC NOT NULL,
  reason TEXT,
  rsi_14 NUMERIC,
  ema_21 NUMERIC,
  candle_open_time TIMESTAMPTZ NOT NULL,
  is_exit BOOLEAN NOT NULL DEFAULT false,
  CONSTRAINT sim_orders_uniq_candle_exit
    UNIQUE(symbol,"interval",strategy,candle_open_time,is_exit)
);
CREATE UNIQUE INDEX ux_sim_orders_one_per_candle
  ON simulated_orders(symbol,"interval",strategy,candle_open_time);
CREATE UNIQUE INDEX ux_sim_orders_one_per_candle_isexit
  ON simulated_orders(symbol,"interval",strategy,candle_open_time,is_exit);
CREATE TABLE simulated_execution_fills_v1(
  id BIGSERIAL PRIMARY KEY,
  simulated_order_id BIGINT NOT NULL REFERENCES simulated_orders(id),
  position_id BIGINT NOT NULL REFERENCES positions(id),
  order_purpose TEXT NOT NULL,
  side TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL
);
"""


@pytest.fixture()
def namespace_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_order_namespace"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(LEGACY_SCHEMA)
        cur.execute(LEDGER)
    conn.commit()
    yield conn
    conn.close()


def _position(cur, position_id: int) -> None:
    cur.execute("INSERT INTO positions(id) VALUES (%s)", (position_id,))


def _legacy_order(
    cur,
    *,
    order_id: int | None = None,
    symbol: str = "SOLUSDC",
    interval: str = "1m",
    strategy: str = "BBRANGE",
    candle: datetime,
    reason: str = "LEGACY_ADMINISTRATIVE_CLOSE",
    side: str = "SELL",
    is_exit: bool = True,
) -> int:
    columns = ""
    values = ""
    params: tuple = ()
    if order_id is not None:
        columns = "id,"
        values = "%s,"
        params = (order_id,)
    cur.execute(
        f"""
        INSERT INTO simulated_orders(
          {columns}symbol,"interval",strategy,side,price,quantity_btc,
          reason,candle_open_time,is_exit
        ) VALUES ({values}%s,%s,%s,%s,100,1,%s,%s,%s)
        RETURNING id
        """,
        params + (
            symbol, interval, strategy, side, reason, candle, is_exit,
        ),
    )
    return int(cur.fetchone()[0])


def _fill(
    cur,
    order_id: int,
    position_id: int,
    *,
    environment: str = "paper",
    deployment_id: str = "local-paper",
) -> int:
    cur.execute(
        """
        INSERT INTO simulated_execution_fills_v1(
          simulated_order_id,position_id,order_purpose,side,
          environment,deployment_id
        ) VALUES (%s,%s,'EXIT','SELL',%s,%s) RETURNING id
        """,
        (order_id, position_id, environment, deployment_id),
    )
    return int(cur.fetchone()[0])


def _write(
    cur,
    *,
    candle: datetime,
    price: str = "100",
    reason: str = "FORWARD_INTENT",
    is_exit: bool = False,
    order_class: str = "FORWARD",
    position_id: int | None = None,
    environment: str | None = None,
    deployment_id: str | None = None,
):
    return create_simulated_order_cursor(
        cur,
        symbol="SOLUSDC",
        interval="1m",
        strategy="BBRANGE",
        side="SELL" if is_exit else "BUY",
        price=Decimal(price),
        quantity=Decimal("1"),
        reason=reason,
        candle_open_time=candle,
        is_exit=is_exit,
        order_class=order_class,
        position_id=position_id,
        environment=environment,
        deployment_id=deployment_id,
    )


def _apply(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def test_legacy_code_forward_success_replay_conflict_and_retirement_gate(
    namespace_db,
):
    candle = datetime(2026, 8, 2, 8, 39, tzinfo=timezone.utc)
    legacy_readiness = detect_simulated_order_namespace(namespace_db)
    assert legacy_readiness.forward_writer_readiness == (
        "PRESENT_VALID_LEGACY_COMPAT"
    )
    assert legacy_readiness.retirement_writer_readiness == (
        "SIMULATED_ORDER_NAMESPACE_MIGRATION_REQUIRED"
    )
    with namespace_db.cursor() as cur:
        first = _write(cur, candle=candle)
        replay = _write(cur, candle=candle)
        conflict = _write(cur, candle=candle, price="101")
        assert isinstance(first, int)
        assert replay == SimulatedOrderWriteBlocked(
            "IDEMPOTENT_EXISTING_FORWARD_ORDER", first
        )
        assert conflict == SimulatedOrderWriteBlocked(
            "PAPER_ORDER_SLOT_ALREADY_OCCUPIED", first
        )
        _position(cur, 6394)
        with pytest.raises(
            RuntimeError, match="SIMULATED_ORDER_NAMESPACE_MIGRATION_REQUIRED"
        ):
            _write(
                cur, candle=candle, reason=ADMINISTRATIVE_ORDER_CLASS,
                is_exit=True, order_class=ADMINISTRATIVE_ORDER_CLASS,
                position_id=6394, environment="paper", deployment_id="vps-paper",
            )
        cur.execute("SELECT count(*) FROM simulated_orders")
        assert cur.fetchone()[0] == 1
    namespace_db.rollback()


def test_migration_backfills_preserves_ids_and_is_idempotent(namespace_db):
    candle = datetime(2026, 8, 1, 23, 31, tzinfo=timezone.utc)
    with namespace_db.cursor() as cur:
        for position_id in (10326, 10333, 10340):
            _position(cur, position_id)
            order_id = _legacy_order(
                cur, candle=candle.replace(minute=candle.minute + position_id % 3)
            )
            _fill(cur, order_id, position_id)
    namespace_db.commit()

    _apply(namespace_db)
    _apply(namespace_db)

    readiness = detect_simulated_order_namespace(namespace_db)
    assert readiness.is_namespace_v1, readiness.issues
    assert readiness.public_payload() == {
        "schema_version": "SIMULATED_ORDER_NAMESPACE_V1",
        "status": "PRESENT_VALID",
        "columns": {
            "order_class": True,
            "position_id": True,
            "environment": True,
            "deployment_id": True,
        },
        "forward_slot_constraint": "PRESENT_VALID",
        "administrative_position_idempotency": "PRESENT_VALID",
        "legacy_global_constraints_absent": True,
        "forward_writer_readiness": "PRESENT_VALID",
        "retirement_writer_readiness": "PRESENT_VALID",
        "issues": [],
    }
    with namespace_db.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),count(DISTINCT position_id),min(environment),
                   min(deployment_id)
            FROM simulated_orders
            WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE'
            """
        )
        assert cur.fetchone() == (3, 3, "paper", "local-paper")
        cur.execute(
            "SELECT count(*),min(checksum_sha256),min(schema_baseline_version) "
            "FROM schema_migration_ledger_v1 WHERE migration_id=%s",
            (MIGRATION_ID,),
        )
        assert cur.fetchone() == (1, CONTRACT_CHECKSUM, NAMESPACE_SCHEMA_VERSION)


def test_incident_6394_retirement_then_forward_same_slot(namespace_db):
    candle = datetime(2026, 8, 2, 8, 39, tzinfo=timezone.utc)
    with namespace_db.cursor() as cur:
        _position(cur, 6394)
        order_id = _legacy_order(cur, order_id=33339, candle=candle)
        fill_id = _fill(
            cur, order_id, 6394,
            environment="paper", deployment_id="vps-paper",
        )
    namespace_db.commit()
    _apply(namespace_db)

    with namespace_db.cursor() as cur:
        forward_id = _write(cur, candle=candle)
        assert isinstance(forward_id, int)
        cur.execute(
            "SELECT id,price,quantity_btc,order_class,position_id "
            "FROM simulated_orders WHERE id=33339"
        )
        assert cur.fetchone() == (
            33339, Decimal("100"), Decimal("1"),
            ADMINISTRATIVE_ORDER_CLASS, 6394,
        )
        cur.execute(
            "SELECT id FROM simulated_execution_fills_v1 "
            "WHERE simulated_order_id=33339"
        )
        assert cur.fetchone()[0] == fill_id
        cur.execute(
            "SELECT count(*) FROM simulated_orders WHERE symbol='SOLUSDC' "
            "AND \"interval\"='1m' AND strategy='BBRANGE' "
            "AND candle_open_time=%s",
            (candle,),
        )
        assert cur.fetchone()[0] == 2


def test_forward_then_two_distinct_administrative_positions_same_slot(namespace_db):
    candle = datetime(2026, 8, 2, 9, 0, tzinfo=timezone.utc)
    _apply(namespace_db)
    with namespace_db.cursor() as cur:
        forward_id = _write(cur, candle=candle)
        for position_id in (7001, 7002):
            _position(cur, position_id)
            admin_id = _write(
                cur, candle=candle, reason=ADMINISTRATIVE_ORDER_CLASS,
                is_exit=True, order_class=ADMINISTRATIVE_ORDER_CLASS,
                position_id=position_id, environment="paper",
                deployment_id="local-paper",
            )
            assert isinstance(admin_id, int)
        cur.execute(
            "SELECT count(*) FROM simulated_orders WHERE candle_open_time=%s",
            (candle,),
        )
        assert cur.fetchone()[0] == 3
        assert isinstance(forward_id, int)


def test_duplicate_administrative_position_is_idempotent(namespace_db):
    candle = datetime(2026, 8, 2, 9, 1, tzinfo=timezone.utc)
    _apply(namespace_db)
    with namespace_db.cursor() as cur:
        _position(cur, 7003)
        first = _write(
            cur, candle=candle, reason=ADMINISTRATIVE_ORDER_CLASS,
            is_exit=True, order_class=ADMINISTRATIVE_ORDER_CLASS,
            position_id=7003, environment="paper", deployment_id="local-paper",
        )
        replay = _write(
            cur, candle=candle, reason=ADMINISTRATIVE_ORDER_CLASS,
            is_exit=True, order_class=ADMINISTRATIVE_ORDER_CLASS,
            position_id=7003, environment="paper", deployment_id="local-paper",
        )
        assert replay == SimulatedOrderWriteBlocked(
            "IDEMPOTENT_EXISTING_ADMINISTRATIVE_ORDER", first
        )
        cur.execute(
            "SELECT count(*) FROM simulated_orders WHERE position_id=7003"
        )
        assert cur.fetchone()[0] == 1


def test_unexpected_unique_violation_remains_a_technical_error(namespace_db):
    first_candle = datetime(2026, 8, 2, 9, 2, tzinfo=timezone.utc)
    _apply(namespace_db)
    with namespace_db.cursor() as cur:
        cur.execute(
            "CREATE UNIQUE INDEX test_unexpected_reason_unique "
            "ON simulated_orders(reason)"
        )
        assert isinstance(_write(cur, candle=first_candle), int)
    namespace_db.commit()
    with namespace_db.cursor() as cur:
        with pytest.raises(psycopg2.errors.UniqueViolation):
            _write(cur, candle=first_candle.replace(minute=3))
    namespace_db.rollback()


@pytest.mark.parametrize("failure", ("missing_fill", "ambiguous_fill", "duplicate"))
def test_invalid_backfill_rolls_back_atomically(namespace_db, failure):
    candle = datetime(2026, 8, 2, 10, 0, tzinfo=timezone.utc)
    with namespace_db.cursor() as cur:
        _position(cur, 8001)
        first = _legacy_order(cur, candle=candle)
        if failure != "missing_fill":
            _fill(cur, first, 8001)
        if failure == "ambiguous_fill":
            _fill(cur, first, 8001)
        if failure == "duplicate":
            second = _legacy_order(
                cur, candle=candle.replace(minute=1)
            )
            _fill(cur, second, 8001)
    namespace_db.commit()

    with pytest.raises(psycopg2.Error):
        _apply(namespace_db)
    namespace_db.rollback()
    with namespace_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='simulated_orders' "
            "AND column_name='order_class'"
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 "
            "WHERE migration_id=%s",
            (MIGRATION_ID,),
        )
        assert cur.fetchone()[0] == 0
