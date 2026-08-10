from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.entry_fill_reconciliation import reconcile_pending_entry_fills
from common.exchange_fill_change_control import (
    FillMutationDecision,
    InventoryRowGeneration,
    _resolve_pending_entry_generation,
    attribute_fill_change_position,
    authoritative_fill_fingerprint,
    authoritative_fill_payload,
    mark_fill_change_applied,
    register_fill_change,
)


SCHEMA = """
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGSERIAL PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  status TEXT NOT NULL,
  adopted_at TIMESTAMPTZ,
  UNIQUE (contract_name,environment,deployment_id,generation)
);
CREATE UNIQUE INDEX ux_adoption_active_test
  ON runtime_contract_adoption_v2(contract_name,environment,deployment_id)
  WHERE status='ACTIVE';

CREATE TABLE positions (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  status TEXT NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  entry_price NUMERIC NOT NULL,
  entry_time TIMESTAMPTZ NOT NULL,
  exit_price NUMERIC,
  exit_time TIMESTAMPTZ,
  exit_reason TEXT,
  entry_order_id TEXT,
  exit_order_id TEXT,
  entry_client_order_id TEXT,
  fees_usdc NUMERIC,
  entry_context_json JSONB,
  entry_hour_utc SMALLINT,
  entry_day_utc DATE,
  inventory_contract_adoption_id BIGINT,
  inventory_contract_generation BIGINT,
  gross_entry_executed_qty NUMERIC,
  entry_base_fee_qty NUMERIC,
  net_entry_inventory_qty NUMERIC,
  cumulative_exit_executed_qty NUMERIC NOT NULL DEFAULT 0,
  exit_inventory_reduction_qty NUMERIC NOT NULL DEFAULT 0,
  remaining_inventory_qty NUMERIC,
  inventory_evidence_status TEXT,
  inventory_calculated_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX ux_positions_open
  ON positions(symbol,strategy,"interval") WHERE status='OPEN';

CREATE OR REPLACE FUNCTION is_existing_projected_c2_2_compatible(
  BIGINT,TEXT
) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$ SELECT false $$;

CREATE TABLE binance_orders (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  order_type TEXT NOT NULL,
  client_order_id TEXT,
  order_id TEXT,
  status TEXT,
  raw JSONB,
  position_id BIGINT,
  is_exit BOOLEAN,
  strategy TEXT,
  "interval" TEXT,
  order_purpose TEXT,
  requested_qty NUMERIC,
  order_accepted BOOLEAN,
  exchange_source TEXT,
  reconciliation_status TEXT,
  reconciled_position_id BIGINT,
  reconciled_at TIMESTAMPTZ,
  reconciled_fill_count INTEGER NOT NULL DEFAULT 0,
  reconciled_executed_qty NUMERIC NOT NULL DEFAULT 0,
  unreconciled_qty NUMERIC NOT NULL DEFAULT 0,
  reconciliation_error TEXT,
  last_reconciliation_action TEXT,
  UNIQUE(exchange_source,symbol,order_id)
);

CREATE TABLE binance_order_fills (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  role TEXT,
  executed_qty NUMERIC NOT NULL,
  avg_price NUMERIC NOT NULL,
  quote_notional_usdc NUMERIC,
  commission_amount NUMERIC,
  commission_asset TEXT,
  commission_usdc NUMERIC,
  event_time TIMESTAMPTZ NOT NULL,
  fill_idx INTEGER NOT NULL DEFAULT 0,
  raw JSONB,
  UNIQUE(source,trade_id)
);

CREATE TABLE exchange_fill_ingestion_state_v2 (
  ingestion_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  account_identity_key TEXT NOT NULL,
  symbol TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  side TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT,
  applied_at TIMESTAMPTZ,
  application_status TEXT NOT NULL,
  correction_revision INTEGER NOT NULL DEFAULT 0,
  authoritative_payload JSONB NOT NULL,
  last_decision TEXT NOT NULL,
  local_fill_id BIGINT,
  adoption_id BIGINT,
  contract_generation BIGINT,
  UNIQUE(source,account_identity_key,symbol,trade_id)
);

CREATE TABLE strategy_events (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  event_type TEXT NOT NULL,
  decision TEXT,
  info JSONB NOT NULL
);
"""


def _database(disposable_postgres_v16, purpose: str):
    name = f"waltrade_baseline_test_bootstrap_{purpose}_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()
    return conn


def _active_adoption(
    cur, *, adopted_at: datetime, deployment_id: str = "local-live"
):
    cur.execute(
        """
        INSERT INTO runtime_contract_adoption_v2(
          contract_name,environment,deployment_id,generation,status,adopted_at
        ) VALUES (
          'FEE_AWARE_INVENTORY_C2_2','live',%s,1,'ACTIVE',%s
        ) RETURNING adoption_id
        """,
        (deployment_id, adopted_at),
    )
    return int(cur.fetchone()[0])


def _fill_row(
    *,
    trade_id: str,
    order_id: str,
    client_order_id: str | None,
    side: str,
    event_time: datetime,
    qty: str,
    price: str,
    fee: str,
    fee_asset: str,
    deployment_id: str = "local-live",
    symbol: str = "BNBUSDC",
):
    base_asset = symbol.removesuffix("USDC")
    return {
        "source": "okx",
        "trade_id": trade_id,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "symbol": symbol,
        "side": side,
        "role": "TAKER",
        "executed_qty": qty,
        "avg_price": price,
        "quote_notional_usdc": str(Decimal(qty) * Decimal(price)),
        "commission_amount": fee,
        "commission_asset": fee_asset,
        "commission_usdc": (
            str(Decimal(fee) * Decimal(price))
            if fee_asset == base_asset
            else fee
        ),
        "event_time_ms": int(event_time.timestamp() * 1000),
        "fill_idx": 0,
        "raw": json.dumps({"clOrdId": client_order_id}),
        "environment": "live",
        "deployment_id": deployment_id,
        "account_identity_id": 1,
        "account_identity_status": "VERIFIED",
    }


def _insert_canonical_fill(cur, row):
    cur.execute(
        """
        INSERT INTO binance_order_fills(
          source,trade_id,order_id,symbol,side,role,executed_qty,avg_price,
          quote_notional_usdc,commission_amount,commission_asset,
          commission_usdc,event_time,fill_idx,raw
        ) VALUES (
          %(source)s,%(trade_id)s,%(order_id)s,%(symbol)s,%(side)s,%(role)s,
          %(executed_qty)s,%(avg_price)s,%(quote_notional_usdc)s,
          %(commission_amount)s,%(commission_asset)s,%(commission_usdc)s,
          to_timestamp(%(event_time_ms)s/1000.0),%(fill_idx)s,%(raw)s::jsonb
        ) ON CONFLICT(source,trade_id) DO NOTHING
        """,
        row,
    )


def _insert_live_order_event(
    cur,
    *,
    created_at: datetime,
    order_id: str,
    local_cid: str,
    wire_cid: str,
    symbol: str = "BNBUSDC",
    strategy: str = "TREND",
):
    cur.execute(
        """
        INSERT INTO strategy_events(
          created_at,symbol,strategy,"interval",event_type,decision,info
        ) VALUES (
          %s,%s,%s,'1m','LIVE_ORDER_SENT','BUY',%s::jsonb
        )
        """,
        (
            created_at,
            symbol,
            strategy,
            json.dumps({
                "resp": {
                    "orderId": order_id,
                    "clientOrderId": wire_cid,
                },
                "client_order_id": local_cid,
                "exchange_source": "okx",
                "order_accepted": True,
                "is_exit": False,
                "order_purpose": "ENTRY",
                "status": "NEW",
                "executed_qty": 0,
            }),
        ),
    )


@pytest.mark.parametrize(
    (
        "deployment_id", "symbol", "strategy", "local_order_id",
        "order_id", "trade_id", "qty", "fee", "expected_net",
    ),
    (
        (
            "local-live", "BNBUSDC", "TREND", None,
            "forward-delayed-order-1", "forward-delayed-fill-1",
            "0.10000000", "0.00040000", "0.09960000",
        ),
        (
            "vps-live", "BTCUSDC", "RSI", 3825,
            "3819011200977670144", "4400542",
            "0.00030817", "0.000001078595", "0.000307091405",
        ),
    ),
    ids=("local-live", "vps-live-forward-incident"),
)
def test_delayed_new_zero_fill_bootstraps_canonical_then_fee_aware_position(
    disposable_postgres_v16, monkeypatch, deployment_id, symbol, strategy,
    local_order_id, order_id, trade_id, qty, fee, expected_net
):
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", deployment_id)
    conn = _database(
        disposable_postgres_v16, f"forward_{deployment_id.replace('-', '_')}"
    )
    account = "1"
    event_ms = (int(time.time() * 1000) // 1000) * 1000
    event_time = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc)
    order_time = event_time - timedelta(seconds=3)
    strategy_prefix = strategy[:4].upper()
    local_cid = f"ORC-L-{symbol}-{strategy_prefix}-1m-E-abcdef12"
    wire_cid = local_cid.replace("-", "")
    row = _fill_row(
        trade_id=trade_id,
        order_id=order_id,
        client_order_id=wire_cid,
        side="BUY",
        event_time=event_time,
        qty=qty,
        price="60000.00" if symbol == "BTCUSDC" else "600.00",
        fee=fee,
        fee_asset=symbol.removesuffix("USDC"),
        deployment_id=deployment_id,
        symbol=symbol,
    )
    try:
        with conn.cursor() as cur:
            adoption_id = _active_adoption(
                cur,
                adopted_at=order_time - timedelta(minutes=1),
                deployment_id=deployment_id,
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,created_at,symbol,side,order_type,client_order_id,order_id,
                  status,raw,position_id,is_exit,strategy,"interval",
                  order_purpose,requested_qty,order_accepted,exchange_source
                ) VALUES (
                  COALESCE(%s,nextval('binance_orders_id_seq')),%s,%s,'BUY',
                  'MARKET',%s,%s,'NEW',%s::jsonb,NULL,false,%s,'1m','ENTRY',
                  %s,true,'okx'
                )
                """,
                (
                    local_order_id,
                    order_time,
                    symbol,
                    local_cid,
                    order_id,
                    json.dumps({
                        "orderId": order_id,
                        "clientOrderId": wire_cid,
                        "status": "NEW",
                        "executedQty": "0",
                    }),
                    strategy,
                    qty,
                ),
            )
            assert _resolve_pending_entry_generation(
                cur, row, account_identity_key=account
            ) == (None, None, None)
            _insert_live_order_event(
                cur,
                created_at=order_time,
                order_id=order_id,
                local_cid=local_cid,
                wire_cid=wire_cid,
                symbol=symbol,
                strategy=strategy,
            )

            change = register_fill_change(
                cur, row, account_identity_key=account
            )
            assert change.row_generation is (
                InventoryRowGeneration.FORWARD_C2_2_PENDING_ENTRY
            )
            assert change.permits_mutation is True
            assert change.adoption_id == adoption_id
            attribute_fill_change_position(cur, row, change)
            assert cur.rowcount == 0
            _insert_canonical_fill(cur, row)

        stats = reconcile_pending_entry_fills(
            conn, batch_size=10, trading_mode="LIVE"
        )
        assert (stats.scanned, stats.created, stats.failed) == (1, 1, 0)
        with conn.cursor() as cur:
            mark_fill_change_applied(cur, change)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT qty,gross_entry_executed_qty,entry_base_fee_qty,
                       net_entry_inventory_qty,remaining_inventory_qty,
                       inventory_evidence_status,entry_order_id
                FROM positions WHERE entry_order_id=%s
                """,
                (order_id,),
            )
            assert cur.fetchone() == (
                Decimal(expected_net),
                Decimal(qty),
                Decimal(fee),
                Decimal(expected_net),
                Decimal(expected_net),
                "COMPLETE",
                order_id,
            )
            cur.execute(
                "SELECT application_status,adoption_id,contract_generation "
                "FROM exchange_fill_ingestion_state_v2 WHERE trade_id=%s",
                (row["trade_id"],),
            )
            assert cur.fetchone() == ("APPLIED", adoption_id, 1)

            replay = register_fill_change(
                cur, row, account_identity_key=account
            )
            assert replay.decision is FillMutationDecision.NO_CHANGE
            assert replay.permits_mutation is False
            _insert_canonical_fill(cur, row)
        second = reconcile_pending_entry_fills(
            conn, batch_size=10, trading_mode="LIVE"
        )
        assert (second.scanned, second.created, second.updated) == (0, 0, 0)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM binance_order_fills WHERE trade_id=%s",
                (row["trade_id"],),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM positions WHERE entry_order_id=%s",
                (order_id,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.parametrize(
    ("deployment_id", "source", "side", "identity_status"),
    (
        ("local-paper", "okx", "BUY", "VERIFIED"),
        ("vps-paper", "okx", "BUY", "VERIFIED"),
        ("UNKNOWN", "okx", "BUY", "VERIFIED"),
        (None, "okx", "BUY", "VERIFIED"),
        ("other-live", "okx", "BUY", "VERIFIED"),
        ("vps-live", "manual", "BUY", "VERIFIED"),
        ("vps-live", "okx", "SELL", "VERIFIED"),
        ("vps-live", "okx", "BUY", "CONFLICTING"),
    ),
)
def test_delayed_entry_bootstrap_fails_closed_before_database_lookup(
    deployment_id, source, side, identity_status
):
    class NoQueryCursor:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("fail-closed input must not query the database")

    event_time = datetime.now(timezone.utc)
    row = _fill_row(
        trade_id="fail-closed-trade",
        order_id="fail-closed-order",
        client_order_id="ORCLBTCUSDCRSI1mEabcdef12",
        side=side,
        event_time=event_time,
        qty="0.00030817",
        price="60000.00",
        fee="0.000001078595",
        fee_asset="BTC",
        deployment_id=deployment_id,
        symbol="BTCUSDC",
    )
    row["source"] = source
    row["account_identity_status"] = identity_status

    assert _resolve_pending_entry_generation(
        NoQueryCursor(), row, account_identity_key="1"
    ) == (None, None, None)


@pytest.mark.parametrize(
    ("processing_order", "batch_mode", "preobserved"),
    (
        (("A", "B"), False, False),
        (("B", "A"), False, False),
        (("A", "B"), True, False),
        (("A", "B"), True, True),
    ),
    ids=(
        "chronological",
        "reverse-retry",
        "single-ingest-batch",
        "existing-observed-cohort",
    ),
)
def test_two_delayed_buy_orders_share_one_fee_aware_open_position(
    disposable_postgres_v16, monkeypatch, processing_order, batch_mode,
    preobserved
):
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "vps-live")
    conn = _database(
        disposable_postgres_v16,
        f"two_delayed_{''.join(processing_order).lower()}_"
        f"{int(batch_mode)}_{int(preobserved)}",
    )
    account = "1"
    now_ms = (int(time.time() * 1000) // 1000) * 1000
    base_time = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    incidents = {
        "A": {
            "local_order_id": 3825,
            "order_id": "3819011200977670144",
            "trade_id": "4400542",
            "qty": "0.00030817",
            "fee": "0.000001078595",
            "price": "120000.00",
            "event_time": base_time - timedelta(minutes=2),
            "cid": "ORC-L-BTCUSDC-RSI-1m-E-aaaaaa01",
        },
        "B": {
            "local_order_id": 3826,
            "order_id": "3820730809883758592",
            "trade_id": "4414544",
            "qty": "0.00030888",
            "fee": "0.00000108108",
            "price": "121000.00",
            "event_time": base_time - timedelta(minutes=1),
            "cid": "ORC-L-BTCUSDC-RSI-1m-E-bbbbbb02",
        },
    }
    changes = {}

    def fill_row(label):
        incident = incidents[label]
        return _fill_row(
            trade_id=incident["trade_id"],
            order_id=incident["order_id"],
            client_order_id=incident["cid"].replace("-", ""),
            side="BUY",
            event_time=incident["event_time"],
            qty=incident["qty"],
            price=incident["price"],
            fee=incident["fee"],
            fee_asset="BTC",
            deployment_id="vps-live",
            symbol="BTCUSDC",
        )

    try:
        with conn.cursor() as cur:
            _active_adoption(
                cur,
                adopted_at=base_time - timedelta(minutes=5),
                deployment_id="vps-live",
            )
            for incident in incidents.values():
                wire_cid = incident["cid"].replace("-", "")
                cur.execute(
                    """
                    INSERT INTO binance_orders(
                      id,created_at,symbol,side,order_type,client_order_id,
                      order_id,status,raw,position_id,is_exit,strategy,
                      "interval",order_purpose,requested_qty,order_accepted,
                      exchange_source
                    ) VALUES (
                      %s,%s,'BTCUSDC','BUY','MARKET',%s,%s,'NEW',%s::jsonb,
                      NULL,false,'RSI','1m','ENTRY',%s,true,'okx'
                    )
                    """,
                    (
                        incident["local_order_id"],
                        incident["event_time"] - timedelta(seconds=3),
                        incident["cid"],
                        incident["order_id"],
                        json.dumps({
                            "orderId": incident["order_id"],
                            "clientOrderId": wire_cid,
                            "status": "NEW",
                            "executedQty": "0",
                        }),
                        incident["qty"],
                    ),
                )
                _insert_live_order_event(
                    cur,
                    created_at=incident["event_time"] - timedelta(seconds=3),
                    order_id=incident["order_id"],
                    local_cid=incident["cid"],
                    wire_cid=wire_cid,
                    symbol="BTCUSDC",
                    strategy="RSI",
                )

        if preobserved:
            with conn.cursor() as cur:
                for label in processing_order:
                    observed = register_fill_change(
                        cur, fill_row(label), account_identity_key=account
                    )
                    assert observed.permits_mutation is True
            conn.commit()

        for label in processing_order:
            row = fill_row(label)
            with conn.cursor() as cur:
                change = register_fill_change(
                    cur, row, account_identity_key=account
                )
                assert change.row_generation is (
                    InventoryRowGeneration.FORWARD_C2_2_PENDING_ENTRY
                )
                assert change.permits_mutation is True
                attribute_fill_change_position(cur, row, change)
                _insert_canonical_fill(cur, row)
            changes[label] = (row, change)
            if not batch_mode:
                stats = reconcile_pending_entry_fills(
                    conn, batch_size=10, trading_mode="LIVE"
                )
                assert stats.failed == 0
                assert stats.ambiguous == 0
                assert stats.created + stats.updated == 1
                with conn.cursor() as cur:
                    mark_fill_change_applied(cur, change)
                conn.commit()

        if batch_mode:
            stats = reconcile_pending_entry_fills(
                conn, batch_size=10, trading_mode="LIVE"
            )
            assert (stats.scanned, stats.created, stats.updated) == (2, 1, 1)
            assert (stats.failed, stats.ambiguous) == (0, 0)
            with conn.cursor() as cur:
                for _row, change in changes.values():
                    mark_fill_change_applied(cur, change)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,qty,gross_entry_executed_qty,entry_base_fee_qty,
                       net_entry_inventory_qty,remaining_inventory_qty,
                       inventory_evidence_status
                FROM positions
                WHERE symbol='BTCUSDC' AND strategy='RSI' AND "interval"='1m'
                  AND status='OPEN'
                """
            )
            positions = cur.fetchall()
            assert len(positions) == 1
            position_id = positions[0][0]
            assert positions[0][1:] == (
                Decimal("0.000614890325"),
                Decimal("0.00061705"),
                Decimal("0.000002159675"),
                Decimal("0.000614890325"),
                Decimal("0.000614890325"),
                "COMPLETE",
            )
            cur.execute(
                """
                SELECT order_id,reconciled_position_id
                FROM binance_orders WHERE id IN (3825,3826) ORDER BY id
                """
            )
            assert cur.fetchall() == [
                (incidents["A"]["order_id"], position_id),
                (incidents["B"]["order_id"], position_id),
            ]
            cur.execute(
                """
                SELECT count(*),count(DISTINCT trade_id)
                FROM binance_order_fills WHERE trade_id IN ('4400542','4414544')
                """
            )
            assert cur.fetchone() == (2, 2)
            cur.execute(
                """
                SELECT f.trade_id,bo.reconciled_position_id
                FROM binance_order_fills f
                JOIN binance_orders bo
                  ON bo.exchange_source=f.source
                 AND bo.symbol=f.symbol AND bo.order_id=f.order_id
                WHERE f.trade_id IN ('4400542','4414544')
                ORDER BY f.trade_id
                """
            )
            assert cur.fetchall() == [
                ("4400542", position_id),
                ("4414544", position_id),
            ]
            cur.execute(
                """
                SELECT count(*)
                FROM exchange_fill_ingestion_state_v2
                WHERE trade_id IN ('4400542','4414544')
                  AND application_status='APPLIED'
                  AND applied_fingerprint IS NOT NULL
                  AND applied_at IS NOT NULL
                  AND local_fill_id IS NOT NULL
                  AND adoption_id IS NOT NULL
                  AND contract_generation=1
                """
            )
            assert cur.fetchone()[0] == 2

        for label in ("A", "B", "A", "B"):
            row, _change = changes[label]
            with conn.cursor() as cur:
                replay = register_fill_change(
                    cur, row, account_identity_key=account
                )
                assert replay.decision is FillMutationDecision.NO_CHANGE
                assert replay.permits_mutation is False
                _insert_canonical_fill(cur, row)
        assert reconcile_pending_entry_fills(
            conn, batch_size=10, trading_mode="LIVE"
        ).scanned == 0
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM positions WHERE status='OPEN'")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT count(*) FROM binance_order_fills")
            assert cur.fetchone()[0] == 2
    finally:
        conn.rollback()
        conn.close()


def test_historical_bnb_four_fill_cohort_remains_observed_not_applied(
    disposable_postgres_v16, monkeypatch
):
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    conn = _database(disposable_postgres_v16, "bnb_non_replay")
    account = "1"
    historical = (
        (
            "341287", "3788537826749489152",
            "ORC-L-BNBUSDC-TREN-1m-E-3bf088c1",
            "ORCLBNBUSDCTREN1mE3bf088c1", "BUY",
            "0.033895", "587.1", "0.0001186325", "BNB",
            datetime(2026, 7, 30, 11, 7, 5, 691000, tzinfo=timezone.utc),
        ),
        (
            "341291", "3788542253183049728",
            "ORC-L-BNBUSDC-TREN-1m-E-020e9734",
            "ORCLBNBUSDCTREN1mE020e9734", "BUY",
            "0.033878", "587.4", "0.000118573", "BNB",
            datetime(2026, 7, 30, 11, 9, 17, 610000, tzinfo=timezone.utc),
        ),
        (
            "341316", "3788553373692239872",
            "ORC-L-BNBUSDC-TREN-1m-E-2fcb739d",
            "ORCLBNBUSDCTREN1mE2fcb739d", "BUY",
            "0.033745", "589.7", "0.0001181075", "BNB",
            datetime(2026, 7, 30, 11, 14, 49, 27000, tzinfo=timezone.utc),
        ),
        (
            "341617", "3789163681263689728", None, None, "SELL",
            "0.101163", "592.8", "0.1199388528", "USDC",
            datetime(2026, 7, 30, 16, 19, 22, 517000, tzinfo=timezone.utc),
        ),
    )
    try:
        with conn.cursor() as cur:
            _active_adoption(
                cur,
                adopted_at=datetime(
                    2026, 7, 29, 22, 42, 28, tzinfo=timezone.utc
                ),
            )
            rows = []
            for (
                trade_id, order_id, local_cid, wire_cid, side,
                qty, price, fee, fee_asset, event_time,
            ) in historical:
                row = _fill_row(
                    trade_id=trade_id,
                    order_id=order_id,
                    client_order_id=wire_cid,
                    side=side,
                    event_time=event_time,
                    qty=qty,
                    price=price,
                    fee=fee,
                    fee_asset=fee_asset,
                )
                rows.append(row)
                if side == "BUY":
                    cur.execute(
                        """
                        INSERT INTO binance_orders(
                          created_at,symbol,side,order_type,client_order_id,
                          order_id,status,raw,is_exit,strategy,"interval",
                          order_purpose,requested_qty,order_accepted,
                          exchange_source
                        ) VALUES (
                          %s,'BNBUSDC','BUY','MARKET',%s,%s,'NEW',%s::jsonb,
                          false,'TREND','1m','ENTRY',%s,true,'okx'
                        )
                        """,
                        (
                            event_time + timedelta(milliseconds=100),
                            local_cid,
                            order_id,
                            json.dumps({
                                "orderId": order_id,
                                "clientOrderId": wire_cid,
                                "status": "NEW",
                                "executedQty": "0",
                            }),
                            qty,
                        ),
                    )
                    _insert_live_order_event(
                        cur,
                        created_at=event_time + timedelta(milliseconds=100),
                        order_id=order_id,
                        local_cid=local_cid,
                        wire_cid=wire_cid,
                    )
                payload = authoritative_fill_payload(
                    row, account_identity_key=account
                )
                cur.execute(
                    """
                    INSERT INTO exchange_fill_ingestion_state_v2(
                      source,account_identity_key,symbol,trade_id,order_id,
                      side,source_fingerprint,application_status,
                      authoritative_payload,last_decision
                    ) VALUES (
                      'okx',%s,'BNBUSDC',%s,%s,%s,%s,
                      'OBSERVED_NOT_APPLIED',%s::jsonb,'OBSERVED_NOT_APPLIED'
                    )
                    """,
                    (
                        account,
                        trade_id,
                        order_id,
                        side,
                        authoritative_fill_fingerprint(payload),
                        json.dumps(payload, sort_keys=True),
                    ),
                )

            for row in rows:
                replay = register_fill_change(
                    cur, row, account_identity_key=account
                )
                assert replay.decision is (
                    FillMutationDecision.OBSERVED_NOT_APPLIED
                )
                assert replay.permits_mutation is False

            cur.execute("SELECT count(*) FROM binance_order_fills")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM positions")
            assert cur.fetchone()[0] == 0
            cur.execute(
                """
                SELECT application_status,count(*)
                FROM exchange_fill_ingestion_state_v2
                GROUP BY application_status
                """
            )
            assert cur.fetchall() == [("OBSERVED_NOT_APPLIED", 4)]
    finally:
        conn.rollback()
        conn.close()
