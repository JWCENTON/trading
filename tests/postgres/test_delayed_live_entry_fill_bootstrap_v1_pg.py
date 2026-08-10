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
  account_identity_id BIGINT,
  account_identity_status TEXT,
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
  account_identity_id BIGINT,
  account_identity_status TEXT,
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
  ownership_classification TEXT,
  UNIQUE(source,account_identity_key,symbol,trade_id)
);

CREATE TABLE canonical_financial_truth_v1 (
  position_id BIGINT PRIMARY KEY,
  financial_truth_status TEXT NOT NULL,
  executed_entry_qty NUMERIC,
  executed_exit_qty NUMERIC,
  remaining_qty NUMERIC,
  authoritative_entry_fees_usdc NUMERIC,
  authoritative_exit_fees_usdc NUMERIC,
  authoritative_gross_pnl NUMERIC,
  authoritative_net_pnl NUMERIC,
  authoritative_source TEXT,
  authoritative_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_fill_ids JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE canonical_financial_truth_audit_v1 (
  id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL,
  new_fingerprint TEXT NOT NULL,
  new_values JSONB NOT NULL
);

CREATE TABLE position_lifecycle_events_c2_2 (
  id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL,
  mutation_kind TEXT NOT NULL,
  evidence JSONB NOT NULL
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
    cur, *, adopted_at: datetime, deployment_id: str = "local-live",
    generation: int = 1,
):
    cur.execute(
        """
        INSERT INTO runtime_contract_adoption_v2(
          contract_name,environment,deployment_id,generation,status,adopted_at
        ) VALUES (
          'FEE_AWARE_INVENTORY_C2_2','live',%s,%s,'ACTIVE',%s
        ) RETURNING adoption_id
        """,
        (deployment_id, generation, adopted_at),
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
          commission_usdc,event_time,fill_idx,raw,account_identity_id,
          account_identity_status
        ) VALUES (
          %(source)s,%(trade_id)s,%(order_id)s,%(symbol)s,%(side)s,%(role)s,
          %(executed_qty)s,%(avg_price)s,%(quote_notional_usdc)s,
          %(commission_amount)s,%(commission_asset)s,%(commission_usdc)s,
          to_timestamp(%(event_time_ms)s/1000.0),%(fill_idx)s,%(raw)s::jsonb,
          %(account_identity_id)s,%(account_identity_status)s
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
    "case",
    (
        "different-order-id",
        "different-account",
        "wrong-symbol",
        "missing-live-order-sent",
        "old-unrelated-fill",
        "conflicting-position-ownership",
        "conflicting-canonical-fill",
    ),
)
def test_delayed_entry_bootstrap_causal_guards_remain_fail_closed(
    disposable_postgres_v16, case
):
    conn = _database(
        disposable_postgres_v16, f"causal_guard_{case.replace('-', '_')}"
    )
    now_ms = (int(time.time() * 1000) // 1000) * 1000
    now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    event_time = (
        now - timedelta(days=8) if case == "old-unrelated-fill" else now
    )
    order_time = (
        now if case == "old-unrelated-fill"
        else event_time + timedelta(milliseconds=96)
    )
    order_id = "causal-order-1"
    trade_id = "causal-trade-1"
    local_cid = "ORC-L-BTCUSDC-RSI-1m-E-abcdef12"
    wire_cid = local_cid.replace("-", "")
    row = _fill_row(
        trade_id=trade_id,
        order_id=order_id,
        client_order_id=wire_cid,
        side="BUY",
        event_time=event_time,
        qty="0.00030817",
        price="60000.00",
        fee="0.000001078595",
        fee_asset="BTC",
        deployment_id="vps-live",
        symbol="BTCUSDC",
    )
    if case == "different-order-id":
        row["order_id"] = "another-exchange-order"
    elif case == "different-account":
        row["account_identity_id"] = 2
    elif case == "wrong-symbol":
        row["symbol"] = "ETHUSDC"

    try:
        with conn.cursor() as cur:
            _active_adoption(
                cur,
                adopted_at=event_time - timedelta(minutes=1),
                deployment_id="vps-live",
                generation=3,
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  created_at,symbol,side,order_type,client_order_id,order_id,
                  status,raw,position_id,is_exit,strategy,"interval",
                  order_purpose,requested_qty,order_accepted,exchange_source
                ) VALUES (
                  %s,'BTCUSDC','BUY','MARKET',%s,%s,'NEW',%s::jsonb,NULL,
                  false,'RSI','1m','ENTRY',0.00030817,true,'okx'
                )
                """,
                (
                    order_time,
                    local_cid,
                    order_id,
                    json.dumps({
                        "orderId": order_id,
                        "clientOrderId": wire_cid,
                        "status": "NEW",
                        "executedQty": "0",
                    }),
                ),
            )
            if case != "missing-live-order-sent":
                _insert_live_order_event(
                    cur,
                    created_at=order_time,
                    order_id=order_id,
                    local_cid=local_cid,
                    wire_cid=wire_cid,
                    symbol="BTCUSDC",
                    strategy="RSI",
                )
            if case == "conflicting-position-ownership":
                cur.execute(
                    """
                    INSERT INTO positions(
                      symbol,strategy,"interval",status,side,qty,entry_price,
                      entry_time,entry_order_id
                    ) VALUES (
                      'BTCUSDC','RSI','1m','OPEN','LONG',0.1,60000,%s,%s
                    )
                    """,
                    (event_time, order_id),
                )
            if case == "conflicting-canonical-fill":
                _insert_canonical_fill(cur, row)

            assert _resolve_pending_entry_generation(
                cur, row, account_identity_key="1"
            ) == (None, None, None)
    finally:
        conn.rollback()
        conn.close()


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
            adoption_id = _active_adoption(
                cur,
                adopted_at=base_time - timedelta(minutes=5),
                deployment_id="vps-live",
                generation=3,
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
                        incident["event_time"] + timedelta(milliseconds=96),
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
                    created_at=incident["event_time"] + timedelta(milliseconds=96),
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
                SELECT inventory_contract_adoption_id,
                       inventory_contract_generation
                FROM positions WHERE id=%s
                """,
                (position_id,),
            )
            assert cur.fetchone() == (adoption_id, 3)
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
                  AND contract_generation=3
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


def test_partial_applied_fill_recovers_after_sibling_terminal_exit(
    disposable_postgres_v16, monkeypatch
):
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "vps-live")
    conn = _database(disposable_postgres_v16, "partial_applied_after_exit")
    now_ms = (int(time.time() * 1000) // 1000) * 1000
    now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    incidents = {
        "A": {
            "local_id": 3825,
            "order_id": "3819011200977670144",
            "trade_id": "4400542",
            "qty": "0.00030817",
            "fee": "0.000001078595",
            "net": Decimal("0.000307091405"),
            "event_time": now - timedelta(minutes=3),
            "cid": "ORC-L-BTCUSDC-RSI-1m-E-aaaaaa01",
        },
        "B": {
            "local_id": 3826,
            "order_id": "3820730809883758592",
            "trade_id": "4414544",
            "qty": "0.00030888",
            "fee": "0.00000108108",
            "net": Decimal("0.000307798920"),
            "event_time": now - timedelta(minutes=2),
            "cid": "ORC-L-BTCUSDC-RSI-1m-E-bbbbbb02",
        },
    }

    def row_for(label):
        item = incidents[label]
        return _fill_row(
            trade_id=item["trade_id"],
            order_id=item["order_id"],
            client_order_id=item["cid"].replace("-", ""),
            side="BUY",
            event_time=item["event_time"],
            qty=item["qty"],
            price="120000.00",
            fee=item["fee"],
            fee_asset="BTC",
            deployment_id="vps-live",
            symbol="BTCUSDC",
        )

    try:
        with conn.cursor() as cur:
            adoption_id = _active_adoption(
                cur,
                adopted_at=now - timedelta(minutes=10),
                deployment_id="vps-live",
                generation=3,
            )
            for label, item in incidents.items():
                row = row_for(label)
                wire_cid = item["cid"].replace("-", "")
                cur.execute(
                    """
                    INSERT INTO binance_orders(
                      id,created_at,symbol,side,order_type,client_order_id,
                      order_id,status,raw,is_exit,strategy,"interval",
                      order_purpose,requested_qty,order_accepted,
                      exchange_source,account_identity_id,
                      account_identity_status
                    ) VALUES (
                      %s,%s,'BTCUSDC','BUY','MARKET',%s,%s,'NEW',%s::jsonb,
                      false,'RSI','1m','ENTRY',%s,true,'okx',1,'VERIFIED'
                    )
                    """,
                    (
                        item["local_id"],
                        item["event_time"] + timedelta(milliseconds=96),
                        item["cid"],
                        item["order_id"],
                        json.dumps({
                            "orderId": item["order_id"],
                            "clientOrderId": wire_cid,
                            "status": "NEW",
                            "executedQty": "0",
                        }),
                        item["qty"],
                    ),
                )
                _insert_live_order_event(
                    cur,
                    created_at=item["event_time"] + timedelta(milliseconds=96),
                    order_id=item["order_id"],
                    local_cid=item["cid"],
                    wire_cid=wire_cid,
                    symbol="BTCUSDC",
                    strategy="RSI",
                )
                _insert_canonical_fill(cur, row)
                cur.execute(
                    "SELECT id FROM binance_order_fills WHERE trade_id=%s",
                    (item["trade_id"],),
                )
                item["fill_id"] = int(cur.fetchone()[0])
                payload = authoritative_fill_payload(
                    row, account_identity_key="1"
                )
                fingerprint = authoritative_fill_fingerprint(payload)
                cur.execute(
                    """
                    INSERT INTO exchange_fill_ingestion_state_v2(
                      source,account_identity_key,symbol,trade_id,order_id,
                      side,source_fingerprint,applied_fingerprint,applied_at,
                      application_status,authoritative_payload,last_decision,
                      local_fill_id,adoption_id,contract_generation
                    ) VALUES (
                      'okx','1','BTCUSDC',%s,%s,'BUY',%s,%s,%s,%s,
                      %s::jsonb,'NEW_AUTHORITATIVE_EVIDENCE',%s,%s,3
                    )
                    """,
                    (
                        item["trade_id"],
                        item["order_id"],
                        fingerprint,
                        fingerprint,
                        item["event_time"] + timedelta(seconds=1),
                        (
                            "APPLIED" if label == "A"
                            else "OBSERVED_NOT_APPLIED"
                        ),
                        json.dumps(payload, sort_keys=True),
                        item["fill_id"],
                        adoption_id,
                    ),
                )

        first = reconcile_pending_entry_fills(
            conn, batch_size=1, trading_mode="LIVE"
        )
        assert (first.scanned, first.created, first.ambiguous) == (1, 1, 0)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM positions WHERE entry_order_id=%s",
                (incidents["A"]["order_id"],),
            )
            position_a = int(cur.fetchone()[0])

        second = reconcile_pending_entry_fills(
            conn, batch_size=1, trading_mode="LIVE"
        )
        assert (second.scanned, second.created, second.ambiguous) == (1, 0, 1)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reconciliation_status,reconciled_position_id
                FROM binance_orders WHERE id=3826
                """
            )
            assert cur.fetchone() == ("OPEN_POSITION_ORDER_MISMATCH", None)

            exit_order_id = "3822000000000000000"
            exit_trade_id = "4420000"
            exit_time = now - timedelta(minutes=1)
            cur.execute(
                """
                INSERT INTO binance_orders(
                  created_at,symbol,side,order_type,client_order_id,order_id,
                  status,raw,position_id,is_exit,strategy,"interval",
                  order_purpose,requested_qty,order_accepted,exchange_source,
                  reconciled_position_id,account_identity_id,
                  account_identity_status
                ) VALUES (
                  %s,'BTCUSDC','SELL','MARKET','exit-a',%s,'FILLED','{}',%s,
                  true,'RSI','1m','EXIT',0.00030709,true,'okx',%s,1,'VERIFIED'
                )
                """,
                (exit_time, exit_order_id, position_a, position_a),
            )
            cur.execute(
                """
                INSERT INTO binance_order_fills(
                  source,trade_id,order_id,symbol,side,role,executed_qty,
                  avg_price,quote_notional_usdc,commission_amount,
                  commission_asset,commission_usdc,event_time,raw,
                  account_identity_id,account_identity_status
                ) VALUES (
                  'okx',%s,%s,'BTCUSDC','SELL','TAKER',0.00030709,118500,
                  36.390165,0.01,'USDC',0.01,%s,'{}',1,'VERIFIED'
                ) RETURNING id
                """,
                (exit_trade_id, exit_order_id, exit_time),
            )
            exit_fill_id = int(cur.fetchone()[0])
            terminal_dust = Decimal("0.000000001405")
            cur.execute(
                """
                UPDATE positions SET
                  status='CLOSED',exit_order_id=%s,exit_price=118500,
                  exit_time=%s,exit_reason='TERMINAL_DUST',qty=%s,
                  cumulative_exit_executed_qty=0.00030709,
                  exit_inventory_reduction_qty=0.00030709,
                  remaining_inventory_qty=%s
                WHERE id=%s
                """,
                (
                    exit_order_id, exit_time, terminal_dust,
                    terminal_dust, position_a,
                ),
            )
            cur.execute(
                """
                INSERT INTO canonical_financial_truth_v1(
                  position_id,financial_truth_status,executed_entry_qty,
                  executed_exit_qty,remaining_qty,
                  authoritative_entry_fees_usdc,
                  authoritative_exit_fees_usdc,authoritative_gross_pnl,
                  authoritative_net_pnl,authoritative_source,
                  authoritative_evidence,source_fill_ids
                ) VALUES (
                  %s,'COMPLETE',0.00030817,0.00030709,%s,0.1294314,0.01,
                  -0.32,-0.459506302708,'EXCHANGE_EXECUTION',%s::jsonb,
                  %s::jsonb
                )
                """,
                (
                    position_a,
                    terminal_dust,
                    json.dumps({"provenance": "A_TERMINAL_COMPLETE"}),
                    json.dumps([
                        f"exchange:{incidents['A']['fill_id']}",
                        f"exchange:{exit_fill_id}",
                    ]),
                ),
            )
            cur.execute(
                """
                INSERT INTO canonical_financial_truth_audit_v1(
                  position_id,new_fingerprint,new_values
                ) VALUES (%s,'a-ft-fingerprint',%s::jsonb)
                """,
                (position_a, json.dumps({"status": "COMPLETE"})),
            )
            cur.execute(
                """
                INSERT INTO position_lifecycle_events_c2_2(
                  position_id,mutation_kind,evidence
                ) VALUES (%s,'POSITION_CLOSED_TERMINAL_DUST',%s::jsonb)
                """,
                (position_a, json.dumps({"exit_fill_id": exit_fill_id})),
            )
            cur.execute(
                """
                SELECT p.*,ft.*,audit.*,lifecycle.*
                FROM positions p
                JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
                JOIN canonical_financial_truth_audit_v1 audit
                  ON audit.position_id=p.id
                JOIN position_lifecycle_events_c2_2 lifecycle
                  ON lifecycle.position_id=p.id
                WHERE p.id=%s
                """,
                (position_a,),
            )
            immutable_a_before = cur.fetchone()
        conn.commit()

        recovery = reconcile_pending_entry_fills(
            conn, batch_size=10, trading_mode="LIVE"
        )
        assert (recovery.scanned, recovery.created, recovery.failed) == (1, 1, 0)
        assert recovery.recovered == 1
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,qty,gross_entry_executed_qty,entry_base_fee_qty,
                       net_entry_inventory_qty,remaining_inventory_qty,
                       inventory_evidence_status,
                       inventory_contract_adoption_id,
                       inventory_contract_generation
                FROM positions
                WHERE entry_order_id=%s AND status='OPEN'
                """,
                (incidents["B"]["order_id"],),
            )
            recovered = cur.fetchone()
            assert recovered[1:] == (
                incidents["B"]["net"],
                Decimal(incidents["B"]["qty"]),
                Decimal(incidents["B"]["fee"]),
                incidents["B"]["net"],
                incidents["B"]["net"],
                "COMPLETE",
                adoption_id,
                3,
            )
            position_b = int(recovered[0])
            cur.execute(
                """
                SELECT reconciled_position_id,last_reconciliation_action
                FROM binance_orders WHERE id=3826
                """
            )
            assert cur.fetchone() == (
                position_b, "RECOVER_PARTIAL_APPLIED_ENTRY"
            )
            cur.execute(
                """
                SELECT application_status,local_fill_id,applied_fingerprint,
                       applied_at,adoption_id,contract_generation,last_decision
                FROM exchange_fill_ingestion_state_v2 WHERE trade_id='4414544'
                """
            )
            state_b = cur.fetchone()
            assert state_b[0] == "APPLIED"
            assert state_b[1] == incidents["B"]["fill_id"]
            assert state_b[2] is not None and state_b[3] is not None
            assert state_b[4:] == (
                adoption_id, 3, "PARTIAL_APPLIED_POSITION_RECOVERED"
            )
            cur.execute(
                """
                SELECT p.*,ft.*,audit.*,lifecycle.*
                FROM positions p
                JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
                JOIN canonical_financial_truth_audit_v1 audit
                  ON audit.position_id=p.id
                JOIN position_lifecycle_events_c2_2 lifecycle
                  ON lifecycle.position_id=p.id
                WHERE p.id=%s
                """,
                (position_a,),
            )
            assert cur.fetchone() == immutable_a_before
            cur.execute("SELECT count(*) FROM binance_order_fills")
            assert cur.fetchone()[0] == 3
            cur.execute("SELECT count(*) FROM positions")
            assert cur.fetchone()[0] == 2
            cur.execute(
                """
                SELECT count(*)
                FROM binance_order_fills f
                JOIN binance_orders bo
                  ON bo.exchange_source=f.source
                 AND bo.symbol=f.symbol AND bo.order_id=f.order_id
                WHERE f.trade_id='4414544'
                  AND bo.reconciled_position_id IS NULL
                """
            )
            assert cur.fetchone()[0] == 0

            external = Decimal("0.002716415960")
            account_btc = Decimal("0.003024216285")
            assert external + incidents["B"]["net"] + terminal_dust == account_btc

        for _ in range(3):
            repeated = reconcile_pending_entry_fills(
                conn, batch_size=10, trading_mode="LIVE"
            )
            assert (repeated.scanned, repeated.created, repeated.updated) == (
                0, 0, 0
            )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM binance_order_fills")
            assert cur.fetchone()[0] == 3
            cur.execute(
                """
                SELECT count(*),sum(remaining_inventory_qty),
                       sum(entry_base_fee_qty)
                FROM positions WHERE status='OPEN'
                """
            )
            assert cur.fetchone() == (
                1, incidents["B"]["net"], Decimal(incidents["B"]["fee"])
            )
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.parametrize(
    "case",
    (
        "different-account",
        "manual-external",
        "sell",
        "wrong-deployment",
        "wrong-slot",
        "already-linked",
        "conflicting-open",
        "missing-adoption",
        "invalid-generation",
        "consumed-by-terminal",
    ),
)
def test_partial_applied_recovery_negative_matrix(
    disposable_postgres_v16, monkeypatch, case
):
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "vps-live")
    conn = _database(
        disposable_postgres_v16,
        f"partial_negative_{case.replace('-', '_')}",
    )
    now_ms = (int(time.time() * 1000) // 1000) * 1000
    now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    order_id = "partial-negative-order"
    trade_id = "partial-negative-trade"
    local_cid = "ORC-L-BTCUSDC-RSI-1m-E-abcdef12"
    wire_cid = local_cid.replace("-", "")
    side = "SELL" if case == "sell" else "BUY"
    row = _fill_row(
        trade_id=trade_id,
        order_id=order_id,
        client_order_id=wire_cid,
        side=side,
        event_time=now - timedelta(minutes=1),
        qty="0.00030888",
        price="120000.00",
        fee="0.00000108108",
        fee_asset="BTC",
        deployment_id="vps-live",
        symbol="BTCUSDC",
    )
    try:
        with conn.cursor() as cur:
            adoption_id = _active_adoption(
                cur,
                adopted_at=now - timedelta(minutes=10),
                deployment_id=(
                    "local-live" if case == "wrong-deployment" else "vps-live"
                ),
                generation=3,
            )
            cur.execute(
                """
                INSERT INTO positions(
                  id,symbol,strategy,"interval",status,side,qty,entry_price,
                  entry_time,entry_order_id,exit_order_id,
                  inventory_contract_adoption_id,
                  inventory_contract_generation,remaining_inventory_qty,
                  inventory_evidence_status
                ) VALUES (
                  3110,%s,'RSI','1m','CLOSED','LONG',0.000000001405,
                  120000,%s,'closed-a-order','closed-a-exit',%s,3,
                  0.000000001405,'COMPLETE'
                )
                """,
                (
                    "ETHUSDC" if case == "wrong-slot" else "BTCUSDC",
                    now - timedelta(minutes=3),
                    adoption_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  created_at,symbol,side,order_type,client_order_id,order_id,
                  status,raw,position_id,is_exit,strategy,"interval",
                  order_purpose,requested_qty,order_accepted,exchange_source,
                  reconciliation_status,reconciled_position_id,
                  account_identity_id,account_identity_status
                ) VALUES (
                  %s,'BTCUSDC',%s,'MARKET',%s,%s,'NEW',%s::jsonb,NULL,false,
                  'RSI','1m','ENTRY',0.00030888,true,'okx',
                  'OPEN_POSITION_ORDER_MISMATCH',%s,1,'VERIFIED'
                )
                """,
                (
                    now - timedelta(minutes=1)
                    + timedelta(milliseconds=96),
                    side,
                    local_cid,
                    order_id,
                    json.dumps({
                        "orderId": order_id,
                        "clientOrderId": wire_cid,
                        "status": "NEW",
                        "executedQty": "0",
                    }),
                    3110 if case == "already-linked" else None,
                ),
            )
            _insert_live_order_event(
                cur,
                created_at=now - timedelta(minutes=1),
                order_id=order_id,
                local_cid=local_cid,
                wire_cid=wire_cid,
                symbol="BTCUSDC",
                strategy="RSI",
            )
            _insert_canonical_fill(cur, row)
            cur.execute(
                "SELECT id FROM binance_order_fills WHERE trade_id=%s",
                (trade_id,),
            )
            fill_id = int(cur.fetchone()[0])
            if case == "different-account":
                cur.execute(
                    """
                    UPDATE binance_order_fills
                    SET account_identity_id=2 WHERE id=%s
                    """,
                    (fill_id,),
                )
            payload = authoritative_fill_payload(
                row, account_identity_key="1"
            )
            fingerprint = authoritative_fill_fingerprint(payload)
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,account_identity_key,symbol,trade_id,order_id,side,
                  source_fingerprint,applied_fingerprint,applied_at,
                  application_status,authoritative_payload,last_decision,
                  local_fill_id,adoption_id,contract_generation,
                  ownership_classification
                ) VALUES (
                  'okx','1','BTCUSDC',%s,%s,%s,%s,%s,%s,
                  'OBSERVED_NOT_APPLIED',%s::jsonb,
                  'NEW_AUTHORITATIVE_EVIDENCE',%s,%s,%s,%s
                )
                """,
                (
                    trade_id,
                    order_id,
                    side,
                    fingerprint,
                    fingerprint,
                    now,
                    json.dumps(payload, sort_keys=True),
                    fill_id,
                    999 if case == "missing-adoption" else adoption_id,
                    99 if case == "invalid-generation" else 3,
                    (
                        "MANUAL_OR_EXTERNAL"
                        if case == "manual-external" else None
                    ),
                ),
            )
            cur.execute(
                """
                INSERT INTO canonical_financial_truth_v1(
                  position_id,financial_truth_status,authoritative_source,
                  authoritative_evidence,source_fill_ids
                ) VALUES (3110,'COMPLETE','EXCHANGE_EXECUTION',%s::jsonb,%s::jsonb)
                """,
                (
                    json.dumps({"provenance": "TERMINAL_A"}),
                    json.dumps(
                        [f"exchange:{fill_id}"]
                        if case == "consumed-by-terminal" else []
                    ),
                ),
            )
            if case == "conflicting-open":
                cur.execute(
                    """
                    INSERT INTO positions(
                      symbol,strategy,"interval",status,side,qty,entry_price,
                      entry_time,entry_order_id,remaining_inventory_qty,
                      inventory_evidence_status
                    ) VALUES (
                      'BTCUSDC','RSI','1m','OPEN','LONG',0.1,120000,%s,
                      'another-open-order',0.1,'COMPLETE'
                    )
                    """,
                    (now,),
                )

        result = reconcile_pending_entry_fills(
            conn, batch_size=10, trading_mode="LIVE"
        )
        assert (result.scanned, result.created, result.updated) == (0, 0, 0)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM positions WHERE entry_order_id=%s",
                (order_id,),
            )
            assert cur.fetchone()[0] == 0
            cur.execute(
                """
                SELECT application_status,last_decision
                FROM exchange_fill_ingestion_state_v2 WHERE trade_id=%s
                """,
                (trade_id,),
            )
            assert cur.fetchone() == (
                "OBSERVED_NOT_APPLIED", "NEW_AUTHORITATIVE_EVIDENCE"
            )
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
