from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

from common.exit_guards.economic_floor_boundary_evidence import (
    EVIDENCE_EVENT,
    FINAL_EVENT,
)
from common.exit_guards.economic_floor_v2 import (
    evaluate_economic_floor_v2_owner_cycle,
    reconcile_economic_floor_v2_closures,
)


SCHEMA = """
CREATE TABLE positions(
  id BIGINT PRIMARY KEY,symbol TEXT,strategy TEXT,interval TEXT,status TEXT,
  side TEXT,remaining_inventory_qty NUMERIC,inventory_evidence_status TEXT,
  entry_time TIMESTAMPTZ,exit_order_id TEXT,entry_opportunity_snapshot_id UUID,
  exit_reason TEXT
);
CREATE TABLE entry_opportunity_evidence_v1(
  snapshot_id UUID PRIMARY KEY,fee_rate_exit_assumption NUMERIC,
  fee_model_version TEXT
);
CREATE TABLE simulated_execution_fills_v1(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT,order_purpose TEXT,fill_qty NUMERIC,
  fill_notional NUMERIC,authoritative_fee_usdc NUMERIC,
  simulation_fee_rate NUMERIC,fee_model_version TEXT
);
CREATE TABLE candles(
  id BIGSERIAL PRIMARY KEY,symbol TEXT,interval TEXT,open_time TIMESTAMPTZ,
  close_time TIMESTAMPTZ,close NUMERIC,high NUMERIC,low NUMERIC,atr_14 NUMERIC
);
CREATE TABLE market_regime(
  symbol TEXT,interval TEXT,ts TIMESTAMPTZ,regime TEXT
);
CREATE TABLE strategy_events(
  id BIGSERIAL PRIMARY KEY,symbol TEXT,interval TEXT,strategy TEXT,
  event_type TEXT,decision TEXT,reason TEXT,price NUMERIC,
  candle_open_time TIMESTAMPTZ,info JSONB
);
CREATE TABLE canonical_financial_truth_v1(
  position_id BIGINT PRIMARY KEY,financial_truth_status TEXT,
  authoritative_net_pnl NUMERIC
);
"""


def test_forward_evidence_is_complete_idempotent_and_finalized(
    disposable_postgres_v16, monkeypatch,
):
    database = f"waltrade_baseline_test_boundary_evidence_{uuid.uuid4().hex[:10]}"
    disposable_postgres_v16.create_database(database)

    def connect():
        return disposable_postgres_v16.connect(database)

    monkeypatch.setenv("ACTIVE_ECONOMIC_FLOOR_VERSION", "V2")
    monkeypatch.setenv("ECONOMIC_FLOOR_V2_MODE", "TREATMENT")
    snapshot = uuid.uuid4()
    entry_at = datetime(2026, 9, 3, 11, 50, tzinfo=timezone.utc)
    first_close = datetime(2026, 9, 3, 12, 0, 59, 999000, tzinfo=timezone.utc)

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            cur.execute(
                "INSERT INTO entry_opportunity_evidence_v1 VALUES(%s,0.0035,%s)",
                (str(snapshot), "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"),
            )
            cur.execute(
                "INSERT INTO positions VALUES(77,'BTCUSDC','TREND','5m','OPEN',"
                "'LONG',1,'COMPLETE',%s,NULL,%s,NULL)",
                (entry_at, str(snapshot)),
            )
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1(position_id,order_purpose,"
                "fill_qty,fill_notional,authoritative_fee_usdc,simulation_fee_rate,"
                "fee_model_version) VALUES(77,'ENTRY',1,100,0.35,0.0035,"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')"
            )
            for offset, close in enumerate((100, 100.1, 100.2, 100.3, 100.4, 101)):
                close_at = first_close - timedelta(minutes=5 - offset)
                cur.execute(
                    "INSERT INTO candles(symbol,interval,open_time,close_time,close,"
                    "high,low,atr_14) VALUES('BTCUSDC','1m',%s,%s,%s,%s,%s,0.25)",
                    (close_at - timedelta(seconds=59, milliseconds=999), close_at,
                     close, Decimal(str(close)) + 1, Decimal(str(close)) - 1),
                )
            cur.execute(
                "INSERT INTO market_regime VALUES('BTCUSDC','1m',%s,'TREND_UP')",
                (first_close,),
            )
        conn.commit()
    finally:
        conn.close()

    first = evaluate_economic_floor_v2_owner_cycle(
        trading_mode="PAPER", position_id=77, symbol="BTCUSDC", interval="5m",
        strategy="TREND", evaluated_at=first_close + timedelta(milliseconds=1),
        connection_factory=connect,
    )
    assert first.status == "ARMED_NOW_NO_SAME_SOURCE_EXIT"

    retry = evaluate_economic_floor_v2_owner_cycle(
        trading_mode="PAPER", position_id=77, symbol="BTCUSDC", interval="5m",
        strategy="TREND", evaluated_at=first_close + timedelta(seconds=30),
        connection_factory=connect,
    )
    assert retry.status == "IDEMPOTENT_SOURCE_ALREADY_EVALUATED"

    second_close = first_close + timedelta(minutes=1)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO candles(symbol,interval,open_time,close_time,close,high,"
                "low,atr_14) VALUES('BTCUSDC','1m',%s,%s,101.1,101.3,100.9,0.26)",
                (second_close - timedelta(seconds=59, milliseconds=999), second_close),
            )
        conn.commit()
    finally:
        conn.close()

    second = evaluate_economic_floor_v2_owner_cycle(
        trading_mode="PAPER", position_id=77, symbol="BTCUSDC", interval="5m",
        strategy="TREND", evaluated_at=second_close + timedelta(milliseconds=1),
        connection_factory=connect,
    )
    assert second.status == "ARMED_UPSIDE_OPEN"

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT decision,info FROM strategy_events WHERE event_type=%s "
                "ORDER BY id", (EVIDENCE_EVENT,),
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            assert all(row[0] == "OBSERVE" for row in rows)
            first_info, second_info = (dict(row[1]) for row in rows)
            required = {
                "position_id", "symbol", "originating_interval", "strategy",
                "source_1m_candle_id", "source_1m_close_time", "position_notional",
                "realizable_net", "realizable_net_pct_of_notional",
                "previous_realizable_net", "realizable_net_change",
                "realizable_net_slope_per_minute", "peak_realizable_net",
                "distance_from_peak_usdc", "distance_from_peak_pct",
                "one_minute_candle_range", "atr_pct",
                "recent_realized_volatility", "regime", "armed_at",
                "seconds_since_arm", "distinct_evaluations_since_arm",
                "existing_exit_decision", "existing_exit_reason",
                "existing_exit_committed",
            }
            assert required <= second_info.keys()
            assert first_info["active_boundary_influence"] == "OFF"
            assert second_info["previous_realizable_net"] == first_info["realizable_net"]
            assert second_info["distinct_evaluations_since_arm"] == 2
            assert second_info["recent_realized_volatility"] is not None
            assert second_info["regime"] == "TREND_UP"
            cur.execute("UPDATE positions SET status='CLOSED',exit_reason='TAKE_PROFIT' WHERE id=77")
            cur.execute("INSERT INTO canonical_financial_truth_v1 VALUES(77,'COMPLETE',0.05)")
        conn.commit()
    finally:
        conn.close()

    assert reconcile_economic_floor_v2_closures(
        trading_mode="PAPER", connection_factory=connect,
    ) == 1
    assert reconcile_economic_floor_v2_closures(
        trading_mode="PAPER", connection_factory=connect,
    ) == 0

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT info FROM strategy_events WHERE event_type=%s", (FINAL_EVENT,))
            final = dict(cur.fetchone()[0])
            assert final["final_financial_truth_status"] == "COMPLETE"
            assert Decimal(str(final["final_net_after_fees"])) == Decimal("0.05")
            assert final["actual_exit_reason"] == "TAKE_PROFIT"
    finally:
        conn.close()
