from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

from common.exit_guards.economic_floor_v2 import (
    V2_ACTIVE_EXIT_INTENT_EVENT,
    V2_ARM_EVENT,
    V2_FINAL_EVENT,
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
  close_time TIMESTAMPTZ,close NUMERIC
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


def test_five_minute_position_uses_distinct_finalized_one_minute_sources(
    disposable_postgres_v16, monkeypatch,
):
    database = f"waltrade_baseline_test_economic_floor_v2_{uuid.uuid4().hex[:10]}"
    disposable_postgres_v16.create_database(database)

    def connect():
        return disposable_postgres_v16.connect(database)

    monkeypatch.setenv("ACTIVE_ECONOMIC_FLOOR_VERSION", "V2")
    monkeypatch.setenv("ECONOMIC_FLOOR_V2_MODE", "TREATMENT")
    snapshot = uuid.uuid4()
    entry_at = datetime(2026, 9, 2, 11, 55, tzinfo=timezone.utc)
    first_close = datetime(2026, 9, 2, 12, 0, 59, 999000, tzinfo=timezone.utc)
    second_close = first_close + timedelta(minutes=1)

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            cur.execute(
                "INSERT INTO entry_opportunity_evidence_v1 VALUES(%s,0.0035,%s)",
                (str(snapshot), "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"),
            )
            cur.execute(
                """
                INSERT INTO positions VALUES(
                  77,'BTCUSDC','TREND','5m','OPEN','LONG',1,'COMPLETE',
                  %s,NULL,%s,NULL
                )
                """,
                (entry_at, str(snapshot)),
            )
            cur.execute(
                """
                INSERT INTO simulated_execution_fills_v1(
                  position_id,order_purpose,fill_qty,fill_notional,
                  authoritative_fee_usdc,simulation_fee_rate,fee_model_version
                ) VALUES(77,'ENTRY',1,100,0.35,0.0035,
                         'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')
                """
            )
            # Historical V1 evidence must not arm V2.
            cur.execute(
                """
                INSERT INTO strategy_events(symbol,interval,strategy,event_type,info)
                VALUES('BTCUSDC','5m','TREND','ECONOMIC_FLOOR_SHADOW_ARMED',
                       '{"position_id":77}')
                """
            )
            cur.execute(
                """
                INSERT INTO candles(symbol,interval,open_time,close_time,close)
                VALUES('BTCUSDC','1m',%s,%s,101)
                """,
                (first_close - timedelta(seconds=59, milliseconds=999), first_close),
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
    assert first.event_type == V2_ARM_EVENT
    assert not first.exit_requested

    retry = evaluate_economic_floor_v2_owner_cycle(
        trading_mode="PAPER", position_id=77, symbol="BTCUSDC", interval="5m",
        strategy="TREND", evaluated_at=first_close + timedelta(seconds=30),
        connection_factory=connect,
    )
    assert retry.status == "IDEMPOTENT_SOURCE_ALREADY_EVALUATED"

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO candles(symbol,interval,open_time,close_time,close)
                VALUES('BTCUSDC','1m',%s,%s,100.70)
                """,
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
    assert second.status == "V2_EXIT_CLAIMED"
    assert second.event_type == V2_ACTIVE_EXIT_INTENT_EVENT
    assert second.exit_requested
    assert second.realizable_net == Decimal("-0.002450")

    duplicate = evaluate_economic_floor_v2_owner_cycle(
        trading_mode="PAPER", position_id=77, symbol="BTCUSDC", interval="5m",
        strategy="TREND", evaluated_at=second_close + timedelta(seconds=30),
        connection_factory=connect,
    )
    assert duplicate.status == "IDEMPOTENT_SOURCE_ALREADY_EVALUATED"

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE positions SET status='CLOSED',exit_reason=%s WHERE id=77",
                ("ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION",),
            )
            cur.execute(
                "INSERT INTO canonical_financial_truth_v1 VALUES(77,'COMPLETE',-0.002450)"
            )
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
            cur.execute(
                "SELECT event_type,count(*) FROM strategy_events "
                "WHERE event_type LIKE 'ECONOMIC_FLOOR_V2_%' "
                "GROUP BY event_type ORDER BY event_type"
            )
            assert dict(cur.fetchall()) == {
                V2_ACTIVE_EXIT_INTENT_EVENT: 1,
                V2_ARM_EVENT: 1,
                V2_FINAL_EVENT: 1,
            }
            cur.execute(
                "SELECT info->>'final_financial_truth_status' FROM strategy_events "
                "WHERE event_type=%s",
                (V2_FINAL_EVENT,),
            )
            assert cur.fetchone() == ("COMPLETE",)
    finally:
        conn.close()
