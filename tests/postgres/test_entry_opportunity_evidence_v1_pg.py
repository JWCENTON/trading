from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import os
import uuid

import psycopg2
import pytest

from common.entry_opportunity_evidence import (
    capture_entry_opportunity_snapshot_cursor,
    link_entry_opportunity_order_fail_open_cursor,
    link_entry_opportunity_position_fail_open_cursor,
)
from common.paper_simulation_fee_config import PaperSimulationFeeConfig


def _connection():
    database = os.getenv("ENTRY_OPPORTUNITY_TEST_DATABASE")
    if not database:
        pytest.skip("ENTRY_OPPORTUNITY_TEST_DATABASE is required")
    assert database.startswith("waltrade_baseline_test_")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "trading-paper-db-1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _realtime(_symbol, _interval, _candle_open_time):
    return {
        "ok": True,
        "realtime_score": 42.5,
        "realtime_status": "REALTIME_OBSERVE",
        "atr_component": 10,
        "ema_component": 8,
        "momentum_component": 12,
        "volume_component": 7.5,
        "breakout_component": 5,
    }


def test_snapshot_immutability_late_link_and_frozen_replay():
    conn = _connection()
    decision_id = uuid.uuid4()
    decision_time = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO decision_registry_v1(
                  decision_id,legacy_decision_key,deployment_id,environment,
                  decision_type,decision_source,symbol,interval,strategy,
                  market_regime,decision_timestamp,source_table,
                  source_record_id,source_natural_key,source_created_at,
                  observed_at,ingested_at,engine_name,engine_version,
                  schema_version,decision_action,decision_reason,decision_payload
                ) VALUES(
                  %s,'test-key','LOCAL','trading_paper','ENTRY_DECISION',
                  'test','BTCUSDC','1m','RSI','TREND_DOWN',%s,
                  'test','test-record','test-natural',%s,%s,%s,
                  'RSI','FORWARD_DECISION_REGISTRY_CONTINUITY_V1','V1',
                  'EXECUTE','RSI_REBOUND',%s::jsonb
                )
                """,
                (
                    decision_id, decision_time, decision_time, decision_time,
                    decision_time,
                    '{"source_revision":"' + "a" * 40 + '",'
                    '"regime_attribution_version":"CANONICAL_REGIME_ATTRIBUTION_V1",'
                    '"regime_source":{"regime_source_confidence":"0.9"}}',
                ),
            )
            cur.execute(
                """
                INSERT INTO strategy_events(
                  symbol,interval,strategy,event_type,decision,reason,price,
                  candle_open_time,info,created_at
                ) VALUES(
                  'BTCUSDC','1m','RSI','SIGNAL','BUY','OK',100,%s,
                  '{"rsi_14":31.5}'::jsonb,%s
                )
                """,
                (decision_time, decision_time),
            )
            snapshot_id = capture_entry_opportunity_snapshot_cursor(
                cur,
                decision_id=decision_id,
                simulated_order_id=9001,
                planned_entry_notional=Decimal("20"),
                fee_config=PaperSimulationFeeConfig(
                    Decimal("0.0035"),
                    "PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
                    "ENV:PAPER_SIMULATION_FEE_RATE",
                ),
                realtime_provider=_realtime,
                captured_at=decision_time.replace(second=30),
            )
            cur.execute(
                """
                SELECT fee_rate_entry_assumption,expected_round_trip_fee_usdc,
                       expected_round_trip_fee_pct,round(break_even_move_pct,6),
                       expected_move_pct,realtime_context->>'realtime_score',
                       mme_context,slot_brain_context
                  FROM entry_opportunity_evidence_v1 WHERE snapshot_id=%s
                """,
                (snapshot_id,),
            )
            assert cur.fetchone() == (
                Decimal("0.0035"), Decimal("0.1400"), Decimal("0.7000"),
                Decimal("0.702459"), None, "42.5", None, None,
            )

            # A later fee/config/current-state change cannot mutate the first row.
            assert capture_entry_opportunity_snapshot_cursor(
                cur,
                decision_id=decision_id,
                simulated_order_id=9001,
                planned_entry_notional=Decimal("20"),
                fee_config=PaperSimulationFeeConfig(
                    Decimal("0.009"), "FUTURE", "test-future"
                ),
                realtime_provider=lambda *_: {"ok": True, "realtime_score": 99},
                captured_at=decision_time.replace(minute=1),
            ) == snapshot_id
            cur.execute(
                "SELECT fee_rate_entry_assumption,realtime_context->>'realtime_score' "
                "FROM entry_opportunity_evidence_v1 WHERE snapshot_id=%s",
                (snapshot_id,),
            )
            assert cur.fetchone() == (Decimal("0.0035"), "42.5")

            cur.execute(
                """
                INSERT INTO simulated_orders(
                  id,symbol,interval,strategy,side,price,quantity_btc,reason,
                  candle_open_time,is_exit,decision_id
                ) VALUES(9001,'BTCUSDC','1m','RSI','BUY',100,.2,
                         'RSI_REBOUND',%s,false,%s)
                """,
                (decision_time, decision_id),
            )
            link_entry_opportunity_order_fail_open_cursor(
                cur, decision_id=decision_id, simulated_order_id=9001,
            )
            cur.execute(
                """
                INSERT INTO positions(
                  id,symbol,strategy,interval,status,side,qty,entry_price,
                  entry_time,market_regime
                ) VALUES(9001,'BTCUSDC','RSI','1m','OPEN','LONG',.2,100,%s,
                         'TREND_DOWN')
                """,
                (decision_time,),
            )
            link_entry_opportunity_position_fail_open_cursor(
                cur, simulated_order_id=9001, position_id=9001, fill_id=None,
            )
            cur.execute(
                "SELECT entry_opportunity_snapshot_id FROM positions WHERE id=9001"
            )
            assert cur.fetchone()[0] == snapshot_id

            cur.execute(
                """
                INSERT INTO decision_replay_v1(
                  environment,decision_key,position_id,symbol,interval,strategy,
                  replay_status,decision_vector
                ) VALUES('trading_paper','test-key',9001,'BTCUSDC','1m','RSI',
                         'REPLAY_READY','{}'::jsonb)
                RETURNING entry_opportunity_snapshot_id
                """
            )
            assert cur.fetchone()[0] == snapshot_id

            cur.execute("SAVEPOINT immutable_test")
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    "UPDATE entry_opportunity_evidence_v1 "
                    "SET break_even_move_pct=0 WHERE snapshot_id=%s",
                    (snapshot_id,),
                )
            cur.execute("ROLLBACK TO SAVEPOINT immutable_test")
        conn.rollback()
    finally:
        conn.close()
