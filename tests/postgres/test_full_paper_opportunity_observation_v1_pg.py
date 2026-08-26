from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest

from common.paper_opportunity_observation import (
    persist_paper_opportunity_observation_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
SOURCE = (
    ROOT / "db/migrations/20260826_full_paper_opportunity_observation_v1.sql"
).read_text()
MIGRATION = "\n".join(
    line for line in SOURCE.splitlines() if not line.lstrip().startswith("\\")
)

BASE = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE causal_decision_observation_v1(event_id uuid PRIMARY KEY);
CREATE TABLE entry_trace_events(
  id bigint PRIMARY KEY,strategy text,symbol text,interval text,
  candle_open_time timestamptz,created_at timestamptz
);
CREATE TABLE entry_opportunity_evidence_v1(
  snapshot_id uuid PRIMARY KEY,decision_key text,captured_at timestamptz,
  realtime_availability_status text,mme_availability_status text,
  mme_context jsonb,orc_availability_status text
);
CREATE TABLE market_memory_sequence(
  symbol text,interval text,direction text,sequence_stage text,
  refreshed_at timestamptz
);
CREATE TABLE orc_apply_slot_decisions_v1(
  run_id uuid,deployment_id text,environment text,strategy text,symbol text,
  interval text,recorded_at timestamptz
);
CREATE TABLE automation_kv(
  key text PRIMARY KEY,value text NOT NULL,updated_at timestamptz NOT NULL
);
CREATE TABLE candles(
  id bigserial PRIMARY KEY,symbol text NOT NULL,interval text NOT NULL,
  open_time timestamptz NOT NULL,high numeric,low numeric
);
"""


def database(pg, suffix):
    name = f"waltrade_baseline_test_full_paper_opportunity_v1_{suffix}"
    pg.create_database(name)
    conn = pg.connect(name)
    with conn.cursor() as cur:
        cur.execute(BASE)
        cur.execute(MIGRATION)
    conn.commit()
    return conn


def insert_observation(cur, *, key="key-1", event_id=None, observed_at=None):
    event_id = event_id or uuid.uuid4()
    observed_at = observed_at or datetime(2026, 1, 1, tzinfo=timezone.utc)
    cur.execute(
        "INSERT INTO causal_decision_observation_v1(event_id) VALUES(%s)",
        (str(event_id),),
    )
    cur.execute(
        """
        INSERT INTO paper_opportunity_observation_v1(
          observation_key,causal_event_id,decision_key,environment,deployment_id,
          strategy,symbol,interval,observed_at,candle_open_time,
          evaluation_started_at,observation_type,decision_type,decision_subtype,
          decision_reason,raw_signal_state,base_decision,final_decision,
          data_readiness,indicator_readiness,gate_state,already_open_state,
          containment_state,outcome_eligible,opportunity_direction,reference_price,
          treatment_status,fee_rate_entry,fee_rate_exit,full_cost_hurdle_pct,
          fee_model_version,fee_config_source,source_revision,engine_name,
          contract_version,realtime_availability_status,mme_availability_status,
          orc_availability_status,observation_payload_hash
        ) VALUES(
          %s,%s,%s,'trading_paper','local-paper','RSI','BTCUSDC','1m',%s,%s,%s,
          'NO_SIGNAL','NO_TRADE','NO_SIGNAL','NO_SIGNAL','ABSENT','NO_TRADE',
          'NO_TRADE','READY','READY','PASS','CLEAR','CLEAR',true,'LONG',100,
          'NOT_APPLICABLE',0.0035,0.0035,0.702458605117912694,
          'PAPER_SIMULATOR_FINANCIAL_MODEL_V2','ENV:PAPER_SIMULATION_FEE_RATE',
          %s,'RSI','FULL_PAPER_OPPORTUNITY_OBSERVATION_V1',
          'AVAILABLE','MISSING_AT_OBSERVATION','MISSING_AT_OBSERVATION',%s
        ) RETURNING observation_id
        """,
        (
            key, str(event_id), f"decision-{key}", observed_at, observed_at,
            observed_at, "a" * 40, "0" * 64,
        ),
    )
    return cur.fetchone()[0], observed_at


def candles(cur, start, count=240):
    for offset in range(count):
        at = start + timedelta(minutes=offset)
        high = Decimal("101") if offset == 4 else Decimal("100.2")
        low = Decimal("99.5") if offset == 7 else Decimal("99.9")
        cur.execute(
            "INSERT INTO candles(symbol,interval,open_time,high,low) "
            "VALUES('BTCUSDC','1m',%s,%s,%s)",
            (at, high, low),
        )


def test_migration_idempotency_outcomes_maturity_and_immutability(
    disposable_postgres_v16,
):
    conn = database(disposable_postgres_v16, "contract")
    with conn.cursor() as cur:
        # The migration itself is idempotent.
        cur.execute(MIGRATION)
        observation_id, start = insert_observation(cur)
        candles(cur, start)
        cur.execute(
            "SELECT refresh_paper_opportunity_outcomes_v1('local-paper',500)"
        )
        assert cur.fetchone()[0] == 4
        cur.execute(
            "SELECT economic_label,mfe_pct,mae_pct,covered_full_costs,"
            "time_to_full_cost_cover_seconds,time_to_mfe_seconds "
            "FROM paper_opportunity_outcome_v1 "
            "WHERE observation_id=%s AND horizon_minutes=60",
            (str(observation_id),),
        )
        row = cur.fetchone()
        assert row[0] == "ECONOMICALLY_VIABLE"
        assert row[1] == Decimal("1.00")
        assert row[2] == Decimal("-0.500")
        assert row[3] is True
        assert row[4] == Decimal("240")
        assert row[5] == Decimal("240")
        cur.execute(
            "SELECT refresh_paper_opportunity_outcomes_v1('local-paper',500)"
        )
        assert cur.fetchone()[0] == 0
        with pytest.raises(Exception, match="PAPER_OPPORTUNITY_OBSERVATION_IMMUTABLE"):
            cur.execute(
                "UPDATE paper_opportunity_observation_v1 SET reason_text='changed' "
                "WHERE observation_id=%s",
                (str(observation_id),),
            )
        conn.rollback()
    conn.close()


def test_not_yet_mature_and_incomplete_are_distinct(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "maturity")
    with conn.cursor() as cur:
        fresh = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        observation_id, _ = insert_observation(cur, observed_at=fresh)
        cur.execute(
            "SELECT DISTINCT economic_label FROM v_paper_opportunity_outcome_v1 "
            "WHERE observation_id=%s",
            (str(observation_id),),
        )
        assert {row[0] for row in cur.fetchall()} == {"NOT_YET_MATURE"}
    conn.rollback()
    conn.close()


def test_real_consumer_projection_sql_is_idempotent(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "consumer")
    event_id = uuid.uuid4()
    at = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    payload = {
        "event_id": str(event_id),
        "decision_key": "causal-key",
        "environment": "trading_paper",
        "deployment_id": "local-paper",
        "strategy": "RSI",
        "symbol": "BTCUSDC",
        "interval": "1m",
        "decision_created_at": at.isoformat(),
        "decision_reason": "NO_SIGNAL",
        "paper_opportunity": {
            "observation_key": "f" * 64,
            "candle_open_time": at.isoformat(),
            "evaluation_started_at": at.isoformat(),
            "observation_type": "NO_SIGNAL",
            "decision_type": "NO_TRADE",
            "decision_subtype": "NO_SIGNAL",
            "reason_text": "NO_SIGNAL",
            "raw_signal_state": "ABSENT",
            "base_decision": "NO_TRADE",
            "final_decision": "NO_TRADE",
            "data_readiness": "READY",
            "indicator_readiness": "READY",
            "gate_state": "PASS",
            "gate_reason": "NO_SIGNAL",
            "already_open_state": "CLEAR",
            "containment_state": "CLEAR",
            "outcome_eligible": True,
            "opportunity_direction": "LONG",
            "reference_price": "100",
            "runtime_enabled": True,
            "live_orders_enabled": False,
            "treatment_name": None,
            "treatment_status": "NOT_APPLICABLE",
            "treatment_base_decision": None,
            "treatment_decision": None,
            "treatment_reason": None,
            "fee_rate_entry": "0.0035",
            "fee_rate_exit": "0.0035",
            "full_cost_hurdle_pct": "0.702458605117912694",
            "fee_model_version": "PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
            "fee_config_source": "ENV:PAPER_SIMULATION_FEE_RATE",
            "source_revision": "a" * 40,
            "engine_name": "RSI",
            "engine_version": "test",
            "position_id": None,
            "strategy_event_id": None,
            "simulated_order_id": None,
            "contract_version": "FULL_PAPER_OPPORTUNITY_OBSERVATION_V1",
        },
    }
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO causal_decision_observation_v1(event_id) VALUES(%s)",
            (str(event_id),),
        )
        assert persist_paper_opportunity_observation_cursor(cur, payload) is True
        assert persist_paper_opportunity_observation_cursor(cur, payload) is True
        cur.execute("SELECT count(*) FROM paper_opportunity_observation_v1")
        assert cur.fetchone()[0] == 1
    conn.rollback()
    conn.close()
