#!/usr/bin/env python3
"""Disposable-only real PostgreSQL transport integration harness."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from common.db import get_db_conn
from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.decision_observation_transport import (
    DecisionObservationOutboxConsumer, DurableDecisionObservationProducer, TransportFlags,
)


def make_decision(deployment: str, strategy: str, kind: str) -> FinalDecision:
    now = datetime.now(timezone.utc)
    context = EvaluationContext(
        deployment_id=deployment, environment="trading_paper", symbol="BTCUSDT", interval="1m",
        strategy=strategy, candle_open_time=now, evaluation_started_at=now, paper_mode=True,
        engine_name=strategy,
    )
    if kind == "TRADE":
        return FinalDecision.paper_simulation(
            context, DecisionReason.SSOT_EXECUTE_AND_RECORD, finished_at=now,
            simulated_order_id=None,
        )
    if kind == "HOLD":
        return FinalDecision.position_hold(
            context, DecisionReason.POSITION_HOLD, finished_at=now,
        )
    if kind == "EXIT":
        return FinalDecision.exit_result(
            context, DecisionReason.SSOT_EXECUTE_AND_RECORD, finished_at=now,
        )
    return FinalDecision.no_trade(context, DecisionReason.NO_SIGNAL, finished_at=now)


def flags(deployment: str, retry_limit: int = 3) -> TransportFlags:
    return TransportFlags(decision_observation_enabled=True, shadow_observation_enabled=False,
                          auto_apply=False, kill_switch=False, deployment_id=deployment,
                          batch_size=20, retry_limit=retry_limit, retry_backoff_seconds=3600)


def scalar(sql: str, params=()):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        conn.close()


def main() -> int:
    if not os.getenv("WALTRADE_DISPOSABLE_CAUSAL_TEST") == "1":
        raise SystemExit("refusing: WALTRADE_DISPOSABLE_CAUSAL_TEST=1 required")
    protected_before = {
        name: scalar(f"SELECT count(*) FROM {name}")
        for name in ("positions", "simulated_orders", "binance_order_fills",
                     "learning_recommendation_activations_v1",
                     "learning_would_trade_decisions_v1", "causal_promotion_consumption_v1")
    }
    scenarios = (
        ("local-paper", "RSI", "NO_TRADE"),
        ("local-paper", "TREND", "TRADE"),
        ("local-paper", "SUPERTREND", "HOLD"),
        ("local-paper", "BBRANGE", "EXIT"),
    )
    for deployment, strategy, kind in scenarios:
        item = make_decision(deployment, strategy, kind)
        key = f"harness-v8:{deployment}:{strategy.lower()}:{kind.lower()}"
        producer = DurableDecisionObservationProducer(get_db_conn, flags(deployment), source_service="harness")
        assert producer.observe(item, decision_key=key) is item
        assert producer.observe(item, decision_key=key) is item  # identical producer retry
        consumer = DecisionObservationOutboxConsumer(get_db_conn, flags(deployment),
                                                       consumer_id=f"harness:{deployment}")
        assert consumer.poll() == 1
        assert consumer.poll() == 0  # duplicate consumer delivery is idempotent

    changed = make_decision("local-paper", "RSI", "NO_TRADE")
    conflict_producer = DurableDecisionObservationProducer(
        get_db_conn, flags("local-paper"), source_service="harness"
    )
    conflict_producer.observe(changed, decision_key="harness-v8:local-paper:trend:trade")
    assert conflict_producer.last_error_code == "IDEMPOTENCY_CONFLICT"

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            bad_id = str(uuid.uuid4())
            cur.execute("""INSERT INTO causal_decision_observation_outbox_v1
                (event_id,deployment_id,decision_key,event_schema_version,event_payload,event_payload_hash,
                 semantic_digest,source_service,decision_created_at,processing_status)
                VALUES (%s,'local-paper','harness-v8:retry','V1','{}','bad','bad','harness',now(),'PENDING')""",
                (bad_id,))
        conn.commit()
    finally:
        conn.close()
    DecisionObservationOutboxConsumer(get_db_conn, flags("local-paper"), consumer_id="harness:retry").poll()

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO causal_decision_observation_outbox_v1
                (event_id,deployment_id,decision_key,event_schema_version,event_payload,event_payload_hash,
                 semantic_digest,source_service,decision_created_at,processing_status)
                VALUES (%s,'local-paper','harness-v8:dead-letter','V1','{}','bad','bad','harness',now(),'PENDING')""",
                (str(uuid.uuid4()),))
        conn.commit()
    finally:
        conn.close()
    DecisionObservationOutboxConsumer(get_db_conn, flags("local-paper", retry_limit=1),
                                       consumer_id="harness:dead-letter").poll()

    assert scalar("SELECT count(*) FROM causal_decision_observation_v1 WHERE decision_key LIKE 'harness-v8:%%'") == 4
    assert scalar("SELECT count(*) FROM decision_replay_v1 WHERE observation_decision_key LIKE 'harness-v8:%%'") == 4
    assert scalar("SELECT count(*) FROM learning_feature_warehouse_v1 WHERE observation_decision_key LIKE 'harness-v8:%%'") == 4
    assert scalar("SELECT count(DISTINCT strategy) FROM causal_decision_observation_v1 WHERE decision_key LIKE 'harness-v8:%%'") == 4
    assert scalar("SELECT count(*) FROM causal_decision_observation_outbox_v1 WHERE decision_key='harness-v8:local-paper:trend:trade' AND processing_status='IDEMPOTENCY_CONFLICT'") == 1
    assert scalar("SELECT count(*) FROM causal_decision_observation_outbox_v1 WHERE decision_key='harness-v8:retry' AND processing_status='RETRY'") == 1
    assert scalar("SELECT count(*) FROM causal_decision_observation_outbox_v1 WHERE decision_key='harness-v8:dead-letter' AND processing_status='DEAD_LETTER' AND event_payload='{}'::jsonb") == 1
    for name, before in protected_before.items():
        assert scalar(f"SELECT count(*) FROM {name}") == before
    assert scalar("SELECT count(*) FROM decision_registry_v1 WHERE experiment_arm='TREATMENT'") == 0
    print("integration_harness=PASS observations=4 replay=4 warehouse=4 strategies=4 "
          "idempotency_conflict=1 retry=1 dead_letter=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
