from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import uuid

from common.simulated_execution_evidence import (
    execute_paper_exit_after_preflight,
)


ADOPTION_GIT_SHA = "a" * 40
RUNTIME_GIT_SHA = "b" * 40


def test_two_workers_create_at_most_one_exit_order(
    disposable_postgres_v16, monkeypatch
):
    name = f"waltrade_baseline_test_exit_{uuid.uuid4().hex[:10]}"
    disposable_postgres_v16.create_database(name)

    def connect():
        return disposable_postgres_v16.connect(name)

    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE runtime_contract_adoption_v2 (
                      adoption_id BIGINT PRIMARY KEY,contract_name TEXT,
                      environment TEXT,deployment_id TEXT,generation BIGINT,
                      status TEXT,git_revision TEXT,adopted_at TIMESTAMPTZ
                    );
                    CREATE TABLE positions (
                      id BIGINT PRIMARY KEY,symbol TEXT,strategy TEXT,
                      interval TEXT,status TEXT,
                      inventory_contract_adoption_id BIGINT,
                      inventory_contract_generation BIGINT,
                      entry_time TIMESTAMPTZ
                    );
                    CREATE TABLE simulated_orders (
                      id BIGSERIAL PRIMARY KEY,position_id BIGINT UNIQUE
                    );
                    CREATE FUNCTION is_existing_projected_c2_2_compatible(
                      BIGINT,TEXT
                    ) RETURNS BOOLEAN LANGUAGE sql STABLE AS
                    $$ SELECT FALSE $$;
                    INSERT INTO runtime_contract_adoption_v2 VALUES (
                      41,'FEE_AWARE_INVENTORY_C2_2','paper','local-paper',7,
                      'ACTIVE',%s,'2026-08-01T08:00:00Z'
                    );
                    INSERT INTO positions VALUES (
                      77,'BTCUSDC','RSI','1m','OPEN',41,7,
                      '2026-08-01T08:01:00Z'
                    );
                    """,
                    (ADOPTION_GIT_SHA,),
                )
    finally:
        conn.close()

    monkeypatch.setenv("GIT_SHA", RUNTIME_GIT_SHA)
    events = []

    def action(result):
        assert result.active_adoption_git_revision == ADOPTION_GIT_SHA
        assert result.runtime_git_revision == RUNTIME_GIT_SHA
        assert result.runtime_revision_matches_adoption_provenance is False
        action_conn = connect()
        try:
            with action_conn:
                with action_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO simulated_orders(position_id) VALUES (%s)",
                        (result.position_id,),
                    )
                    cur.execute(
                        """
                        UPDATE positions SET status='CLOSED'
                        WHERE id=%s AND status='OPEN' RETURNING id
                        """,
                        (result.position_id,),
                    )
                    assert cur.fetchone() == (77,)
            return {"ledger_ok": True, "position_id": result.position_id}
        finally:
            action_conn.close()

    def worker():
        return execute_paper_exit_after_preflight(
            connect, deployment_id="local-paper", symbol="BTCUSDC",
            strategy="RSI", interval="1m", exit_trigger="STOP_LOSS",
            decision="SELL", price=99.0,
            candle_open_time=datetime.now(timezone.utc),
            emit_event=lambda **event: events.append(event), action=action,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _item: worker(), range(2)))

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM positions WHERE id=77")
            assert cur.fetchone() == ("CLOSED",)
            cur.execute("SELECT count(*) FROM simulated_orders")
            assert cur.fetchone() == (1,)
    finally:
        conn.close()
    assert sum(result["ledger_ok"] for result in results) == 1
    successful = next(result for result in results if result["ledger_ok"])
    assert successful["position_id"] == 77
    denied = [
        result for result in results
        if result.get("blocked_reason") == "PAPER_EXIT_PREFLIGHT_BLOCKED"
    ]
    assert len(denied) == 1
    assert denied[0]["preflight_reason_code"] == "POSITION_ALREADY_CLOSED"
    assert len(events) == 1

    retry = worker()
    assert retry["blocked_reason"] == "PAPER_EXIT_PREFLIGHT_BLOCKED"
    assert retry["preflight_reason_code"] == "POSITION_ALREADY_CLOSED"
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM simulated_orders")
            assert cur.fetchone() == (1,)
    finally:
        conn.close()
    assert len(events) == 2
