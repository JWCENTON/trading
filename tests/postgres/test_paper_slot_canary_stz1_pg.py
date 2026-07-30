from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common.supertrend_terminal_outcome import (
    expire_paper_supertrend_slot_canaries,
    paper_supertrend_entries_enabled,
)


ROOT = Path(__file__).resolve().parents[2]


def test_single_slot_atomic_consume_and_expiry(disposable_postgres_v16):
    database = "waltrade_baseline_test_stz1_canary"
    disposable_postgres_v16.create_database(database)

    def factory():
        return disposable_postgres_v16.connect(database)

    conn = factory()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE paper_strategy_entry_gate_v1(
                environment TEXT,deployment_id TEXT,strategy TEXT,
                entries_enabled BOOLEAN,operator_reason TEXT,
                PRIMARY KEY(environment,deployment_id,strategy));
                INSERT INTO paper_strategy_entry_gate_v1 VALUES
                ('paper','local-paper','SUPERTREND',false,'GLOBAL_CONTAINMENT');"""
            )
            cur.execute(
                (ROOT / "db/migrations/20260730_paper_strategy_slot_canary_v1.sql").read_text()
            )
            cur.execute(
                """INSERT INTO paper_strategy_slot_canary_v1(
                environment,deployment_id,strategy,symbol,"interval",enabled,
                maximum_entries,accepted_entries_count,expires_at,
                operator_reason,changed_by)
                VALUES ('paper','local-paper','SUPERTREND','SOLUSDC','1m',true,
                1,0,clock_timestamp()+interval '6 hours','TEST_CANARY','TEST')"""
            )
    conn.close()

    def consume(_):
        return paper_supertrend_entries_enabled(
            factory, deployment_id="local-paper",
            symbol="SOLUSDC", interval="1m",
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(consume, range(4)))
    assert sum(1 for allowed, _ in results if allowed) == 1
    assert sum(1 for allowed, _ in results if not allowed) == 3
    assert paper_supertrend_entries_enabled(
        factory, deployment_id="local-paper",
        symbol="BTCUSDC", interval="1m",
    )[0] is False

    conn = factory()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT enabled,maximum_entries,accepted_entries_count
                FROM paper_strategy_slot_canary_v1"""
            )
            assert cur.fetchone() == (False, 1, 1)
            cur.execute(
                "SELECT count(*) FROM paper_strategy_slot_canary_audit_v1"
            )
            assert cur.fetchone()[0] == 2
            cur.execute(
                """UPDATE paper_strategy_slot_canary_v1 SET
                enabled=true,accepted_entries_count=0,
                expires_at=clock_timestamp()-interval '1 second'"""
            )
    conn.close()
    assert expire_paper_supertrend_slot_canaries(
        factory, deployment_id="local-paper"
    ) == 1
    assert consume(None)[0] is False
