from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.supertrend_candle_checkpoint import (
    CandleEvidencePending,
    CheckpointIdentity,
    FreshnessState,
    PostgresCheckpointStore,
    process_resume_workset,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260802_supertrend_persistent_candle_checkpoint_v1.sql"
).read_text()
LEDGER = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()


def test_persistent_checkpoint_resume_and_failure_state(disposable_postgres_v16):
    database = "waltrade_baseline_test_supertrend_checkpoint_v1"
    disposable_postgres_v16.create_database(database)

    def factory():
        return disposable_postgres_v16.connect(database)

    conn = factory()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(LEDGER)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    conn.close()

    identity = CheckpointIdentity(
        "live", "local-live", "BTCUSDC", "1m",
    )
    store = PostgresCheckpointStore(factory, identity)
    t0 = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
    calls = []

    first = process_resume_workset(
        store=store, interval="1m",
        latest_closed_candle_open_time=t0,
        work_items=[t0], open_time_of=lambda value: value,
        processor=lambda item, context: calls.append((item, context.state)),
    )
    assert first.checkpoint_after == t0
    assert calls == [(t0, FreshnessState.READY)]
    assert store.load().state is FreshnessState.READY

    process_resume_workset(
        store=store, interval="1m",
        latest_closed_candle_open_time=t0,
        work_items=[], open_time_of=lambda value: value,
        processor=lambda *_args: pytest.fail("completed candle reprocessed"),
    )

    t1 = t0 + timedelta(minutes=1)
    t2 = t0 + timedelta(minutes=2)

    def pending_evidence(_item, _context):
        raise CandleEvidencePending("indicator evidence pending")

    pending = process_resume_workset(
        store=store, interval="1m",
        latest_closed_candle_open_time=t1,
        work_items=[t1], open_time_of=lambda value: value,
        processor=pending_evidence,
    )
    assert pending.assessment.state is FreshnessState.CATCHING_UP
    assert pending.assessment.reason == "CANDLE_DEPENDENT_EVIDENCE_PENDING"
    assert pending.checkpoint_after == t0
    assert store.load().last_processed_candle_open_time == t0

    process_resume_workset(
        store=store, interval="1m",
        latest_closed_candle_open_time=t1,
        work_items=[t1], open_time_of=lambda value: value,
        processor=lambda *_args: None,
    )
    assert store.load().last_processed_candle_open_time == t1

    def fail_on_latest(item, _context):
        if item == t2:
            raise RuntimeError("targeted failure")

    with pytest.raises(RuntimeError, match="targeted failure"):
        process_resume_workset(
            store=store, interval="1m",
            latest_closed_candle_open_time=t2,
            work_items=[t2], open_time_of=lambda value: value,
            processor=fail_on_latest,
        )

    stalled = store.load()
    assert stalled.last_processed_candle_open_time == t1
    assert stalled.state is FreshnessState.STALLED
    assert stalled.reason == "CANDLE_PROCESSING_DID_NOT_COMPLETE"

    conn = factory()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type,checkpoint_before,checkpoint_after,state,
                   backlog_size,reason,resume_source
            FROM supertrend_candle_checkpoint_event_v1
            ORDER BY event_id
            """
        )
        events = cur.fetchall()
        assert [row[0] for row in events].count("ADVANCED") == 2
        assert events[-1][0] == "STALLED"
        assert events[-1][1] == t1 and events[-1][2] == t1
        assert events[-1][3] == "STALLED"
        assert events[-1][4] == 1
        assert events[-1][5] == "CANDLE_PROCESSING_DID_NOT_COMPLETE"
        assert events[-1][6] == "PERSISTED_CHECKPOINT"
        cur.execute(
            """
            SELECT count(*) FROM supertrend_candle_checkpoint_v1
            WHERE environment='live' AND deployment_id='local-live'
              AND symbol='BTCUSDC' AND "interval"='1m'
              AND strategy='SUPERTREND'
            """
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            """
            SELECT count(*) FROM schema_migration_ledger_v1
            WHERE migration_id=
              '20260802_supertrend_persistent_candle_checkpoint_v1.sql'
              AND success=true
            """
        )
        assert cur.fetchone()[0] == 1
    conn.close()
