from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LEDGER = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()
CHECKPOINT = (
    ROOT / "db/migrations/20260802_supertrend_persistent_candle_checkpoint_v1.sql"
).read_text()
INDICATOR_STATE = (
    ROOT / "db/migrations/20260803_supertrend_incremental_indicator_state_v1.sql"
).read_text()


def test_indicator_high_water_is_persistent_monotonic_and_idempotent(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_supertrend_incremental_state_v1"
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(LEDGER)
        cur.execute(CHECKPOINT)
        cur.execute(INDICATOR_STATE)
        cur.execute(INDICATOR_STATE)
        cur.execute(
            """
            SELECT count(*) FROM schema_migration_ledger_v1
            WHERE migration_id=
              '20260803_supertrend_incremental_indicator_state_v1.sql'
              AND success=true
            """
        )
        assert cur.fetchone()[0] == 1
        t0 = datetime(2026, 8, 3, 20, 0, tzinfo=timezone.utc)
        cur.execute(
            """
            INSERT INTO supertrend_indicator_state_v1(
              environment,deployment_id,symbol,"interval",strategy,
              last_calculated_candle_open_time,last_close,ema_value,atr_value,
              final_upper_band,final_lower_band,supertrend_direction,
              parameter_fingerprint
            ) VALUES('paper','local-paper','BNBUSDC','1m','SUPERTREND',
                     %s,582.1,581.9,0.8,584.3,579.5,1,%s)
            """,
            (t0, "a" * 64),
        )
    conn.close()

    restarted = disposable_postgres_v16.connect(database)
    restarted.autocommit = True
    with restarted.cursor() as cur:
        cur.execute(
            """
            SELECT last_calculated_candle_open_time,last_close,ema_value,
                   atr_value,final_upper_band,final_lower_band,
                   supertrend_direction,parameter_fingerprint
            FROM supertrend_indicator_state_v1
            WHERE environment='paper' AND deployment_id='local-paper'
              AND symbol='BNBUSDC' AND "interval"='1m'
            """
        )
        row = cur.fetchone()
        assert row == (t0, 582.1, 581.9, 0.8, 584.3, 579.5, 1, "a" * 64)
        t1 = t0 + timedelta(minutes=1)
        cur.execute(
            """
            UPDATE supertrend_indicator_state_v1
            SET last_calculated_candle_open_time=%s,last_close=582.2
            WHERE environment='paper' AND deployment_id='local-paper'
              AND symbol='BNBUSDC' AND "interval"='1m'
              AND last_calculated_candle_open_time=%s
            """,
            (t1, t0),
        )
        assert cur.rowcount == 1
        cur.execute(
            """
            SELECT last_calculated_candle_open_time,count(*) OVER ()
            FROM supertrend_indicator_state_v1
            """
        )
        assert cur.fetchone() == (t1, 1)
    restarted.close()
