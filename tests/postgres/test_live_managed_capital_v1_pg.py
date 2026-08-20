"""Disposable PostgreSQL gates for LIVE managed-capital persistence."""

from __future__ import annotations

import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260820_live_managed_capital_authority_v1.sql"
).read_text()


def _apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def test_migration_is_idempotent_empty_and_append_only(disposable_postgres_v16):
    name = "waltrade_baseline_test_live_capital_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn)
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM live_managed_capital_baseline_v1),"
                "(SELECT count(*) FROM owner_capital_flow_v1),"
                "(SELECT count(*) FROM live_managed_equity_observation_v1)"
            )
            assert cur.fetchone() == (0, 0, 0)
            cur.execute(
                "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND "
                "tgname IN ('trg_live_managed_capital_baseline_v1_append_only',"
                "'trg_owner_capital_flow_v1_append_only',"
                "'trg_live_managed_equity_observation_v1_append_only')"
            )
            assert cur.fetchone()[0] == 3
    finally:
        conn.close()
