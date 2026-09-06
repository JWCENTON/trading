"""Isolated PostgreSQL proof for causal regime SSOT reads and policy coverage."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

from common import regime_gate


NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260906_regime_ssot_direct_paper_enforcement_v1.sql").read_text()


def _database(disposable_postgres_v16):
    name = "waltrade_baseline_test_regime_ssot_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE market_regime(
              symbol text, interval text, ts timestamptz, regime text,
              created_at timestamptz
            );
            CREATE TABLE regime_policy(
              strategy text, regime text, allow_entry boolean, note text,
              PRIMARY KEY(strategy,regime)
            );
            """
        )
    conn.commit()
    return conn


def test_causal_lookup_excludes_future_and_uses_latest_prior(
    disposable_postgres_v16, monkeypatch
):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO market_regime VALUES(%s,%s,%s,%s,%s)",
                [
                    ("BTCUSDC", "1m", NOW - timedelta(minutes=1), "TREND_DOWN", NOW),
                    ("BTCUSDC", "1m", NOW, "TREND_UP", NOW),
                    ("BTCUSDC", "1m", NOW + timedelta(minutes=1), "SHOCK", NOW),
                ],
            )
        conn.commit()
        monkeypatch.setattr(regime_gate, "get_db_conn", lambda: disposable_postgres_v16.connect(conn.info.dbname))
        item = regime_gate.get_current_regime_record(
            "BTCUSDC", "1m", decision_candle_timestamp=NOW
        )
        assert item.regime == "TREND_UP" and item.source_ts == NOW
    finally:
        conn.close()


def test_canonical_policy_lookup_has_no_legacy_runtime_identity(
    disposable_postgres_v16, monkeypatch
):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO regime_policy VALUES('SUPERTREND','RANGE_LOWVOL',false,'fixture')"
            )
        conn.commit()
        monkeypatch.setattr(regime_gate, "get_db_conn", lambda: disposable_postgres_v16.connect(conn.info.dbname))
        assert regime_gate.get_policy("SUPER_TREND", "RANGE_LOWVOL") == (False, "fixture")
    finally:
        conn.close()


def test_migration_is_idempotent_and_enforces_exact_policy_and_slots(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION pgcrypto")
            cur.execute("ALTER TABLE regime_policy ADD COLUMN updated_at timestamptz")
            cur.execute(
                """CREATE TABLE bot_control(
                     symbol text,interval text,strategy text,regime_enabled boolean,
                     regime_mode text,updated_at timestamptz,
                     PRIMARY KEY(symbol,interval,strategy));
                   CREATE TABLE schema_migration_ledger_v1(
                     ledger_id bigserial PRIMARY KEY,migration_id text,
                     checksum_sha256 text,applied_at timestamptz default now(),
                     environment text,deployment_id text,database_name text,
                     applied_by text,status text,success boolean,
                     execution_duration_ms bigint,git_sha text,error_summary text,
                     schema_baseline_version text);
                """
            )
            for strategy in ("RSI", "TREND", "SUPERTREND", "BBRANGE"):
                for interval in ("1m", "5m"):
                    for symbol in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"):
                        cur.execute(
                            "INSERT INTO bot_control VALUES(%s,%s,%s,false,'DRY_RUN',now())",
                            (symbol, interval, strategy),
                        )
            cur.execute(
                "INSERT INTO regime_policy VALUES('SUPER_TREND','RANGE_LOWVOL',false,'legacy',now())"
            )
        conn.commit()
        for _ in range(2):
            with conn.cursor() as cur:
                cur.execute("SET waltrade.test_database='on'")
                cur.execute("SET waltrade.migration_git_sha=%s", ("a" * 40,))
                cur.execute("SET waltrade.migration_checksum=%s", ("b" * 64,))
                cur.execute(MIGRATION)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                     (SELECT count(*) FROM regime_policy),
                     (SELECT count(*) FROM regime_policy WHERE strategy='SUPER_TREND'),
                     (SELECT count(*) FROM bot_control WHERE regime_enabled AND regime_mode='ENFORCE'),
                     (SELECT count(*) FROM schema_migration_ledger_v1 WHERE success)
                """
            )
            assert cur.fetchone() == (20, 0, 32, 1)
    finally:
        conn.close()
