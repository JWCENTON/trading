from hashlib import sha256
from pathlib import Path
import uuid

import pytest
import psycopg2


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    ROOT
    / "db/migrations/20260906_regime_ssot_direct_vps_paper_enforcement_v1.sql"
)
MIGRATION = MIGRATION_PATH.read_text()
MIGRATION_SHA = sha256(MIGRATION_PATH.read_bytes()).hexdigest()
POLICY_SHA = "585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e"


def _database(disposable_postgres_v16):
    name = "waltrade_baseline_test_regime_vps_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION pgcrypto")
        cur.execute(
            """
            CREATE TABLE regime_policy(
              strategy text, regime text, allow_entry boolean, note text,
              updated_at timestamptz, PRIMARY KEY(strategy,regime)
            );
            CREATE TABLE bot_control(
              symbol text, interval text, strategy text, regime_enabled boolean,
              regime_mode text, updated_at timestamptz,
              PRIMARY KEY(symbol,interval,strategy)
            );
            CREATE TABLE schema_migration_ledger_v1(
              ledger_id bigserial PRIMARY KEY, migration_id text,
              checksum_sha256 text, applied_at timestamptz default now(),
              environment text, deployment_id text, database_name text,
              applied_by text, status text, success boolean,
              execution_duration_ms bigint, git_sha text, error_summary text,
              schema_baseline_version text
            );
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
            "INSERT INTO regime_policy VALUES"
            "('SUPER_TREND','RANGE_LOWVOL',false,'legacy',now())"
        )
    return conn


def _set_identity(cur, environment, deployment_id, runtime_deployment_id):
    cur.execute("SET waltrade.target_environment=%s", (environment,))
    cur.execute("SET waltrade.target_deployment_id=%s", (deployment_id,))
    cur.execute(
        "SET waltrade.target_runtime_deployment_id=%s", (runtime_deployment_id,)
    )
    cur.execute("SET waltrade.migration_git_sha=%s", ("a" * 40,))
    cur.execute("SET waltrade.migration_checksum=%s", (MIGRATION_SHA,))


def test_vps_first_apply_and_second_apply_are_exactly_idempotent(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _set_identity(cur, "PAPER", "vps-paper", "vps-paper")
            cur.execute(MIGRATION)
            cur.execute(
                """
                SELECT count(*), max(updated_at),
                       (SELECT count(*) FROM schema_migration_ledger_v1
                         WHERE environment='PAPER' AND deployment_id='vps-paper'),
                       (SELECT count(*) FROM schema_migration_ledger_v1
                         WHERE deployment_id='local-paper')
                  FROM regime_policy
                """
            )
            first = cur.fetchone()
            cur.execute(MIGRATION)
            cur.execute(
                """
                SELECT count(*), max(updated_at),
                       (SELECT count(*) FROM schema_migration_ledger_v1
                         WHERE environment='PAPER' AND deployment_id='vps-paper'),
                       (SELECT count(*) FROM schema_migration_ledger_v1
                         WHERE deployment_id='local-paper')
                  FROM regime_policy
                """
            )
            assert cur.fetchone() == first == (20, first[1], 1, 0)
            cur.execute(
                """
                SELECT encode(digest(string_agg(
                    strategy||'|'||regime||'|'||
                    CASE WHEN allow_entry THEN 'ALLOW' ELSE 'BLOCK' END,
                    E'\\n' ORDER BY strategy,regime)||E'\\n','sha256'),'hex'),
                    (SELECT count(*) FROM bot_control
                      WHERE regime_enabled AND regime_mode='ENFORCE'),
                    count(*) FILTER (WHERE strategy='SUPER_TREND')
                  FROM regime_policy
                 WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND')
                """
            )
            assert cur.fetchone() == (POLICY_SHA, 32, 0)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "environment,deployment_id,runtime_deployment_id",
    [
        ("PAPER", "local-paper", "local-paper"),
        ("LIVE", "vps-live", "vps-live"),
        ("LIVE", "local-live", "local-live"),
    ],
)
def test_non_vps_paper_identity_is_rejected_before_any_write(
    disposable_postgres_v16,
    environment,
    deployment_id,
    runtime_deployment_id,
):
    conn = _database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            _set_identity(cur, environment, deployment_id, runtime_deployment_id)
            with pytest.raises(
                psycopg2.errors.RaiseException,
                match="VPS_PAPER_TARGET_NOT_ALLOWED",
            ):
                cur.execute(MIGRATION)
            cur.execute("ROLLBACK")
            cur.execute(
                """SELECT
                    (SELECT count(*) FROM regime_policy),
                    (SELECT count(*) FROM bot_control
                      WHERE regime_enabled OR regime_mode='ENFORCE'),
                    (SELECT count(*) FROM schema_migration_ledger_v1)
                """
            )
            assert cur.fetchone() == (1, 0, 0)
    finally:
        conn.close()
