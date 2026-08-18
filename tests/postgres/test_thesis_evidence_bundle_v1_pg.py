from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

import psycopg2
import pytest

from common.thesis_evidence_bundle import capture_thesis_evidence_bundle_cycle


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db" / "migrations" / "20260818_thesis_evidence_bundle_v1.sql"
GIT_REVISION = "a" * 40
ENVIRON = {
    "TRADING_MODE": "PAPER",
    "ENVIRONMENT": "paper",
    "DEPLOYMENT_ID": "local-paper",
    "GIT_SHA": GIT_REVISION,
}


def _prepare_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE schema_migration_ledger_v1(
                migration_id text PRIMARY KEY,checksum_sha256 text NOT NULL,
                environment text NOT NULL,deployment_id text NOT NULL,
                database_name text NOT NULL,applied_by text NOT NULL,
                status text NOT NULL,success boolean NOT NULL,
                execution_duration_ms bigint NOT NULL,git_sha text NOT NULL,
                schema_baseline_version text
            );
            CREATE TABLE candles(
                id bigserial PRIMARY KEY,symbol text NOT NULL,interval text NOT NULL,
                open_time timestamptz NOT NULL,open numeric,high numeric,low numeric,
                close numeric,volume numeric,close_time timestamptz,
                UNIQUE(symbol,interval,open_time)
            );
            CREATE TABLE market_memory_sequence(
                symbol text NOT NULL,interval text NOT NULL,sequence_key text,
                sequence_type text,sequence_stage text,direction text,
                sequence_quality numeric,continuation_score numeric,
                reversal_score numeric,late_entry_risk numeric,
                orc_readiness_score numeric,orc_hint text,reason text,
                ranking_status text,action_hint text,first_event_at timestamptz,
                last_event_at timestamptz,expires_at timestamptz,
                refreshed_at timestamptz,payload jsonb,
                PRIMARY KEY(symbol,interval)
            );
            CREATE TABLE strategy_events(
                id bigserial PRIMARY KEY,created_at timestamptz NOT NULL,
                symbol text,interval text,strategy text,event_type text,
                decision text,reason text,candle_open_time timestamptz,
                run_id text,info jsonb
            );
            CREATE TABLE decision_registry_v1(
                decision_id uuid PRIMARY KEY,deployment_id text,environment text,
                decision_type text,symbol text,interval text,strategy text,
                decision_timestamp timestamptz,observed_at timestamptz,
                engine_version text,schema_version text
            );
            CREATE TABLE bot_control(
                symbol text,interval text,enabled boolean
            );
            CREATE TABLE positions(id bigint PRIMARY KEY);
            CREATE TABLE orders(id bigint PRIMARY KEY);
            CREATE TABLE fills(id bigint PRIMARY KEY);
            INSERT INTO positions VALUES(1);
            INSERT INTO orders VALUES(1);
            INSERT INTO fills VALUES(1);
            """
        )
    conn.commit()


def _apply_migration(conn):
    with conn.cursor() as cur:
        cur.execute("SET waltrade.migration_checksum=%s", ("b" * 64,))
        cur.execute("SET waltrade.target_environment='PAPER'")
        cur.execute("SET waltrade.target_deployment_id='LOCAL'")
        cur.execute("SET waltrade.git_sha=%s", (GIT_REVISION,))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(MIGRATION.read_text())
    conn.commit()


def _insert_complete_candles(conn, *, symbol: str, cutoff: datetime):
    start = cutoff - timedelta(days=3)
    rows = []
    for index in range(864):
        open_time = start + timedelta(minutes=5 * index)
        price = 100 + index / 100
        rows.append((
            symbol, "5m", open_time, price, price + 1, price - 1,
            price + 0.25, 10 + index, open_time + timedelta(minutes=5, microseconds=-1),
        ))
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO candles(
                symbol,interval,open_time,open,high,low,close,volume,close_time
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    conn.commit()


def _append_candle(conn, *, symbol: str, open_time: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO candles(
                symbol,interval,open_time,open,high,low,close,volume,close_time
            ) VALUES(%s,'5m',%s,110,111,109,110.5,20,%s)
            """,
            (symbol, open_time, open_time + timedelta(minutes=5, microseconds=-1)),
        )
    conn.commit()


@pytest.mark.usefixtures("disposable_postgres_v16")
def test_full_shadow_contract_is_append_only_and_idempotent(disposable_postgres_v16):
    logical_database = f"waltrade_baseline_test_thesis_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(logical_database)
    conn = disposable_postgres_v16.connect(logical_database)
    cutoff = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    try:
        _prepare_schema(conn)
        _apply_migration(conn)
        _apply_migration(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO bot_control VALUES('BTCUSDC','5m',true)")
        conn.commit()
        _insert_complete_candles(conn, symbol="BTCUSDC", cutoff=cutoff)

        factory = lambda: disposable_postgres_v16.connect(logical_database)
        first = capture_thesis_evidence_bundle_cycle(
            factory, evaluated_at=cutoff + timedelta(minutes=2), environ=ENVIRON,
        )
        assert first["evidence_status"] == "COMPLETE"
        assert first["structural"] == 3
        assert first["mme_observations"] == 1
        assert first["mme_transitions"] == 1
        assert first["tactical_sets"] == 1
        assert first["bundles"] == 1

        retry = capture_thesis_evidence_bundle_cycle(
            factory, evaluated_at=cutoff + timedelta(minutes=4), environ=ENVIRON,
        )
        assert retry["pipeline_run_id"] == first["pipeline_run_id"]
        assert retry["pipeline_runs"] == 0
        assert retry["structural"] == 0
        assert retry["mme_observations"] == 0
        assert retry["mme_transitions"] == 0
        assert retry["tactical_sets"] == 0
        assert retry["bundles"] == 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT member_count,completeness_status "
                "FROM thesis_tactical_opportunity_set_v1"
            )
            assert cur.fetchone() == (0, "COMPLETE")
            cur.execute(
                "SELECT availability_state FROM thesis_mme_sequence_observation_v1"
            )
            assert cur.fetchone()[0] == "ABSENT"
            cur.execute(
                "SELECT transition_category FROM thesis_mme_transition_observation_v1"
            )
            assert cur.fetchone()[0] == "SOURCE_ABSENT"
            cur.execute("SELECT count(*) FROM schema_migration_ledger_v1")
            assert cur.fetchone()[0] == 1

            cur.execute("SAVEPOINT immutable_update")
            with pytest.raises(psycopg2.Error):
                cur.execute(
                    "UPDATE thesis_evidence_bundle_v1 SET evidence_status='INCOMPLETE'"
                )
            cur.execute("ROLLBACK TO SAVEPOINT immutable_update")
            cur.execute("SAVEPOINT immutable_delete")
            with pytest.raises(psycopg2.Error):
                cur.execute("DELETE FROM thesis_structural_observation_v1")
            cur.execute("ROLLBACK TO SAVEPOINT immutable_delete")
        conn.rollback()

        _append_candle(conn, symbol="BTCUSDC", open_time=cutoff)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO market_memory_sequence(
                    symbol,interval,sequence_key,sequence_type,sequence_stage,
                    direction,sequence_quality,continuation_score,reversal_score,
                    late_entry_risk,orc_readiness_score,orc_hint,reason,
                    ranking_status,action_hint,first_event_at,last_event_at,
                    expires_at,refreshed_at,payload
                ) VALUES(
                    'BTCUSDC','5m','key','ACTIVE_IMPULSE_SEQUENCE','EXPANSION',
                    'UP',70,70,10,10,72,'ORC_WATCH_CANDIDATE','test',
                    'WATCH','WATCH',%s,%s,%s,%s,'{}'::jsonb
                )
                """,
                (
                    cutoff - timedelta(minutes=10), cutoff,
                    cutoff + timedelta(hours=1), cutoff + timedelta(minutes=1),
                ),
            )
        conn.commit()
        second = capture_thesis_evidence_bundle_cycle(
            factory, evaluated_at=cutoff + timedelta(minutes=7), environ=ENVIRON,
        )
        assert second["mme_transitions"] == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transition_category FROM thesis_mme_transition_observation_v1 "
                "ORDER BY evidence_cutoff DESC LIMIT 1"
            )
            assert cur.fetchone()[0] == "SOURCE_APPEARED"
        conn.rollback()

        _append_candle(conn, symbol="BTCUSDC", open_time=cutoff + timedelta(minutes=5))
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE market_memory_sequence SET sequence_stage='EXHAUSTION',"
                "last_event_at=%s,refreshed_at=%s WHERE symbol='BTCUSDC'",
                (cutoff + timedelta(minutes=5), cutoff + timedelta(minutes=6)),
            )
        conn.commit()
        third = capture_thesis_evidence_bundle_cycle(
            factory, evaluated_at=cutoff + timedelta(minutes=12), environ=ENVIRON,
        )
        assert third["mme_transitions"] == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transition_category FROM thesis_mme_transition_observation_v1 "
                "ORDER BY evidence_cutoff DESC LIMIT 1"
            )
            assert cur.fetchone()[0] == "STAGE_CHANGED"

            cur.execute(
                "SELECT (SELECT count(*) FROM positions),"
                "(SELECT count(*) FROM orders),(SELECT count(*) FROM fills)"
            )
            assert cur.fetchone() == (1, 1, 1)
            cur.execute("SELECT * FROM v_thesis_evidence_integrity_v1")
            assert cur.fetchone() == (0, 0, 0, 0, 0)
        conn.rollback()
    finally:
        conn.close()

def test_missing_structure_and_future_mme_are_explicit(disposable_postgres_v16):
    logical_database = f"waltrade_baseline_test_thesis_missing_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(logical_database)
    conn = disposable_postgres_v16.connect(logical_database)
    cutoff = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    try:
        _prepare_schema(conn)
        _apply_migration(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO bot_control VALUES('ETHUSDC','5m',true)")
            cur.execute(
                """
                INSERT INTO market_memory_sequence(
                    symbol,interval,sequence_key,sequence_type,sequence_stage,
                    direction,first_event_at,last_event_at,expires_at,
                    refreshed_at,payload
                ) VALUES(
                    'ETHUSDC','5m','future','WEAK_SEQUENCE','OBSERVE','UP',
                    %s,%s,%s,%s,'{}'::jsonb
                )
                """,
                (
                    cutoff, cutoff + timedelta(minutes=1),
                    cutoff + timedelta(hours=1), cutoff + timedelta(minutes=2),
                ),
            )
        conn.commit()
        result = capture_thesis_evidence_bundle_cycle(
            lambda: disposable_postgres_v16.connect(logical_database),
            evaluated_at=cutoff + timedelta(minutes=2), environ=ENVIRON,
        )
        assert result["evidence_status"] == "INCOMPLETE"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT missing_sources FROM thesis_evidence_bundle_v1 "
                "WHERE symbol='ETHUSDC'"
            )
            missing = cur.fetchone()[0]
            assert "STRUCTURAL_3D_INCOMPLETE" in missing
            assert "MME_FUTURE_SOURCE:5m" in missing
            cur.execute(
                "SELECT availability_state FROM thesis_mme_sequence_observation_v1"
            )
            assert cur.fetchone()[0] == "FUTURE_SOURCE"
        conn.rollback()
    finally:
        conn.close()
