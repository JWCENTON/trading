from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from common.long_horizon_l3 import (
    canonical_opportunity_identity,
    prepare_admission_cursor,
    sampling_selected,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v1.sql").read_text()


def test_l3_migration_is_local_only_idempotent_and_sets_32_dry_run_slots(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_l3_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION pgcrypto")
            cur.execute(
                """
                CREATE TABLE regime_gate_events(
                  id bigserial PRIMARY KEY,regime text,mode text,would_block boolean,
                  why text,meta jsonb);
                CREATE TABLE simulated_orders(
                  id bigserial PRIMARY KEY,decision_id uuid,entry_opportunity_snapshot_id uuid);
                CREATE TABLE positions(id bigserial PRIMARY KEY,status text);
                CREATE TABLE paper_managed_equity_observation_v1(
                  deployment_id text,managed_equity_status text,managed_equity numeric,
                  observed_at timestamptz);
                CREATE TABLE simulated_execution_fills_v1(
                  position_id bigint,order_purpose text,fill_notional numeric,
                  environment text,deployment_id text);
                CREATE TABLE bot_control(
                  symbol text,interval text,strategy text,regime_enabled boolean,
                  regime_mode text,updated_at timestamptz,
                  PRIMARY KEY(symbol,interval,strategy));
                CREATE TABLE schema_migration_ledger_v1(
                  ledger_id bigserial PRIMARY KEY,migration_id text,checksum_sha256 text,
                  applied_at timestamptz default now(),environment text,deployment_id text,
                  database_name text,applied_by text,status text,success boolean,
                  execution_duration_ms bigint,git_sha text,error_summary text,
                  schema_baseline_version text);
                """
            )
            for strategy in ("RSI", "TREND", "SUPERTREND", "BBRANGE"):
                for interval in ("1m", "5m"):
                    for symbol in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"):
                        cur.execute(
                            "INSERT INTO bot_control VALUES(%s,%s,%s,true,'ENFORCE',now())",
                            (symbol, interval, strategy),
                        )
        conn.commit()
        for _ in range(2):
            with conn.cursor() as cur:
                cur.execute("SET waltrade.test_database='on'")
                cur.execute("SET waltrade.target_deployment_id='local-paper'")
                cur.execute("SET waltrade.migration_git_sha=%s", ("a" * 40,))
                cur.execute("SET waltrade.migration_checksum=%s", ("b" * 64,))
                cur.execute(MIGRATION)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                  (SELECT count(*) FROM long_horizon_l3_contract_v1 WHERE status='ACTIVE'),
                  (SELECT count(*) FROM bot_control WHERE regime_enabled AND regime_mode='DRY_RUN'),
                  (SELECT count(*) FROM schema_migration_ledger_v1
                    WHERE migration_id='20260907_long_horizon_l3_direct_local_paper_v1')"""
            )
            assert cur.fetchone() == (1, 32, 1)
            cur.execute(
                """SELECT contract_payload->>'entry_notional_usdc',
                          contract_payload->>'allow_sampling_probability',
                          contract_payload->>'block_sampling_probability',
                          contract_payload->>'sampling_contract_fingerprint',
                          contract_payload->>'minimum_equity_for_block_capacity_usdc'
                     FROM long_horizon_l3_contract_v1"""
            )
            assert cur.fetchone() == (
                "9", "0.13194281540", "0.07920637611",
                "0f67b3c42d19ab5447dc4b1e9a9f0e553f62c3b72d88bdc8d9dc64b36f0312d1",
                "585",
            )
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        at = datetime(2026, 9, 7, 18, 30, tzinfo=timezone.utc)
        with conn.cursor() as cur:
            gate_id = None
            for _ in range(100):
                cur.execute(
                    "INSERT INTO regime_gate_events(regime,mode,would_block,why,meta) "
                    "VALUES('TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id"
                )
                candidate = int(cur.fetchone()[0])
                identity = canonical_opportunity_identity(
                    gate_event_id=candidate, symbol="BTCUSDC", interval="1m",
                    strategy="TREND", side="BUY", candle_open_time=at,
                )
                if sampling_selected(identity, "L3_REGIME_WOULD_ALLOW"):
                    gate_id = candidate
                    break
            assert gate_id is not None
            cur.execute(
                "INSERT INTO paper_managed_equity_observation_v1 VALUES"
                "('local-paper','CANONICAL',635.430007829136,%s)", (at,),
            )
            result = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND",
                side="BUY", candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": gate_id},
                entry_price=Decimal("79244.2"), instrument_step=Decimal("0.00000001"),
                instrument_min_qty=Decimal("0.0001"), instrument_min_notional=Decimal("0"),
            )
            assert result.accepted and result.cohort == "L3_REGIME_WOULD_ALLOW"
            retry = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND",
                side="BUY", candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": gate_id},
                entry_price=Decimal("79244.2"), instrument_step=Decimal("0.00000001"),
                instrument_min_qty=Decimal("0.0001"), instrument_min_notional=Decimal("0"),
            )
            assert retry.accepted and retry.admission_id == result.admission_id
            rejected_gate_id = None
            for _ in range(100):
                cur.execute(
                    "INSERT INTO regime_gate_events(regime,mode,would_block,why,meta) "
                    "VALUES('TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id"
                )
                candidate = int(cur.fetchone()[0])
                identity = canonical_opportunity_identity(
                    gate_event_id=candidate, symbol="BTCUSDC", interval="1m",
                    strategy="TREND", side="BUY", candle_open_time=at,
                )
                if sampling_selected(identity, "L3_REGIME_WOULD_ALLOW"):
                    rejected_gate_id = candidate
                    break
            assert rejected_gate_id is not None
            rejected = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND",
                side="BUY", candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": rejected_gate_id},
                entry_price=Decimal("100000"), instrument_step=Decimal("0.00000001"),
                instrument_min_qty=Decimal("0.0001"), instrument_min_notional=Decimal("0"),
            )
            assert not rejected.accepted and rejected.status == "MIN_NOTIONAL_NOT_MET"
            cur.execute(
                """SELECT status,entry_notional,effective_min_notional,position_id
                     FROM long_horizon_l3_admission_v1 WHERE admission_id=%s""",
                (rejected.admission_id,),
            )
            assert cur.fetchone() == ("MIN_NOTIONAL_NOT_MET", Decimal("9"), Decimal("10.0000"), None)
        conn.rollback()
    finally:
        conn.close()


def test_l3_capacity_pause_below_frozen_minimum_equity(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_l3_pause_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION pgcrypto")
            cur.execute(
                """
                CREATE TABLE regime_gate_events(id bigserial PRIMARY KEY,regime text,mode text,
                  would_block boolean,why text,meta jsonb);
                CREATE TABLE simulated_orders(id bigserial PRIMARY KEY,decision_id uuid,
                  entry_opportunity_snapshot_id uuid);
                CREATE TABLE positions(id bigserial PRIMARY KEY,status text);
                CREATE TABLE paper_managed_equity_observation_v1(deployment_id text,
                  managed_equity_status text,managed_equity numeric,observed_at timestamptz);
                CREATE TABLE simulated_execution_fills_v1(position_id bigint,order_purpose text,
                  fill_notional numeric,environment text,deployment_id text);
                CREATE TABLE bot_control(symbol text,interval text,strategy text,regime_enabled boolean,
                  regime_mode text,updated_at timestamptz,PRIMARY KEY(symbol,interval,strategy));
                CREATE TABLE schema_migration_ledger_v1(ledger_id bigserial PRIMARY KEY,
                  migration_id text,checksum_sha256 text,applied_at timestamptz default now(),
                  environment text,deployment_id text,database_name text,applied_by text,status text,
                  success boolean,execution_duration_ms bigint,git_sha text,error_summary text,
                  schema_baseline_version text);
                """
            )
            for strategy in ("RSI", "TREND", "SUPERTREND", "BBRANGE"):
                for interval in ("1m", "5m"):
                    for symbol in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"):
                        cur.execute("INSERT INTO bot_control VALUES(%s,%s,%s,true,'ENFORCE',now())",
                                    (symbol, interval, strategy))
            cur.execute("SET waltrade.test_database='on'")
            cur.execute("SET waltrade.target_deployment_id='local-paper'")
            cur.execute("SET waltrade.migration_git_sha=%s", ("a" * 40,))
            cur.execute("SET waltrade.migration_checksum=%s", ("b" * 64,))
            cur.execute(MIGRATION)
        conn.commit()
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        at = datetime(2026, 9, 7, 19, 0, tzinfo=timezone.utc)
        with conn.cursor() as cur:
            gate_id = None
            for _ in range(100):
                cur.execute("INSERT INTO regime_gate_events(regime,mode,would_block,why,meta) "
                            "VALUES('TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id")
                candidate = int(cur.fetchone()[0])
                identity = canonical_opportunity_identity(
                    gate_event_id=candidate, symbol="BTCUSDC", interval="1m",
                    strategy="TREND", side="BUY", candle_open_time=at,
                )
                if sampling_selected(identity, "L3_REGIME_WOULD_ALLOW"):
                    gate_id = candidate
                    break
            assert gate_id is not None
            cur.execute("INSERT INTO paper_managed_equity_observation_v1 VALUES"
                        "('local-paper','CANONICAL',584.999999999999,%s)", (at,))
            result = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND", side="BUY",
                candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": gate_id},
                entry_price=Decimal("79244.2"), instrument_step=Decimal("0.00000001"),
                instrument_min_qty=Decimal("0.0001"), instrument_min_notional=Decimal("0"),
            )
            assert not result.accepted and result.status == "L3_CAPACITY_PAUSE"
        conn.rollback()
    finally:
        conn.close()


def test_l3_migration_rejects_non_local_deployment(disposable_postgres_v16):
    database = "waltrade_baseline_test_l3_guard_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        for deployment in ("vps-paper", "local-live", "vps-live"):
            with conn.cursor() as cur:
                cur.execute("SET waltrade.test_database='on'")
                cur.execute("SET waltrade.target_deployment_id=%s", (deployment,))
                try:
                    cur.execute(MIGRATION)
                except Exception as exc:
                    assert "LONG_HORIZON_L3_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in str(exc)
                    conn.rollback()
                else:
                    raise AssertionError(f"{deployment} migration unexpectedly succeeded")
    finally:
        conn.close()
