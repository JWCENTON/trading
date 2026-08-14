from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
TOOL_PATH = ROOT / "tools/install_entry_opportunity_evidence_v1_portable.py"
SPEC = importlib.util.spec_from_file_location("entry_portability_pg", TOOL_PATH)
assert SPEC and SPEC.loader
PORTABILITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PORTABILITY)
ORIGINAL = (
    ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1.sql"
).read_text()


BASE_SCHEMA = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE schema_migration_ledger_v1(
  ledger_id bigserial PRIMARY KEY,migration_id text NOT NULL,
  checksum_sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now(),
  environment text NOT NULL,deployment_id text NOT NULL,database_name text NOT NULL,
  applied_by text NOT NULL,status text NOT NULL,success boolean NOT NULL,
  execution_duration_ms bigint NOT NULL,git_sha text NOT NULL,error_summary text,
  schema_baseline_version text NOT NULL
);
CREATE TABLE decision_registry_v1(
  decision_id uuid PRIMARY KEY,legacy_decision_key text,
  decision_timestamp timestamptz NOT NULL,environment text NOT NULL,
  deployment_id text NOT NULL,strategy text,symbol text,interval text,
  market_regime text,decision_action text,decision_reason text,
  decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,position_id bigint
);
CREATE TABLE simulated_orders(id bigint PRIMARY KEY,decision_id uuid);
CREATE TABLE simulated_execution_fills_v1(id bigint PRIMARY KEY);
CREATE TABLE positions(id bigint PRIMARY KEY);
CREATE TABLE decision_replay_v1(
  id bigserial PRIMARY KEY,position_id bigint,decision_key text
);
CREATE TABLE learning_feature_warehouse_v1(
  id bigserial PRIMARY KEY,position_id bigint,decision_key text
);
CREATE TABLE decision_outcomes_v1(
  position_id bigint,outcome_status text,mfe_pct numeric,mae_pct numeric,
  net_pnl_usdc numeric,outcome_type text
);
"""


def _database(pg, suffix: str):
    name = f"waltrade_baseline_test_entry_portability_{suffix}"
    pg.create_database(name)
    connection = pg.connect(name)
    with connection.cursor() as cur:
        cur.execute(BASE_SCHEMA)
    connection.commit()
    return connection


def _apply_original_local(connection):
    with connection.cursor() as cur:
        cur.execute(
            "SELECT set_config('waltrade.migration_checksum',%s,false)",
            (PORTABILITY.ORIGINAL_CHECKSUM,),
        )
        cur.execute("SELECT set_config('waltrade.git_sha',%s,false)", ("a" * 40,))
        cur.execute(ORIGINAL)


def _seed_snapshot(connection, deployment: str, key: str):
    decision_id = uuid.uuid4()
    snapshot_id = uuid.uuid4()
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO decision_registry_v1(
              decision_id,legacy_decision_key,decision_timestamp,environment,
              deployment_id,strategy,symbol,interval,decision_payload
            ) VALUES(%s,%s,now(),'trading_paper',%s,'RSI','BTCUSDC','1m','{}')
            """,
            (str(decision_id), key, deployment),
        )
        cur.execute(
            """
            INSERT INTO entry_opportunity_evidence_v1(
              snapshot_id,decision_id,decision_created_at,environment,deployment_id,
              strategy,symbol,interval,market_availability_status,
              strategy_availability_status,realtime_availability_status,
              mme_availability_status,slot_brain_availability_status,
              orc_availability_status,planned_entry_notional,
              fee_rate_entry_assumption,fee_rate_exit_assumption,
              expected_round_trip_fee_usdc,expected_round_trip_fee_pct,
              break_even_move_pct,fee_model_version,fee_config_source,
              execution_quality_status,evidence_payload_hash
            ) VALUES(
              %s,%s,now(),'trading_paper',%s,'RSI','BTCUSDC','1m','AVAILABLE',
              'AVAILABLE','AVAILABLE','MISSING','MISSING','MISSING',20,
              .0035,.0035,.14,.7,.702459,'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',
              'ENV:PAPER_SIMULATION_FEE_RATE','MISSING',repeat('b',64)
            )
            """,
            (str(snapshot_id), str(decision_id), deployment),
        )
        cur.execute(
            "UPDATE decision_registry_v1 SET entry_opportunity_snapshot_id=%s "
            "WHERE decision_id=%s",
            (str(snapshot_id), str(decision_id)),
        )
    connection.commit()
    return str(decision_id), str(snapshot_id)


def test_local_existing_and_vps_fresh_install_are_portable_and_schema_equal(
    disposable_postgres_v16,
):
    local = _database(disposable_postgres_v16, "local")
    vps = _database(disposable_postgres_v16, "vps")
    try:
        _apply_original_local(local)
        local_decision, local_snapshot = _seed_snapshot(local, "LOCAL", "local-key")
        local_before = PORTABILITY.schema_fingerprint(local)

        local_result_1 = PORTABILITY.install(
            local,
            environment="PAPER",
            deployment_id="LOCAL",
            runtime_deployment_id="local-paper",
            git_sha="c" * 40,
        )
        local_result_2 = PORTABILITY.install(
            local,
            environment="PAPER",
            deployment_id="LOCAL",
            runtime_deployment_id="local-paper",
            git_sha="c" * 40,
        )
        vps_result = PORTABILITY.install(
            vps,
            environment="PAPER",
            deployment_id="VPS",
            runtime_deployment_id="vps-paper",
            git_sha="c" * 40,
        )

        assert local_result_1["fresh_install"] is False
        assert local_result_2["fresh_install"] is False
        assert vps_result["fresh_install"] is True
        assert local_before == PORTABILITY.schema_fingerprint(local)
        assert PORTABILITY.schema_fingerprint(local) == PORTABILITY.schema_fingerprint(vps)

        with local.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM entry_opportunity_evidence_v1 "
                "WHERE snapshot_id=%s AND decision_id=%s AND deployment_id='LOCAL'",
                (local_snapshot, local_decision),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 WHERE migration_id=%s",
                (PORTABILITY.PORTABILITY_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 1

        with vps.cursor() as cur:
            cur.execute(
                "SELECT environment,deployment_id,checksum_sha256 "
                "FROM schema_migration_ledger_v1 WHERE migration_id=%s",
                (PORTABILITY.ORIGINAL_MIGRATION_ID,),
            )
            assert cur.fetchone() == (
                "PAPER", "VPS", PORTABILITY.ORIGINAL_CHECKSUM,
            )
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s AND deployment_id='LOCAL'",
                (PORTABILITY.ORIGINAL_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        local.close()
        vps.close()


def test_replay_and_warehouse_retain_snapshot_deployment_provenance(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "lineage")
    try:
        PORTABILITY.install(
            connection,
            environment="PAPER",
            deployment_id="VPS",
            runtime_deployment_id="vps-paper",
            git_sha="d" * 40,
        )
        _, snapshot_id = _seed_snapshot(connection, "VPS", "vps-key")
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO decision_replay_v1(decision_key) VALUES('vps-key') "
                "RETURNING entry_opportunity_snapshot_id"
            )
            actual_replay_snapshot_id = cur.fetchone()[0]
            assert uuid.UUID(str(actual_replay_snapshot_id)) == uuid.UUID(
                str(snapshot_id)
            )
            cur.execute(
                "INSERT INTO learning_feature_warehouse_v1(decision_key) "
                "VALUES('vps-key') RETURNING entry_opportunity_snapshot_id"
            )
            actual_warehouse_snapshot_id = cur.fetchone()[0]
            assert uuid.UUID(str(actual_warehouse_snapshot_id)) == uuid.UUID(
                str(snapshot_id)
            )
            cur.execute(
                "SELECT count(*) FROM entry_opportunity_evidence_v1 e "
                "JOIN decision_replay_v1 r "
                "ON r.entry_opportunity_snapshot_id=e.snapshot_id "
                "JOIN learning_feature_warehouse_v1 w "
                "ON w.entry_opportunity_snapshot_id=e.snapshot_id "
                "WHERE e.deployment_id='VPS'"
            )
            assert cur.fetchone()[0] == 1
        connection.rollback()
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("environment", "deployment", "runtime"),
    [
        ("LIVE", "LOCAL", "local-live"),
        ("trading_live", "VPS", "vps-live"),
        ("PAPER", "UNKNOWN", "unknown"),
    ],
)
def test_installation_target_is_fail_closed_before_database_mutation(
    disposable_postgres_v16, environment, deployment, runtime,
):
    connection = _database(
        disposable_postgres_v16,
        hashlib.sha256(f"{environment}{deployment}".encode()).hexdigest()[:8],
    )
    try:
        with pytest.raises(ValueError):
            PORTABILITY.install(
                connection,
                environment=environment,
                deployment_id=deployment,
                runtime_deployment_id=runtime,
                git_sha="e" * 40,
            )
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('entry_opportunity_evidence_v1')")
            assert cur.fetchone()[0] is None
            cur.execute("SELECT count(*) FROM schema_migration_ledger_v1")
            assert cur.fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("environment", "deployment", "runtime"),
    [
        ("LIVE", "LOCAL", "local-live"),
        ("PAPER", "UNKNOWN", "unknown"),
        ("PAPER", "LOCAL", "vps-paper"),
    ],
)
def test_sql_adoption_contract_itself_rejects_invalid_identity(
    disposable_postgres_v16, environment, deployment, runtime,
):
    suffix = hashlib.sha256(
        f"sql{environment}{deployment}{runtime}".encode()
    ).hexdigest()[:8]
    connection = _database(disposable_postgres_v16, suffix)
    try:
        _apply_original_local(connection)
        with connection.cursor() as cur:
            for setting, value in (
                ("waltrade.target_environment", environment),
                ("waltrade.target_deployment_id", deployment),
                ("waltrade.target_runtime_deployment_id", runtime),
                ("waltrade.migration_checksum", "f" * 64),
                ("waltrade.git_sha", "f" * 40),
            ):
                cur.execute("SELECT set_config(%s,%s,true)", (setting, value))
            with pytest.raises(Exception):
                cur.execute(PORTABILITY._transaction_body(PORTABILITY.PORTABILITY_MIGRATION))
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 WHERE migration_id=%s",
                (PORTABILITY.PORTABILITY_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        connection.close()
