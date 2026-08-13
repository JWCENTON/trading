from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
from decimal import Decimal

import pytest


ROOT = Path(__file__).resolve().parents[2]
REPROJECTION_PATH = (
    ROOT / "tools/reproject_decision_outcomes_from_canonical_ft_v1.sql"
)
CUTOVER_PATH = ROOT / "tools/register_paper_simulation_fee_cutover_v2.sql"
PORTABILITY = (
    ROOT
    / "db/migrations/20260813_paper_economic_truth_deployment_portability_v1.sql"
).read_text()
OLD_DECISION_MIGRATION = (
    ROOT
    / "db/migrations/20260813_decision_outcome_canonical_financial_truth_source_v1.sql"
)


SCHEMA = r"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE schema_migration_ledger_v1(
    ledger_id bigserial PRIMARY KEY,migration_id text NOT NULL,
    checksum_sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now(),
    environment text NOT NULL,deployment_id text NOT NULL,
    database_name text NOT NULL,applied_by text NOT NULL,status text NOT NULL,
    success boolean NOT NULL,execution_duration_ms bigint NOT NULL,
    git_sha text NOT NULL,error_summary text,schema_baseline_version text NOT NULL
);
CREATE TABLE paper_simulation_fee_cutover_v2(
    cutover_name text PRIMARY KEY,effective_at timestamptz NOT NULL,
    simulation_fee_rate numeric NOT NULL,fee_model_version text NOT NULL,
    fee_config_source text NOT NULL,git_sha text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE positions(id bigint PRIMARY KEY);
CREATE TABLE canonical_financial_truth_v1(
    position_id bigint PRIMARY KEY,financial_truth_status text NOT NULL,
    authoritative_gross_pnl numeric,authoritative_entry_fees_usdc numeric,
    authoritative_exit_fees_usdc numeric,authoritative_fees_usdc numeric,
    authoritative_net_pnl numeric
);
CREATE TABLE decision_outcomes_v1(
    outcome_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),position_id bigint NOT NULL,
    environment text NOT NULL,deployment_id text NOT NULL,outcome_type text NOT NULL,
    gross_pnl_usdc numeric,fees_usdc numeric,net_pnl_usdc numeric,
    outcome_status text NOT NULL,evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);
"""


def _database(pg, suffix: str):
    name = f"waltrade_baseline_test_portability_{suffix}"
    pg.create_database(name)
    connection = pg.connect(name)
    with connection.cursor() as cur:
        cur.execute(SCHEMA)
    connection.commit()
    return connection


def _seed_ledgers(cur):
    for migration, baseline in (
        (
            "20260813_decision_outcome_canonical_financial_truth_source_v1.sql",
            "DECISION_OUTCOME_CANONICAL_FT_SOURCE_V1",
        ),
        (
            "20260813_paper_simulation_fee_contract_v2.sql",
            "PAPER_SIMULATION_FEE_CONTRACT_V2",
        ),
    ):
        cur.execute(
            """
            INSERT INTO schema_migration_ledger_v1(
                migration_id,checksum_sha256,environment,deployment_id,
                database_name,applied_by,status,success,execution_duration_ms,
                git_sha,schema_baseline_version
            ) VALUES(%s,repeat('a',64),'PAPER','LOCAL',current_database(),
                     'test','APPLIED',true,0,repeat('b',40),%s)
            """,
            (migration, baseline),
        )


def _apply_portability(connection, deployment: str, fee_cutover: bool = True):
    with connection.cursor() as cur:
        _seed_ledgers(cur)
        if fee_cutover:
            cur.execute(
                """
                INSERT INTO paper_simulation_fee_cutover_v2 VALUES(
                    'COST_CORRECTED_PAPER_ECONOMIC_CUTOVER',
                    '2026-08-13 12:14:30+00',0.0035,
                    'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',
                    'ENV:PAPER_SIMULATION_FEE_RATE',repeat('c',40),now()
                )
                """
            )
        cur.execute("SELECT set_config('waltrade.target_environment','trading_paper',false)")
        cur.execute(
            "SELECT set_config('waltrade.target_deployment_id',%s,false)",
            (deployment,),
        )
        cur.execute("SELECT set_config('waltrade.paper_simulation_fee_rate','0.0035',false)")
        cur.execute("SELECT set_config('waltrade.git_sha',%s,false)", ("d" * 40,))
        cur.execute(PORTABILITY)


def _psql(pg, connection, path: Path, variables: dict[str, str]):
    params = connection.get_dsn_parameters()
    command = [
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", params["host"],
        "-p", params["port"], "-U", params["user"], "-d", params["dbname"],
    ]
    for key, value in variables.items():
        command.extend(("-v", f"{key}={value}"))
    command.extend(("-f", str(path)))
    env = dict(os.environ)
    env["PGPASSWORD"] = pg.password
    return subprocess.run(command, text=True, capture_output=True, env=env)


@pytest.mark.parametrize("deployment", ["LOCAL", "VPS"])
def test_reprojection_explicit_paper_targets_and_idempotency(
    disposable_postgres_v16, deployment
):
    connection = _database(disposable_postgres_v16, f"reproject_{deployment.lower()}")
    try:
        with connection.cursor() as cur:
            cur.execute("INSERT INTO positions VALUES(1)")
            cur.execute(
                """
                INSERT INTO canonical_financial_truth_v1 VALUES(
                    1,'COMPLETE',2.0,0.2,0.2,0.4,1.6
                )
                """
            )
            cur.execute(
                """
                INSERT INTO decision_outcomes_v1(
                    position_id,environment,deployment_id,outcome_type,
                    gross_pnl_usdc,fees_usdc,net_pnl_usdc,outcome_status
                ) VALUES(1,'trading_paper',%s,'ACTUAL_TRADE',0,0,0,'COMPLETE')
                """,
                (deployment,),
            )
        connection.commit()

        variables = {
            "target_position_ids": "1",
            "target_environment": "trading_paper",
            "target_deployment_id": deployment,
        }
        first = _psql(disposable_postgres_v16, connection, REPROJECTION_PATH, variables)
        assert first.returncode == 0, first.stderr
        with connection.cursor() as cur:
            cur.execute(
                "SELECT gross_pnl_usdc,fees_usdc,net_pnl_usdc,refreshed_at "
                "FROM decision_outcomes_v1 WHERE position_id=1"
            )
            first_row = cur.fetchone()

        second = _psql(disposable_postgres_v16, connection, REPROJECTION_PATH, variables)
        assert second.returncode == 0, second.stderr
        with connection.cursor() as cur:
            cur.execute(
                "SELECT gross_pnl_usdc,fees_usdc,net_pnl_usdc,refreshed_at "
                "FROM decision_outcomes_v1 WHERE position_id=1"
            )
            assert cur.fetchone() == first_row
        assert first_row[:3] == (
            Decimal("2.0"), Decimal("0.4"), Decimal("1.6")
        )
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("environment", "deployment"),
    (("trading_live", "LOCAL"), ("trading_paper", "UNKNOWN")),
)
def test_reprojection_rejects_live_and_unknown_target(
    disposable_postgres_v16, environment, deployment
):
    connection = _database(
        disposable_postgres_v16,
        "reject_live" if environment == "trading_live" else "reject_unknown",
    )
    try:
        result = _psql(
            disposable_postgres_v16,
            connection,
            REPROJECTION_PATH,
            {
                "target_position_ids": "1",
                "target_environment": environment,
                "target_deployment_id": deployment,
            },
        )
        assert result.returncode != 0
        assert "REPROJECTION_TARGET_NOT_ALLOWED" in result.stderr
    finally:
        connection.close()


def test_reprojection_rejects_partial_qualifying_cohort(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "partial")
    try:
        result = _psql(
            disposable_postgres_v16,
            connection,
            REPROJECTION_PATH,
            {
                "target_position_ids": "1,2",
                "target_environment": "trading_paper",
                "target_deployment_id": "LOCAL",
            },
        )
        assert result.returncode != 0
        assert "Refusing partial reprojection" in result.stderr
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("environment", "deployment"),
    (("trading_live", "LOCAL"), ("trading_paper", "UNKNOWN")),
)
def test_provenance_migration_rejects_live_and_unknown_target(
    disposable_postgres_v16, environment, deployment
):
    suffix = "migration_live" if environment == "trading_live" else "migration_unknown"
    connection = _database(disposable_postgres_v16, suffix)
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT set_config('waltrade.target_environment',%s,false)", (environment,))
            cur.execute("SELECT set_config('waltrade.target_deployment_id',%s,false)", (deployment,))
            cur.execute(
                "SELECT set_config('waltrade.paper_simulation_fee_rate','0.0035',false)"
            )
            with pytest.raises(Exception, match="PAPER_ECONOMIC_TRUTH_TARGET_NOT_ALLOWED"):
                cur.execute(PORTABILITY)
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('public.migration_provenance_correction_v1')")
            assert cur.fetchone()[0] is None
    finally:
        connection.close()


@pytest.mark.parametrize("deployment", ["LOCAL", "VPS"])
def test_ledger_and_fee_v2_provenance_are_deployment_correct(
    disposable_postgres_v16, deployment
):
    connection = _database(disposable_postgres_v16, f"ledger_{deployment.lower()}")
    try:
        _apply_portability(connection, deployment)
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM schema_migration_ledger_v1
                WHERE environment='PAPER' AND deployment_id=%s
                  AND migration_id LIKE '20260813%%'
                """,
                (deployment,),
            )
            assert cur.fetchone()[0] == 3
            cur.execute(
                """
                SELECT contract_name,environment,deployment_id,
                       simulation_fee_rate,fee_model_version,fee_config_source
                FROM paper_economic_contract_provenance_v1
                ORDER BY contract_name
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            assert {row[2] for row in rows} == {deployment}
            assert {row[1] for row in rows} == {"trading_paper"}
            fee = next(row for row in rows if row[0] == "PAPER_SIMULATION_FEE_CONTRACT_V2")
            assert str(fee[3]) == "0.0035"
            assert fee[4:] == (
                "PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
                "ENV:PAPER_SIMULATION_FEE_RATE",
            )
            cur.execute("SELECT count(*) FROM migration_provenance_correction_v1")
            assert cur.fetchone()[0] == (0 if deployment == "LOCAL" else 2)
    finally:
        connection.close()


def test_vps_fee_cutover_registration_persists_vps_provenance(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "vps_cutover")
    try:
        _apply_portability(connection, "VPS", fee_cutover=False)
        variables = {
            "target_environment": "trading_paper",
            "target_deployment_id": "VPS",
            "effective_at": "2026-08-13T14:00:00+00:00",
            "simulation_fee_rate": "0.0035",
            "git_sha": "e" * 40,
        }
        first = _psql(disposable_postgres_v16, connection, CUTOVER_PATH, variables)
        assert first.returncode == 0, first.stderr
        second = _psql(disposable_postgres_v16, connection, CUTOVER_PATH, variables)
        assert second.returncode == 0, second.stderr
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT deployment_id,simulation_fee_rate,economic_cutover_at
                FROM paper_economic_contract_provenance_v1
                WHERE contract_name='PAPER_SIMULATION_FEE_CONTRACT_V2'
                """
            )
            row = cur.fetchone()
            assert row[0] == "VPS"
            assert str(row[1]) == "0.0035"
            assert row[2].isoformat() == "2026-08-13T14:00:00+00:00"
    finally:
        connection.close()


def test_old_decision_migration_checksum_and_protected_tables_are_unchanged():
    digest = hashlib.sha256(OLD_DECISION_MIGRATION.read_bytes()).hexdigest()
    assert digest == "fa8fd2f6568e05eb8fbe2e2506a6b2ce912433fd661dcec640688f05e754eefd"
    reprojection = REPROJECTION_PATH.read_text()
    assert "UPDATE public.positions" not in reprojection
    assert "UPDATE public.canonical_financial_truth_v1" not in reprojection
    assert "target_deployment_id" in reprojection
    assert "NOT IN ('LOCAL','VPS')" in reprojection
