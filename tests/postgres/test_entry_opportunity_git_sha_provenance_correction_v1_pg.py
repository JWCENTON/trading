from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from tests.postgres.test_entry_opportunity_evidence_portability_v1_pg import (
    PORTABILITY,
    _apply_original_local,
    _database,
    _seed_snapshot,
)


ROOT = Path(__file__).resolve().parents[2]
CORRECTION_PATH = (
    ROOT
    / "db/migrations/20260814_entry_opportunity_evidence_v1_1_git_sha_provenance_correction.sql"
)
CORRECTION_ID = CORRECTION_PATH.name
BAD_SHA = "3ccbdd8f0000000000000000000000000000000000"
CANONICAL_SHA = "3ccbdd82989a917ce04ab1ff6856f3e3f3854688"
SOURCE_SHA = "f" * 40


def _apply_correction(connection, environment: str, deployment: str, runtime: str):
    checksum = hashlib.sha256(CORRECTION_PATH.read_bytes()).hexdigest()
    with connection.cursor() as cur:
        for setting, value in (
            ("waltrade.target_environment", environment),
            ("waltrade.target_deployment_id", deployment),
            ("waltrade.target_runtime_deployment_id", runtime),
            ("waltrade.original_git_sha", BAD_SHA),
            ("waltrade.corrected_git_sha", CANONICAL_SHA),
            ("waltrade.git_sha", SOURCE_SHA),
            ("waltrade.migration_checksum", checksum),
        ):
            cur.execute("SELECT set_config(%s,%s,true)", (setting, value))
        cur.execute(PORTABILITY._transaction_body(CORRECTION_PATH))
    connection.commit()


def _seed_bad_local(connection):
    _apply_original_local(connection)
    with connection.cursor() as cur:
        for setting, value in (
            ("waltrade.target_environment", "PAPER"),
            ("waltrade.target_deployment_id", "LOCAL"),
            ("waltrade.target_runtime_deployment_id", "local-paper"),
            ("waltrade.migration_checksum", PORTABILITY._checksum(
                PORTABILITY.PORTABILITY_MIGRATION
            )),
            ("waltrade.git_sha", "a" * 40),
        ):
            cur.execute("SELECT set_config(%s,%s,false)", (setting, value))
        cur.execute(PORTABILITY._transaction_body(PORTABILITY.PORTABILITY_MIGRATION))
        cur.execute(
            "UPDATE schema_migration_ledger_v1 SET git_sha=%s "
            "WHERE migration_id=%s",
            (BAD_SHA, PORTABILITY.PORTABILITY_MIGRATION_ID),
        )
    connection.commit()


def test_local_bad_row_is_immutable_and_corrected_append_only(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "git_correction_local")
    try:
        _seed_bad_local(connection)
        _, snapshot_id = _seed_snapshot(connection, "LOCAL", "correction-key")
        fingerprint_before = PORTABILITY.schema_fingerprint(connection)
        with connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM schema_migration_ledger_v1 WHERE migration_id=%s",
                (PORTABILITY.PORTABILITY_MIGRATION_ID,),
            )
            original_before = cur.fetchone()

        _apply_correction(connection, "PAPER", "LOCAL", "local-paper")
        _apply_correction(connection, "PAPER", "LOCAL", "local-paper")

        assert PORTABILITY.schema_fingerprint(connection) == fingerprint_before
        with connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM schema_migration_ledger_v1 WHERE migration_id=%s",
                (PORTABILITY.PORTABILITY_MIGRATION_ID,),
            )
            assert cur.fetchone() == original_before
            cur.execute(
                """
                SELECT count(*),min(original_git_sha),min(corrected_git_sha),
                       min(correction_reason),min(original_environment),
                       min(original_deployment_id),min(runtime_deployment_id),
                       min(correction_git_sha)
                  FROM migration_provenance_correction_v1
                 WHERE correction_contract=
                   'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
                """
            )
            assert cur.fetchone() == (
                1, BAD_SHA, CANONICAL_SHA,
                "NON_CANONICAL_GIT_SHA_PROVENANCE_CORRECTION",
                "PAPER", "LOCAL", "local-paper", SOURCE_SHA,
            )
            cur.execute(
                """
                SELECT coalesce(c.corrected_git_sha,l.git_sha)
                  FROM schema_migration_ledger_v1 l
                  LEFT JOIN migration_provenance_correction_v1 c
                    ON c.corrected_ledger_id=l.ledger_id
                   AND c.correction_contract=
                    'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'
                 WHERE l.migration_id=%s
                """,
                (PORTABILITY.PORTABILITY_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == CANONICAL_SHA
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (CORRECTION_ID,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM entry_opportunity_evidence_v1 "
                "WHERE snapshot_id=%s",
                (snapshot_id,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        connection.close()


def test_vps_without_bad_row_is_safe_noop(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "git_correction_vps")
    try:
        PORTABILITY.install(
            connection,
            environment="PAPER",
            deployment_id="VPS",
            runtime_deployment_id="vps-paper",
            git_sha=CANONICAL_SHA,
        )
        fingerprint_before = PORTABILITY.schema_fingerprint(connection)
        _apply_correction(connection, "PAPER", "VPS", "vps-paper")
        _apply_correction(connection, "PAPER", "VPS", "vps-paper")
        assert PORTABILITY.schema_fingerprint(connection) == fingerprint_before
        with connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM migration_provenance_correction_v1 "
                "WHERE correction_contract="
                "'ENTRY_OPPORTUNITY_EVIDENCE_V1_1_GIT_SHA_PROVENANCE_CORRECTION'"
            )
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (CORRECTION_ID,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        connection.close()


def test_live_target_fails_before_mutation(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "git_correction_live")
    try:
        _seed_bad_local(connection)
        with pytest.raises(Exception):
            _apply_correction(connection, "LIVE", "LOCAL", "local-live")
        connection.rollback()
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('migration_provenance_correction_v1')")
            assert cur.fetchone()[0] is None
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (CORRECTION_ID,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        connection.close()
