from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from common.legacy_recovery_schema import (
    MANIFEST_CHECKSUM as LEGACY_MANIFEST_CHECKSUM,
    MIGRATION_ID as LEGACY_MIGRATION_ID,
    SCHEMA_VERSION as LEGACY_SCHEMA_VERSION,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / (
    "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
)
MANIFEST = ROOT / (
    "db/migrations/20260801_schema_migration_ledger_v1_baseline_manifest.json"
)
LEGACY_RECOVERY = ROOT / (
    "db/migrations/20260730_legacy_position_fill_recovery_v1.sql"
)

EXPECTED_COLUMNS = [
    (1, "ledger_id", "bigint", True),
    (2, "migration_id", "text", True),
    (3, "checksum_sha256", "text", True),
    (4, "applied_at", "timestamp with time zone", True),
    (5, "environment", "text", True),
    (6, "deployment_id", "text", True),
    (7, "database_name", "text", True),
    (8, "applied_by", "text", True),
    (9, "status", "text", True),
    (10, "success", "boolean", True),
    (11, "execution_duration_ms", "bigint", True),
    (12, "git_sha", "text", True),
    (13, "error_summary", "text", False),
    (14, "schema_baseline_version", "text", True),
]

EXPECTED_CONSTRAINTS = {
    "schema_migration_ledger_v1_checksum_sha256_check",
    "schema_migration_ledger_v1_environment_check",
    "schema_migration_ledger_v1_execution_duration_ms_check",
    "schema_migration_ledger_v1_pkey",
    "schema_migration_ledger_v1_status_check",
    "schema_migration_ledger_v1_status_success_ck",
}


def _database(disposable_postgres_v16, purpose: str):
    database = f"waltrade_baseline_test_ledger_{purpose}"
    disposable_postgres_v16.create_database(database)
    return disposable_postgres_v16.connect(database)


def _apply(connection, sql: str) -> None:
    with connection.cursor() as cur:
        cur.execute(sql)
    connection.commit()


def _signature(connection):
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT attribute.attnum,attribute.attname,
                   format_type(attribute.atttypid,attribute.atttypmod),
                   attribute.attnotnull,
                   pg_get_expr(definition.adbin,definition.adrelid)
            FROM pg_attribute attribute
            LEFT JOIN pg_attrdef definition
              ON definition.adrelid=attribute.attrelid
             AND definition.adnum=attribute.attnum
            WHERE attribute.attrelid=
                  'public.schema_migration_ledger_v1'::regclass
              AND attribute.attnum>0 AND NOT attribute.attisdropped
            ORDER BY attribute.attnum
            """
        )
        columns = cur.fetchall()
        cur.execute(
            """
            SELECT conname,contype,pg_get_constraintdef(oid,true),convalidated
            FROM pg_constraint
            WHERE conrelid='public.schema_migration_ledger_v1'::regclass
            ORDER BY conname
            """
        )
        constraints = cur.fetchall()
        cur.execute(
            """
            SELECT indexname,indexdef
            FROM pg_indexes
            WHERE schemaname='public'
              AND tablename='schema_migration_ledger_v1'
            ORDER BY indexname
            """
        )
        indexes = cur.fetchall()
        cur.execute(
            """
            SELECT obj_description(
                'public.schema_migration_ledger_v1'::regclass,'pg_class'
            ),count(*)
            FROM public.schema_migration_ledger_v1
            """
        )
        metadata = cur.fetchone()
    return columns, constraints, indexes, metadata


def _assert_contract(connection) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT attribute.attnum,attribute.attname,
                   format_type(attribute.atttypid,attribute.atttypmod),
                   attribute.attnotnull
            FROM pg_attribute attribute
            WHERE attribute.attrelid=
                  'public.schema_migration_ledger_v1'::regclass
              AND attribute.attnum>0 AND NOT attribute.attisdropped
            ORDER BY attribute.attnum
            """
        )
        assert cur.fetchall() == EXPECTED_COLUMNS
        cur.execute(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid='public.schema_migration_ledger_v1'::regclass
            """
        )
        assert {row[0] for row in cur.fetchall()} == EXPECTED_CONSTRAINTS
        cur.execute(
            """
            SELECT pg_get_serial_sequence(
                'public.schema_migration_ledger_v1','ledger_id'
            ),to_regclass('public.ix_schema_migration_ledger_v1_lookup'),
              count(*)
            FROM public.schema_migration_ledger_v1
            """
        )
        assert cur.fetchone() == (
            "public.schema_migration_ledger_v1_ledger_id_seq",
            "ix_schema_migration_ledger_v1_lookup",
            0,
        )


def test_manifest_checksum_and_dependency_order():
    manifest = json.loads(MANIFEST.read_text())
    assert manifest["migration_sha256"] == hashlib.sha256(
        MIGRATION.read_bytes()
    ).hexdigest()
    assert manifest["required_before"] == [LEGACY_RECOVERY.name]
    assert manifest["data_policy"] == {
        "self_registration": False,
        "backfill": False,
        "trading_data_changes": False,
        "existing_ledger_rows_preserved": True,
    }


def test_fresh_database_and_second_run_have_zero_schema_drift(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "fresh_idempotent")
    try:
        _apply(connection, MIGRATION.read_text())
        _assert_contract(connection)
        first = _signature(connection)

        _apply(connection, MIGRATION.read_text())
        _assert_contract(connection)
        second = _signature(connection)

        assert second == first
    finally:
        connection.close()


def test_existing_compatible_ledger_preserves_existing_record(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "compatible_rows")
    try:
        _apply(connection, MIGRATION.read_text())
        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO schema_migration_ledger_v1(
                  migration_id,checksum_sha256,environment,deployment_id,
                  database_name,applied_by,status,success,
                  execution_duration_ms,git_sha,error_summary,
                  schema_baseline_version
                ) VALUES (
                  'technical.sql',%s,'PAPER','TEST',current_database(),
                  'test','APPLIED',true,7,%s,NULL,'TEST_V1'
                )
                RETURNING *
                """,
                ("a" * 64, "b" * 40),
            )
            before = cur.fetchone()
        connection.commit()

        _apply(connection, MIGRATION.read_text())
        with connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM schema_migration_ledger_v1 "
                "WHERE migration_id='technical.sql'"
            )
            assert cur.fetchone() == before
            cur.execute("SELECT count(*) FROM schema_migration_ledger_v1")
            assert cur.fetchone()[0] == 1
    finally:
        connection.close()


@pytest.mark.parametrize(
    "case_name,mutation,expected_error",
    [
        (
            "wrong_type",
            "ALTER TABLE schema_migration_ledger_v1 "
            "ALTER COLUMN migration_id TYPE BIGINT USING 0",
            "COLUMN_CONTRACT_MISMATCH",
        ),
        (
            "missing_column",
            "ALTER TABLE schema_migration_ledger_v1 "
            "DROP COLUMN error_summary",
            "COLUMN_CONTRACT_MISMATCH",
        ),
        (
            "wrong_checksum",
            "ALTER TABLE schema_migration_ledger_v1 DROP CONSTRAINT "
            "schema_migration_ledger_v1_checksum_sha256_check; "
            "ALTER TABLE schema_migration_ledger_v1 ADD CONSTRAINT "
            "schema_migration_ledger_v1_checksum_sha256_check "
            "CHECK(checksum_sha256 <> '')",
            "CONSTRAINT_CONTRACT_MISMATCH",
        ),
        (
            "wrong_status",
            "ALTER TABLE schema_migration_ledger_v1 DROP CONSTRAINT "
            "schema_migration_ledger_v1_status_check; "
            "ALTER TABLE schema_migration_ledger_v1 ADD CONSTRAINT "
            "schema_migration_ledger_v1_status_check "
            "CHECK(status IN ('APPLIED'))",
            "CONSTRAINT_CONTRACT_MISMATCH",
        ),
        (
            "incompatible_unique",
            "ALTER TABLE schema_migration_ledger_v1 ADD CONSTRAINT "
            "schema_migration_ledger_v1_migration_id_key "
            "UNIQUE(migration_id)",
            "CONSTRAINT_CONTRACT_MISMATCH",
        ),
    ],
)
def test_incompatible_existing_ledger_fails_closed_and_rolls_back(
    disposable_postgres_v16, case_name, mutation, expected_error,
):
    connection = _database(disposable_postgres_v16, case_name)
    try:
        _apply(connection, MIGRATION.read_text())
        _apply(connection, mutation)
        before = _signature(connection)

        with pytest.raises(Exception, match=expected_error):
            with connection.cursor() as cur:
                cur.execute(MIGRATION.read_text())
        connection.rollback()

        assert _signature(connection) == before
    finally:
        connection.close()


def _install_legacy_prerequisites(connection) -> None:
    _apply(
        connection,
        """
        CREATE TABLE positions(id BIGINT PRIMARY KEY);
        CREATE TABLE binance_order_fills(id BIGINT PRIMARY KEY);
        CREATE TABLE exchange_fill_ingestion_state_v2(
          ingestion_id BIGSERIAL PRIMARY KEY,
          source TEXT NOT NULL,
          account_identity_key TEXT NOT NULL,
          symbol TEXT NOT NULL,
          trade_id TEXT NOT NULL,
          order_id TEXT NOT NULL,
          side TEXT NOT NULL,
          source_fingerprint TEXT NOT NULL,
          applied_fingerprint TEXT,
          applied_at TIMESTAMPTZ,
          application_status TEXT NOT NULL,
          correction_revision INTEGER DEFAULT 0,
          authoritative_payload JSONB NOT NULL,
          last_decision TEXT NOT NULL,
          CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
            CHECK(application_status IN (
              'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED',
              'AMBIGUOUS','REJECTED'
            ))
        );
        """,
    )


def test_baseline_then_legacy_recovery_dependency_chain_is_idempotent(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "dependency_chain")
    try:
        _install_legacy_prerequisites(connection)
        _apply(connection, MIGRATION.read_text())
        _apply(connection, LEGACY_RECOVERY.read_text())

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT to_regclass('public.legacy_repair_audit_v1'),
                       to_regclass('public.legacy_repair_provenance_v1'),
                       EXISTS (
                         SELECT 1 FROM information_schema.columns
                         WHERE table_schema='public'
                           AND table_name='exchange_fill_ingestion_state_v2'
                           AND column_name='local_fill_id'
                       )
                """
            )
            assert cur.fetchone() == (
                "legacy_repair_audit_v1",
                "legacy_repair_provenance_v1",
                True,
            )
            cur.execute(
                """
                SELECT count(*),min(checksum_sha256),
                       min(schema_baseline_version),bool_and(success)
                FROM schema_migration_ledger_v1
                WHERE migration_id=%s
                """,
                (LEGACY_MIGRATION_ID,),
            )
            assert cur.fetchone() == (
                1,
                LEGACY_MANIFEST_CHECKSUM,
                LEGACY_SCHEMA_VERSION,
                True,
            )
        first = _signature(connection)

        _apply(connection, MIGRATION.read_text())
        _apply(connection, LEGACY_RECOVERY.read_text())

        assert _signature(connection) == first
        with connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (LEGACY_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        connection.close()
