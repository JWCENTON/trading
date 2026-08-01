from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from common.totp_schema import require_totp_schema


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/20260724_runtime_ddl_schema_parity_v1.sql"
MANIFEST = ROOT / (
    "db/migrations/20260724_runtime_ddl_schema_parity_v1_manifest.json"
)


def _database(disposable_postgres_v16, purpose):
    database = f"waltrade_baseline_test_totp_schema_{purpose}"
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE public.users(id SERIAL PRIMARY KEY)")
    conn.commit()
    return conn


def _public_relations(cur):
    cur.execute(
        """
        SELECT relname,relkind
        FROM pg_class rel
        JOIN pg_namespace ns ON ns.oid=rel.relnamespace
        WHERE ns.nspname='public'
        ORDER BY relname,relkind
        """
    )
    return cur.fetchall()


def test_manifest_checksum_matches_existing_canonical_migration():
    manifest = json.loads(MANIFEST.read_text())
    checksum = hashlib.sha256(MIGRATION.read_bytes()).hexdigest()

    assert manifest["migration_sha256"] == checksum
    assert manifest["runtime_policy"] == {
        "schema_installation": "explicit_migration_only",
        "startup_schema_mutation": False,
        "startup_verification": "read_only_fail_closed",
        "backfill": False,
        "data_repair": False,
    }


def test_canonical_migration_and_runtime_verifier_are_in_parity(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "parity")
    try:
        with conn.cursor() as cur:
            cur.execute(MIGRATION.read_text())
        with conn.cursor() as cur:
            before = _public_relations(cur)
            cur.execute("SELECT txid_current_if_assigned()")
            assert cur.fetchone()[0] is None

            report = require_totp_schema(cur)

            cur.execute("SELECT txid_current_if_assigned()")
            assert cur.fetchone()[0] is None
            after = _public_relations(cur)
        assert report.ready
        assert before == after
    finally:
        conn.close()


def test_missing_schema_fails_closed_without_creating_objects(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "missing")
    try:
        with conn.cursor() as cur:
            before = _public_relations(cur)
            with pytest.raises(RuntimeError, match="TOTP_SCHEMA_NOT_READY") as exc:
                require_totp_schema(cur)
            after = _public_relations(cur)
        assert "table:public.user_totp" in str(exc.value)
        assert "table:public.user_recovery_codes" in str(exc.value)
        assert before == after
    finally:
        conn.close()


def test_missing_or_mismatched_index_is_reported_without_repair(
    disposable_postgres_v16,
):
    conn = _database(disposable_postgres_v16, "index_mismatch")
    try:
        with conn.cursor() as cur:
            cur.execute(MIGRATION.read_text())
            cur.execute("DROP INDEX public.ix_user_recovery_codes_user_active")
            cur.execute(
                """
                CREATE INDEX ix_user_recovery_codes_user_active
                ON public.user_recovery_codes(code_hash)
                """
            )
        conn.commit()

        with conn.cursor() as cur:
            before = _public_relations(cur)
            with pytest.raises(RuntimeError, match="mismatched_objects") as exc:
                require_totp_schema(cur)
            after = _public_relations(cur)
        assert "index:public.ix_user_recovery_codes_user_active" in str(exc.value)
        assert before == after
    finally:
        conn.close()
