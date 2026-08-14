#!/usr/bin/env python3
"""Install Entry Opportunity Evidence V1 with explicit PAPER provenance.

The unchanged V1 migration remains the sole schema source. On a fresh target
this tool executes its schema section and writes the original migration ledger
row with the explicit target identity before adopting portability V1.1.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
from typing import Any, Mapping

import psycopg2


ROOT = Path(__file__).resolve().parents[1]
ORIGINAL_MIGRATION = (
    ROOT / "db/migrations/20260814_entry_opportunity_evidence_v1.sql"
)
PORTABILITY_MIGRATION = (
    ROOT
    / "db/migrations/20260814_entry_opportunity_evidence_v1_1_portability.sql"
)
ORIGINAL_MIGRATION_ID = ORIGINAL_MIGRATION.name
PORTABILITY_MIGRATION_ID = PORTABILITY_MIGRATION.name
ORIGINAL_CHECKSUM = (
    "ed6f0bd1f0ac22a0e540b960319a117e3850a858907d85b300e613677c28576d"
)
TARGETS = {
    ("PAPER", "LOCAL", "local-paper"),
    ("PAPER", "VPS", "vps-paper"),
}
_LEDGER_MARKER = "INSERT INTO public.schema_migration_ledger_v1("


def validate_target_identity(
    environment: str,
    deployment_id: str,
    runtime_deployment_id: str,
) -> tuple[str, str, str]:
    target = (
        str(environment).strip(),
        str(deployment_id).strip(),
        str(runtime_deployment_id).strip(),
    )
    if target not in TARGETS:
        raise ValueError("ENTRY_OPPORTUNITY_PORTABILITY_TARGET_NOT_ALLOWED")
    return target


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def original_schema_sql() -> str:
    text = ORIGINAL_MIGRATION.read_text()
    if _checksum(ORIGINAL_MIGRATION) != ORIGINAL_CHECKSUM:
        raise RuntimeError("ENTRY_OPPORTUNITY_ORIGINAL_V1_CHECKSUM_MISMATCH")
    if text.count(_LEDGER_MARKER) != 1:
        raise RuntimeError("ENTRY_OPPORTUNITY_ORIGINAL_V1_LEDGER_BOUNDARY_INVALID")
    before_ledger = text.split(_LEDGER_MARKER, 1)[0]
    return re.sub(r"\A\s*BEGIN;\s*", "", before_ledger, count=1)


def _transaction_body(path: Path) -> str:
    text = path.read_text()
    before, marker, after = text.partition("BEGIN;")
    if not marker or after.count("COMMIT;") != 1:
        raise RuntimeError("ENTRY_OPPORTUNITY_PORTABILITY_BOUNDARY_INVALID")
    return before + after.rsplit("COMMIT;", 1)[0]


def install(
    connection: Any,
    *,
    environment: str,
    deployment_id: str,
    runtime_deployment_id: str,
    git_sha: str,
) -> dict[str, Any]:
    environment, deployment_id, runtime_deployment_id = validate_target_identity(
        environment, deployment_id, runtime_deployment_id,
    )
    git_sha = str(git_sha).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", git_sha):
        raise ValueError("ENTRY_OPPORTUNITY_GIT_SHA_INVALID")

    portability_checksum = _checksum(PORTABILITY_MIGRATION)
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("ENTRY_OPPORTUNITY_EVIDENCE_V1_PORTABLE_INSTALL",),
            )
            for setting, value in (
                ("waltrade.target_environment", environment),
                ("waltrade.target_deployment_id", deployment_id),
                ("waltrade.target_runtime_deployment_id", runtime_deployment_id),
                ("waltrade.git_sha", git_sha),
            ):
                cur.execute("SELECT set_config(%s,%s,true)", (setting, value))

            cur.execute(
                """
                SELECT checksum_sha256,environment,deployment_id
                  FROM public.schema_migration_ledger_v1
                 WHERE migration_id=%s
                 ORDER BY ledger_id
                """,
                (ORIGINAL_MIGRATION_ID,),
            )
            original_rows = cur.fetchall()
            fresh_install = not original_rows
            if len(original_rows) > 1:
                raise RuntimeError("ENTRY_OPPORTUNITY_ORIGINAL_V1_LEDGER_DUPLICATE")

            if fresh_install:
                cur.execute(original_schema_sql())
                cur.execute(
                    """
                    INSERT INTO public.schema_migration_ledger_v1(
                      migration_id,checksum_sha256,environment,deployment_id,
                      database_name,applied_by,status,success,
                      execution_duration_ms,git_sha,schema_baseline_version
                    ) VALUES(%s,%s,%s,%s,current_database(),
                             'operator-migration','APPLIED',TRUE,0,%s,
                             'ENTRY_OPPORTUNITY_EVIDENCE_V1')
                    """,
                    (
                        ORIGINAL_MIGRATION_ID, ORIGINAL_CHECKSUM,
                        environment, deployment_id, git_sha,
                    ),
                )
            elif original_rows[0] != (
                ORIGINAL_CHECKSUM, environment, deployment_id,
            ):
                raise RuntimeError(
                    "ENTRY_OPPORTUNITY_ORIGINAL_V1_PROVENANCE_INVALID"
                )

            cur.execute(
                "SELECT set_config('waltrade.migration_checksum',%s,true)",
                (portability_checksum,),
            )
            cur.execute(_transaction_body(PORTABILITY_MIGRATION))
            cur.execute(
                """
                SELECT count(*)
                  FROM public.schema_migration_ledger_v1
                 WHERE migration_id=%s
                   AND environment=%s AND deployment_id=%s
                """,
                (PORTABILITY_MIGRATION_ID, environment, deployment_id),
            )
            if cur.fetchone()[0] != 1:
                raise RuntimeError("ENTRY_OPPORTUNITY_PORTABILITY_POSTCHECK_FAILED")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {
        "fresh_install": fresh_install,
        "environment": environment,
        "deployment_id": deployment_id,
        "runtime_deployment_id": runtime_deployment_id,
        "original_checksum": ORIGINAL_CHECKSUM,
        "portability_checksum": portability_checksum,
    }


def schema_fingerprint(connection: Any) -> str:
    """Fingerprint only the functional V1 schema, never deployment ledger data."""
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH target_relations(name) AS (VALUES
              ('entry_opportunity_evidence_v1'),
              ('entry_opportunity_evidence_audit_v1'),
              ('decision_registry_v1'),('simulated_orders'),
              ('simulated_execution_fills_v1'),('positions'),
              ('decision_replay_v1'),('learning_feature_warehouse_v1')
            )
            SELECT kind,identity,definition FROM (
              SELECT 'column' AS kind,c.table_name||'.'||c.column_name AS identity,
                     c.data_type||':'||c.is_nullable||':'||coalesce(c.column_default,'') AS definition
                FROM information_schema.columns c JOIN target_relations t
                  ON t.name=c.table_name WHERE c.table_schema='public'
              UNION ALL
              SELECT 'constraint',con.conrelid::regclass::text||'.'||con.conname,
                     pg_get_constraintdef(con.oid,true)
                FROM pg_constraint con JOIN target_relations t
                  ON con.conrelid=('public.'||t.name)::regclass
              UNION ALL
              SELECT 'index',i.tablename||'.'||i.indexname,i.indexdef
                FROM pg_indexes i JOIN target_relations t ON t.name=i.tablename
               WHERE i.schemaname='public'
              UNION ALL
              SELECT 'trigger',tr.tgrelid::regclass::text||'.'||tr.tgname,
                     pg_get_triggerdef(tr.oid,true)
                FROM pg_trigger tr JOIN target_relations t
                  ON tr.tgrelid=('public.'||t.name)::regclass
               WHERE NOT tr.tgisinternal
              UNION ALL
              SELECT 'function',p.proname,pg_get_functiondef(p.oid)
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
               WHERE n.nspname='public' AND p.proname IN (
                 'guard_entry_opportunity_snapshot_immutable_v1',
                 'guard_entry_opportunity_reference_v1',
                 'propagate_entry_opportunity_reference_v1'
               )
              UNION ALL
              SELECT 'view','v_entry_opportunity_outcome_labels_v1',
                     pg_get_viewdef('public.v_entry_opportunity_outcome_labels_v1'::regclass,true)
            ) contract
            ORDER BY kind,identity,definition
            """
        )
        payload = "\n".join("|".join(map(str, row)) for row in cur.fetchall())
    return hashlib.sha256(payload.encode()).hexdigest()


def _connection_from_env(environ: Mapping[str, str]):
    return psycopg2.connect(
        host=environ.get("DB_HOST") or environ.get("PGHOST") or "localhost",
        port=int(environ.get("DB_PORT") or environ.get("PGPORT") or "5432"),
        dbname=environ.get("DB_NAME") or environ.get("PGDATABASE"),
        user=environ.get("DB_USER") or environ.get("PGUSER"),
        password=environ.get("DB_PASSWORD") or environ.get("PGPASSWORD"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-environment", required=True)
    parser.add_argument("--target-deployment-id", required=True)
    parser.add_argument("--target-runtime-deployment-id", required=True)
    parser.add_argument("--git-sha", required=True)
    args = parser.parse_args()
    connection = _connection_from_env(os.environ)
    try:
        result = install(
            connection,
            environment=args.target_environment,
            deployment_id=args.target_deployment_id,
            runtime_deployment_id=args.target_runtime_deployment_id,
            git_sha=args.git_sha,
        )
        print("ENTRY_OPPORTUNITY_PORTABLE_INSTALL_PASS", result)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
