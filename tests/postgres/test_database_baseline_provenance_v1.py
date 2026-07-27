from __future__ import annotations

from common.schema_provenance import (
    hierarchical_entry,
    hierarchical_readiness,
    identity_string,
)
from scripts.waltrade_schema_baseline_v1 import inventory


def _catalog_snapshot(conn):
    return [
        row for row in inventory(conn)
        if row["object_name"] in {
            "audit_input",
            "audit_input_name_idx",
            "audit_read_only",
            "digest",
            "pgcrypto",
        }
    ]


def test_disposable_catalog_collection_is_read_only_and_deterministic(
    disposable_postgres_v16,
):
    pg = disposable_postgres_v16
    database = "waltrade_baseline_test_catalog"
    pg.create_database(database)
    conn = pg.connect(database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE public.audit_input(
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE INDEX audit_input_name_idx ON public.audit_input(name);
                CREATE FUNCTION public.audit_read_only()
                RETURNS bigint LANGUAGE sql
                AS 'SELECT count(*) FROM public.audit_input';
                CREATE EXTENSION IF NOT EXISTS pgcrypto;
                """
            )
        conn.commit()

        first = _catalog_snapshot(conn)
        second = _catalog_snapshot(conn)
        assert first == second

        by_name = {row["object_name"]: row for row in first}
        assert by_name["audit_input"]["management"] == "APPLICATION"
        assert by_name["audit_input_name_idx"]["parent_relation"] == "audit_input"
        assert by_name["audit_read_only"]["object_type"] == "FUNCTION"
        assert by_name["pgcrypto"]["management"] == "EXTENSION"
        assert by_name["digest"]["management"] == "EXTENSION"
    finally:
        conn.close()


def test_disposable_catalog_rows_support_blocked_readiness_split(
    disposable_postgres_v16,
):
    pg = disposable_postgres_v16
    conn = pg.connect("waltrade_baseline_test_catalog")
    try:
        rows = _catalog_snapshot(conn)
        hierarchical = []
        for row in rows:
            row = {
                **row,
                "applicability": "COMMON",
                "source_file": "disposable fixture",
                "risk_class": "P1",
                "provenance_status": "RUNTIME_OBSERVED_PENDING_ADOPTION",
            }
            parent_identity = ""
            if row["object_type"] == "INDEX":
                parent = next(
                    candidate for candidate in rows
                    if candidate["object_type"] == "TABLE"
                    and candidate["object_name"] == row["parent_relation"]
                )
                parent_identity = identity_string(parent)
            hierarchical.append(
                hierarchical_entry(
                    row,
                    sequence_root=parent_identity or None,
                    canonical_blocker=row["object_name"] == "audit_read_only",
                )
            )
        identities = {row["identity"] for row in hierarchical}
        for row in hierarchical:
            if row["object_type"] == "INDEX":
                row["root_identity"] = next(
                    identity for identity in identities
                    if identity.startswith("TABLE:public:audit_input:")
                )
        state = hierarchical_readiness(hierarchical)
        assert state["catalog_coverage_ready"] is True
        assert state["adoption_ready"] is False
        assert state["blocked"]
    finally:
        conn.close()
