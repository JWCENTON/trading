from __future__ import annotations

import uuid

import pytest

from scripts.learning_quarantine_contract_fingerprint_v1 import (
    DEFAULT_MANIFEST,
    build_artifact,
    collect_with_cursor,
    diff_artifacts,
    load_manifest,
    normalize_sql,
    validate_manifest,
)
from tests.postgres.test_financial_truth_learning_quarantine_contract_v1_pg import (
    MIGRATIONS,
    SCHEMA,
)


MANIFEST = load_manifest(DEFAULT_MANIFEST)
BASE_CONTRACT_COMPAT = """
ALTER TABLE learning_outcome_exclusion_v1
  RENAME CONSTRAINT learning_outcome_exclusion_v1_position_id_fkey
  TO fk_learning_outcome_exclusion_v1_position;
CREATE INDEX ix_learning_outcome_exclusion_v1_position
  ON learning_outcome_exclusion_v1(position_id);
"""


def _database(disposable, suffix: str, *, before: str = "", after: str = ""):
    name = f"waltrade_baseline_test_lqfp_{suffix}_{uuid.uuid4().hex[:6]}"
    disposable.create_database(name)
    connection = disposable.connect(name)
    with connection.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute(BASE_CONTRACT_COMPAT)
        if before:
            cur.execute(before)
        for migration in MIGRATIONS:
            cur.execute(migration)
        if after:
            cur.execute(after)
    connection.commit()
    return connection


def _artifact(connection):
    with connection.cursor() as cur:
        major, records, missing = collect_with_cursor(cur, MANIFEST)
    assert missing == []
    return build_artifact(MANIFEST, major, records)


def _artifact_allow_missing(connection):
    with connection.cursor() as cur:
        major, records, missing = collect_with_cursor(cur, MANIFEST)
    return build_artifact(MANIFEST, major, records), missing


def test_01_same_schema_twice_has_identical_hash(disposable_postgres_v16):
    left = _database(disposable_postgres_v16, "same_a")
    right = _database(disposable_postgres_v16, "same_b")
    try:
        assert _artifact(left) == _artifact(right)
    finally:
        left.close()
        right.close()


def test_02_different_oids_and_creation_order_have_identical_hash(
    disposable_postgres_v16,
):
    noise = "CREATE TABLE unrelated_oid_noise(id BIGSERIAL PRIMARY KEY)"
    left = _database(disposable_postgres_v16, "oid_a", before=noise)
    right = _database(disposable_postgres_v16, "oid_b", after=noise)
    try:
        assert _artifact(left)["canonical_sha256"] == _artifact(right)[
            "canonical_sha256"
        ]
    finally:
        left.close()
        right.close()


def test_03_owner_and_acl_are_excluded(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "owner_acl")
    try:
        baseline = _artifact(connection)
        role = "lqfp_owner_" + uuid.uuid4().hex[:10]
        with connection.cursor() as cur:
            cur.execute(f'CREATE ROLE "{role}"')
            cur.execute(
                f'ALTER TABLE learning_outcome_exclusion_resolution_v1 '
                f'OWNER TO "{role}"'
            )
            cur.execute(
                "GRANT SELECT ON learning_outcome_exclusion_v1 TO PUBLIC"
            )
        assert _artifact(connection)["canonical_sha256"] == baseline[
            "canonical_sha256"
        ]
    finally:
        connection.rollback()
        connection.close()


def test_04_row_data_are_excluded(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "row_data")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,status,exit_time,net_pnl_usdc) "
                "VALUES (501,'CLOSED',now(),1)"
            )
            cur.execute(
                """
                INSERT INTO learning_outcome_exclusion_v1(
                  environment,deployment_id,position_id,exclusion_reason,
                  source_type,semantic_fingerprint_v2,created_by,git_sha
                ) VALUES ('PAPER','test',501,'LEGACY_REPAIR',
                  'LEGACY_POSITION_REPAIR',repeat('a',64),'TEST',repeat('b',40))
                """
            )
        assert _artifact(connection)["canonical_sha256"] == baseline[
            "canonical_sha256"
        ]
    finally:
        connection.rollback()
        connection.close()


def test_05_check_constraint_change_changes_hash(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "check_change")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute(
                "ALTER TABLE learning_outcome_exclusion_resolution_v1 "
                "DROP CONSTRAINT ck_learning_outcome_exclusion_resolution_v1_contract"
            )
            cur.execute(
                """
                ALTER TABLE learning_outcome_exclusion_resolution_v1
                ADD CONSTRAINT ck_learning_outcome_exclusion_resolution_v1_contract
                CHECK (resolution_action IN ('REVOKE','TEST_ONLY'))
                """
            )
        changed = _artifact(connection)
        different, lines = diff_artifacts(baseline, changed)
        assert different
        assert any("changed constraint" in line for line in lines)
    finally:
        connection.rollback()
        connection.close()


def test_06_function_body_change_changes_hash(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "function_change")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION learning_outcome_is_eligible_v1(
                    p_position_id BIGINT
                )
                RETURNS BOOLEAN LANGUAGE SQL STABLE STRICT
                AS $$ SELECT false $$
                """
            )
        changed = _artifact(connection)
        different, lines = diff_artifacts(baseline, changed)
        assert different
        assert any("changed function" in line for line in lines)
    finally:
        connection.rollback()
        connection.close()


def test_07_view_predicate_change_changes_hash(disposable_postgres_v16):
    connection = _database(disposable_postgres_v16, "view_change")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE VIEW v_learning_eligible_closed_positions_v1 AS
                SELECT position.* FROM positions position WHERE false
                """
            )
        changed = _artifact(connection)
        different, lines = diff_artifacts(baseline, changed)
        assert different
        assert any("changed view" in line for line in lines)
    finally:
        connection.rollback()
        connection.close()


def test_08_missing_index_changes_hash_and_has_exact_diff(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "missing_index")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute("DROP INDEX ix_learning_outcome_exclusion_v1_position")
        changed, missing = _artifact_allow_missing(connection)
        assert missing == [(
            "index", "public", "ix_learning_outcome_exclusion_v1_position",
            "learning_outcome_exclusion_v1",
        )]
        different, lines = diff_artifacts(baseline, changed)
        assert different
        assert (
            "missing object: index | public | "
            "ix_learning_outcome_exclusion_v1_position | "
            "learning_outcome_exclusion_v1"
        ) in lines
    finally:
        connection.rollback()
        connection.close()


def test_09_unrelated_learning_object_does_not_change_hash(
    disposable_postgres_v16,
):
    connection = _database(disposable_postgres_v16, "unrelated")
    try:
        baseline = _artifact(connection)
        with connection.cursor() as cur:
            cur.execute(
                "CREATE TABLE learning_unrelated_noise_v1(id BIGINT PRIMARY KEY)"
            )
        assert _artifact(connection)["canonical_sha256"] == baseline[
            "canonical_sha256"
        ]
    finally:
        connection.rollback()
        connection.close()


def test_10_manifest_is_explicit_and_contains_no_wildcards():
    validate_manifest(MANIFEST)
    assert len(MANIFEST["objects"]) == 53
    for entry in MANIFEST["objects"]:
        identity_value = "|".join(
            str(entry[field]) for field in (
                "object_type", "schema", "object_name", "subidentity"
            )
        )
        assert not any(token in identity_value for token in ("*", "%", "?"))


def test_sql_normalization_is_explicit_and_literal_safe():
    assert normalize_sql("SELECT  *\r\n  FROM public.x") == "SELECT * FROM public.x"
    assert normalize_sql("SELECT 'a  b', \"x  y\"  FROM x") == (
        "SELECT 'a  b', \"x  y\" FROM x"
    )
    assert normalize_sql("SELECT 1 /* ignored */ +  2 -- ignored\n") == (
        "SELECT 1 + 2"
    )
    assert normalize_sql("AS $tag$ BEGIN  RETURN  true; END $tag$") == (
        "AS $body$BEGIN RETURN true; END$body$"
    )
