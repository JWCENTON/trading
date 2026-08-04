from __future__ import annotations

from pathlib import Path

import pytest
from psycopg2 import errors


ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / "db" / "migrations" / name).read_text()
    for name in (
        "20260804_learning_quarantine_vocabulary_v1.sql",
        "20260804_learning_quarantine_resolution_v1.sql",
        "20260804_learning_ft_eligibility_v1.sql",
        "20260804_learning_canonical_universe_ft_quarantine_v1.sql",
    )
)

GIT_SHA = "c2cef02cbff0c34cef97886f86458ee30020e229"

SCHEMA = r"""
CREATE TABLE positions(
    id BIGINT PRIMARY KEY,
    status TEXT NOT NULL,
    exit_time TIMESTAMPTZ,
    net_pnl_usdc NUMERIC
);

CREATE TABLE canonical_financial_truth_v1(
    position_id BIGINT PRIMARY KEY REFERENCES positions(id),
    financial_truth_status TEXT NOT NULL CHECK (
        financial_truth_status IN ('UNKNOWN','INCOMPLETE','COMPLETE','FAILED')
    )
);

CREATE TABLE learning_outcome_exclusion_v1(
    exclusion_id BIGSERIAL PRIMARY KEY,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE RESTRICT,
    exclusion_reason TEXT NOT NULL,
    source_type TEXT NOT NULL,
    semantic_fingerprint_v2 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by TEXT NOT NULL,
    git_sha TEXT NOT NULL,
    CONSTRAINT ux_learning_outcome_exclusion_v1_identity
        UNIQUE(environment,deployment_id,position_id),
    CONSTRAINT ck_learning_outcome_exclusion_v1_contract CHECK (
        environment IN ('PAPER','LIVE')
        AND btrim(deployment_id) <> ''
        AND exclusion_reason='LEGACY_REPAIR'
        AND source_type='LEGACY_POSITION_REPAIR'
        AND semantic_fingerprint_v2 ~ '^[0-9a-f]{64}$'
        AND btrim(created_by) <> ''
        AND git_sha ~ '^[0-9a-f]{40}$'
    )
);

CREATE FUNCTION prevent_legacy_recovery_history_mutation_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'APPEND_ONLY_HISTORY';
END
$$;

CREATE TRIGGER trg_learning_outcome_exclusion_v1_append_only
BEFORE UPDATE OR DELETE ON learning_outcome_exclusion_v1
FOR EACH ROW EXECUTE FUNCTION prevent_legacy_recovery_history_mutation_v1();

CREATE TABLE schema_migration_ledger_v1(
    ledger_id BIGSERIAL PRIMARY KEY,
    migration_id TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    applied_by TEXT NOT NULL,
    status TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    execution_duration_ms BIGINT NOT NULL,
    git_sha TEXT NOT NULL,
    error_summary TEXT,
    schema_baseline_version TEXT NOT NULL
);

CREATE TABLE exit_trace_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE exit_trace_v2(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE exit_trace_v3(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE learning_feedback_shadow_recommendations(
    id BIGSERIAL PRIMARY KEY,position_id BIGINT
);
CREATE TABLE learning_feature_warehouse_v1(
    id BIGSERIAL PRIMARY KEY,position_id BIGINT
);
CREATE TABLE decision_replay_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE decision_registry_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE decision_outcomes_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);

CREATE TABLE test_learning_source_v1(
    environment TEXT,symbol TEXT,"interval" TEXT,strategy TEXT,
    decision_key TEXT,decision_id UUID,position_id BIGINT,
    entry_time TIMESTAMPTZ,exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,mfe_pct NUMERIC,
    mae_pct NUMERIC,regime_identity TEXT,regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ,has_full_context BOOLEAN,
    has_avoid_review BOOLEAN,has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN,eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ,outcome_available_at TIMESTAMPTZ
);

CREATE FUNCTION learning_canonical_evidence_universe_live_v1(
    p_environment TEXT,p_sample_from TIMESTAMPTZ,p_sample_to TIMESTAMPTZ,
    p_evidence_cutoff_at TIMESTAMPTZ
)
RETURNS TABLE(
    environment TEXT,symbol TEXT,"interval" TEXT,strategy TEXT,
    decision_key TEXT,decision_id UUID,position_id BIGINT,
    entry_time TIMESTAMPTZ,exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,mfe_pct NUMERIC,
    mae_pct NUMERIC,regime_identity TEXT,regime_context JSONB,
    source_refreshed_at TIMESTAMPTZ,has_full_context BOOLEAN,
    has_avoid_review BOOLEAN,has_entry_quality_review BOOLEAN,
    has_positive_confirmation BOOLEAN,eligibility_reason TEXT,
    registry_available_at TIMESTAMPTZ,outcome_available_at TIMESTAMPTZ
)
LANGUAGE SQL STABLE AS $$
    SELECT * FROM test_learning_source_v1
    WHERE environment=p_environment
    ORDER BY decision_key
$$;
"""


@pytest.fixture(scope="module")
def contract_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_ft_learning_quarantine"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(SCHEMA)
        for migration in MIGRATIONS:
            cur.execute(migration)
        for migration in MIGRATIONS:
            cur.execute(migration)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def rollback_case(contract_db):
    yield
    contract_db.rollback()


def _position(cur, position_id: int, *, status: str = "CLOSED", pnl=None):
    cur.execute(
        "INSERT INTO positions(id,status,exit_time,net_pnl_usdc) "
        "VALUES (%s,%s,CASE WHEN %s='CLOSED' THEN now() ELSE NULL END,%s)",
        (position_id, status, status, pnl),
    )


def _ft(cur, position_id: int, status: str):
    cur.execute(
        "INSERT INTO canonical_financial_truth_v1(position_id,financial_truth_status) "
        "VALUES (%s,%s)",
        (position_id, status),
    )


def _excluded(cur, position_id: int, *, reason="FINANCIAL_TRUTH_INCOMPLETE"):
    source = (
        "LEGACY_POSITION_REPAIR"
        if reason == "LEGACY_REPAIR"
        else "FINANCIAL_TRUTH_CONTAINMENT"
    )
    cur.execute(
        """
        INSERT INTO learning_outcome_exclusion_v1(
          environment,deployment_id,position_id,exclusion_reason,source_type,
          semantic_fingerprint_v2,source_reference,detail_json,created_by,git_sha
        ) VALUES ('PAPER','local-paper',%s,%s,%s,%s,'governance:test',
                  '{"fixture":true}','TEST',%s)
        RETURNING exclusion_id
        """,
        (position_id, reason, source, "a" * 64, GIT_SHA),
    )
    return int(cur.fetchone()[0])


def _eligible(cur, position_id: int) -> bool:
    cur.execute("SELECT learning_outcome_is_eligible_v1(%s)", (position_id,))
    return bool(cur.fetchone()[0])


def _source_row(cur, position_id: int, key: str):
    cur.execute(
        """
        INSERT INTO test_learning_source_v1(
          environment,symbol,"interval",strategy,decision_key,position_id,
          exit_time,outcome_timestamp,realized_pnl_usdc,source_refreshed_at,
          has_full_context,has_avoid_review,has_entry_quality_review,
          has_positive_confirmation,eligibility_reason
        ) VALUES ('trading_paper','BTCUSDC','1m','RSI',%s,%s,now(),now(),1,
                  now(),true,false,true,false,'ELIGIBLE')
        """,
        (key, position_id),
    )


def _live_keys(cur):
    cur.execute(
        "SELECT decision_key FROM learning_canonical_evidence_universe_live_v1("
        "'trading_paper',now()-interval '1 day',now(),now()) ORDER BY 1"
    )
    return [row[0] for row in cur.fetchall()]


def test_01_closed_ft_absent_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 1)
        assert not _eligible(cur, 1)


def test_02_closed_ft_incomplete_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 2)
        _ft(cur, 2, "INCOMPLETE")
        assert not _eligible(cur, 2)


def test_03_closed_ft_failed_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 3)
        _ft(cur, 3, "FAILED")
        assert not _eligible(cur, 3)


def test_04_legacy_resolved_read_model_without_ft_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 4, pnl=7)
        cur.execute("INSERT INTO decision_outcomes_v1(position_id) VALUES (4)")
        assert not _eligible(cur, 4)


def test_05_stored_position_pnl_without_ft_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 5, pnl=11)
        assert not _eligible(cur, 5)


def test_06_closed_ft_complete_without_exclusion_is_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 6)
        _ft(cur, 6, "COMPLETE")
        assert _eligible(cur, 6)


def test_07_complete_with_active_exclusion_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 7)
        _ft(cur, 7, "COMPLETE")
        _excluded(cur, 7)
        assert not _eligible(cur, 7)


def test_08_authorized_append_only_revoke_restores_eligibility(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 8)
        _ft(cur, 8, "COMPLETE")
        exclusion_id = _excluded(cur, 8)
        cur.execute(
            """
            INSERT INTO learning_outcome_exclusion_resolution_v1(
              exclusion_id,resolution_action,reason,source_type,
              source_reference,created_by
            ) VALUES (%s,'REVOKE','FT evidence reviewed complete',
                      'MANUAL_GOVERNANCE_DECISION','governance:approval-8','TEST')
            """,
            (exclusion_id,),
        )
        assert _eligible(cur, 8)


def test_09_open_with_complete_ft_is_not_eligible(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 9, status="OPEN")
        _ft(cur, 9, "COMPLETE")
        assert not _eligible(cur, 9)


def test_10_ft_for_another_position_identity_does_not_qualify(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 10)
        _position(cur, 1010)
        _ft(cur, 1010, "COMPLETE")
        assert not _eligible(cur, 10)


def test_11_new_canonical_snapshot_source_omits_ft_absent(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 11)
        _source_row(cur, 11, "ft-absent")
        assert _live_keys(cur) == []


def test_12_historical_warehouse_row_cannot_reenter_new_generation(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 12)
        _ft(cur, 12, "COMPLETE")
        cur.execute(
            "INSERT INTO learning_feature_warehouse_v1(position_id) VALUES (12)"
        )
        _source_row(cur, 12, "historical-warehouse")
        _excluded(cur, 12)
        cur.execute(
            "SELECT count(*) FROM learning_feature_warehouse_v1 "
            "WHERE position_id=12"
        )
        assert cur.fetchone()[0] == 1
        assert _live_keys(cur) == []


def test_13_legacy_repair_exclusion_semantics_are_preserved(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 13)
        _ft(cur, 13, "COMPLETE")
        exclusion_id = _excluded(cur, 13, reason="LEGACY_REPAIR")
        cur.execute(
            "SELECT source_reference,detail_json FROM learning_outcome_exclusion_v1 "
            "WHERE exclusion_id=%s",
            (exclusion_id,),
        )
        source_reference, detail = cur.fetchone()
        assert source_reference == "governance:test"
        assert detail == {"fixture": True}
        assert not _eligible(cur, 13)


def test_14_duplicate_exclusion_is_unambiguously_rejected(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 14)
        _excluded(cur, 14)
        with pytest.raises(errors.UniqueViolation):
            _excluded(cur, 14)


def test_15_compensation_history_is_append_only(contract_db):
    with contract_db.cursor() as cur:
        _position(cur, 15)
        exclusion_id = _excluded(cur, 15)
        cur.execute(
            """
            INSERT INTO learning_outcome_exclusion_resolution_v1(
              exclusion_id,resolution_action,reason,source_type,
              source_reference,created_by
            ) VALUES (%s,'REVOKE','reviewed','MANUAL_GOVERNANCE_DECISION',
                      'governance:approval-15','TEST') RETURNING resolution_id
            """,
            (exclusion_id,),
        )
        resolution_id = int(cur.fetchone()[0])
        cur.execute("SAVEPOINT before_update")
        with pytest.raises(errors.RaiseException):
            cur.execute(
                "UPDATE learning_outcome_exclusion_resolution_v1 "
                "SET reason='changed' WHERE resolution_id=%s",
                (resolution_id,),
            )
        cur.execute("ROLLBACK TO SAVEPOINT before_update")
        cur.execute("SAVEPOINT before_delete")
        with pytest.raises(errors.RaiseException):
            cur.execute(
                "DELETE FROM learning_outcome_exclusion_resolution_v1 "
                "WHERE resolution_id=%s",
                (resolution_id,),
            )
        cur.execute("ROLLBACK TO SAVEPOINT before_delete")
        cur.execute(
            "SELECT count(*) FROM learning_outcome_exclusion_resolution_v1 "
            "WHERE resolution_id=%s",
            (resolution_id,),
        )
        assert cur.fetchone()[0] == 1
