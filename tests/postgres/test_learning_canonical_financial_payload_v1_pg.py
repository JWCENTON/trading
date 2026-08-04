from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2 import errors


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT
    / "db/migrations/20260804_learning_canonical_financial_payload_v1.sql"
).read_text()


SCHEMA = r"""
CREATE EXTENSION pgcrypto;

CREATE TABLE positions(
    id BIGINT PRIMARY KEY,
    status TEXT NOT NULL,
    exit_time TIMESTAMPTZ
);
CREATE TABLE canonical_financial_truth_v1(
    position_id BIGINT PRIMARY KEY REFERENCES positions(id),
    financial_truth_status TEXT NOT NULL,
    authoritative_gross_pnl NUMERIC,
    authoritative_net_pnl NUMERIC,
    authoritative_fees_usdc NUMERIC,
    authoritative_entry_notional NUMERIC,
    authoritative_exit_notional NUMERIC,
    authoritative_entry_fees_usdc NUMERIC,
    authoritative_exit_fees_usdc NUMERIC,
    calculation_version TEXT,
    source_authority TEXT
);
CREATE TABLE learning_exclusions(position_id BIGINT PRIMARY KEY);
CREATE FUNCTION learning_outcome_is_excluded_v1(p_position_id BIGINT)
RETURNS BOOLEAN LANGUAGE SQL STABLE STRICT AS $$
    SELECT EXISTS(SELECT 1 FROM learning_exclusions WHERE position_id=p_position_id)
$$;
CREATE VIEW v_learning_eligible_closed_positions_v1 AS
SELECT position.*
FROM positions position
JOIN canonical_financial_truth_v1 financial_truth
  ON financial_truth.position_id=position.id
WHERE position.status='CLOSED'
  AND position.exit_time IS NOT NULL
  AND financial_truth.financial_truth_status='COMPLETE'
  AND NOT learning_outcome_is_excluded_v1(position.id);

CREATE TABLE legacy_learning_source(
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
CREATE FUNCTION learning_canonical_evidence_universe_pre_ft_quarantine_v1(
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
) LANGUAGE SQL STABLE AS $$
    SELECT * FROM legacy_learning_source
    WHERE legacy_learning_source.environment=p_environment ORDER BY decision_key
$$;
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
) LANGUAGE SQL STABLE AS $$
    SELECT * FROM learning_canonical_evidence_universe_pre_ft_quarantine_v1(
        p_environment,p_sample_from,p_sample_to,p_evidence_cutoff_at
    )
$$;
CREATE FUNCTION learning_canonical_evidence_universe_v1(
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
) LANGUAGE SQL STABLE AS $$
    SELECT * FROM learning_canonical_evidence_universe_live_v1(
        p_environment,p_sample_from,p_sample_to,p_evidence_cutoff_at
    )
$$;

CREATE TABLE learning_feedback_refresh_runs_v1(
    id BIGSERIAL PRIMARY KEY,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    window_days INTEGER NOT NULL
);
CREATE TABLE learning_canonical_source_snapshots_v2(
    snapshot_token UUID PRIMARY KEY,
    feedback_run_id BIGINT NOT NULL UNIQUE REFERENCES learning_feedback_refresh_runs_v1(id),
    deployment_instance_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    source_environment TEXT NOT NULL,
    evidence_window_start TIMESTAMPTZ NOT NULL,
    evidence_window_end TIMESTAMPTZ NOT NULL,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    source_snapshot_at TIMESTAMPTZ NOT NULL,
    snapshot_status TEXT NOT NULL,
    source_row_count INTEGER NOT NULL DEFAULT 0,
    eligible_row_count INTEGER NOT NULL DEFAULT 0,
    snapshot_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ
);
CREATE TABLE learning_canonical_source_snapshot_rows_v2(
    snapshot_token UUID NOT NULL REFERENCES learning_canonical_source_snapshots_v2,
    ordinal INTEGER NOT NULL,
    environment TEXT NOT NULL,symbol TEXT NOT NULL,interval TEXT NOT NULL,
    strategy TEXT NOT NULL,decision_key TEXT NOT NULL,decision_id UUID,
    position_id BIGINT,entry_time TIMESTAMPTZ,exit_time TIMESTAMPTZ,
    outcome_timestamp TIMESTAMPTZ,realized_pnl_usdc NUMERIC,
    gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,mfe_pct NUMERIC,mae_pct NUMERIC,
    regime_identity TEXT,regime_context JSONB,source_refreshed_at TIMESTAMPTZ,
    has_full_context BOOLEAN NOT NULL,has_avoid_review BOOLEAN NOT NULL,
    has_entry_quality_review BOOLEAN NOT NULL,
    has_positive_confirmation BOOLEAN NOT NULL,eligibility_reason TEXT NOT NULL,
    registry_available_at TIMESTAMPTZ,outcome_available_at TIMESTAMPTZ,
    row_hash TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(snapshot_token,ordinal),UNIQUE(snapshot_token,decision_key)
);
CREATE FUNCTION learning_evidence_runtime_identity_v1()
RETURNS TABLE(deployment_instance_id TEXT,environment TEXT,deployment_id TEXT)
LANGUAGE SQL STABLE AS $$
    SELECT 'local'::TEXT,'paper'::TEXT,'local-paper'::TEXT
$$;
CREATE FUNCTION capture_learning_canonical_source_snapshot_v2(BIGINT)
RETURNS UUID LANGUAGE SQL AS $$ SELECT NULL::UUID $$;

CREATE FUNCTION prevent_learning_frozen_source_mutation_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'LEARNING_FROZEN_SOURCE_IMMUTABLE'; END
$$;
CREATE TRIGGER learning_frozen_snapshot_immutable_v2
BEFORE UPDATE OR DELETE ON learning_canonical_source_snapshots_v2
FOR EACH ROW EXECUTE FUNCTION prevent_learning_frozen_source_mutation_v2();
CREATE TRIGGER learning_frozen_snapshot_rows_immutable_v2
BEFORE UPDATE OR DELETE ON learning_canonical_source_snapshot_rows_v2
FOR EACH ROW EXECUTE FUNCTION prevent_learning_frozen_source_mutation_v2();

CREATE TABLE schema_migration_ledger_v1(
    ledger_id BIGSERIAL PRIMARY KEY,migration_id TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    environment TEXT NOT NULL,deployment_id TEXT NOT NULL,database_name TEXT NOT NULL,
    applied_by TEXT NOT NULL,status TEXT NOT NULL,success BOOLEAN NOT NULL,
    execution_duration_ms BIGINT NOT NULL,git_sha TEXT NOT NULL,error_summary TEXT,
    schema_baseline_version TEXT NOT NULL
);

INSERT INTO learning_feedback_refresh_runs_v1(
    id,environment,status,requested_at,started_at,window_days
) VALUES (322,'trading_paper','OK','2026-01-01','2026-01-01',30);
INSERT INTO learning_canonical_source_snapshots_v2(
    snapshot_token,feedback_run_id,deployment_instance_id,environment,deployment_id,
    source_environment,evidence_window_start,evidence_window_end,evidence_cutoff_at,
    source_snapshot_at,snapshot_status,source_row_count,eligible_row_count,
    snapshot_hash,completed_at
) VALUES (
    '32200000-0000-4000-8000-000000000322',322,'local','paper','local-paper',
    'trading_paper','2025-12-02','2026-01-01','2026-01-01','2026-01-01',
    'COMPLETE',1,1,repeat('a',64),'2026-01-01'
);
INSERT INTO learning_canonical_source_snapshot_rows_v2(
    snapshot_token,ordinal,environment,symbol,interval,strategy,decision_key,
    position_id,realized_pnl_usdc,gross_pnl_usdc,fees_usdc,has_full_context,
    has_avoid_review,has_entry_quality_review,has_positive_confirmation,
    eligibility_reason,row_hash
) VALUES (
    '32200000-0000-4000-8000-000000000322',1,'trading_paper','BTCUSDC','1m',
    'RSI','legacy-322',322,9,10,1,true,false,true,false,'ELIGIBLE',repeat('b',64)
);
"""


@pytest.fixture(scope="module")
def payload_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_learning_canonical_financial_payload"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def rollback_case(payload_db):
    yield
    payload_db.rollback()


def _position(cur, position_id: int, *, excluded: bool = False, ft: bool = True):
    cur.execute(
        "INSERT INTO positions VALUES (%s,'CLOSED',now())", (position_id,)
    )
    if ft:
        cur.execute(
            """
            INSERT INTO canonical_financial_truth_v1 VALUES (
              %s,'COMPLETE',1.25,1.10,0.15,100,101.25,0.05,0.10,
              'FT_CALC_V1','SIMULATED_EXECUTION'
            )
            """,
            (position_id,),
        )
    if excluded:
        cur.execute("INSERT INTO learning_exclusions VALUES (%s)", (position_id,))


def _legacy(cur, position_id: int, key: str, gross=0, fees=0, net=0):
    cur.execute(
        """
        INSERT INTO legacy_learning_source(
          environment,symbol,"interval",strategy,decision_key,position_id,
          exit_time,outcome_timestamp,realized_pnl_usdc,gross_pnl_usdc,
          fees_usdc,source_refreshed_at,has_full_context,has_avoid_review,
          has_entry_quality_review,has_positive_confirmation,eligibility_reason
        ) VALUES (
          'trading_paper','BTCUSDC','1m','RSI',%s,%s,now(),now(),%s,%s,%s,
          now(),true,false,true,false,'ELIGIBLE'
        )
        """,
        (key, position_id, net, gross, fees),
    )


def test_positions_a_b_c_d_use_same_position_ft_and_block_ineligible(payload_db):
    with payload_db.cursor() as cur:
        _position(cur, 1001)
        _legacy(cur, 1001, "A", 0, 0, 0)
        _position(cur, 1002, ft=False)
        _legacy(cur, 1002, "B", 0, 0, 0)
        _position(cur, 1003, excluded=True)
        _legacy(cur, 1003, "C", 0, 0, 0)
        _position(cur, 1004)
        _legacy(cur, 1004, "D", 77, 7, 70)

        cur.execute(
            """
            SELECT decision_key,position_id,gross_pnl_usdc,fees_usdc,
                   net_pnl_usdc,realized_pnl_usdc,entry_notional_usdc,
                   exit_notional_usdc,entry_fee_usdc,exit_fee_usdc,
                   financial_truth_status,financial_truth_calculation_version,
                   financial_truth_source_authority,
                   financial_payload_contract_version
            FROM learning_canonical_evidence_universe_live_v2(
              'trading_paper',now()-interval '30 days',now(),now()
            ) ORDER BY decision_key
            """
        )
        rows = cur.fetchall()
        assert [row[0] for row in rows] == ["A", "D"]
        for row in rows:
            assert row[1:] == (
                1001 if row[0] == "A" else 1004,
                Decimal("1.25"),
                Decimal("0.15"),
                Decimal("1.10"),
                Decimal("1.10"),
                Decimal("100"),
                Decimal("101.25"),
                Decimal("0.05"),
                Decimal("0.10"),
                "COMPLETE",
                "FT_CALC_V1",
                "SIMULATED_EXECUTION",
                "CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1",
            )


def test_new_snapshot_payload_hash_and_capture_are_deterministic(payload_db):
    with payload_db.cursor() as cur:
        _position(cur, 2001)
        _legacy(cur, 2001, "SNAPSHOT-A", 0, 0, 0)
        cur.execute(
            """
            INSERT INTO learning_feedback_refresh_runs_v1(
              environment,status,requested_at,started_at,window_days
            ) VALUES ('trading_paper','RUNNING',now(),now(),30) RETURNING id
            """
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            "SELECT capture_learning_canonical_source_snapshot_v2(%s)", (run_id,)
        )
        token = cur.fetchone()[0]
        cur.execute(
            """
            SELECT h.financial_payload_contract_version,h.source_row_count,
                   h.snapshot_hash,r.gross_pnl_usdc,r.fees_usdc,r.net_pnl_usdc,
                   r.entry_fee_usdc,r.exit_fee_usdc,
                   r.financial_payload_contract_version,
                   encode(digest(r.row_hash,'sha256'),'hex') IS NOT NULL
            FROM learning_canonical_source_snapshots_v2 h
            JOIN learning_canonical_source_snapshot_rows_v2 r USING(snapshot_token)
            WHERE h.snapshot_token=%s
            """,
            (token,),
        )
        row = cur.fetchone()
        assert row[:9] == (
            "CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1",
            1,
            row[2],
            Decimal("1.25"),
            Decimal("0.15"),
            Decimal("1.10"),
            Decimal("0.05"),
            Decimal("0.10"),
            "CANONICAL_FINANCIAL_TRUTH_PAYLOAD_V1",
        )
        assert len(row[2]) == 64
        assert row[9]
        cur.execute(
            "SELECT capture_learning_canonical_source_snapshot_v2(%s)", (run_id,)
        )
        assert cur.fetchone()[0] == token
        cur.execute(
            "SELECT snapshot_hash FROM learning_canonical_source_snapshots_v2 "
            "WHERE snapshot_token=%s",
            (token,),
        )
        assert cur.fetchone()[0] == row[2]


def test_old_snapshot_is_legacy_marked_and_immutable(payload_db):
    with payload_db.cursor() as cur:
        cur.execute(
            "SELECT financial_payload_contract_version "
            "FROM learning_canonical_source_snapshots_v2 WHERE feedback_run_id=322"
        )
        assert cur.fetchone()[0] is None
        cur.execute(
            "SELECT set_config('waltrade.learning_source_snapshot_token',"
            "'32200000-0000-4000-8000-000000000322',true)"
        )
        cur.execute(
            "SELECT financial_payload_contract_version,gross_pnl_usdc "
            "FROM learning_canonical_evidence_universe_v2("
            "'trading_paper','2025-12-02','2026-01-01','2026-01-01')"
        )
        assert cur.fetchone() == ("LEGACY_WAREHOUSE_PAYLOAD_V0", 10)
        with pytest.raises(errors.RaiseException) as caught:
            cur.execute(
                "UPDATE learning_canonical_source_snapshot_rows_v2 "
                "SET gross_pnl_usdc=1 WHERE decision_key='legacy-322'"
            )
        assert "LEARNING_FROZEN_SOURCE_IMMUTABLE" in str(caught.value)


def test_migration_and_identity_contract_are_exactly_once(payload_db):
    with payload_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 "
            "WHERE migration_id='20260804_learning_canonical_financial_payload_v1.sql'"
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            """
            SELECT count(*)
            FROM pg_indexes
            WHERE schemaname='public'
              AND indexname='ux_learning_canonical_snapshot_rows_v2_canonical_position'
              AND indexdef LIKE '%snapshot_token, position_id%'
            """
        )
        assert cur.fetchone()[0] == 1
