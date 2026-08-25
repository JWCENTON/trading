"""Disposable PostgreSQL conformance for JOINT_AUTHORITY_EPOCH_V1."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260825_pre_calibration_joint_authority_epoch_v1.sql"
).read_text()
D = datetime(2026, 8, 25, 16, 15, tzinfo=timezone.utc)
R = datetime(2026, 8, 25, 16, 1, 3, tzinfo=timezone.utc)
REVISION = "a" * 40

SCHEMA = """
CREATE TABLE paper_equity_baseline_v2(
  baseline_id bigint primary key, deployment_id text, baseline_timestamp timestamptz,
  activation_fingerprint text
);
CREATE TABLE paper_portfolio_replay_cutover_v1(
  cutover_id bigint primary key, deployment_id text, cutover_at timestamptz,
  cutover_fingerprint text
);
CREATE TABLE paper_drawdown_history_activation_v1(
  activation_id bigint primary key, deployment_id text, generation integer,
  activated_at timestamptz, activation_evidence_fingerprint text
);
CREATE TABLE risk_budget_event_v1(
  event_id uuid primary key, event_type text, environment text,
  deployment_id text, event_at timestamptz
);
"""


def database(disposable_postgres_v16):
    name = "waltrade_baseline_test_joint_epoch_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    conn.commit()
    return conn


def seed_sources(cur):
    cur.execute(
        "INSERT INTO paper_equity_baseline_v2 VALUES (1,'local-paper',%s,%s)",
        (R - timedelta(days=10), "b" * 64),
    )
    cur.execute(
        "INSERT INTO paper_portfolio_replay_cutover_v1 VALUES "
        "(2,'local-paper',%s,%s)", (R, "c" * 64),
    )
    cur.execute(
        "INSERT INTO paper_drawdown_history_activation_v1 VALUES "
        "(3,'local-paper',2,%s,%s)", (D, "d" * 64),
    )


def insert_attempt(cur, *, status="ACTIVATED", boundary=D):
    attempt = uuid.uuid4()
    cur.execute(
        """INSERT INTO joint_authority_activation_attempt_v1(
             attempt_id,deployment_id,authority_identity,attempt_status,
             requested_activation_boundary,prepared_at,failure_reason,
             activated_at,source_fingerprints,producer_revision,
             attempt_fingerprint,contract_version)
           VALUES (%s,'local-paper','epoch',%s,%s,%s,%s,%s,%s::jsonb,%s,%s,
                   'JOINT_AUTHORITY_EPOCH_V1')""",
        (
            str(attempt), status, boundary, R,
            "seed failed" if status == "FAILED" else None,
            boundary if status == "ACTIVATED" else None,
            '{"baseline":"' + "b" * 64 + '"}', REVISION, uuid.uuid4().hex * 2,
        ),
    )
    return attempt


def insert_epoch(cur, attempt):
    cur.execute(
        """INSERT INTO joint_authority_epoch_v1(
             deployment_id,baseline_id,baseline_fingerprint,replay_cutover_id,
             replay_cutover_fingerprint,drawdown_activation_id,
             drawdown_generation,drawdown_generation_fingerprint,
             drawdown_activation_boundary,first_required_cadence,
             activation_attempt_id,git_revision,contract_versions,
             contract_fingerprints,deployment_identity,epoch_fingerprint)
           VALUES ('local-paper',1,%s,2,%s,3,2,%s,%s,%s,%s,%s,
                   %s::jsonb,%s::jsonb,'test',%s)
           RETURNING authority_epoch_id""",
        (
            "b" * 64, "c" * 64, "d" * 64, D, D + timedelta(minutes=15),
            str(attempt), REVISION, '{"joint":"V1"}',
            '{"joint":"' + "c" * 64 + '"}', "e" * 64,
        ),
    )
    return int(cur.fetchone()[0])


def test_migration_is_idempotent_empty_additive_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            for table in (
                "joint_authority_activation_attempt_v1", "joint_authority_epoch_v1",
                "joint_authority_epoch_selection_v1",
                "risk_budget_authority_epoch_binding_v1",
            ):
                cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                assert cur.fetchone()[0] == table
            seed_sources(cur)
            attempt = insert_attempt(cur)
            epoch_id = insert_epoch(cur, attempt)
            cur.execute(
                """INSERT INTO joint_authority_epoch_selection_v1(
                     authority_epoch_id,deployment_id,selected_at,selection_reason,
                     selection_fingerprint,git_revision)
                   VALUES (%s,'local-paper',%s,'INITIAL_JOINT_AUTHORITY_EPOCH',%s,%s)""",
                (epoch_id, D, "5" * 64, REVISION),
            )
        conn.commit()
        with pytest.raises(Exception, match="JOINT_AUTHORITY_EPOCH_V1_APPEND_ONLY"):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE joint_authority_epoch_v1 SET deployment_identity='changed' "
                    "WHERE authority_epoch_id=%s", (epoch_id,),
                )
        conn.rollback()
    finally:
        conn.close()


def test_failed_attempt_cannot_select_generation_and_retry_is_new(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            seed_sources(cur)
            failed = insert_attempt(cur, status="FAILED")
            with pytest.raises(Exception, match="SOURCE_INVALID"):
                insert_epoch(cur, failed)
        conn.rollback()
        with conn.cursor() as cur:
            seed_sources(cur)
            failed = insert_attempt(cur, status="FAILED")
            retry = uuid.uuid4()
            cur.execute(
                """INSERT INTO joint_authority_activation_attempt_v1(
                     attempt_id,deployment_id,authority_identity,
                     previous_failed_attempt_id,attempt_status,
                     requested_activation_boundary,prepared_at,activated_at,
                     source_fingerprints,producer_revision,attempt_fingerprint,
                     contract_version)
                   VALUES (%s,'local-paper','epoch',%s,'ACTIVATED',%s,%s,%s,
                           %s::jsonb,%s,%s,'JOINT_AUTHORITY_EPOCH_V1')""",
                (
                    str(retry), str(failed), D, R + timedelta(seconds=1), D,
                    '{"baseline":"' + "b" * 64 + '"}', REVISION, "f" * 64,
                ),
            )
            assert insert_epoch(cur, retry) > 0
        conn.commit()
    finally:
        conn.close()


def test_q0_binding_enforces_c1_epoch_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            seed_sources(cur)
            epoch_id = insert_epoch(cur, insert_attempt(cur))
            cur.execute(
                "INSERT INTO joint_authority_epoch_selection_v1("
                "authority_epoch_id,deployment_id,selected_at,selection_reason,"
                "selection_fingerprint,git_revision) VALUES "
                "(%s,'local-paper',%s,'INITIAL_JOINT_AUTHORITY_EPOCH',%s,%s)",
                (epoch_id, D, "5" * 64, REVISION),
            )
            event = uuid.uuid4()
            q0 = D + timedelta(minutes=15)
            cur.execute(
                "INSERT INTO risk_budget_event_v1 VALUES "
                "(%s,'STATE_EVALUATION','PAPER','local-paper',%s)",
                (str(event), q0),
            )
            cur.execute(
                """INSERT INTO risk_budget_authority_epoch_binding_v1(
                     event_id,authority_epoch_id,evaluation_as_of,
                     calibration_replay_eligible,baseline_fingerprint,
                     replay_cutover_fingerprint,drawdown_generation_fingerprint,
                     risk_budget_source_fingerprint,binding_fingerprint)
                   VALUES (%s,%s,%s,TRUE,%s,%s,%s,%s,%s)""",
                (
                    str(event), epoch_id, q0, "b" * 64, "c" * 64,
                    "d" * 64, "6" * 64, "7" * 64,
                ),
            )
        conn.commit()
        with pytest.raises(Exception, match="APPEND_ONLY"):
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM risk_budget_authority_epoch_binding_v1 "
                    "WHERE event_id=%s", (str(event),),
                )
        conn.rollback()
        with conn.cursor() as cur:
            too_early = uuid.uuid4()
            cur.execute(
                "INSERT INTO risk_budget_event_v1 VALUES "
                "(%s,'STATE_EVALUATION','PAPER','local-paper',%s)",
                (str(too_early), D),
            )
            with pytest.raises(Exception, match="BINDING_INVALID"):
                cur.execute(
                    """INSERT INTO risk_budget_authority_epoch_binding_v1(
                         event_id,authority_epoch_id,evaluation_as_of,
                         calibration_replay_eligible,baseline_fingerprint,
                         replay_cutover_fingerprint,drawdown_generation_fingerprint,
                         risk_budget_source_fingerprint,binding_fingerprint)
                       VALUES (%s,%s,%s,TRUE,%s,%s,%s,%s,%s)""",
                    (
                        str(too_early), epoch_id, D, "b" * 64, "c" * 64,
                        "d" * 64, "6" * 64, "8" * 64,
                    ),
                )
        conn.rollback()
    finally:
        conn.close()
