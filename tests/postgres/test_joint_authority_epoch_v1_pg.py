"""Disposable PostgreSQL conformance for JOINT_AUTHORITY_EPOCH_V1."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import pytest

from common.joint_authority_epoch import (
    activate_drawdown_epoch_cursor,
    bind_risk_budget_event_cursor,
    load_active_epoch_cursor,
    resolve_risk_budget_boundary_cursor,
)
from common.paper_drawdown_history import (
    ActivationEvidence,
    PRODUCER_IDENTITY,
    capture_observation_candidate,
    ensure_activation_cursor,
    persist_observation_candidate,
    read_paper_drawdown_history,
)
from common.paper_equity_baseline_v2 import fetch_paper_equity_baseline_v2
from common.portfolio_state import PortfolioStateV1


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260825_pre_calibration_joint_authority_epoch_v1.sql"
).read_text()
REAL_MIGRATIONS = tuple(
    (ROOT / "db/migrations" / name).read_text()
    for name in (
        "20260811_paper_equity_baseline_v2.sql",
        "20260825_paper_portfolio_replay_cutover_v1.sql",
        "20260825_paper_drawdown_history_authority_v1.sql",
        "20260825_paper_drawdown_history_numeric_scale_v1.sql",
        "20260825_paper_drawdown_history_activation_generations_v1.sql",
        "20260824_risk_budget_authority_v1.sql",
        "20260825_pre_calibration_joint_authority_epoch_v1.sql",
    )
)
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


def real_schema_database(disposable_postgres_v16):
    """Apply production migrations with only the replay FK prerequisite stubbed."""
    name = "waltrade_baseline_test_real_schema_joint_epoch_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE positions(id BIGINT PRIMARY KEY)")
        for migration in REAL_MIGRATIONS:
            cur.execute(migration)
    conn.commit()
    return conn


def seed_real_upstream(cur):
    cur.execute(
        """INSERT INTO paper_equity_baseline_v2(
             deployment_id,baseline_version,baseline_timestamp,cutover_boundary,
             baseline_account_total,baseline_managed_equity,
             baseline_external_manual,baseline_available,
             baseline_inventory_value,baseline_realized_net_pnl,
             baseline_unrealized_pnl,baseline_fees,baseline_open_positions,
             frozen_pre_baseline_unresolved_count,evidence_status,
             source_authority,approved_by,approval_provenance,
             activation_fingerprint)
           VALUES ('local-paper','PAPER_EQUITY_BASELINE_V2',%s,%s,
                   100,100,0,100,0,0,0,0,0,0,'COMPLETE',
                   'CANONICAL_PAPER_ACCOUNT_READ_MODEL_V1','product-owner',
                   '{"approved":true}'::jsonb,%s)
           RETURNING baseline_id""",
        (R - timedelta(days=10), R - timedelta(days=10), "b" * 64),
    )
    baseline_id = int(cur.fetchone()[0])
    cur.execute(
        """INSERT INTO paper_portfolio_replay_cutover_v1(
             deployment_id,cutover_at,git_revision,contract_version,
             portfolio_state_fingerprint,cutover_fingerprint,
             inventory_position_count,source_evidence)
           VALUES ('local-paper',%s,%s,'PAPER_PORTFOLIO_REPLAY_CUTOVER_V1',
                   %s,%s,0,'{"status":"CANONICAL_EMPTY"}'::jsonb)
           RETURNING cutover_id""",
        (R, REVISION, "9" * 64, "c" * 64),
    )
    return baseline_id, int(cur.fetchone()[0])


def canonical_state(at):
    return PortfolioStateV1(
        portfolio_state_version="PORTFOLIO_STATE_V1", environment="PAPER",
        deployment_id="local-paper", as_of=at, runtime_revision=REVISION,
        capital_scope="MANAGED_PORTFOLIO_EQUITY", total_capital=Decimal("100"),
        total_capital_status="CANONICAL", available_capital=Decimal("100"),
        available_capital_status="CANONICAL", reserved_capital=Decimal("0"),
        reserved_capital_status="CANONICAL", deployed_capital=Decimal("0"),
        deployed_capital_status="CANONICAL", realized_pnl=Decimal("0"),
        realized_pnl_status="CANONICAL", unrealized_pnl=Decimal("0"),
        unrealized_pnl_status="CANONICAL", open_positions_count=0,
        open_positions_status="CANONICAL", open_exposure_notional=Decimal("0"),
        open_exposure_status="CANONICAL", exposure_by_symbol=(),
        exposure_by_strategy=(), exposure_by_regime=(), open_risk=Decimal("0"),
        open_risk_status="CANONICAL_EMPTY", portfolio_heat=None,
        portfolio_heat_status="NOT_YET_CANONICAL", drawdown=None,
        drawdown_status="NOT_YET_CANONICAL", source_timestamps={},
        source_freshness={}, source_authorities={
            "total_capital": "PAPER_EQUITY_BASELINE_V2",
            "realized_pnl": "CANONICAL_FINANCIAL_TRUTH_V1",
            "inventory_quantity": "REMAINING_INVENTORY_QTY",
            "mark_price": "CANONICAL_MARK_PRICE",
            "account_reporting_excluded": "RECONSTRUCTED_PARTIAL_MIXED",
        }, incomplete_reasons=(),
    )


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


def test_real_migrations_replay_join_atomic_activation_c1_and_q0(
    disposable_postgres_v16,
):
    """Exercise the public B/R/D/C1/Q0 path against production column names."""
    conn = real_schema_database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            baseline_id, replay_cutover_id = seed_real_upstream(cur)
            cur.execute(
                """SELECT kcu.column_name,ccu.column_name
                   FROM information_schema.table_constraints tc
                   JOIN information_schema.key_column_usage kcu
                     ON kcu.constraint_schema=tc.constraint_schema
                    AND kcu.constraint_name=tc.constraint_name
                   JOIN information_schema.constraint_column_usage ccu
                     ON ccu.constraint_schema=tc.constraint_schema
                    AND ccu.constraint_name=tc.constraint_name
                   WHERE tc.table_schema='public'
                     AND tc.table_name='joint_authority_epoch_v1'
                     AND tc.constraint_type='FOREIGN KEY'
                     AND kcu.column_name='replay_cutover_id'"""
            )
            assert cur.fetchone() == ("replay_cutover_id", "cutover_id")

            # Establish the prior selected Drawdown generation required by a
            # clean forward generation transition, using the real producer.
            genesis = ensure_activation_cursor(
                cur, deployment_id="local-paper", now=R,
                producer_identity=PRODUCER_IDENTITY, git_revision=REVISION,
            )
            assert genesis is not None
            baseline_state = canonical_state(R)
            baseline = fetch_paper_equity_baseline_v2(
                cur, deployment_id="local-paper",
            )
            assert baseline is not None and baseline.baseline_id == baseline_id
            genesis_candidate = capture_observation_candidate(
                state=baseline_state, baseline=baseline, activation=genesis,
                observed_at=R, observation_trigger="BASELINE_ACTIVATION",
                trigger_reference="REAL_SCHEMA_GENESIS",
                producer_identity=PRODUCER_IDENTITY, git_revision=REVISION,
            )
            assert genesis_candidate.status == "CANONICAL"
            assert persist_observation_candidate(
                cur, genesis_candidate.candidate,
            ).status == "CANONICAL"
            previous_status = read_paper_drawdown_history(
                cur, deployment_id="local-paper", as_of=D,
                generation=genesis.generation,
            ).history_status

            activation = activate_drawdown_epoch_cursor(
                cur, deployment_id="local-paper", requested_boundary=D,
                supersession_reason="UPSTREAM_REPLAY_CUTOVER_EPOCH_CHANGE",
                expected_previous_history_status=previous_status,
                approval_evidence={"approved": True, "source": "test"},
                producer_identity=PRODUCER_IDENTITY, git_revision=REVISION,
                deployment_identity="disposable-postgres",
                contract_versions={"joint": "JOINT_AUTHORITY_EPOCH_V1"},
                contract_fingerprints={"joint": "f" * 64},
                portfolio_state_reader=lambda *args, **kwargs: canonical_state(D),
                prepared_at=R,
            )
            assert activation.status == "ACTIVATED", activation.failure_reason
            assert activation.epoch is not None

            # This public lookup is the exact production query that previously
            # used USING(replay_cutover_id) against replay.cutover_id.
            loaded = load_active_epoch_cursor(cur, deployment_id="local-paper")
            assert loaded is not None
            assert loaded.authority_epoch_id == activation.epoch.authority_epoch_id
            assert loaded.replay_cutover_id == replay_cutover_id
            cur.execute(
                """SELECT e.replay_cutover_id,r.cutover_id
                   FROM joint_authority_epoch_v1 e
                   JOIN paper_portfolio_replay_cutover_v1 r
                     ON e.replay_cutover_id=r.cutover_id
                   WHERE e.authority_epoch_id=%s""",
                (loaded.authority_epoch_id,),
            )
            assert cur.fetchone() == (replay_cutover_id, replay_cutover_id)

            c1 = D + timedelta(minutes=15)
            c1_candidate = capture_observation_candidate(
                state=canonical_state(c1), baseline=baseline,
                activation=ActivationEvidence(
                    activation.epoch.drawdown_activation_id, baseline_id,
                    "local-paper", D, D, "b" * 64, "0" * 64,
                    activation.epoch.drawdown_generation_fingerprint, False,
                    activation.epoch.drawdown_generation,
                ),
                observed_at=c1, observation_trigger="CADENCE_15M",
                trigger_reference=c1.isoformat(),
                producer_identity=PRODUCER_IDENTITY, git_revision=REVISION,
            )
            assert c1_candidate.status == "CANONICAL"
            assert persist_observation_candidate(
                cur, c1_candidate.candidate,
            ).status == "CANONICAL"
            boundary = resolve_risk_budget_boundary_cursor(
                cur, deployment_id="local-paper", scheduler_time=c1,
            )
            assert boundary.status == "CANONICAL"
            assert boundary.as_of == c1
            assert boundary.epoch == loaded

            event_id = uuid.uuid4()
            cur.execute(
                """INSERT INTO risk_budget_event_v1(
                     event_id,event_type,event_identity,environment,deployment_id,
                     account_identity_fingerprint,event_at,policy_version,
                     policy_fingerprint,authority_status,policy_state,
                     drawdown_history_status,reason_codes,source_fingerprints,
                     evidence,producer_identity,git_revision,contract_version,
                     event_fingerprint)
                   VALUES (%s,'STATE_EVALUATION','Q0','PAPER','local-paper',%s,%s,
                           'MISSING_POLICY',%s,'MISSING_POLICY',NULL,'CANONICAL',
                           '[]'::jsonb,%s::jsonb,%s::jsonb,'test',%s,
                           'RISK_BUDGET_AUTHORITY_V1',%s)""",
                (
                    str(event_id), "a" * 64, c1, "1" * 64,
                    '{"joint":"' + "f" * 64 + '"}',
                    '{"shadow_only":true}', REVISION, "2" * 64,
                ),
            )
            assert bind_risk_budget_event_cursor(
                cur, event_id=event_id, epoch=loaded, evaluation_as_of=c1,
                risk_budget_source_fingerprint="3" * 64,
            ) == "INSERTED"
            cur.execute(
                """SELECT authority_epoch_id,evaluation_as_of
                   FROM risk_budget_authority_epoch_binding_v1
                   WHERE event_id=%s""",
                (str(event_id),),
            )
            assert cur.fetchone() == (loaded.authority_epoch_id, c1)
        conn.commit()
    finally:
        conn.close()
