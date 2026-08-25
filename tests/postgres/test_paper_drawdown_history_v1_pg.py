"""Disposable PostgreSQL proof for PAPER drawdown history authority V1."""

from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.drawdown_history import canonical_json, canonical_numeric_38_18, fingerprint
from common.paper_drawdown_history import (
    CONTRACT_VERSION,
    capture_observation_candidate,
    create_activation_generation_cursor,
    ensure_activation_cursor,
    get_active_paper_drawdown_history_generation_cursor,
    persist_observation_candidate,
    read_paper_drawdown_history,
    select_observation_triggers_cursor,
    _legacy_final_evidence_payload,
)
from common.paper_equity_baseline_v2 import fetch_paper_equity_baseline_v2


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260825_paper_drawdown_history_authority_v1.sql").read_text()
CORRECTIVE_MIGRATION = (
    ROOT / "db/migrations/20260825_paper_drawdown_history_numeric_scale_v1.sql"
).read_text()
GENERATION_MIGRATION = (
    ROOT / "db/migrations/20260825_paper_drawdown_history_activation_generations_v1.sql"
).read_text()
T0 = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
D = Decimal
REVISION = "d" * 40
ACTIVATION_FP = "b" * 64


BASELINE_SCHEMA = """
CREATE TABLE paper_equity_baseline_v2 (
  baseline_id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT NOT NULL,
  baseline_version TEXT NOT NULL,
  baseline_timestamp TIMESTAMPTZ NOT NULL,
  baseline_account_total NUMERIC NOT NULL,
  baseline_managed_equity NUMERIC NOT NULL,
  baseline_external_manual NUMERIC NOT NULL,
  baseline_available NUMERIC NOT NULL,
  baseline_inventory_value NUMERIC NOT NULL,
  baseline_realized_net_pnl NUMERIC NOT NULL,
  baseline_unrealized_pnl NUMERIC NOT NULL,
  baseline_fees NUMERIC NOT NULL,
  baseline_open_positions INTEGER NOT NULL,
  frozen_pre_baseline_unresolved_count INTEGER NOT NULL,
  evidence_status TEXT NOT NULL,
  source_authority TEXT NOT NULL,
  approved_by TEXT NOT NULL,
  approval_provenance JSONB NOT NULL,
  activation_fingerprint TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(deployment_id, baseline_version)
);
CREATE TABLE canonical_financial_truth_v1 (
  position_id BIGINT PRIMARY KEY,
  financial_truth_status TEXT NOT NULL,
  evidence_observed_at TIMESTAMPTZ,
  source_fingerprint TEXT
);
"""


def database(disposable_postgres_v16, prefix):
    name = prefix + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(BASELINE_SCHEMA)
        cur.execute(MIGRATION)
        cur.execute(CORRECTIVE_MIGRATION)
        cur.execute(GENERATION_MIGRATION)
    conn.commit()
    return conn


def insert_baseline(conn):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO paper_equity_baseline_v2(
                 deployment_id,baseline_version,baseline_timestamp,
                 baseline_account_total,baseline_managed_equity,
                 baseline_external_manual,baseline_available,
                 baseline_inventory_value,baseline_realized_net_pnl,
                 baseline_unrealized_pnl,baseline_fees,baseline_open_positions,
                 frozen_pre_baseline_unresolved_count,evidence_status,
                 source_authority,approved_by,approval_provenance,
                 activation_fingerprint
               ) VALUES (
                 'local-paper','PAPER_EQUITY_BASELINE_V2',%s,120,100,20,
                 100,20,0,1,0,1,7,'COMPLETE','CANONICAL','PO',%s::jsonb,%s
               ) RETURNING baseline_id""",
            (T0 - timedelta(days=10), json.dumps({"approved": True}), ACTIVATION_FP),
        )
        baseline_id = int(cur.fetchone()[0])
    conn.commit()
    return baseline_id


class State:
    environment = "PAPER"
    deployment_id = "local-paper"
    total_capital_status = "CANONICAL"
    realized_pnl_status = "CANONICAL"
    unrealized_pnl_status = "CANONICAL"

    def __init__(self, at, equity, *, realized_status="CANONICAL"):
        self.at = at
        self.total_capital = D(equity)
        self.realized_pnl = self.total_capital - D("100")
        self.unrealized_pnl = D("1")
        self.realized_pnl_status = realized_status

    def serializable(self):
        return {
            "portfolio_state_version": "PORTFOLIO_STATE_V1",
            "environment": "PAPER", "deployment_id": "local-paper",
            "as_of": self.at.isoformat(),
            "total_capital": str(self.total_capital),
            "total_capital_status": self.total_capital_status,
            "realized_pnl": str(self.realized_pnl),
            "realized_pnl_status": self.realized_pnl_status,
            "unrealized_pnl": str(self.unrealized_pnl),
            "unrealized_pnl_status": "CANONICAL",
        }


def prepare(conn):
    insert_baseline(conn)
    with conn.cursor() as cur:
        activation = ensure_activation_cursor(
            cur, deployment_id="local-paper", now=T0,
            producer_identity="test", git_revision=REVISION,
        )
        baseline = fetch_paper_equity_baseline_v2(cur, deployment_id="local-paper")
    conn.commit()
    return activation, baseline


def candidate(activation, baseline, minutes, equity, trigger="CADENCE_15M"):
    at = T0 + timedelta(minutes=minutes)
    result = capture_observation_candidate(
        state=State(at, equity), baseline=baseline, activation=activation,
        observed_at=at, observation_trigger=trigger,
        trigger_reference=f"{trigger}:{at.isoformat()}",
        producer_identity="test", git_revision=REVISION,
    )
    assert result.status == "CANONICAL"
    return result.candidate


def insert_legacy_generation_one_candidate(cur, item):
    legacy_fingerprint = fingerprint(_legacy_final_evidence_payload(item))
    cur.execute(
        """INSERT INTO paper_managed_equity_observation_v1(
             activation_id,baseline_id,deployment_id,observed_at,
             observation_bucket_at,observation_trigger,trigger_reference,
             observation_identity,managed_equity,managed_equity_status,
             realized_pnl,realized_pnl_status,unrealized_pnl,
             unrealized_pnl_status,baseline_activation_fingerprint,
             portfolio_state_fingerprint,source_fingerprints,
             portfolio_state_evidence,evidence_fingerprint,
             history_evidence_status,producer_identity,git_revision,
             contract_version,activation_generation
           ) VALUES (
             %s,%s,%s,%s,%s,%s,%s,%s,%s,'CANONICAL',%s,'CANONICAL',
             %s,'CANONICAL',%s,%s,%s::jsonb,%s::jsonb,%s,'CANONICAL',%s,%s,%s,1
           )""",
        (
            item.activation_id, item.baseline_id, item.deployment_id,
            item.observed_at, item.observation_bucket_at,
            item.observation_trigger, item.trigger_reference,
            item.observation_identity, item.managed_equity, item.realized_pnl,
            item.unrealized_pnl, item.baseline_activation_fingerprint,
            item.portfolio_state_fingerprint,
            canonical_json(item.source_fingerprints),
            canonical_json(item.portfolio_state_evidence), legacy_fingerprint,
            item.producer_identity, item.git_revision, CONTRACT_VERSION,
        ),
    )
    return legacy_fingerprint


def test_migration_is_empty_idempotent_additive_and_append_only(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_paper_dd_schema_")
    try:
        with conn.cursor() as cur:
            cur.execute(MIGRATION)
            cur.execute(CORRECTIVE_MIGRATION)
            cur.execute(CORRECTIVE_MIGRATION)
            cur.execute(GENERATION_MIGRATION)
            cur.execute(GENERATION_MIGRATION)
            cur.execute("SELECT count(*) FROM paper_managed_equity_observation_v1")
            assert cur.fetchone()[0] == 0
            cur.execute(
                """SELECT count(*) FROM pg_trigger
                   WHERE tgname IN (
                     'trg_paper_drawdown_activation_v1_append_only',
                     'trg_paper_managed_equity_observation_v1_append_only',
                     'trg_paper_managed_equity_observation_v1_validate',
                     'trg_paper_drawdown_generation_selection_v1_validate',
                     'trg_paper_drawdown_generation_selection_v1_append_only'
                   ) AND NOT tgisinternal"""
            )
            assert cur.fetchone()[0] == 5
            cur.execute(
                "SELECT count(*) FROM paper_drawdown_history_generation_selection_v1"
            )
            assert cur.fetchone()[0] == 0
        conn.commit()
        activation, baseline = prepare(conn)
        item = candidate(activation, baseline, 0, "100", "BASELINE_ACTIVATION")
        with conn.cursor() as cur:
            assert persist_observation_candidate(cur, item).status == "CANONICAL"
        conn.commit()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="FORWARD_BOUNDARY_INVALID"):
                cur.execute(
                    """INSERT INTO paper_managed_equity_observation_v1(
                         activation_id,baseline_id,deployment_id,observed_at,
                         observation_bucket_at,observation_trigger,trigger_reference,
                         observation_identity,managed_equity,managed_equity_status,
                         realized_pnl,realized_pnl_status,unrealized_pnl,
                         unrealized_pnl_status,baseline_activation_fingerprint,
                         portfolio_state_fingerprint,source_fingerprints,
                         portfolio_state_evidence,evidence_fingerprint,
                         history_evidence_status,producer_identity,git_revision,
                         contract_version
                       ) SELECT activation_id,baseline_id,deployment_id,
                         observed_at-INTERVAL '1 second',observation_bucket_at,
                         observation_trigger,'forbidden-backfill',%s,
                         managed_equity,managed_equity_status,realized_pnl,
                         realized_pnl_status,unrealized_pnl,unrealized_pnl_status,
                         baseline_activation_fingerprint,portfolio_state_fingerprint,
                         source_fingerprints,portfolio_state_evidence,evidence_fingerprint,
                         history_evidence_status,producer_identity,git_revision,
                         contract_version
                       FROM paper_managed_equity_observation_v1 LIMIT 1""",
                    ("e" * 64,),
                )
        conn.rollback()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    "UPDATE paper_managed_equity_observation_v1 SET managed_equity=101"
                )
        conn.rollback()
    finally:
        conn.close()


def test_numeric_38_18_roundtrip_and_confirmed_natural_first_observation(
    disposable_postgres_v16,
):
    conn = database(
        disposable_postgres_v16,
        "waltrade_baseline_test_paper_dd_numeric_scale_",
    )
    try:
        values = (
            "785.153275660783716231082848",
            "1.0000000000000000005",
            "-1.0000000000000000005",
            "0.0000000000000000004",
            "-0.0000000000000000005",
        )
        with conn.cursor() as cur:
            for raw in values:
                cur.execute("SELECT %s::NUMERIC(38,18)", (raw,))
                assert cur.fetchone()[0] == canonical_numeric_38_18(D(raw))

            cur.execute(
                """INSERT INTO paper_equity_baseline_v2(
                     deployment_id,baseline_version,baseline_timestamp,
                     baseline_account_total,baseline_managed_equity,
                     baseline_external_manual,baseline_available,
                     baseline_inventory_value,baseline_realized_net_pnl,
                     baseline_unrealized_pnl,baseline_fees,baseline_open_positions,
                     frozen_pre_baseline_unresolved_count,evidence_status,
                     source_authority,approved_by,approval_provenance,
                     activation_fingerprint
                   ) VALUES (
                     'local-paper','PAPER_EQUITY_BASELINE_V2',%s,1000,
                     925.729373904606433753,0,1000,0,0,
                     0.197552812000000000,0,0,0,'COMPLETE','CANONICAL',
                     'PO',%s::jsonb,%s
                   )""",
                (T0 - timedelta(days=1), json.dumps({"approved": True}), ACTIVATION_FP),
            )
            activation = ensure_activation_cursor(
                cur, deployment_id="local-paper", now=T0,
                producer_identity="test", git_revision=REVISION,
            )
            baseline = fetch_paper_equity_baseline_v2(
                cur, deployment_id="local-paper"
            )
            state = State(T0, "785.153275660783716231082848")
            state.realized_pnl = D("-139.783285143822717521917152")
            state.unrealized_pnl = D("-0.595260288")
            captured = capture_observation_candidate(
                state=state, baseline=baseline, activation=activation,
                observed_at=T0, observation_trigger="BASELINE_ACTIVATION",
                trigger_reference="natural-first-observation",
                producer_identity="test", git_revision=REVISION,
            )
            assert captured.status == "CANONICAL"
            persisted = persist_observation_candidate(cur, captured.candidate)
            assert persisted.status == "CANONICAL"
            cur.execute(
                """SELECT managed_equity,portfolio_state_evidence->>'total_capital'
                   FROM paper_managed_equity_observation_v1
                   WHERE observation_id=%s""",
                (persisted.observation_id,),
            )
            stored, raw_source = cur.fetchone()
            assert stored == D("785.153275660783716231")
            assert raw_source == "785.153275660783716231082848"
        conn.rollback()
    finally:
        conn.close()


def test_existing_generation_one_fingerprint_remains_readable_without_rewrite(
    disposable_postgres_v16,
):
    conn = database(
        disposable_postgres_v16,
        "waltrade_baseline_test_paper_dd_legacy_generation_one_",
    )
    try:
        activation, baseline = prepare(conn)
        item = candidate(
            activation, baseline, 0, "100", "BASELINE_ACTIVATION"
        )
        with conn.cursor() as cur:
            legacy_fingerprint = insert_legacy_generation_one_candidate(cur, item)
        conn.commit()
        with conn.cursor() as cur:
            history = read_paper_drawdown_history(
                cur, deployment_id="local-paper", as_of=T0, generation=1,
            )
            assert history.history_status == "CANONICAL"
            assert history.activation_generation == 1
            cur.execute(
                """SELECT evidence_fingerprint
                   FROM paper_managed_equity_observation_v1"""
            )
            assert cur.fetchone()[0] == legacy_fingerprint
        conn.rollback()
    finally:
        conn.close()


def test_idempotent_replay_conflict_fingerprint_and_incomplete_do_not_persist(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_paper_dd_identity_")
    try:
        activation, baseline = prepare(conn)
        item = candidate(activation, baseline, 0, "100", "BASELINE_ACTIVATION")
        with conn.cursor() as cur:
            first = persist_observation_candidate(cur, item)
            replay = persist_observation_candidate(cur, item)
            assert first.status == replay.status == "CANONICAL"
            assert first.observation_id == replay.observation_id
            conflict = replace(item, managed_equity=D("101"))
            assert persist_observation_candidate(cur, conflict).status == "SOURCE_FINGERPRINT_MISMATCH"
            mismatch = replace(item, portfolio_state_fingerprint="f" * 64)
            assert persist_observation_candidate(cur, mismatch).status == "SOURCE_FINGERPRINT_MISMATCH"
            incomplete = capture_observation_candidate(
                state=State(T0 + timedelta(minutes=15), "100", realized_status="INCOMPLETE"),
                baseline=baseline, activation=activation,
                observed_at=T0 + timedelta(minutes=15),
                observation_trigger="CADENCE_15M", trigger_reference="incomplete",
                producer_identity="test", git_revision=REVISION,
            )
            assert incomplete.status == "INCOMPLETE_FINANCIAL_TRUTH"
            assert incomplete.candidate is None
            incomplete_state = State(T0 + timedelta(minutes=15), "100")
            incomplete_state.total_capital_status = "INCOMPLETE"
            state_result = capture_observation_candidate(
                state=incomplete_state, baseline=baseline, activation=activation,
                observed_at=T0 + timedelta(minutes=15),
                observation_trigger="CADENCE_15M",
                trigger_reference="incomplete-state",
                producer_identity="test", git_revision=REVISION,
            )
            assert state_result.status == "INCOMPLETE_PORTFOLIO_STATE"
            assert state_result.candidate is None
            cur.execute("SELECT count(*) FROM paper_managed_equity_observation_v1")
            assert cur.fetchone()[0] == 1
        conn.commit()
    finally:
        conn.close()


def test_activation_is_exact_forward_cutover_and_ft_activity_is_coalesced(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_paper_dd_trigger_")
    try:
        insert_baseline(conn)
        cutover = T0 + timedelta(minutes=7, seconds=13)
        with conn.cursor() as cur:
            activation = ensure_activation_cursor(
                cur, deployment_id="local-paper", now=cutover,
                producer_identity="test", git_revision=REVISION,
            )
            assert activation.activated_at == cutover
            assert activation.activation_bucket_at == T0
            cur.execute(
                """INSERT INTO canonical_financial_truth_v1 VALUES
                   (1,'COMPLETE',%s,%s),(2,'COMPLETE',%s,%s)""",
                (
                    cutover + timedelta(minutes=1), "1" * 64,
                    cutover + timedelta(minutes=2), "2" * 64,
                ),
            )
            triggers = select_observation_triggers_cursor(
                cur, activation=activation, now=cutover + timedelta(minutes=3),
            )
            assert [row[0] for row in triggers] == [
                "BASELINE_ACTIVATION", "FINANCIAL_TRUTH_COMPLETE"
            ]
            assert "position:2:" in triggers[1][1]
            assert triggers[1][2] == cutover + timedelta(minutes=2)
        conn.rollback()
    finally:
        conn.close()


def test_read_model_peak_drawdown_max_recovery_and_gap(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_paper_dd_read_")
    try:
        activation, baseline = prepare(conn)
        sequence = (
            candidate(activation, baseline, 0, "100", "BASELINE_ACTIVATION"),
            candidate(activation, baseline, 15, "120"),
            candidate(activation, baseline, 30, "110"),
            candidate(activation, baseline, 45, "90"),
            candidate(activation, baseline, 60, "120"),
            candidate(activation, baseline, 75, "130"),
        )
        with conn.cursor() as cur:
            for item in sequence:
                assert persist_observation_candidate(cur, item).status == "CANONICAL"
        conn.commit()
        with conn.cursor() as cur:
            history = read_paper_drawdown_history(
                cur, deployment_id="local-paper", as_of=T0 + timedelta(minutes=75)
            )
            assert history.history_status == "CANONICAL"
            assert history.peak_flow_adjusted_equity == D("130")
            assert history.current_drawdown_abs == D("0")
            assert history.max_drawdown_abs == D("-30")
            assert history.max_drawdown_pct == D("-25")
            assert history.recovery_status == "RECOVERED"
            assert history.recovery_timestamp == T0 + timedelta(minutes=60)
            assert history.source_fingerprint is not None
        conn.rollback()
    finally:
        conn.close()


def test_gap_does_not_bridge_and_no_legacy_backfill_source_exists(disposable_postgres_v16):
    conn = database(disposable_postgres_v16, "waltrade_baseline_test_paper_dd_gap_")
    try:
        activation, baseline = prepare(conn)
        with conn.cursor() as cur:
            for item in (
                candidate(activation, baseline, 0, "100", "BASELINE_ACTIVATION"),
                candidate(activation, baseline, 15, "100"),
                candidate(activation, baseline, 45, "99"),
            ):
                persist_observation_candidate(cur, item)
        conn.commit()
        with conn.cursor() as cur:
            history = read_paper_drawdown_history(
                cur, deployment_id="local-paper", as_of=T0 + timedelta(minutes=45)
            )
            assert history.history_status == "OBSERVATION_GAP"
            cur.execute("SELECT to_regclass('public.equity_daily_snapshot_v1')")
            assert cur.fetchone()[0] is None
        conn.rollback()
    finally:
        conn.close()


def test_immutable_generation_supersession_isolated_active_only_and_fail_closed(
    disposable_postgres_v16,
):
    conn = database(
        disposable_postgres_v16,
        "waltrade_baseline_test_paper_dd_generation_",
    )
    try:
        generation_one, baseline = prepare(conn)
        assert generation_one.generation == 1
        with conn.cursor() as cur:
            for item in (
                candidate(
                    generation_one, baseline, 0, "100", "BASELINE_ACTIVATION"
                ),
                candidate(generation_one, baseline, 30, "99"),
            ):
                assert persist_observation_candidate(cur, item).status == "CANONICAL"
        conn.commit()
        with conn.cursor() as cur:
            old_history = read_paper_drawdown_history(
                cur, deployment_id="local-paper",
                as_of=T0 + timedelta(minutes=45), generation=1,
            )
            assert old_history.history_status == "OBSERVATION_GAP"
            cur.execute(
                """SELECT activation_evidence_fingerprint,activation_evidence
                   FROM paper_drawdown_history_activation_v1
                   WHERE activation_id=%s""",
                (generation_one.activation_id,),
            )
            old_activation_evidence = cur.fetchone()
            cur.execute(
                """SELECT observation_id,evidence_fingerprint
                   FROM paper_managed_equity_observation_v1
                   WHERE activation_id=%s ORDER BY observation_id""",
                (generation_one.activation_id,),
            )
            old_observations = cur.fetchall()
            generation_two = create_activation_generation_cursor(
                cur, deployment_id="local-paper",
                cutover_at=T0 + timedelta(minutes=45),
                selection_reason=(
                    "FAILED_INITIAL_FORWARD_ACTIVATION_AFTER_PRECISION_DEFECT"
                ),
                approval_evidence={
                    "approved": True,
                    "approved_by": "Product Owner",
                    "incident": "NUMERIC_SCALE_PRECISION_DEFECT",
                },
                expected_previous_status="OBSERVATION_GAP",
                producer_identity="test", git_revision=REVISION,
            )
            assert generation_two.generation == 2
            assert generation_two.baseline_id == generation_one.baseline_id
            active = get_active_paper_drawdown_history_generation_cursor(
                cur, baseline_id=baseline.baseline_id
            )
            assert active.status == "CANONICAL"
            assert active.activation.activation_id == generation_two.activation_id
            producer_active = ensure_activation_cursor(
                cur, deployment_id="local-paper",
                now=T0 + timedelta(minutes=60),
                producer_identity="test", git_revision=REVISION,
            )
            assert producer_active.activation_id == generation_two.activation_id
            assert producer_active.generation == 2
            cur.execute(
                """SELECT count(*) FROM paper_managed_equity_observation_v1
                   WHERE activation_id=%s""",
                (generation_two.activation_id,),
            )
            assert cur.fetchone()[0] == 0
            for item in (
                candidate(
                    producer_active, baseline, 45, "101", "BASELINE_ACTIVATION"
                ),
                candidate(producer_active, baseline, 60, "102"),
                candidate(generation_one, baseline, 60, "80"),
            ):
                assert persist_observation_candidate(cur, item).status == "CANONICAL"
        conn.commit()

        with conn.cursor() as cur:
            active_history = read_paper_drawdown_history(
                cur, deployment_id="local-paper",
                as_of=T0 + timedelta(minutes=60),
            )
            historical = read_paper_drawdown_history(
                cur, deployment_id="local-paper",
                as_of=T0 + timedelta(minutes=60), generation=1,
            )
            assert active_history.history_status == "CANONICAL"
            assert active_history.activation_generation == 2
            assert active_history.current_managed_equity == D("102")
            assert historical.history_status == "OBSERVATION_GAP"
            assert historical.activation_generation == 1
            assert active_history.source_fingerprint != historical.source_fingerprint
            cur.execute(
                """SELECT count(DISTINCT activation_id),count(*)
                   FROM paper_managed_equity_observation_v1
                   WHERE observation_bucket_at=%s""",
                (T0 + timedelta(minutes=60),),
            )
            assert cur.fetchone() == (2, 2)
            cur.execute(
                """SELECT activation_evidence_fingerprint,activation_evidence
                   FROM paper_drawdown_history_activation_v1
                   WHERE activation_id=%s""",
                (generation_one.activation_id,),
            )
            assert cur.fetchone() == old_activation_evidence
            cur.execute(
                """SELECT observation_id,evidence_fingerprint
                   FROM paper_managed_equity_observation_v1
                   WHERE activation_id=%s AND observation_id<=%s
                   ORDER BY observation_id""",
                (generation_one.activation_id, old_observations[-1][0]),
            )
            assert cur.fetchall() == old_observations
            cur.execute(
                """SELECT baseline_id,count(*) FROM paper_equity_baseline_v2
                   GROUP BY baseline_id"""
            )
            assert cur.fetchall() == [(baseline.baseline_id, 1)]
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    """UPDATE paper_drawdown_history_generation_selection_v1
                       SET selection_reason='FORBIDDEN' WHERE generation=1"""
                )
        conn.rollback()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="APPEND_ONLY"):
                cur.execute(
                    """UPDATE paper_drawdown_history_activation_v1
                       SET generation=7 WHERE activation_id=%s""",
                    (generation_one.activation_id,),
                )
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO paper_drawdown_history_activation_v1(
                     baseline_id,deployment_id,activated_at,activation_bucket_at,
                     baseline_activation_fingerprint,activation_identity,
                     activation_evidence_fingerprint,activation_evidence,
                     producer_identity,git_revision,contract_version,generation
                   ) VALUES (%s,'local-paper',%s,%s,%s,%s,%s,%s::jsonb,
                     'test',%s,%s,3)""",
                (
                    baseline.baseline_id, T0 + timedelta(minutes=75),
                    T0 + timedelta(minutes=75), ACTIVATION_FP, "9" * 64,
                    "8" * 64, json.dumps({"unselected": True}), REVISION,
                    CONTRACT_VERSION,
                ),
            )
            ambiguous = get_active_paper_drawdown_history_generation_cursor(
                cur, baseline_id=baseline.baseline_id
            )
            assert ambiguous.status == "AMBIGUOUS_ACTIVE_GENERATION"
            failed = read_paper_drawdown_history(
                cur, deployment_id="local-paper",
                as_of=T0 + timedelta(minutes=75),
            )
            assert failed.history_status == "AMBIGUOUS_ACTIVE_GENERATION"
            assert failed.current_managed_equity is None
        conn.rollback()
    finally:
        conn.close()
