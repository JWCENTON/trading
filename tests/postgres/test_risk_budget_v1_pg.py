"""Disposable PostgreSQL gates for Risk Budget Authority V1."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.risk_budget import (
    NumericPolicyEvidence,
    RiskBudgetIdempotencyConflict,
    RiskBudgetInputs,
    account_scope_lock_identity,
    evaluate_and_persist_account_scoped_shadow_gate_cursor,
    evaluate_account_scoped_shadow_gate_cursor,
    evaluate_state,
    persist_event_cursor,
)
from common.risk_budget_runtime import (
    persist_shadow_gate_evaluation_cursor,
    persist_state_evaluation_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260824_risk_budget_authority_v1.sql"
).read_text()
NOW = datetime(2026, 8, 24, 15, tzinfo=timezone.utc)
IDENTITY = "a" * 64
REVISION = "6" * 40


def database(disposable_postgres_v16):
    name = "waltrade_baseline_test_risk_budget_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(name)
    return name, disposable_postgres_v16.connect(name)


def apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def inputs(**changes):
    value = RiskBudgetInputs(
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=IDENTITY, as_of=NOW,
        total_capital=Decimal("100"), total_capital_status="CANONICAL",
        open_risk=Decimal("2"), open_risk_status="CANONICAL",
        pre_entry_committed_risk=Decimal("1"),
        pre_entry_risk_status="CANONICAL",
        current_drawdown_abs=Decimal("-5"),
        current_drawdown_pct=Decimal("-5"),
        max_drawdown_abs=Decimal("-10"), max_drawdown_pct=Decimal("-10"),
        recovery_status="IN_DRAWDOWN", drawdown_history_status="CANONICAL",
        source_fingerprints={
            "portfolio_state": "1" * 64, "open_risk": "2" * 64,
            "pre_entry_risk": "3" * 64, "drawdown_history": "4" * 64,
        },
    )
    return replace(value, **changes)


def policy(capacity=None):
    return NumericPolicyEvidence(
        policy_version="RISK_BUDGET_PRODUCT_POLICY_V1",
        policy_fingerprint="b" * 64,
        status="MISSING_POLICY" if capacity is None else "CANONICAL",
        policy_state=None if capacity is None else "NORMAL",
        total_risk_capacity=None if capacity is None else Decimal(capacity),
    )


def test_migration_is_idempotent_additive_empty_and_append_only(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        apply(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*),to_regclass('public.v_risk_budget_current_v1') "
                "FROM risk_budget_event_v1"
            )
            assert cur.fetchone() == (0, "v_risk_budget_current_v1")
            snapshot = evaluate_state(inputs(), policy())
            result = persist_event_cursor(
                cur, snapshot, event_type="STATE_EVALUATION",
                event_identity="state-1", producer_identity="pytest",
                git_revision=REVISION,
            )
            assert result.status == "INSERTED"
        conn.commit()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="RISK_BUDGET_AUTHORITY_V1_APPEND_ONLY"):
                cur.execute(
                    "UPDATE risk_budget_event_v1 SET producer_identity='changed' "
                    "WHERE event_identity='state-1'"
                )
        conn.rollback()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="RISK_BUDGET_AUTHORITY_V1_APPEND_ONLY"):
                cur.execute(
                    "DELETE FROM risk_budget_event_v1 WHERE event_identity='state-1'"
                )
        conn.rollback()
    finally:
        conn.close()


def test_state_and_gate_events_are_idempotent_and_conflicts_fail(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        snapshot = evaluate_state(inputs(), policy())
        with conn.cursor() as cur:
            first = persist_event_cursor(
                cur, snapshot, event_type="STATE_EVALUATION",
                event_identity="state-1", producer_identity="pytest",
                git_revision=REVISION,
            )
            second = persist_event_cursor(
                cur, snapshot, event_type="STATE_EVALUATION",
                event_identity="state-1", producer_identity="pytest",
                git_revision=REVISION,
            )
            assert first.status == "INSERTED"
            assert second.status == "IDEMPOTENT"
            _, decision, gate_first = (
                evaluate_and_persist_account_scoped_shadow_gate_cursor(
                    cur, environment="PAPER", deployment_id="local-paper",
                    account_identity_fingerprint=IDENTITY,
                    input_loader=lambda _cur: inputs(), policy=policy(),
                    candidate_pre_entry_risk=Decimal("0.5"),
                    candidate_evidence_fingerprint="c" * 64,
                    decision_identity="decision-1", producer_identity="pytest",
                    git_revision=REVISION,
                )
            )
            _, replay_decision, gate_second = (
                evaluate_and_persist_account_scoped_shadow_gate_cursor(
                    cur, environment="PAPER", deployment_id="local-paper",
                    account_identity_fingerprint=IDENTITY,
                    input_loader=lambda _cur: inputs(), policy=policy(),
                    candidate_pre_entry_risk=Decimal("0.5"),
                    candidate_evidence_fingerprint="c" * 64,
                    decision_identity="decision-1", producer_identity="pytest",
                    git_revision=REVISION,
                )
            )
            assert replay_decision == decision
            assert gate_first.status == "INSERTED"
            assert gate_second.status == "IDEMPOTENT"
            changed = replace(snapshot, total_capital=Decimal("101"))
            with pytest.raises(RiskBudgetIdempotencyConflict):
                persist_event_cursor(
                    cur, changed, event_type="STATE_EVALUATION",
                    event_identity="state-1", producer_identity="pytest",
                    git_revision=REVISION,
                )
        conn.rollback()
    finally:
        conn.close()


def test_runtime_state_producer_and_shadow_candidate_are_idempotent(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)

        def loader(_cur, **kwargs):
            return inputs()

        candidate_id = uuid.uuid4()
        with conn.cursor() as cur:
            state_first = persist_state_evaluation_cursor(
                cur, deployment_id="local-paper", boundary=NOW, as_of=NOW,
                git_revision=REVISION, input_loader=loader,
            )
            state_replay = persist_state_evaluation_cursor(
                cur, deployment_id="local-paper", boundary=NOW, as_of=NOW,
                git_revision=REVISION, input_loader=loader,
            )
            assert state_first.status == "INSERTED"
            assert state_first.authority_status == "MISSING_POLICY"
            assert state_replay.status == "IDEMPOTENT"
            with pytest.raises(RiskBudgetIdempotencyConflict):
                persist_state_evaluation_cursor(
                    cur, deployment_id="local-paper", boundary=NOW, as_of=NOW,
                    git_revision=REVISION,
                    input_loader=lambda _cur, **kwargs: replace(
                        inputs(), total_capital=Decimal("101")
                    ),
                )

            gate_first = persist_shadow_gate_evaluation_cursor(
                cur, pre_entry_risk_id=candidate_id,
                deployment_id="local-paper", as_of=NOW,
                git_revision=REVISION,
                candidate_pre_entry_risk=Decimal("0.5"),
                candidate_evidence_fingerprint="c" * 64,
                candidate_account_identity_fingerprint=IDENTITY,
                decision_identity="natural-decision-1", input_loader=loader,
            )
            gate_replay = persist_shadow_gate_evaluation_cursor(
                cur, pre_entry_risk_id=candidate_id,
                deployment_id="local-paper", as_of=NOW,
                git_revision=REVISION,
                candidate_pre_entry_risk=Decimal("0.5"),
                candidate_evidence_fingerprint="c" * 64,
                candidate_account_identity_fingerprint=IDENTITY,
                decision_identity="natural-decision-1", input_loader=loader,
            )
            assert gate_first.status == "INSERTED"
            assert gate_first.decision.result == "BLOCK_NEW_RISK"
            assert gate_first.decision.authority_status == "MISSING_POLICY"
            assert gate_replay.status == "IDEMPOTENT"
            cur.execute(
                "SELECT event_type,advisory_result,authority_status,"
                "total_risk_capacity,available_risk_capacity,policy_state "
                "FROM risk_budget_event_v1 ORDER BY event_type"
            )
            assert cur.fetchall() == [
                (
                    "PRE_ENTRY_GATE_DECISION", "BLOCK_NEW_RISK", "MISSING_POLICY",
                    None, None, None,
                ),
                ("STATE_EVALUATION", None, "MISSING_POLICY", None, None, None),
            ]
        conn.rollback()
    finally:
        conn.close()


def test_runtime_producer_persists_incomplete_truth_before_missing_policy(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        incomplete = replace(
            inputs(), drawdown_history_status="INCOMPLETE",
            max_drawdown_abs=None, max_drawdown_pct=None, recovery_status=None,
        )
        with conn.cursor() as cur:
            result = persist_state_evaluation_cursor(
                cur, deployment_id="local-paper", boundary=NOW, as_of=NOW,
                git_revision=REVISION,
                input_loader=lambda _cur, **kwargs: incomplete,
            )
            assert result.authority_status == "INCOMPLETE_DRAWDOWN_HISTORY"
            cur.execute(
                "SELECT authority_status,reason_codes FROM risk_budget_event_v1"
            )
            assert cur.fetchone() == (
                "INCOMPLETE_DRAWDOWN_HISTORY", ["INCOMPLETE_DRAWDOWN_HISTORY"],
            )
        conn.rollback()
    finally:
        conn.close()


def test_account_scope_transaction_lock_serializes_reread(disposable_postgres_v16):
    name, first = database(disposable_postgres_v16)
    second = disposable_postgres_v16.connect(name)
    try:
        apply(first)
        lock_identity = account_scope_lock_identity("PAPER", "local-paper", IDENTITY)
        with first.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (lock_identity,),
            )
        started = threading.Event()
        completed = threading.Event()
        result = {}

        def worker():
            try:
                with second.cursor() as cur:
                    started.set()
                    snapshot, decision = evaluate_account_scoped_shadow_gate_cursor(
                        cur, environment="PAPER", deployment_id="local-paper",
                        account_identity_fingerprint=IDENTITY,
                        input_loader=lambda _cur: inputs(), policy=policy(),
                        candidate_pre_entry_risk=Decimal("0.5"),
                        candidate_evidence_fingerprint="c" * 64,
                    )
                    result["value"] = (snapshot.used_risk, decision.result)
            finally:
                completed.set()

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        assert started.wait(timeout=2)
        time.sleep(0.2)
        assert not completed.is_set()
        first.rollback()
        assert completed.wait(timeout=3)
        thread.join(timeout=1)
        assert result["value"] == (Decimal("3"), "BLOCK_NEW_RISK")
        second.rollback()
    finally:
        first.close()
        second.close()
