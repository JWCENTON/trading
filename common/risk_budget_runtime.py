"""Runtime adapters for observational Risk Budget V1 evidence.

The module is PAPER-fenced and append-only.  It has no API that can alter an
order, quantity, decision, reservation, or Pre-Entry Risk lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import uuid
from typing import Any, Callable

from common.capital_reservation import paper_account_identity_fingerprint
from common.db import get_db_conn
from common.portfolio_state import PortfolioStateV1, read_portfolio_state
from common.pre_entry_risk import load_committed_pre_entry_risk_evidence_cursor
from common.risk_budget import (
    AdvisoryDecision,
    PersistResult,
    RiskBudgetInputs,
    RiskBudgetSnapshot,
    evaluate_pre_entry_gate,
    evaluate_state,
    fingerprint,
    missing_numeric_policy_evidence,
    persist_event_cursor,
)
from common.thesis_evidence_bundle import canonical_evidence_cutoff


PRODUCER_IDENTITY = "automation-runner:RISK_BUDGET_AUTHORITY_V1"
SHADOW_PRODUCER_IDENTITY = "paper-entry:RISK_BUDGET_AUTHORITY_V1"
PAPER_DRAWDOWN_HISTORY_STATUS = "INCOMPLETE_DRAWDOWN_HISTORY"
RISK_BUDGET_EXECUTION_INFLUENCE = False
_last_state_evaluation_cutoff: datetime | None = None


@dataclass(frozen=True)
class StateEvaluationResult:
    status: str
    boundary: datetime
    authority_status: str | None = None
    persisted: PersistResult | None = None


@dataclass(frozen=True)
class ShadowGateResult:
    status: str
    snapshot: RiskBudgetSnapshot | None = None
    decision: AdvisoryDecision | None = None
    persisted: PersistResult | None = None


def risk_budget_schema_available_cursor(cur: Any) -> bool:
    cur.execute("SELECT to_regclass('public.risk_budget_event_v1')")
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def _paper_current_drawdown_evidence_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
    state: PortfolioStateV1,
) -> tuple[Decimal | None, Decimal | None, str, dict[str, Any]]:
    """Expose current PAPER drawdown and state missing forward history exactly."""
    cur.execute(
        """
        SELECT b.baseline_managed_equity,
               (SELECT MAX(s.waltrade_managed_equity_usdc)
                  FROM equity_daily_snapshot_v1 s
                 WHERE s.deployment_id=b.deployment_id
                   AND s.evidence_status='COMPLETE'
                   AND s.source_timestamp>=b.baseline_timestamp
                   AND s.source_timestamp<=%s)
          FROM paper_equity_baseline_v2 b
         WHERE b.deployment_id=%s
           AND b.baseline_version='PAPER_EQUITY_BASELINE_V2'
           AND b.evidence_status='COMPLETE'
           AND b.baseline_timestamp<=%s
        """,
        (as_of, deployment_id, as_of),
    )
    row = cur.fetchone()
    current_abs = None
    peak = None
    if (
        row is not None and row[0] is not None
        and state.total_capital_status == "CANONICAL"
        and state.total_capital is not None
        and state.drawdown_status == "CANONICAL"
    ):
        peak = max(
            Decimal(str(row[0])),
            Decimal(str(row[1])) if row[1] is not None else Decimal(str(row[0])),
            state.total_capital,
        )
        current_abs = state.total_capital - peak
    evidence = {
        "authority": "PORTFOLIO_STATE_V1/PAPER_EQUITY_BASELINE_V2",
        "current_drawdown_abs": current_abs,
        "current_drawdown_pct": state.drawdown,
        "current_peak": peak,
        "full_forward_history": "UNAVAILABLE",
        "max_drawdown_abs": None,
        "max_drawdown_pct": None,
        "recovery_status": None,
        "status": PAPER_DRAWDOWN_HISTORY_STATUS,
    }
    return current_abs, state.drawdown, PAPER_DRAWDOWN_HISTORY_STATUS, evidence


def load_canonical_risk_budget_inputs_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
    runtime_revision: str | None = None,
    exclude_pre_entry_risk_id: uuid.UUID | None = None,
    portfolio_state_reader: Callable[..., PortfolioStateV1] = read_portfolio_state,
) -> RiskBudgetInputs:
    """Adapt existing authorities; never derives risk from capital reservation."""
    deployment = str(deployment_id).strip().lower()
    if deployment not in {"local-paper", "vps-paper"}:
        raise ValueError("RISK_BUDGET_PAPER_DEPLOYMENT_REQUIRED")
    if as_of.tzinfo is None:
        raise ValueError("RISK_BUDGET_AS_OF_TIMEZONE_REQUIRED")
    account = paper_account_identity_fingerprint(deployment)
    state = portfolio_state_reader(
        cur, environment="PAPER", deployment_id=deployment, as_of=as_of,
        runtime_revision=runtime_revision,
    )
    committed = load_committed_pre_entry_risk_evidence_cursor(
        cur, environment="PAPER", deployment_id=deployment,
        account_identity_fingerprint=account,
        exclude_pre_entry_risk_id=exclude_pre_entry_risk_id,
    )
    current_abs, current_pct, history_status, history_evidence = (
        _paper_current_drawdown_evidence_cursor(
            cur, deployment_id=deployment, as_of=as_of, state=state,
        )
    )
    state_payload = state.serializable()
    return RiskBudgetInputs(
        environment="PAPER", deployment_id=deployment,
        account_identity_fingerprint=account,
        as_of=as_of.astimezone(timezone.utc),
        total_capital=state.total_capital,
        total_capital_status=state.total_capital_status,
        open_risk=state.open_risk,
        open_risk_status=state.open_risk_status,
        pre_entry_committed_risk=committed.total_pre_entry_risk,
        pre_entry_risk_status=committed.evidence_status,
        current_drawdown_abs=current_abs,
        current_drawdown_pct=current_pct,
        max_drawdown_abs=None,
        max_drawdown_pct=None,
        recovery_status=None,
        drawdown_history_status=history_status,
        source_fingerprints={
            "portfolio_state": fingerprint(state_payload),
            "open_risk": fingerprint({
                "authority": "PORTFOLIO_STATE_V1.OPEN_RISK",
                "value": state.open_risk,
                "status": state.open_risk_status,
                "as_of": state.as_of,
            }),
            "pre_entry_risk": fingerprint({
                "authority": "PRE_ENTRY_RISK_AUTHORITY_V1",
                "value": committed.total_pre_entry_risk,
                "active_commitment_count": committed.active_commitment_count,
                "status": committed.evidence_status,
                "excluded_candidate": (
                    None if exclude_pre_entry_risk_id is None
                    else str(exclude_pre_entry_risk_id)
                ),
            }),
            "drawdown_history": fingerprint(history_evidence),
        },
    )


def persist_state_evaluation_cursor(
    cur: Any, *, deployment_id: str, boundary: datetime, as_of: datetime,
    git_revision: str,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
) -> StateEvaluationResult:
    if not risk_budget_schema_available_cursor(cur):
        return StateEvaluationResult("SCHEMA_UNAVAILABLE", boundary)
    identity = f"AUTOMATION_5M:{boundary.astimezone(timezone.utc).isoformat()}"
    inputs = input_loader(
        cur, deployment_id=deployment_id, as_of=as_of,
        runtime_revision=git_revision,
    )
    snapshot = evaluate_state(inputs, missing_numeric_policy_evidence())
    persisted = persist_event_cursor(
        cur, snapshot, event_type="STATE_EVALUATION",
        event_identity=identity, producer_identity=PRODUCER_IDENTITY,
        git_revision=git_revision,
    )
    return StateEvaluationResult(
        persisted.status, boundary, snapshot.authority_status, persisted,
    )


def run_paper_risk_budget_state_evaluation_cycle(
    *, now: datetime | None = None,
) -> StateEvaluationResult:
    """Append at most one state evaluation for each existing closed 5m cutoff."""
    global _last_state_evaluation_cutoff
    observed_at = now or datetime.now(timezone.utc)
    boundary = canonical_evidence_cutoff(observed_at)
    if str(os.getenv("TRADING_MODE", "")).upper() != "PAPER":
        return StateEvaluationResult("ENVIRONMENT_FENCE", boundary)
    deployment = str(os.getenv("DEPLOYMENT_ID", "")).strip().lower()
    if deployment not in {"local-paper", "vps-paper"}:
        return StateEvaluationResult("ENVIRONMENT_FENCE", boundary)
    if boundary == _last_state_evaluation_cutoff:
        return StateEvaluationResult("ALREADY_ATTEMPTED_FOR_CUTOFF", boundary)
    revision = str(os.getenv("GIT_SHA", "")).strip().lower()
    conn = get_db_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            result = persist_state_evaluation_cursor(
                cur, deployment_id=deployment, boundary=boundary,
                as_of=boundary, git_revision=revision,
            )
        if result.status == "SCHEMA_UNAVAILABLE":
            conn.rollback()
        else:
            conn.commit()
        _last_state_evaluation_cutoff = boundary
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_paper_pre_entry_shadow_gate_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
) -> ShadowGateResult:
    """Persist advisory evidence after freeze and before caller transaction commit."""
    if not risk_budget_schema_available_cursor(cur):
        return ShadowGateResult("SCHEMA_UNAVAILABLE")
    cur.execute(
        "SELECT total_pre_entry_risk,evidence_fingerprint,"
        "account_identity_fingerprint,decision_id,evidence_status "
        "FROM v_pre_entry_risk_current_v1 WHERE pre_entry_risk_id=%s",
        (str(pre_entry_risk_id),),
    )
    row = cur.fetchone()
    if row is None or str(row[4]) != "CANONICAL":
        return ShadowGateResult("CANDIDATE_EVIDENCE_INCOMPLETE")
    candidate_risk = Decimal(str(row[0]))
    candidate_fp = str(row[1])
    account = str(row[2])
    decision_identity = str(row[3])
    return persist_shadow_gate_evaluation_cursor(
        cur, pre_entry_risk_id=pre_entry_risk_id,
        deployment_id=deployment_id, as_of=as_of,
        git_revision=git_revision, candidate_pre_entry_risk=candidate_risk,
        candidate_evidence_fingerprint=candidate_fp,
        candidate_account_identity_fingerprint=account,
        decision_identity=decision_identity, input_loader=input_loader,
    )


def persist_shadow_gate_evaluation_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str,
    candidate_pre_entry_risk: Decimal,
    candidate_evidence_fingerprint: str,
    candidate_account_identity_fingerprint: str,
    decision_identity: str,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
) -> ShadowGateResult:
    """Evaluate/persist a candidate without locking or influencing execution."""
    inputs = input_loader(
        cur, deployment_id=deployment_id, as_of=as_of,
        runtime_revision=git_revision,
        exclude_pre_entry_risk_id=pre_entry_risk_id,
    )
    snapshot = evaluate_state(inputs, missing_numeric_policy_evidence())
    decision = evaluate_pre_entry_gate(
        snapshot, candidate_pre_entry_risk=candidate_pre_entry_risk,
        candidate_evidence_fingerprint=candidate_evidence_fingerprint,
        environment="PAPER", deployment_id=deployment_id,
        account_identity_fingerprint=candidate_account_identity_fingerprint,
    )
    persisted = persist_event_cursor(
        cur, snapshot, event_type="PRE_ENTRY_GATE_DECISION",
        event_identity=decision_identity, decision=decision,
        producer_identity=SHADOW_PRODUCER_IDENTITY,
        git_revision=git_revision,
    )
    return ShadowGateResult(persisted.status, snapshot, decision, persisted)


def record_paper_pre_entry_shadow_gate_fail_open_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str,
) -> ShadowGateResult:
    """Isolate observational SQL so evidence failure cannot abort execution."""
    savepoint = "risk_budget_shadow_gate_v1"
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        result = record_paper_pre_entry_shadow_gate_cursor(
            cur, pre_entry_risk_id=pre_entry_risk_id,
            deployment_id=deployment_id, as_of=as_of,
            git_revision=git_revision,
        )
    except Exception:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        logging.exception("risk_budget_shadow_gate_fail_open")
        result = ShadowGateResult("EVIDENCE_FAILURE_EXECUTION_UNCHANGED")
    cur.execute(f"RELEASE SAVEPOINT {savepoint}")
    return result
