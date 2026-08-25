"""Runtime adapters for observational Risk Budget V1 evidence.

The module is PAPER/LIVE append-only and has no API that can alter an order,
quantity, decision, reservation, or Pre-Entry Risk lifecycle.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
import os
import uuid
from typing import Any, Callable

from common.capital_reservation import paper_account_identity_fingerprint
from common.db import get_db_conn
from common.live_drawdown_history import (
    STALE_AFTER as LIVE_DRAWDOWN_STALE_AFTER,
    _fingerprint as live_drawdown_fingerprint,
    read_live_drawdown_history,
)
from common.live_managed_capital import load_live_managed_capital_evidence
from common.joint_authority_epoch import (
    RiskBudgetEpochBoundary,
    bind_risk_budget_event_cursor,
    resolve_risk_budget_boundary_cursor,
)
from common.owner_capital_flow_sync import load_owner_flow_history_authority
from common.paper_drawdown_history import read_paper_drawdown_history
from common.portfolio_state import PortfolioStateV1, read_portfolio_state
from common.pre_entry_risk import load_committed_pre_entry_risk_evidence_cursor
from common.risk_budget import (
    AdvisoryDecision,
    PersistResult,
    RiskBudgetInputs,
    RiskBudgetSnapshot,
    evaluate_and_persist_account_scoped_shadow_gate_cursor,
    evaluate_state,
    fingerprint,
    is_canonical_open_risk_status,
    missing_numeric_policy_evidence,
    persist_event_cursor,
)
from common.thesis_evidence_bundle import canonical_evidence_cutoff


PRODUCER_IDENTITY = "automation-runner:RISK_BUDGET_AUTHORITY_V1"
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
class CanonicalRiskEvaluationBoundary:
    status: str
    as_of: datetime | None
    account_identity_fingerprint: str | None = None
    authority_status: str | None = None


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


def resolve_live_canonical_risk_evaluation_boundary_cursor(
    cur: Any, *, deployment_id: str, scheduler_time: datetime,
) -> CanonicalRiskEvaluationBoundary:
    """Resolve the newest immutable LIVE boundary shared by upstream truth."""
    if scheduler_time.tzinfo is None:
        raise ValueError("RISK_BUDGET_SCHEDULER_TIMEZONE_REQUIRED")
    cur.execute(
        "SELECT to_regclass('public.v_live_drawdown_history_observation_v1'),"
        "to_regclass('public.v_owner_capital_flow_sync_authority_v1')"
    )
    schema = cur.fetchone()
    if not schema or schema[0] is None or schema[1] is None:
        return CanonicalRiskEvaluationBoundary(
            "SCHEMA_UNAVAILABLE", None, authority_status="INCOMPLETE_DRAWDOWN_HISTORY"
        )
    cur.execute(
        """SELECT observation.observed_at,
                  observation.account_identity_fingerprint,
                  observation.effective_history_status,
                  observation.portfolio_state_evidence,
                  observation.portfolio_state_fingerprint
           FROM v_live_drawdown_history_observation_v1 observation
           JOIN LATERAL (
             SELECT baseline_id,account_identity_fingerprint
             FROM live_managed_capital_baseline_v1
             WHERE environment='LIVE' AND deployment_id=%s
             ORDER BY accepted_at DESC LIMIT 1
           ) baseline ON baseline.baseline_id=observation.baseline_id
           WHERE observation.deployment_id=%s
           ORDER BY observation.observed_at DESC,observation.observation_id DESC
           LIMIT 1""",
        (deployment_id, deployment_id),
    )
    observation = cur.fetchone()
    if not observation:
        return CanonicalRiskEvaluationBoundary(
            "EXPECTED_WAITING_FOR_UPSTREAM_BOUNDARY", None,
            authority_status="INCOMPLETE_DRAWDOWN_HISTORY",
        )
    (
        observed_at, account, history_status, state_payload, state_fingerprint,
    ) = observation
    if str(history_status) != "CANONICAL":
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "INCOMPLETE_DRAWDOWN_HISTORY",
        )
    state_payload = dict(state_payload)
    if live_drawdown_fingerprint(state_payload) != str(state_fingerprint):
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "SOURCE_FINGERPRINT_MISMATCH",
        )
    if str(state_payload.get("total_capital_status")) != "CANONICAL":
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "INCOMPLETE_PORTFOLIO_STATE",
        )
    if not is_canonical_open_risk_status(state_payload.get("open_risk_status")):
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "INCOMPLETE_OPEN_RISK",
        )
    cur.execute(
        """SELECT status,sync_through
           FROM v_owner_capital_flow_sync_authority_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
             AND source='TRADING_ACCOUNT_BILLS'""",
        (deployment_id, str(account)),
    )
    flow = cur.fetchone()
    if not flow or str(flow[0]) != "CANONICAL":
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "INCOMPLETE_CAPITAL_FLOW",
        )
    if flow[1] is None or flow[1] < observed_at:
        return CanonicalRiskEvaluationBoundary(
            "EXPECTED_WAITING_FOR_UPSTREAM_BOUNDARY", observed_at, str(account),
            "INCOMPLETE_CAPITAL_FLOW",
        )
    committed = load_committed_pre_entry_risk_evidence_cursor(
        cur, environment="LIVE", deployment_id=deployment_id,
        account_identity_fingerprint=str(account), as_of=observed_at,
    )
    if committed.evidence_status != "CANONICAL":
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account),
            "INCOMPLETE_PRE_ENTRY_RISK",
        )
    if scheduler_time.astimezone(timezone.utc) - observed_at > LIVE_DRAWDOWN_STALE_AFTER:
        return CanonicalRiskEvaluationBoundary(
            "ACTUAL_STALE_AUTHORITY", observed_at, str(account), "STALE_AUTHORITY"
        )
    return CanonicalRiskEvaluationBoundary("CANONICAL", observed_at, str(account))


def _paper_current_drawdown_evidence_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
    state: PortfolioStateV1,
    history_reader: Callable[..., Any] = read_paper_drawdown_history,
) -> tuple[Any, str, dict[str, Any]]:
    """Consume only the immutable forward PAPER drawdown authority."""
    history = history_reader(cur, deployment_id=deployment_id, as_of=as_of)
    canonical = str(history.history_status) in {
        "CANONICAL", "ZERO_PEAK_PERCENT_UNAVAILABLE"
    }
    status = str(history.history_status) if canonical else PAPER_DRAWDOWN_HISTORY_STATUS
    evidence = {
        "authority": "PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1",
        "history": asdict(history),
        "status": status,
        "portfolio_state_total_capital_status": state.total_capital_status,
    }
    return history, status, evidence


def load_canonical_risk_budget_inputs_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
    runtime_revision: str | None = None,
    exclude_pre_entry_risk_id: uuid.UUID | None = None,
    portfolio_state_reader: Callable[..., PortfolioStateV1] = read_portfolio_state,
    exchange_client: Any | None = None,
    live_managed_loader: Callable[..., Any] = load_live_managed_capital_evidence,
    live_drawdown_reader: Callable[..., Any] = read_live_drawdown_history,
    paper_drawdown_reader: Callable[..., Any] = read_paper_drawdown_history,
    owner_flow_loader: Callable[..., Any] = load_owner_flow_history_authority,
    canonical_live_observation: bool = False,
) -> RiskBudgetInputs:
    """Adapt existing authorities; never derives risk from capital reservation."""
    deployment = str(deployment_id).strip().lower()
    if as_of.tzinfo is None:
        raise ValueError("RISK_BUDGET_AS_OF_TIMEZONE_REQUIRED")
    if deployment in {"local-live", "vps-live"}:
        if exchange_client is None:
            raise ValueError("RISK_BUDGET_LIVE_EXCHANGE_CLIENT_REQUIRED")
        if canonical_live_observation:
            return _load_live_observation_risk_budget_inputs_cursor(
                cur, deployment_id=deployment, as_of=as_of,
                exclude_pre_entry_risk_id=exclude_pre_entry_risk_id,
                live_drawdown_reader=live_drawdown_reader,
                owner_flow_loader=owner_flow_loader,
            )
        bundle = live_managed_loader(
            cur, exchange_client=exchange_client, deployment_id=deployment,
            as_of=as_of,
        )
        live_capital, baseline, _peak, context = bundle
        actual_account = str(context.snapshot.account_identity_fingerprint)
        account = (
            str(baseline.account_identity_fingerprint)
            if baseline is not None else actual_account
        )
        identity_status = (
            "CANONICAL" if baseline is not None and account == actual_account
            else "ACCOUNT_IDENTITY_MISMATCH"
        )
        state = portfolio_state_reader(
            cur, environment="LIVE", deployment_id=deployment, as_of=as_of,
            runtime_revision=runtime_revision, live_managed_bundle=bundle,
        )
        committed = load_committed_pre_entry_risk_evidence_cursor(
            cur, environment="LIVE", deployment_id=deployment,
            account_identity_fingerprint=account,
            exclude_pre_entry_risk_id=exclude_pre_entry_risk_id,
            as_of=as_of,
        )
        history = live_drawdown_reader(
            cur, deployment_id=deployment,
            account_identity_fingerprint=account, as_of=as_of,
        )
        owner_flow = owner_flow_loader(
            cur, deployment_id=deployment,
            account_identity_fingerprint=account, as_of=as_of,
        )
        history_status = str(history.history_status)
        if history_status == "CANONICAL" and (
            str(owner_flow.flow_history_status) != "CANONICAL"
            or str(live_capital.flow_history_status) != "CANONICAL"
        ):
            history_status = "INCOMPLETE_CAPITAL_FLOW"
        source_status = (
            "SOURCE_FINGERPRINT_MISMATCH"
            if history_status == "SOURCE_FINGERPRINT_MISMATCH"
            else "CANONICAL"
        )
        freshness_status = (
            "STALE_AUTHORITY"
            if history_status == "STALE_HISTORY"
            or str(owner_flow.flow_history_status) == "STALE_SYNC"
            else "CANONICAL"
        )
        history_evidence = {
            "authority": "LIVE_DRAWDOWN_HISTORY_AUTHORITY_V1",
            "history": asdict(history),
            "owner_flow": asdict(owner_flow),
            "managed_capital": asdict(live_capital),
            "baseline": None if baseline is None else asdict(baseline),
        }
        state_payload = state.serializable()
        return RiskBudgetInputs(
            environment="LIVE", deployment_id=deployment,
            account_identity_fingerprint=account,
            as_of=as_of.astimezone(timezone.utc),
            total_capital=state.total_capital,
            total_capital_status=state.total_capital_status,
            open_risk=state.open_risk,
            open_risk_status=state.open_risk_status,
            pre_entry_committed_risk=committed.total_pre_entry_risk,
            pre_entry_risk_status=committed.evidence_status,
            current_drawdown_abs=history.current_drawdown_abs,
            current_drawdown_pct=history.current_drawdown_pct,
            max_drawdown_abs=history.max_drawdown_abs,
            max_drawdown_pct=history.max_drawdown_pct,
            recovery_status=history.recovery_status,
            drawdown_history_status=history_status,
            source_fingerprints=_source_fingerprints(
                state, state_payload, committed, exclude_pre_entry_risk_id,
                history_evidence,
            ),
            identity_status=identity_status,
            source_fingerprint_status=source_status,
            freshness_status=freshness_status,
        )
    if deployment not in {"local-paper", "vps-paper"}:
        raise ValueError("RISK_BUDGET_DEPLOYMENT_REQUIRED")
    account = paper_account_identity_fingerprint(deployment)
    state = portfolio_state_reader(
        cur, environment="PAPER", deployment_id=deployment, as_of=as_of,
        runtime_revision=runtime_revision,
    )
    committed = load_committed_pre_entry_risk_evidence_cursor(
        cur, environment="PAPER", deployment_id=deployment,
        account_identity_fingerprint=account,
        exclude_pre_entry_risk_id=exclude_pre_entry_risk_id,
        as_of=as_of,
    )
    history, history_status, history_evidence = (
        _paper_current_drawdown_evidence_cursor(
            cur, deployment_id=deployment, as_of=as_of, state=state,
            history_reader=paper_drawdown_reader,
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
        current_drawdown_abs=history.current_drawdown_abs,
        current_drawdown_pct=history.current_drawdown_pct,
        max_drawdown_abs=history.max_drawdown_abs,
        max_drawdown_pct=history.max_drawdown_pct,
        recovery_status=(
            history.recovery_status
            if history_status != PAPER_DRAWDOWN_HISTORY_STATUS else None
        ),
        drawdown_history_status=history_status,
        source_fingerprints=_source_fingerprints(
            state, state_payload, committed, exclude_pre_entry_risk_id,
            history_evidence,
        ),
    )


def _load_live_observation_risk_budget_inputs_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
    exclude_pre_entry_risk_id: uuid.UUID | None,
    live_drawdown_reader: Callable[..., Any],
    owner_flow_loader: Callable[..., Any],
) -> RiskBudgetInputs:
    """Use an immutable drawdown observation as the shared LIVE time boundary."""
    cur.execute(
        """SELECT account_identity_fingerprint,portfolio_state_evidence,
                  portfolio_state_fingerprint,effective_history_status
           FROM v_live_drawdown_history_observation_v1
           WHERE deployment_id=%s AND observed_at=%s
           ORDER BY observation_id DESC LIMIT 1""",
        (deployment_id, as_of),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("CANONICAL_RISK_EVALUATION_BOUNDARY_UNAVAILABLE")
    account, state_payload, state_fingerprint, observation_status = row
    state_payload = dict(state_payload)
    fingerprint_status = (
        "CANONICAL"
        if live_drawdown_fingerprint(state_payload) == str(state_fingerprint)
        else "SOURCE_FINGERPRINT_MISMATCH"
    )
    history = live_drawdown_reader(
        cur, deployment_id=deployment_id,
        account_identity_fingerprint=str(account), as_of=as_of,
    )
    owner_flow = owner_flow_loader(
        cur, deployment_id=deployment_id,
        account_identity_fingerprint=str(account), as_of=as_of,
    )
    committed = load_committed_pre_entry_risk_evidence_cursor(
        cur, environment="LIVE", deployment_id=deployment_id,
        account_identity_fingerprint=str(account),
        exclude_pre_entry_risk_id=exclude_pre_entry_risk_id,
        as_of=as_of,
    )
    history_status = str(history.history_status)
    if str(observation_status) != "CANONICAL":
        history_status = "INCOMPLETE_DRAWDOWN_HISTORY"
    elif str(owner_flow.flow_history_status) != "CANONICAL":
        history_status = "INCOMPLETE_CAPITAL_FLOW"
    total_capital = state_payload.get("total_capital")
    open_risk = state_payload.get("open_risk")
    total_capital = None if total_capital is None else Decimal(str(total_capital))
    open_risk = None if open_risk is None else Decimal(str(open_risk))
    total_status = str(state_payload.get("total_capital_status") or "INCOMPLETE")
    open_status = str(state_payload.get("open_risk_status") or "INCOMPLETE")
    history_evidence = {
        "authority": "LIVE_DRAWDOWN_HISTORY_AUTHORITY_V1",
        "history": asdict(history),
        "owner_flow": asdict(owner_flow),
        "canonical_observation_as_of": as_of,
    }
    return RiskBudgetInputs(
        environment="LIVE", deployment_id=deployment_id,
        account_identity_fingerprint=str(account), as_of=as_of,
        total_capital=total_capital, total_capital_status=total_status,
        open_risk=open_risk, open_risk_status=open_status,
        pre_entry_committed_risk=committed.total_pre_entry_risk,
        pre_entry_risk_status=committed.evidence_status,
        current_drawdown_abs=history.current_drawdown_abs,
        current_drawdown_pct=history.current_drawdown_pct,
        max_drawdown_abs=history.max_drawdown_abs,
        max_drawdown_pct=history.max_drawdown_pct,
        recovery_status=history.recovery_status,
        drawdown_history_status=history_status,
        source_fingerprints={
            "portfolio_state": str(state_fingerprint),
            "open_risk": fingerprint({
                "authority": "PORTFOLIO_STATE_V1.OPEN_RISK",
                "value": open_risk, "status": open_status, "as_of": as_of,
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
        source_fingerprint_status=fingerprint_status,
        freshness_status=(
            "STALE_AUTHORITY"
            if history_status == "STALE_HISTORY"
            or str(owner_flow.flow_history_status) == "STALE_SYNC"
            else "CANONICAL"
        ),
    )


def _source_fingerprints(
    state: Any, state_payload: Any, committed: Any,
    excluded_candidate: uuid.UUID | None, history_evidence: Any,
) -> dict[str, str]:
    return {
        "portfolio_state": fingerprint(state_payload),
        "open_risk": fingerprint({
            "authority": "PORTFOLIO_STATE_V1.OPEN_RISK",
            "value": state.open_risk, "status": state.open_risk_status,
            "as_of": state.as_of,
        }),
        "pre_entry_risk": fingerprint({
            "authority": "PRE_ENTRY_RISK_AUTHORITY_V1",
            "value": committed.total_pre_entry_risk,
            "active_commitment_count": committed.active_commitment_count,
            "status": committed.evidence_status,
            "excluded_candidate": (
                None if excluded_candidate is None else str(excluded_candidate)
            ),
        }),
        "drawdown_history": fingerprint(history_evidence),
    }


def persist_state_evaluation_cursor(
    cur: Any, *, deployment_id: str, boundary: datetime, as_of: datetime,
    git_revision: str,
    exchange_client: Any | None = None,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
    canonical_live_observation: bool = False,
    paper_epoch_boundary: RiskBudgetEpochBoundary | None = None,
) -> StateEvaluationResult:
    if not risk_budget_schema_available_cursor(cur):
        return StateEvaluationResult("SCHEMA_UNAVAILABLE", boundary)
    identity_source = (
        "CANONICAL_UPSTREAM" if canonical_live_observation
        else "JOINT_AUTHORITY_EPOCH" if paper_epoch_boundary is not None
        else "AUTOMATION_5M"
    )
    identity = f"{identity_source}:{boundary.astimezone(timezone.utc).isoformat()}"
    loader_kwargs = dict(
        deployment_id=deployment_id, as_of=as_of,
        runtime_revision=git_revision,
    )
    if exchange_client is not None:
        loader_kwargs["exchange_client"] = exchange_client
    if canonical_live_observation:
        loader_kwargs["canonical_live_observation"] = True
    inputs = input_loader(cur, **loader_kwargs)
    snapshot = evaluate_state(inputs, missing_numeric_policy_evidence())
    if paper_epoch_boundary is not None and snapshot.authority_status not in {
        "CANONICAL", "MISSING_POLICY", "RISK_CAPACITY_EXHAUSTED",
    }:
        return StateEvaluationResult(
            snapshot.authority_status, boundary, snapshot.authority_status,
        )
    persisted = persist_event_cursor(
        cur, snapshot, event_type="STATE_EVALUATION",
        event_identity=identity, producer_identity=PRODUCER_IDENTITY,
        git_revision=git_revision,
    )
    if paper_epoch_boundary is not None:
        if paper_epoch_boundary.epoch is None:
            raise ValueError("RISK_BUDGET_AUTHORITY_EPOCH_REQUIRED")
        bind_risk_budget_event_cursor(
            cur, event_id=persisted.event_id,
            epoch=paper_epoch_boundary.epoch, evaluation_as_of=boundary,
            risk_budget_source_fingerprint=persisted.event_fingerprint,
        )
    return StateEvaluationResult(
        persisted.status, boundary, snapshot.authority_status, persisted,
    )


def run_risk_budget_state_evaluation_cycle(
    *, exchange_client: Any | None = None, now: datetime | None = None,
) -> StateEvaluationResult:
    """Append once per PAPER cutoff or jointly canonical LIVE boundary."""
    global _last_state_evaluation_cutoff
    observed_at = now or datetime.now(timezone.utc)
    boundary = canonical_evidence_cutoff(observed_at)
    mode = str(os.getenv("TRADING_MODE", "")).upper()
    if mode not in {"PAPER", "LIVE"}:
        return StateEvaluationResult("ENVIRONMENT_FENCE", boundary)
    deployment = str(os.getenv("DEPLOYMENT_ID", "")).strip().lower()
    expected = {
        "PAPER": {"local-paper", "vps-paper"},
        "LIVE": {"local-live", "vps-live"},
    }[mode]
    if deployment not in expected or (mode == "LIVE" and exchange_client is None):
        return StateEvaluationResult("ENVIRONMENT_FENCE", boundary)
    revision = str(os.getenv("GIT_SHA", "")).strip().lower()
    conn = get_db_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            canonical_live_observation = False
            if mode == "LIVE":
                upstream = resolve_live_canonical_risk_evaluation_boundary_cursor(
                    cur, deployment_id=deployment, scheduler_time=observed_at,
                )
                if upstream.status != "CANONICAL" or upstream.as_of is None:
                    conn.rollback()
                    return StateEvaluationResult(
                        upstream.status, upstream.as_of or boundary,
                        upstream.authority_status,
                    )
                boundary = upstream.as_of
                canonical_live_observation = True
            paper_epoch_boundary = None
            if mode == "PAPER":
                paper_epoch_boundary = resolve_risk_budget_boundary_cursor(
                    cur, deployment_id=deployment, scheduler_time=observed_at,
                )
                if (
                    paper_epoch_boundary.status != "CANONICAL"
                    or paper_epoch_boundary.as_of is None
                ):
                    conn.rollback()
                    return StateEvaluationResult(
                        paper_epoch_boundary.status,
                        paper_epoch_boundary.as_of or boundary,
                    )
                boundary = paper_epoch_boundary.as_of
            if boundary == _last_state_evaluation_cutoff:
                conn.rollback()
                return StateEvaluationResult(
                    "EXPECTED_WAITING_FOR_UPSTREAM_BOUNDARY"
                    if mode == "LIVE" else "ALREADY_ATTEMPTED_FOR_CUTOFF",
                    boundary,
                )
            result = persist_state_evaluation_cursor(
                cur, deployment_id=deployment, boundary=boundary,
                as_of=boundary, git_revision=revision,
                exchange_client=exchange_client,
                canonical_live_observation=canonical_live_observation,
                paper_epoch_boundary=paper_epoch_boundary,
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


def run_paper_risk_budget_state_evaluation_cycle(
    *, now: datetime | None = None,
) -> StateEvaluationResult:
    """Backward-compatible PAPER entry point."""
    return run_risk_budget_state_evaluation_cycle(now=now)


def record_pre_entry_shadow_gate_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
    exchange_client: Any | None = None,
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
        exchange_client=exchange_client,
    )


def persist_shadow_gate_evaluation_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str,
    candidate_pre_entry_risk: Decimal,
    candidate_evidence_fingerprint: str,
    candidate_account_identity_fingerprint: str,
    decision_identity: str,
    input_loader: Callable[..., RiskBudgetInputs] = load_canonical_risk_budget_inputs_cursor,
    exchange_client: Any | None = None,
) -> ShadowGateResult:
    """Lock, re-read, and persist a candidate without influencing execution."""
    environment = (
        "LIVE" if str(deployment_id).lower().endswith("-live") else "PAPER"
    )
    def fresh_input_loader(cursor: Any) -> RiskBudgetInputs:
        return input_loader(
            cursor, deployment_id=deployment_id, as_of=as_of,
            runtime_revision=git_revision,
            exclude_pre_entry_risk_id=pre_entry_risk_id,
            **({"exchange_client": exchange_client} if exchange_client is not None else {}),
        )

    snapshot, decision, persisted = (
        evaluate_and_persist_account_scoped_shadow_gate_cursor(
            cur, environment=environment, deployment_id=deployment_id,
            account_identity_fingerprint=candidate_account_identity_fingerprint,
            input_loader=fresh_input_loader,
            policy=missing_numeric_policy_evidence(),
            candidate_pre_entry_risk=candidate_pre_entry_risk,
            candidate_evidence_fingerprint=candidate_evidence_fingerprint,
            decision_identity=decision_identity,
            producer_identity=(
                f"{environment.lower()}-entry:RISK_BUDGET_AUTHORITY_V1"
            ),
            git_revision=git_revision,
        )
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


def record_paper_pre_entry_shadow_gate_cursor(*args: Any, **kwargs: Any) -> ShadowGateResult:
    """Backward-compatible PAPER wrapper."""
    return record_pre_entry_shadow_gate_cursor(*args, **kwargs)


def record_live_pre_entry_shadow_gate_fail_open_cursor(
    cur: Any, *, pre_entry_risk_id: uuid.UUID, deployment_id: str,
    as_of: datetime, git_revision: str, exchange_client: Any,
) -> ShadowGateResult:
    """Persist LIVE shadow evidence in a savepoint; never affect execution."""
    savepoint = "risk_budget_live_shadow_gate_v1"
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        result = record_pre_entry_shadow_gate_cursor(
            cur, pre_entry_risk_id=pre_entry_risk_id,
            deployment_id=deployment_id, as_of=as_of,
            git_revision=git_revision, exchange_client=exchange_client,
        )
    except Exception:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        logging.exception("risk_budget_live_shadow_gate_fail_open")
        result = ShadowGateResult("EVIDENCE_FAILURE_EXECUTION_UNCHANGED")
    cur.execute(f"RELEASE SAVEPOINT {savepoint}")
    return result
