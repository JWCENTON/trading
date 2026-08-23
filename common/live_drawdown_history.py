"""Forward-only LIVE managed-equity and drawdown history authority V1."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from common.live_managed_capital import (
    LiveManagedCapitalBaseline,
    LiveManagedCapitalEvidence,
    LiveManagedCapitalReadContext,
    canonical_json,
)
from common.owner_capital_flow_sync import (
    load_owner_flow_history_authority,
    record_reconciliation_resolution,
)
from common.portfolio_state import PortfolioStateV1


_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts/live_drawdown_history_authority_v1_contract.json"
)
_CONTRACT = json.loads(_CONTRACT_PATH.read_text())
CONTRACT_VERSION = str(_CONTRACT["contract_version"])
CADENCE = timedelta(seconds=int(_CONTRACT["cadence_seconds"]))
STALE_AFTER = timedelta(seconds=int(_CONTRACT["stale_after_seconds"]))
ZERO = Decimal("0")
HUNDRED = Decimal("100")

FAILURE_PRIORITY = (
    "ACCOUNT_IDENTITY_MISMATCH",
    "SOURCE_FINGERPRINT_MISMATCH",
    "INCOMPLETE_CAPITAL_FLOW",
    "INCOMPLETE_PORTFOLIO_STATE",
    "INCOMPLETE_MARK",
    "INCOMPLETE_FINANCIAL_TRUTH",
)


def _decimal(value: object) -> Decimal:
    if value is None or isinstance(value, float):
        raise ValueError("LIVE_DRAWDOWN_DECIMAL_REQUIRED")
    return Decimal(str(value))


def _decimal_text(value: object) -> str:
    decimal_value = _decimal(value)
    return "0" if decimal_value == ZERO else format(decimal_value.normalize(), "f")


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def cadence_bucket(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("LIVE_DRAWDOWN_TIMESTAMP_REQUIRED")
    utc = at.astimezone(timezone.utc)
    seconds = int(utc.timestamp())
    bucket = seconds - seconds % int(CADENCE.total_seconds())
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


@dataclass(frozen=True)
class DrawdownObservation:
    observed_at: datetime
    observation_bucket_at: datetime
    observation_trigger: str
    managed_equity: Decimal
    flow_adjusted_equity: Decimal
    history_status: str = "CANONICAL"


@dataclass(frozen=True)
class LiveDrawdownHistory:
    current_managed_equity: Decimal | None
    current_flow_adjusted_equity: Decimal | None
    peak_flow_adjusted_equity: Decimal | None
    current_drawdown_abs: Decimal | None
    current_drawdown_pct: Decimal | None
    max_drawdown_abs: Decimal | None
    max_drawdown_pct: Decimal | None
    recovery_status: str
    peak_timestamp: datetime | None
    drawdown_start: datetime | None
    recovery_timestamp: datetime | None
    drawdown_duration: timedelta | None
    history_status: str
    latest_observation_at: datetime | None


@dataclass(frozen=True)
class ObservationCandidate:
    baseline_id: int
    deployment_id: str
    account_identity_fingerprint: str
    baseline_activation_fingerprint: str
    observed_at: datetime
    observation_bucket_at: datetime
    observation_trigger: str
    trigger_reference: str
    observation_identity: str
    managed_equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    deployed_capital: Decimal
    reserved_capital: Decimal
    available_capital: Decimal
    portfolio_state_evidence: Mapping[str, Any]
    managed_capital_evidence: Mapping[str, Any]
    portfolio_state_fingerprint: str
    managed_capital_source_fingerprint: str
    producer_identity: str
    git_revision: str


@dataclass(frozen=True)
class CandidateResult:
    status: str
    candidate: ObservationCandidate | None


@dataclass(frozen=True)
class PersistenceResult:
    status: str
    observation_id: int | None


def calculate_drawdown_history(
    *, baseline_managed_equity: Decimal | None,
    baseline_at: datetime | None,
    observations: Iterable[DrawdownObservation],
    as_of: datetime,
) -> LiveDrawdownHistory:
    """Calculate signed drawdown and recovery from canonical timestamps only."""
    if as_of.tzinfo is None:
        raise ValueError("LIVE_DRAWDOWN_AS_OF_REQUIRED")
    if baseline_managed_equity is None or baseline_at is None:
        return LiveDrawdownHistory(
            None, None, None, None, None, None, None, "NO_HISTORY",
            None, None, None, None, "NO_BASELINE", None,
        )
    baseline = _decimal(baseline_managed_equity)
    rows = sorted(
        tuple(observations),
        key=lambda row: (row.observed_at, row.observation_bucket_at),
    )
    for failure in FAILURE_PRIORITY:
        if any(row.history_status == failure for row in rows):
            latest = max((row.observed_at for row in rows), default=None)
            return LiveDrawdownHistory(
                None, None, None, None, None, None, None, "NO_HISTORY",
                None, None, None, None, failure, latest,
            )
    canonical = [row for row in rows if row.history_status == "CANONICAL"]
    if not canonical:
        return LiveDrawdownHistory(
            None, None, baseline, None, None, None, None, "NO_HISTORY",
            baseline_at, None, None, None, "NO_HISTORY", None,
        )
    cadence_rows = sorted(
        (row for row in canonical if row.observation_trigger == "CADENCE_15M"),
        key=lambda row: row.observation_bucket_at,
    )
    if any(
        current.observation_bucket_at - previous.observation_bucket_at > CADENCE
        for previous, current in zip(cadence_rows, cadence_rows[1:])
    ):
        return LiveDrawdownHistory(
            None, None, None, None, None, None, None, "NO_HISTORY",
            None, None, None, None, "OBSERVATION_GAP",
            canonical[-1].observed_at,
        )
    if as_of - canonical[-1].observed_at > STALE_AFTER:
        return LiveDrawdownHistory(
            None, None, None, None, None, None, None, "NO_HISTORY",
            None, None, None, None, "STALE_HISTORY",
            canonical[-1].observed_at,
        )

    peak = baseline
    peak_at = baseline_at
    max_abs = ZERO
    max_pct: Decimal | None = ZERO if peak != ZERO else None
    episode_peak = peak
    episode_peak_at = peak_at
    active_start: datetime | None = None
    last_episode: tuple[datetime, datetime, datetime, timedelta] | None = None

    for row in canonical:
        adjusted = _decimal(row.flow_adjusted_equity)
        if adjusted > peak:
            peak = adjusted
            peak_at = row.observed_at
        drawdown_abs = adjusted - peak
        if drawdown_abs < max_abs:
            max_abs = drawdown_abs
        if peak != ZERO:
            drawdown_pct = drawdown_abs / peak * HUNDRED
            if max_pct is None or drawdown_pct < max_pct:
                max_pct = drawdown_pct
        elif drawdown_abs != ZERO:
            max_pct = None

        if active_start is None and adjusted < episode_peak:
            active_start = row.observed_at
        elif active_start is not None and adjusted >= episode_peak:
            recovered_at = row.observed_at
            last_episode = (
                episode_peak_at, active_start, recovered_at,
                recovered_at - active_start,
            )
            active_start = None
            episode_peak = adjusted
            episode_peak_at = row.observed_at
        elif active_start is None and adjusted >= episode_peak:
            episode_peak = adjusted
            episode_peak_at = row.observed_at

    current = canonical[-1]
    current_adjusted = _decimal(current.flow_adjusted_equity)
    current_abs = current_adjusted - peak
    current_pct = None if peak == ZERO else current_abs / peak * HUNDRED
    history_status = (
        "ZERO_PEAK_PERCENT_UNAVAILABLE" if peak == ZERO else "CANONICAL"
    )
    if active_start is not None:
        recovery_status = "IN_DRAWDOWN"
        drawdown_start = active_start
        recovery_at = None
        duration = current.observed_at - active_start
        applicable_peak_at = episode_peak_at
    elif last_episode is not None:
        recovery_status = "RECOVERED"
        applicable_peak_at, drawdown_start, recovery_at, duration = last_episode
    else:
        recovery_status = "NO_DRAWDOWN"
        applicable_peak_at = peak_at
        drawdown_start = recovery_at = duration = None
    return LiveDrawdownHistory(
        _decimal(current.managed_equity), current_adjusted, peak,
        current_abs, current_pct, max_abs, max_pct, recovery_status,
        applicable_peak_at, drawdown_start, recovery_at, duration,
        history_status, current.observed_at,
    )


def _managed_capital_evidence_payload(
    evidence: LiveManagedCapitalEvidence,
    context: LiveManagedCapitalReadContext,
) -> dict[str, Any]:
    return {
        "balance_source": context.snapshot.source,
        "balance_observed_at": context.snapshot.observed_at.isoformat(),
        "balances": {
            row.asset: {
                "total_balance": str(row.total_balance),
                "available_balance": str(row.available_balance),
                "frozen_balance": str(row.frozen_balance),
                "order_frozen": str(row.order_frozen),
            }
            for row in sorted(context.snapshot.balances, key=lambda item: item.asset)
        },
        "marks": {
            asset: {
                "price": None if value[0] is None else str(value[0]),
                "observed_at": None if value[1] is None else value[1].isoformat(),
                "source": "candles.close/1m",
            }
            for asset, value in sorted(context.marks.items())
        },
        "inventory_quantities": {
            asset: str(value)
            for asset, value in sorted(context.inventory_quantities.items())
        },
        "managed_equity": str(evidence.managed_equity),
        "managed_equity_status": evidence.managed_equity_status,
        "inventory_reconciliation_status": evidence.inventory_reconciliation_status,
    }


def capture_observation_candidate(
    *, state: PortfolioStateV1,
    live_capital: LiveManagedCapitalEvidence,
    context: LiveManagedCapitalReadContext,
    baseline_id: int | None,
    baseline: LiveManagedCapitalBaseline | None,
    observed_at: datetime,
    observation_trigger: str,
    trigger_reference: str,
    producer_identity: str,
    git_revision: str,
) -> CandidateResult:
    """Capture immutable point-in-time evidence; owner flows are attached later."""
    if baseline is None or baseline_id is None:
        return CandidateResult("NO_BASELINE", None)
    if context.snapshot.account_identity_fingerprint != baseline.account_identity_fingerprint:
        return CandidateResult("ACCOUNT_IDENTITY_MISMATCH", None)
    if state.total_capital_status != "CANONICAL" or state.total_capital is None:
        return CandidateResult("INCOMPLETE_PORTFOLIO_STATE", None)
    if state.total_capital != live_capital.managed_equity:
        return CandidateResult("SOURCE_FINGERPRINT_MISMATCH", None)
    if state.deployed_capital_status in {"PRICE_STALE", "PRICE_UNAVAILABLE"} or (
        state.unrealized_pnl_status in {"PRICE_STALE", "PRICE_UNAVAILABLE"}
    ):
        return CandidateResult("INCOMPLETE_MARK", None)
    if state.realized_pnl_status != "CANONICAL" or state.realized_pnl is None:
        return CandidateResult("INCOMPLETE_FINANCIAL_TRUTH", None)
    component_values = (
        state.unrealized_pnl, state.deployed_capital,
        state.reserved_capital, state.available_capital,
    )
    component_statuses = (
        state.unrealized_pnl_status, state.deployed_capital_status,
        state.reserved_capital_status, state.available_capital_status,
    )
    if any(value is None for value in component_values) or any(
        status != "CANONICAL" for status in component_statuses
    ):
        return CandidateResult("INCOMPLETE_PORTFOLIO_STATE", None)
    if observation_trigger not in {
        "CADENCE_15M", "OWNER_CAPITAL_FLOW", "FINANCIAL_TRUTH_COMPLETE"
    }:
        raise ValueError("LIVE_DRAWDOWN_TRIGGER_INVALID")
    if observed_at.tzinfo is None or not trigger_reference.strip():
        raise ValueError("LIVE_DRAWDOWN_TRIGGER_EVIDENCE_REQUIRED")
    if len(git_revision) != 40 or any(ch not in "0123456789abcdef" for ch in git_revision):
        raise ValueError("LIVE_DRAWDOWN_GIT_REVISION_INVALID")
    bucket = cadence_bucket(observed_at)
    identity = _fingerprint({
        "contract_version": CONTRACT_VERSION,
        "baseline_id": baseline_id,
        "observation_bucket_at": bucket.isoformat(),
        "observation_trigger": observation_trigger,
        "trigger_reference": trigger_reference,
    })
    portfolio_evidence = state.serializable()
    managed_evidence = _managed_capital_evidence_payload(live_capital, context)
    return CandidateResult("READY_FOR_FLOW_WATERMARK", ObservationCandidate(
        baseline_id=int(baseline_id), deployment_id=state.deployment_id,
        account_identity_fingerprint=baseline.account_identity_fingerprint,
        baseline_activation_fingerprint=baseline.activation_fingerprint,
        observed_at=observed_at.astimezone(timezone.utc),
        observation_bucket_at=bucket, observation_trigger=observation_trigger,
        trigger_reference=trigger_reference, observation_identity=identity,
        managed_equity=_decimal(state.total_capital),
        realized_pnl=_decimal(state.realized_pnl),
        unrealized_pnl=_decimal(state.unrealized_pnl),
        deployed_capital=_decimal(state.deployed_capital),
        reserved_capital=_decimal(state.reserved_capital),
        available_capital=_decimal(state.available_capital),
        portfolio_state_evidence=portfolio_evidence,
        managed_capital_evidence=managed_evidence,
        portfolio_state_fingerprint=_fingerprint(portfolio_evidence),
        managed_capital_source_fingerprint=_fingerprint(managed_evidence),
        producer_identity=producer_identity, git_revision=git_revision,
    ))


def _final_evidence_payload(
    candidate: ObservationCandidate, *, flow_in: Decimal, flow_out: Decimal,
    flow_adjusted: Decimal, owner_flow_source_fingerprint: str,
) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "observation_identity": candidate.observation_identity,
        "observed_at": candidate.observed_at.isoformat(),
        "managed_equity": _decimal_text(candidate.managed_equity),
        "cumulative_flow_in": _decimal_text(flow_in),
        "cumulative_flow_out": _decimal_text(flow_out),
        "flow_adjusted_equity": _decimal_text(flow_adjusted),
        "baseline_activation_fingerprint": candidate.baseline_activation_fingerprint,
        "portfolio_state_fingerprint": candidate.portfolio_state_fingerprint,
        "managed_capital_source_fingerprint": candidate.managed_capital_source_fingerprint,
        "owner_flow_source_fingerprint": owner_flow_source_fingerprint,
    }


def persist_observation_candidate(cur: Any, candidate: ObservationCandidate) -> PersistenceResult:
    """Persist only after canonical owner-flow coverage reaches captured time."""
    cur.execute(
        """SELECT baseline_id,account_identity_fingerprint,activation_fingerprint
           FROM live_managed_capital_baseline_v1
           WHERE environment='LIVE' AND deployment_id=%s
           ORDER BY accepted_at DESC LIMIT 1""",
        (candidate.deployment_id,),
    )
    baseline = cur.fetchone()
    if not baseline:
        return PersistenceResult("NO_BASELINE", None)
    if int(baseline[0]) != candidate.baseline_id or str(baseline[1]) != candidate.account_identity_fingerprint:
        return PersistenceResult("ACCOUNT_IDENTITY_MISMATCH", None)
    if str(baseline[2]) != candidate.baseline_activation_fingerprint:
        return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
    cur.execute(
        """SELECT observation_id,account_identity_fingerprint,
                  baseline_activation_fingerprint,portfolio_state_fingerprint,
                  managed_capital_source_fingerprint,observation_trigger,
                  trigger_reference
           FROM live_managed_equity_observation_v1
           WHERE observation_identity=%s""",
        (candidate.observation_identity,),
    )
    existing = cur.fetchone()
    if existing:
        stable_evidence = (
            candidate.account_identity_fingerprint,
            candidate.baseline_activation_fingerprint,
            candidate.portfolio_state_fingerprint,
            candidate.managed_capital_source_fingerprint,
            candidate.observation_trigger,
            candidate.trigger_reference,
        )
        if tuple(str(value) for value in existing[1:]) != stable_evidence:
            return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
        return PersistenceResult("CANONICAL", int(existing[0]))
    flow = load_owner_flow_history_authority(
        cur, deployment_id=candidate.deployment_id,
        account_identity_fingerprint=candidate.account_identity_fingerprint,
        as_of=candidate.observed_at,
    )
    if flow.flow_history_status != "CANONICAL":
        return PersistenceResult("INCOMPLETE_CAPITAL_FLOW", None)
    cur.execute(
        """SELECT source_fingerprint,sync_through
           FROM v_owner_capital_flow_sync_authority_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
             AND source='TRADING_ACCOUNT_BILLS' AND status='CANONICAL'""",
        (candidate.deployment_id, candidate.account_identity_fingerprint),
    )
    source = cur.fetchone()
    if not source or source[1] is None or source[1] < candidate.observed_at:
        return PersistenceResult("INCOMPLETE_CAPITAL_FLOW", None)
    owner_fingerprint = str(source[0])
    flow_in = _decimal(flow.cumulative_flow_in)
    flow_out = _decimal(flow.cumulative_flow_out)
    adjusted = candidate.managed_equity - flow_in + flow_out
    source_fingerprints = {
        "baseline": candidate.baseline_activation_fingerprint,
        "portfolio_state": candidate.portfolio_state_fingerprint,
        "managed_capital": candidate.managed_capital_source_fingerprint,
        "owner_flow": owner_fingerprint,
    }
    evidence_fingerprint = _fingerprint(_final_evidence_payload(
        candidate, flow_in=flow_in, flow_out=flow_out,
        flow_adjusted=adjusted,
        owner_flow_source_fingerprint=owner_fingerprint,
    ))
    cur.execute(
        """INSERT INTO live_managed_equity_observation_v1(
             baseline_id,deployment_id,observed_at,raw_managed_equity,
             cumulative_flow_in_usdc,cumulative_flow_out_usdc,
             flow_adjusted_equity,evidence_fingerprint,evidence_status,
             environment,account_identity_fingerprint,observation_bucket_at,
             observation_trigger,trigger_reference,observation_identity,
             managed_equity_status,realized_pnl,realized_pnl_status,
             unrealized_pnl,unrealized_pnl_status,deployed_capital,
             deployed_capital_status,reserved_capital,reserved_capital_status,
             available_capital,available_capital_status,flow_history_status,
             flow_sync_through,baseline_activation_fingerprint,
             portfolio_state_fingerprint,managed_capital_source_fingerprint,
             owner_flow_source_fingerprint,source_fingerprints,
             portfolio_state_evidence,managed_capital_evidence,
             history_evidence_status,contract_version,producer_identity,git_revision
           ) VALUES (
             %s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE','LIVE',%s,%s,%s,%s,%s,
             'CANONICAL',%s,'CANONICAL',%s,'CANONICAL',%s,'CANONICAL',
             %s,'CANONICAL',%s,'CANONICAL','CANONICAL',%s,%s,%s,%s,%s,
             %s::jsonb,%s::jsonb,%s::jsonb,'CANONICAL',%s,%s,%s
           ) RETURNING observation_id""",
        (
            candidate.baseline_id, candidate.deployment_id, candidate.observed_at,
            candidate.managed_equity, flow_in, flow_out, adjusted,
            evidence_fingerprint, candidate.account_identity_fingerprint,
            candidate.observation_bucket_at, candidate.observation_trigger,
            candidate.trigger_reference, candidate.observation_identity,
            candidate.realized_pnl, candidate.unrealized_pnl,
            candidate.deployed_capital, candidate.reserved_capital,
            candidate.available_capital, flow.sync_through,
            candidate.baseline_activation_fingerprint,
            candidate.portfolio_state_fingerprint,
            candidate.managed_capital_source_fingerprint, owner_fingerprint,
            json.dumps(source_fingerprints, sort_keys=True),
            json.dumps(candidate.portfolio_state_evidence, sort_keys=True),
            json.dumps(candidate.managed_capital_evidence, sort_keys=True),
            CONTRACT_VERSION, candidate.producer_identity, candidate.git_revision,
        ),
    )
    return PersistenceResult("CANONICAL", int(cur.fetchone()[0]))


def reemit_late_event_history(
    cur: Any, *, deployment_id: str, account_identity_fingerprint: str,
) -> int:
    """Append exact corrected observation evidence, then resolve invalidations."""
    cur.execute(
        """SELECT run_id,source_cutoff,source_fingerprint,status
           FROM v_owner_capital_flow_sync_authority_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
             AND source='TRADING_ACCOUNT_BILLS'""",
        (deployment_id, account_identity_fingerprint),
    )
    authority = cur.fetchone()
    if not authority or str(authority[3]) != "LATE_EVENT_RECONCILIATION_REQUIRED":
        return 0
    run_id, source_cutoff, owner_fingerprint, _status = authority
    cur.execute(
        """SELECT reconciliation_key,affected_from
           FROM v_owner_capital_flow_reconciliation_current_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s AND state='REQUIRED'
           ORDER BY affected_from,reconciliation_key""",
        (deployment_id, account_identity_fingerprint),
    )
    required = cur.fetchall()
    if not required:
        return 0
    cur.execute(
        """SELECT accepted_at FROM live_managed_capital_baseline_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
           ORDER BY accepted_at DESC LIMIT 1""",
        (deployment_id, account_identity_fingerprint),
    )
    baseline = cur.fetchone()
    if not baseline:
        return 0
    resolved = 0
    for reconciliation_key, affected_from in required:
        cur.execute(
            """SELECT observation_identity,observed_at,raw_managed_equity,
                      baseline_activation_fingerprint,portfolio_state_fingerprint,
                      managed_capital_source_fingerprint
               FROM live_managed_equity_observation_v1
               WHERE contract_version=%s AND deployment_id=%s
                 AND account_identity_fingerprint=%s
                 AND observed_at>=%s AND observed_at<=%s
               ORDER BY observed_at,observation_id""",
            (
                CONTRACT_VERSION, deployment_id, account_identity_fingerprint,
                affected_from, source_cutoff,
            ),
        )
        corrections = {}
        for (
            identity, observed_at, managed, baseline_fingerprint,
            portfolio_fingerprint, managed_fingerprint,
        ) in cur.fetchall():
            cur.execute(
                """SELECT
                     COALESCE(sum(value_usdc) FILTER (
                       WHERE event_type IN ('DEPOSIT','TRANSFER_IN')),0),
                     COALESCE(sum(value_usdc) FILTER (
                       WHERE event_type IN ('WITHDRAWAL','TRANSFER_OUT')),0)
                   FROM owner_capital_flow_v1
                   WHERE environment='LIVE' AND deployment_id=%s
                     AND account_identity_fingerprint=%s
                     AND source='TRADING_ACCOUNT_BILLS'
                     AND evidence_status='COMPLETE'
                     AND event_at>%s AND event_at<=%s""",
                (
                    deployment_id, account_identity_fingerprint,
                    baseline[0], observed_at,
                ),
            )
            flow_in, flow_out = (_decimal(value) for value in cur.fetchone())
            adjusted = _decimal(managed) - flow_in + flow_out
            final_payload = {
                "contract_version": CONTRACT_VERSION,
                "observation_identity": str(identity),
                "observed_at": observed_at.isoformat(),
                "managed_equity": _decimal_text(managed),
                "cumulative_flow_in": _decimal_text(flow_in),
                "cumulative_flow_out": _decimal_text(flow_out),
                "flow_adjusted_equity": _decimal_text(adjusted),
                "baseline_activation_fingerprint": str(baseline_fingerprint),
                "portfolio_state_fingerprint": str(portfolio_fingerprint),
                "managed_capital_source_fingerprint": str(managed_fingerprint),
                "owner_flow_source_fingerprint": str(owner_fingerprint),
            }
            corrections[str(identity)] = {
                "cumulative_flow_in_usdc": _decimal_text(flow_in),
                "cumulative_flow_out_usdc": _decimal_text(flow_out),
                "flow_adjusted_equity": _decimal_text(adjusted),
                "owner_flow_source_fingerprint": str(owner_fingerprint),
                "evidence_fingerprint": _fingerprint(final_payload),
            }
        record_reconciliation_resolution(
            cur, reconciliation_key=str(reconciliation_key),
            source_run_id=str(run_id),
            evidence={
                "authority": CONTRACT_VERSION,
                "affected_from": affected_from.isoformat(),
                "source_cutoff": source_cutoff.isoformat(),
                "reemitted_observations": corrections,
                "arithmetic": "PostgreSQL_NUMERIC_Python_Decimal",
            },
        )
        resolved += 1
    return resolved


def select_observation_trigger(
    cur: Any, *, now: datetime,
    pending_keys: Iterable[tuple[str, str]] = (),
) -> tuple[str, str] | None:
    """Select only current forward events; never bootstrap historical events."""
    pending = set(pending_keys)
    bucket = cadence_bucket(now)
    candidates: list[tuple[str, str]] = [
        ("CADENCE_15M", bucket.isoformat()),
    ]
    cur.execute(
        """SELECT source_event_identity
           FROM owner_capital_flow_v1
           WHERE environment='LIVE' AND created_at>=%s AND event_at<=%s
           ORDER BY created_at DESC,flow_id DESC LIMIT 1""",
        (bucket, now),
    )
    owner_flow = cur.fetchone()
    if owner_flow:
        candidates.append(("OWNER_CAPITAL_FLOW", str(owner_flow[0])))
    cur.execute(
        """SELECT position_id,evidence_observed_at
           FROM canonical_financial_truth_v1
           WHERE financial_truth_status='COMPLETE'
             AND evidence_observed_at>=%s AND evidence_observed_at<=%s
           ORDER BY evidence_observed_at DESC,position_id DESC LIMIT 1""",
        (bucket, now),
    )
    financial_truth = cur.fetchone()
    if financial_truth:
        candidates.append((
            "FINANCIAL_TRUTH_COMPLETE",
            f"position:{financial_truth[0]}:{financial_truth[1].isoformat()}",
        ))
    for trigger, reference in candidates:
        if (trigger, reference) in pending:
            continue
        cur.execute(
            """SELECT 1 FROM live_managed_equity_observation_v1
               WHERE contract_version=%s AND observation_trigger=%s
                 AND trigger_reference=%s LIMIT 1""",
            (CONTRACT_VERSION, trigger, reference),
        )
        if cur.fetchone() is None:
            return trigger, reference
    return None


def read_live_drawdown_history(
    cur: Any, *, deployment_id: str,
    account_identity_fingerprint: str, as_of: datetime,
) -> LiveDrawdownHistory:
    cur.execute(
        """SELECT accepted_at,baseline_managed_equity,activation_fingerprint
           FROM live_managed_capital_baseline_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
           ORDER BY accepted_at DESC LIMIT 1""",
        (deployment_id, account_identity_fingerprint),
    )
    baseline = cur.fetchone()
    if not baseline:
        return calculate_drawdown_history(
            baseline_managed_equity=None, baseline_at=None,
            observations=(), as_of=as_of,
        )
    baseline_at, baseline_equity, activation_fingerprint = baseline
    cur.execute(
        """SELECT status,sync_through
           FROM v_owner_capital_flow_sync_authority_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
             AND source='TRADING_ACCOUNT_BILLS'""",
        (deployment_id, account_identity_fingerprint),
    )
    flow_authority = cur.fetchone()
    cur.execute(
        """SELECT observed_at,observation_bucket_at,observation_trigger,
                  raw_managed_equity,effective_flow_adjusted_equity,
                  effective_history_status,effective_evidence_fingerprint,
                  observation_identity,effective_cumulative_flow_in_usdc,
                  effective_cumulative_flow_out_usdc,baseline_activation_fingerprint,
                  portfolio_state_fingerprint,managed_capital_source_fingerprint,
                  effective_owner_flow_source_fingerprint,source_fingerprints,
                  portfolio_state_evidence,managed_capital_evidence
           FROM v_live_drawdown_history_observation_v1
           WHERE deployment_id=%s AND account_identity_fingerprint=%s
             AND observed_at<=%s
           ORDER BY observed_at,observation_id""",
        (deployment_id, account_identity_fingerprint, as_of),
    )
    observations = []
    for row in cur.fetchall():
        (
            observed_at, bucket_at, trigger, managed, adjusted, status,
            evidence_fingerprint, identity, flow_in, flow_out,
            baseline_fingerprint, portfolio_fingerprint, managed_fingerprint,
            owner_fingerprint, source_fingerprints, portfolio_evidence,
            managed_evidence,
        ) = row
        fingerprint_valid = (
            str(baseline_fingerprint) == str(activation_fingerprint)
            and _fingerprint(portfolio_evidence) == str(portfolio_fingerprint)
            and _fingerprint(managed_evidence) == str(managed_fingerprint)
            and source_fingerprints | {"owner_flow": str(owner_fingerprint)} == {
                "baseline": str(baseline_fingerprint),
                "portfolio_state": str(portfolio_fingerprint),
                "managed_capital": str(managed_fingerprint),
                "owner_flow": str(owner_fingerprint),
            }
        )
        final_payload = {
            "contract_version": CONTRACT_VERSION,
            "observation_identity": str(identity),
            "observed_at": observed_at.isoformat(),
            "managed_equity": _decimal_text(managed),
            "cumulative_flow_in": _decimal_text(flow_in),
            "cumulative_flow_out": _decimal_text(flow_out),
            "flow_adjusted_equity": _decimal_text(adjusted),
            "baseline_activation_fingerprint": str(baseline_fingerprint),
            "portfolio_state_fingerprint": str(portfolio_fingerprint),
            "managed_capital_source_fingerprint": str(managed_fingerprint),
            "owner_flow_source_fingerprint": str(owner_fingerprint),
        }
        if _fingerprint(final_payload) != str(evidence_fingerprint):
            fingerprint_valid = False
        observations.append(DrawdownObservation(
            observed_at=observed_at, observation_bucket_at=bucket_at,
            observation_trigger=str(trigger), managed_equity=_decimal(managed),
            flow_adjusted_equity=_decimal(adjusted),
            history_status=(str(status) if fingerprint_valid else "SOURCE_FINGERPRINT_MISMATCH"),
        ))
    if observations and (
        not flow_authority or str(flow_authority[0]) != "CANONICAL"
        or flow_authority[1] is None
        or flow_authority[1] < max(row.observed_at for row in observations)
    ):
        observations.append(DrawdownObservation(
            observed_at=max(row.observed_at for row in observations),
            observation_bucket_at=max(
                row.observation_bucket_at for row in observations
            ),
            observation_trigger="OWNER_CAPITAL_FLOW",
            managed_equity=ZERO, flow_adjusted_equity=ZERO,
            history_status="INCOMPLETE_CAPITAL_FLOW",
        ))
    return calculate_drawdown_history(
        baseline_managed_equity=_decimal(baseline_equity),
        baseline_at=baseline_at, observations=observations, as_of=as_of,
    )
