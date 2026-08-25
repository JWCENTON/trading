"""Forward-only PAPER managed-equity and drawdown history authority V1."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

from common.drawdown_history import (
    DrawdownHistory,
    DrawdownObservation,
    cadence_bucket as shared_cadence_bucket,
    canonical_numeric_38_18,
    canonical_json,
    calculate_drawdown_history,
    decimal_text,
    decimal_value,
    fingerprint,
)
from common.paper_equity_baseline_v2 import (
    PaperEquityBaselineV2,
    fetch_paper_equity_baseline_v2,
)
from common.portfolio_state import PortfolioStateV1, read_portfolio_state


_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts/paper_drawdown_history_authority_v1_contract.json"
)
_CONTRACT = json.loads(_CONTRACT_PATH.read_text())
CONTRACT_VERSION = str(_CONTRACT["contract_version"])
CADENCE = timedelta(seconds=int(_CONTRACT["cadence_seconds"]))
STALE_AFTER = timedelta(seconds=int(_CONTRACT["stale_after_seconds"]))
PRODUCER_IDENTITY = "automation-runner:PAPER_DRAWDOWN_HISTORY_AUTHORITY_V1"
FAILURE_PRIORITY = (
    "SOURCE_FINGERPRINT_MISMATCH",
    "INCOMPLETE_PORTFOLIO_STATE",
    "INCOMPLETE_MARK",
    "INCOMPLETE_FINANCIAL_TRUTH",
)
PAPER_DEPLOYMENTS = frozenset({"local-paper", "vps-paper"})


@dataclass(frozen=True)
class ActivationEvidence:
    activation_id: int
    baseline_id: int
    deployment_id: str
    activated_at: datetime
    activation_bucket_at: datetime
    baseline_activation_fingerprint: str
    activation_identity: str
    activation_evidence_fingerprint: str
    created: bool = False


@dataclass(frozen=True)
class ObservationCandidate:
    activation_id: int
    baseline_id: int
    deployment_id: str
    observed_at: datetime
    observation_bucket_at: datetime
    observation_trigger: str
    trigger_reference: str
    observation_identity: str
    managed_equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    baseline_activation_fingerprint: str
    portfolio_state_evidence: Mapping[str, Any]
    portfolio_state_fingerprint: str
    source_fingerprints: Mapping[str, str]
    evidence_fingerprint: str
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


@dataclass(frozen=True)
class PaperDrawdownHistory:
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
    source_fingerprint: str | None

    def serializable(self) -> dict[str, Any]:
        return {
            "CURRENT_DRAWDOWN_ABS": self.current_drawdown_abs,
            "CURRENT_DRAWDOWN_PCT": self.current_drawdown_pct,
            "MAX_DRAWDOWN_ABS": self.max_drawdown_abs,
            "MAX_DRAWDOWN_PCT": self.max_drawdown_pct,
            "PEAK_TIMESTAMP": self.peak_timestamp,
            "DRAWDOWN_START": self.drawdown_start,
            "RECOVERY_TIMESTAMP": self.recovery_timestamp,
            "DRAWDOWN_DURATION": self.drawdown_duration,
            "RECOVERY_STATUS": self.recovery_status,
            "HISTORY_STATUS": self.history_status,
            "LATEST_OBSERVATION_AT": self.latest_observation_at,
            "SOURCE_FINGERPRINT": self.source_fingerprint,
        }


@dataclass(frozen=True)
class CalibrationReadiness:
    authority_ready: bool
    continuity_ready: bool
    episode_ready: None
    market_coverage_ready: None
    shadow_outcome_ready: None
    financial_model_stable: None
    accounting_ready: bool
    calibration_ready: bool

    def serializable(self) -> dict[str, bool | None]:
        return {
            "AUTHORITY_READY": self.authority_ready,
            "CONTINUITY_READY": self.continuity_ready,
            "EPISODE_READY": self.episode_ready,
            "MARKET_COVERAGE_READY": self.market_coverage_ready,
            "SHADOW_OUTCOME_READY": self.shadow_outcome_ready,
            "FINANCIAL_MODEL_STABLE": self.financial_model_stable,
            "ACCOUNTING_READY": self.accounting_ready,
            "CALIBRATION_READY": self.calibration_ready,
        }


def cadence_bucket(at: datetime) -> datetime:
    return shared_cadence_bucket(at, CADENCE)


def _valid_revision(revision: str) -> bool:
    return len(revision) == 40 and all(ch in "0123456789abcdef" for ch in revision)


def _activation_payload(
    *, baseline: PaperEquityBaselineV2, activated_at: datetime,
) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "deployment_id": baseline.deployment_id,
        "baseline_id": baseline.baseline_id,
        "baseline_timestamp": baseline.baseline_timestamp,
        "baseline_managed_equity": baseline.baseline_managed_equity,
        "baseline_unrealized_pnl": baseline.baseline_unrealized_pnl,
        "baseline_activation_fingerprint": baseline.activation_fingerprint,
        "activated_at": activated_at,
        "historical_policy": "FORWARD_ONLY_NO_BACKFILL",
    }


def ensure_activation_cursor(
    cur: Any, *, deployment_id: str, now: datetime,
    producer_identity: str, git_revision: str,
) -> ActivationEvidence | None:
    """Append the accepted cutover once; never derive it from historical rows."""
    deployment = str(deployment_id).strip().lower()
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("PAPER_DRAWDOWN_DEPLOYMENT_INVALID")
    if now.tzinfo is None:
        raise ValueError("PAPER_DRAWDOWN_TIMESTAMP_REQUIRED")
    if not _valid_revision(git_revision):
        raise ValueError("PAPER_DRAWDOWN_GIT_REVISION_INVALID")
    baseline = fetch_paper_equity_baseline_v2(cur, deployment_id=deployment)
    if baseline is None or baseline.evidence_status != "COMPLETE":
        return None
    cur.execute(
        """SELECT activation_id,baseline_id,deployment_id,activated_at,
                  activation_bucket_at,baseline_activation_fingerprint,
                  activation_identity,activation_evidence_fingerprint
           FROM paper_drawdown_history_activation_v1
           WHERE baseline_id=%s AND contract_version=%s""",
        (baseline.baseline_id, CONTRACT_VERSION),
    )
    row = cur.fetchone()
    if row:
        return ActivationEvidence(
            int(row[0]), int(row[1]), str(row[2]), row[3], row[4],
            str(row[5]), str(row[6]), str(row[7]), False,
        )
    activated_at = now.astimezone(timezone.utc)
    activation_bucket_at = cadence_bucket(activated_at)
    payload = _activation_payload(baseline=baseline, activated_at=activated_at)
    evidence_fp = fingerprint(payload)
    identity = fingerprint({
        "contract_version": CONTRACT_VERSION,
        "baseline_id": baseline.baseline_id,
        "baseline_activation_fingerprint": baseline.activation_fingerprint,
        "activation_bucket_at": activation_bucket_at,
    })
    cur.execute(
        """INSERT INTO paper_drawdown_history_activation_v1(
             baseline_id,deployment_id,activated_at,activation_bucket_at,
             baseline_activation_fingerprint,activation_identity,
             activation_evidence_fingerprint,activation_evidence,
             producer_identity,git_revision,contract_version
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
           ON CONFLICT (baseline_id,contract_version) DO NOTHING
           RETURNING activation_id""",
        (
            baseline.baseline_id, deployment, activated_at, activation_bucket_at,
            baseline.activation_fingerprint, identity, evidence_fp,
            canonical_json(payload),
            producer_identity, git_revision, CONTRACT_VERSION,
        ),
    )
    inserted = cur.fetchone()
    if inserted:
        return ActivationEvidence(
            int(inserted[0]), baseline.baseline_id, deployment, activated_at,
            activation_bucket_at, baseline.activation_fingerprint, identity,
            evidence_fp, True,
        )
    return ensure_activation_cursor(
        cur, deployment_id=deployment, now=now,
        producer_identity=producer_identity, git_revision=git_revision,
    )


def _final_evidence_payload(candidate: ObservationCandidate) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "observation_identity": candidate.observation_identity,
        "observed_at": candidate.observed_at,
        "managed_equity": decimal_text(candidate.managed_equity),
        "realized_pnl": decimal_text(candidate.realized_pnl),
        "unrealized_pnl": decimal_text(candidate.unrealized_pnl),
        "baseline_activation_fingerprint": candidate.baseline_activation_fingerprint,
        "portfolio_state_fingerprint": candidate.portfolio_state_fingerprint,
        "source_fingerprints": candidate.source_fingerprints,
    }


def _canonical_managed_equity_fingerprint(
    *, baseline_activation_fingerprint: str, managed_equity: Decimal,
) -> str:
    return fingerprint({
        "contract_version": CONTRACT_VERSION,
        "baseline_activation_fingerprint": baseline_activation_fingerprint,
        "authority_canonical_managed_equity": decimal_text(
            canonical_numeric_38_18(managed_equity)
        ),
        "managed_equity_precision": "NUMERIC(38,18)",
    })


def _portfolio_state_capital_payload(state: PortfolioStateV1) -> dict[str, Any]:
    """Freeze only capital-basis fields; legacy diagnostic drawdown is excluded."""
    payload = state.serializable()
    authorities = dict(payload.get("source_authorities") or {})
    return {
        "portfolio_state_version": payload.get("portfolio_state_version"),
        "environment": payload.get("environment"),
        "deployment_id": payload.get("deployment_id"),
        "as_of": payload.get("as_of"),
        "runtime_revision": payload.get("runtime_revision"),
        "capital_scope": payload.get("capital_scope"),
        "total_capital": payload.get("total_capital"),
        "total_capital_status": payload.get("total_capital_status"),
        "realized_pnl": payload.get("realized_pnl"),
        "realized_pnl_status": payload.get("realized_pnl_status"),
        "unrealized_pnl": payload.get("unrealized_pnl"),
        "unrealized_pnl_status": payload.get("unrealized_pnl_status"),
        "source_timestamps": payload.get("source_timestamps") or {},
        "source_freshness": {
            key: value for key, value in
            dict(payload.get("source_freshness") or {}).items()
            if key in {"accepted_baseline", "financial_truth", "mark_price"}
        },
        "source_authorities": {
            key: value for key, value in authorities.items()
            if key in {
                "total_capital", "realized_pnl", "inventory_quantity",
                "mark_price", "account_reporting_excluded",
            }
        },
    }


def capture_observation_candidate(
    *, state: PortfolioStateV1, baseline: PaperEquityBaselineV2,
    activation: ActivationEvidence, observed_at: datetime,
    observation_trigger: str, trigger_reference: str,
    producer_identity: str, git_revision: str,
) -> CandidateResult:
    """Capture only an exact canonical PAPER Portfolio State point."""
    if state.environment != "PAPER" or state.deployment_id != baseline.deployment_id:
        raise ValueError("PAPER_DRAWDOWN_ENVIRONMENT_FENCE")
    if activation.baseline_id != baseline.baseline_id:
        return CandidateResult("SOURCE_FINGERPRINT_MISMATCH", None)
    if activation.baseline_activation_fingerprint != baseline.activation_fingerprint:
        return CandidateResult("SOURCE_FINGERPRINT_MISMATCH", None)
    if observed_at.tzinfo is None or observed_at < activation.activated_at:
        raise ValueError("PAPER_DRAWDOWN_FORWARD_BOUNDARY_REQUIRED")
    if observation_trigger not in {
        "BASELINE_ACTIVATION", "CADENCE_15M", "FINANCIAL_TRUTH_COMPLETE"
    } or not str(trigger_reference).strip():
        raise ValueError("PAPER_DRAWDOWN_TRIGGER_INVALID")
    if not _valid_revision(git_revision):
        raise ValueError("PAPER_DRAWDOWN_GIT_REVISION_INVALID")
    if state.total_capital_status != "CANONICAL" or state.total_capital is None:
        return CandidateResult("INCOMPLETE_PORTFOLIO_STATE", None)
    if state.realized_pnl_status != "CANONICAL" or state.realized_pnl is None:
        return CandidateResult("INCOMPLETE_FINANCIAL_TRUTH", None)
    if state.unrealized_pnl_status in {"PRICE_STALE", "PRICE_UNAVAILABLE"}:
        return CandidateResult("INCOMPLETE_MARK", None)
    if state.unrealized_pnl_status != "CANONICAL" or state.unrealized_pnl is None:
        return CandidateResult("INCOMPLETE_PORTFOLIO_STATE", None)
    expected_equity_raw = (
        baseline.baseline_managed_equity
        + decimal_value(state.realized_pnl)
        + decimal_value(state.unrealized_pnl)
        - baseline.baseline_unrealized_pnl
    )
    canonical_expected_equity = canonical_numeric_38_18(expected_equity_raw)
    canonical_state_equity = canonical_numeric_38_18(state.total_capital)
    if canonical_state_equity != canonical_expected_equity:
        return CandidateResult("SOURCE_FINGERPRINT_MISMATCH", None)
    bucket = cadence_bucket(observed_at)
    state_payload = _portfolio_state_capital_payload(state)
    state_fp = fingerprint(state_payload)
    managed_equity_fp = _canonical_managed_equity_fingerprint(
        baseline_activation_fingerprint=baseline.activation_fingerprint,
        managed_equity=canonical_state_equity,
    )
    source_fingerprints = {
        "baseline": baseline.activation_fingerprint,
        "portfolio_state": state_fp,
        "managed_equity_canonical": managed_equity_fp,
    }
    identity = fingerprint({
        "contract_version": CONTRACT_VERSION,
        "baseline_id": baseline.baseline_id,
        "observation_bucket_at": bucket,
        "observation_trigger": observation_trigger,
        "trigger_reference": trigger_reference,
        "managed_equity_canonical_fingerprint": managed_equity_fp,
    })
    provisional = ObservationCandidate(
        activation.activation_id, baseline.baseline_id, baseline.deployment_id,
        observed_at.astimezone(timezone.utc), bucket, observation_trigger,
        str(trigger_reference), identity, canonical_state_equity,
        canonical_numeric_38_18(state.realized_pnl),
        canonical_numeric_38_18(state.unrealized_pnl),
        baseline.activation_fingerprint, state_payload, state_fp,
        source_fingerprints, "", producer_identity, git_revision,
    )
    return CandidateResult(
        "CANONICAL",
        ObservationCandidate(
            **{**asdict(provisional),
               "evidence_fingerprint": fingerprint(_final_evidence_payload(provisional))}
        ),
    )


def persist_observation_candidate(
    cur: Any, candidate: ObservationCandidate,
) -> PersistenceResult:
    """Idempotently append exact evidence; conflicts and tampering fail closed."""
    if fingerprint(candidate.portfolio_state_evidence) != candidate.portfolio_state_fingerprint:
        return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
    if fingerprint(_final_evidence_payload(candidate)) != candidate.evidence_fingerprint:
        return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
    cur.execute(
        """SELECT activation_id,baseline_id,baseline_activation_fingerprint
           FROM paper_drawdown_history_activation_v1
           WHERE activation_id=%s AND contract_version=%s""",
        (candidate.activation_id, CONTRACT_VERSION),
    )
    activation = cur.fetchone()
    if not activation or (
        int(activation[1]) != candidate.baseline_id
        or str(activation[2]) != candidate.baseline_activation_fingerprint
    ):
        return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
    cur.execute(
        """SELECT observation_id,evidence_fingerprint
           FROM paper_managed_equity_observation_v1
           WHERE observation_identity=%s""",
        (candidate.observation_identity,),
    )
    existing = cur.fetchone()
    if existing:
        if str(existing[1]) != candidate.evidence_fingerprint:
            return PersistenceResult("SOURCE_FINGERPRINT_MISMATCH", None)
        return PersistenceResult("CANONICAL", int(existing[0]))
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
           ) VALUES (
             %s,%s,%s,%s,%s,%s,%s,%s,%s,'CANONICAL',%s,'CANONICAL',
             %s,'CANONICAL',%s,%s,%s::jsonb,%s::jsonb,%s,'CANONICAL',%s,%s,%s
           ) RETURNING observation_id""",
        (
            candidate.activation_id, candidate.baseline_id,
            candidate.deployment_id, candidate.observed_at,
            candidate.observation_bucket_at, candidate.observation_trigger,
            candidate.trigger_reference, candidate.observation_identity,
            candidate.managed_equity, candidate.realized_pnl,
            candidate.unrealized_pnl,
            candidate.baseline_activation_fingerprint,
            candidate.portfolio_state_fingerprint,
            json.dumps(candidate.source_fingerprints, sort_keys=True),
            json.dumps(candidate.portfolio_state_evidence, sort_keys=True),
            candidate.evidence_fingerprint, candidate.producer_identity,
            candidate.git_revision, CONTRACT_VERSION,
        ),
    )
    return PersistenceResult("CANONICAL", int(cur.fetchone()[0]))


def select_observation_triggers_cursor(
    cur: Any, *, activation: ActivationEvidence, now: datetime,
) -> tuple[tuple[str, str, datetime], ...]:
    """Select only current forward triggers, coalescing COMPLETE FT activity."""
    bucket = cadence_bucket(now)
    selected: list[tuple[str, str, datetime]] = []
    cur.execute(
        """SELECT 1 FROM paper_managed_equity_observation_v1
           WHERE activation_id=%s AND observation_trigger='BASELINE_ACTIVATION'""",
        (activation.activation_id,),
    )
    if cur.fetchone() is None:
        selected.append((
            "BASELINE_ACTIVATION", activation.activation_identity,
            activation.activated_at,
        ))
    if bucket > activation.activation_bucket_at:
        cur.execute(
            """SELECT 1 FROM paper_managed_equity_observation_v1
               WHERE activation_id=%s AND observation_trigger='CADENCE_15M'
                 AND observation_bucket_at=%s""",
            (activation.activation_id, bucket),
        )
        if cur.fetchone() is None:
            selected.append(("CADENCE_15M", bucket.isoformat(), bucket))
    cur.execute(
        """SELECT MAX(observed_at) FROM paper_managed_equity_observation_v1
           WHERE activation_id=%s""",
        (activation.activation_id,),
    )
    last_observed = cur.fetchone()[0] or activation.activated_at
    cur.execute(
        """SELECT position_id,evidence_observed_at,source_fingerprint
           FROM canonical_financial_truth_v1
           WHERE financial_truth_status='COMPLETE'
             AND evidence_observed_at>%s AND evidence_observed_at<=%s
           ORDER BY evidence_observed_at DESC,position_id DESC LIMIT 1""",
        (last_observed, now),
    )
    ft = cur.fetchone()
    if ft:
        ft_at = ft[1].astimezone(timezone.utc)
        selected.append((
            "FINANCIAL_TRUTH_COMPLETE",
            f"position:{int(ft[0])}:{ft_at.isoformat()}:{str(ft[2] or 'NO_FINGERPRINT')}",
            ft_at,
        ))
    return tuple(selected)


def read_paper_drawdown_history(
    cur: Any, *, deployment_id: str, as_of: datetime,
) -> PaperDrawdownHistory:
    if as_of.tzinfo is None:
        raise ValueError("PAPER_DRAWDOWN_AS_OF_REQUIRED")
    baseline = fetch_paper_equity_baseline_v2(cur, deployment_id=deployment_id)
    if baseline is None:
        shared = calculate_drawdown_history(
            baseline_managed_equity=None, baseline_at=None, observations=(),
            as_of=as_of, cadence=CADENCE, stale_after=STALE_AFTER,
            failure_priority=FAILURE_PRIORITY,
            timestamp_error="PAPER_DRAWDOWN_AS_OF_REQUIRED",
        )
        return PaperDrawdownHistory(**asdict(shared), source_fingerprint=None)
    cur.execute(
        """SELECT activation_id,activated_at,activation_bucket_at,
                  baseline_activation_fingerprint,activation_evidence_fingerprint,
                  activation_evidence
           FROM paper_drawdown_history_activation_v1
           WHERE baseline_id=%s AND contract_version=%s""",
        (baseline.baseline_id, CONTRACT_VERSION),
    )
    activation = cur.fetchone()
    if not activation:
        shared = calculate_drawdown_history(
            baseline_managed_equity=baseline.baseline_managed_equity,
            baseline_at=baseline.baseline_timestamp, observations=(),
            as_of=as_of, cadence=CADENCE, stale_after=STALE_AFTER,
            failure_priority=FAILURE_PRIORITY,
            timestamp_error="PAPER_DRAWDOWN_AS_OF_REQUIRED",
        )
        return PaperDrawdownHistory(**asdict(shared), source_fingerprint=None)
    (
        activation_id, activated_at, activation_bucket_at, baseline_fp,
        activation_evidence_fp, activation_payload,
    ) = activation
    activation_valid = (
        str(baseline_fp) == baseline.activation_fingerprint
        and fingerprint(activation_payload) == str(activation_evidence_fp)
    )
    cur.execute(
        """SELECT observed_at,observation_bucket_at,observation_trigger,
                  managed_equity,history_evidence_status,observation_identity,
                  evidence_fingerprint,baseline_activation_fingerprint,
                  portfolio_state_fingerprint,source_fingerprints,
                  portfolio_state_evidence,realized_pnl,unrealized_pnl
           FROM paper_managed_equity_observation_v1
           WHERE activation_id=%s AND observed_at<=%s
           ORDER BY observed_at,observation_id""",
        (activation_id, as_of),
    )
    observations = []
    canonical_fingerprints = []
    if not activation_valid:
        observations.append(DrawdownObservation(
            activated_at, activation_bucket_at, "BASELINE_ACTIVATION",
            baseline.baseline_managed_equity, baseline.baseline_managed_equity,
            "SOURCE_FINGERPRINT_MISMATCH",
        ))
    for row in cur.fetchall():
        (
            observed_at, bucket_at, trigger, managed, status, identity,
            evidence_fp, row_baseline_fp, portfolio_fp, sources,
            portfolio_payload, realized, unrealized,
        ) = row
        expected_managed_fp = _canonical_managed_equity_fingerprint(
            baseline_activation_fingerprint=str(row_baseline_fp),
            managed_equity=decimal_value(managed),
        )
        valid = (
            str(row_baseline_fp) == str(baseline_fp) == baseline.activation_fingerprint
            and fingerprint(portfolio_payload) == str(portfolio_fp)
            and dict(sources) == {
                "baseline": str(row_baseline_fp),
                "portfolio_state": str(portfolio_fp),
                "managed_equity_canonical": expected_managed_fp,
            }
        )
        probe = ObservationCandidate(
            int(activation_id), baseline.baseline_id, baseline.deployment_id,
            observed_at, bucket_at, str(trigger), "read-model", str(identity),
            decimal_value(managed), decimal_value(realized),
            decimal_value(unrealized), str(row_baseline_fp), portfolio_payload,
            str(portfolio_fp), dict(sources), str(evidence_fp), "read-model",
            "0" * 40,
        )
        if fingerprint(_final_evidence_payload(probe)) != str(evidence_fp):
            valid = False
        final_status = str(status) if valid else "SOURCE_FINGERPRINT_MISMATCH"
        observations.append(DrawdownObservation(
            observed_at, bucket_at, str(trigger), decimal_value(managed),
            decimal_value(managed), final_status,
        ))
        canonical_fingerprints.append(str(evidence_fp))
    shared = calculate_drawdown_history(
        baseline_managed_equity=baseline.baseline_managed_equity,
        baseline_at=baseline.baseline_timestamp, observations=observations,
        as_of=as_of, cadence=CADENCE, stale_after=STALE_AFTER,
        failure_priority=FAILURE_PRIORITY,
        cadence_anchor_at=activation_bucket_at,
        timestamp_error="PAPER_DRAWDOWN_AS_OF_REQUIRED",
    )
    source_fp = fingerprint({
        "contract_version": CONTRACT_VERSION,
        "activation_id": int(activation_id),
        "as_of": as_of,
        "observation_evidence_fingerprints": canonical_fingerprints,
        "history_status": shared.history_status,
    })
    return PaperDrawdownHistory(**asdict(shared), source_fingerprint=source_fp)


def calibration_readiness(history: PaperDrawdownHistory) -> CalibrationReadiness:
    canonical = history.history_status in {
        "CANONICAL", "ZERO_PEAK_PERCENT_UNAVAILABLE"
    }
    return CalibrationReadiness(
        authority_ready=canonical,
        continuity_ready=canonical,
        episode_ready=None,
        market_coverage_ready=None,
        shadow_outcome_ready=None,
        financial_model_stable=None,
        accounting_ready=canonical,
        calibration_ready=False,
    )


def run_paper_drawdown_history_cycle(
    *, connection_factory: Callable[[], Any], deployment_id: str,
    git_revision: str, now: datetime | None = None,
    portfolio_state_reader: Callable[..., PortfolioStateV1] = read_portfolio_state,
) -> dict[str, Any]:
    """Existing automation-runner producer: Portfolio State then history."""
    observed_now = now or datetime.now(timezone.utc)
    conn = connection_factory()
    conn.autocommit = False
    persisted: list[int] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.paper_managed_equity_observation_v1')"
            )
            if cur.fetchone()[0] is None:
                conn.rollback()
                return {"status": "SCHEMA_UNAVAILABLE", "persisted": persisted}
            activation = ensure_activation_cursor(
                cur, deployment_id=deployment_id, now=observed_now,
                producer_identity=PRODUCER_IDENTITY, git_revision=git_revision,
            )
            if activation is None:
                conn.rollback()
                return {"status": "NO_BASELINE", "persisted": persisted}
            if activation.created:
                # The accepted cutover is durable even when the first canonical
                # Portfolio State is not yet complete; incomplete observations
                # themselves are never persisted.
                conn.commit()
            baseline = fetch_paper_equity_baseline_v2(
                cur, deployment_id=deployment_id
            )
            triggers = select_observation_triggers_cursor(
                cur, activation=activation, now=observed_now,
            )
            for trigger, reference, evidence_at in triggers:
                state = portfolio_state_reader(
                    cur, environment="PAPER", deployment_id=deployment_id,
                    as_of=evidence_at, runtime_revision=git_revision,
                )
                captured = capture_observation_candidate(
                    state=state, baseline=baseline, activation=activation,
                    observed_at=evidence_at, observation_trigger=trigger,
                    trigger_reference=reference,
                    producer_identity=PRODUCER_IDENTITY,
                    git_revision=git_revision,
                )
                if captured.candidate is None:
                    conn.rollback()
                    return {"status": captured.status, "persisted": persisted}
                result = persist_observation_candidate(cur, captured.candidate)
                if result.status != "CANONICAL":
                    conn.rollback()
                    return {"status": result.status, "persisted": persisted}
                persisted.append(int(result.observation_id))
            conn.commit()
            return {"status": "CANONICAL", "persisted": persisted}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
