"""Immutable pre-calibration joint authority lifecycle for PAPER.

This module owns metadata and boundary selection only.  It cannot enable Risk
Budget execution influence, Treatment, sizing, or any trading action.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Callable, Mapping
import uuid

from psycopg2.extras import Json


CONTRACT_VERSION = "JOINT_AUTHORITY_EPOCH_V1"
CADENCE = timedelta(minutes=15)
PAPER_DEPLOYMENTS = frozenset({"local-paper", "vps-paper"})
APPROVED_SUPERSESSION_REASONS = frozenset({
    "PRECISION_DEFECT_RECOVERY",
    "UPSTREAM_REPLAY_CUTOVER_EPOCH_CHANGE",
    "AUTHORITY_ACTIVATION_FAILURE_RECOVERY",
})
SELECTION_REASONS = APPROVED_SUPERSESSION_REASONS | {
    "INITIAL_JOINT_AUTHORITY_EPOCH"
}
_ATTEMPT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "waltrade:joint-authority-attempt:v1")


class JointAuthorityError(RuntimeError):
    pass


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("JOINT_AUTHORITY_TIMEZONE_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, float):
        raise ValueError("JOINT_AUTHORITY_FLOAT_FORBIDDEN")
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _is_fingerprint(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def _require_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("JOINT_AUTHORITY_TIMEZONE_REQUIRED")
    return value.astimezone(timezone.utc)


def is_canonical_boundary(value: datetime) -> bool:
    at = _require_time(value)
    return at.second == 0 and at.microsecond == 0 and at.minute % 15 == 0


def ceil_15(value: datetime) -> datetime:
    at = _require_time(value)
    floored = at.replace(minute=(at.minute // 15) * 15, second=0, microsecond=0)
    return floored if at == floored else floored + CADENCE


def drawdown_activation_boundary(
    replay_cutover_at: datetime, *, operational_recovery_at: datetime | None = None,
) -> datetime:
    """Return D; delayed recovery always chooses a strictly future boundary."""
    replay = _require_time(replay_cutover_at)
    normal = ceil_15(replay)
    if operational_recovery_at is None:
        return normal
    recovery = _require_time(operational_recovery_at)
    if recovery <= normal:
        return normal
    candidate = ceil_15(recovery)
    return candidate + CADENCE if candidate == recovery else candidate


def validate_ordering(
    *, baseline_at: datetime, replay_at: datetime, drawdown_at: datetime,
    first_cadence_at: datetime, q0_at: datetime | None = None,
    calibration_dataset_as_of: datetime | None = None,
) -> None:
    b, r, d, c1 = map(
        _require_time, (baseline_at, replay_at, drawdown_at, first_cadence_at)
    )
    if not is_canonical_boundary(d) or c1 != d + CADENCE or not (b <= r <= d < c1):
        raise JointAuthorityError("JOINT_AUTHORITY_ORDERING_INVALID")
    if q0_at is not None:
        q0 = _require_time(q0_at)
        if q0 < c1:
            raise JointAuthorityError("JOINT_AUTHORITY_Q0_BEFORE_C1")
        if calibration_dataset_as_of is not None and q0 > _require_time(
            calibration_dataset_as_of
        ):
            raise JointAuthorityError("JOINT_AUTHORITY_DATASET_BEFORE_Q0")


@dataclass(frozen=True)
class AuthorityEpoch:
    authority_epoch_id: int
    deployment_id: str
    baseline_id: int
    baseline_fingerprint: str
    replay_cutover_id: int
    replay_cutover_at: datetime
    replay_cutover_fingerprint: str
    drawdown_activation_id: int
    drawdown_generation: int
    drawdown_generation_fingerprint: str
    drawdown_activation_boundary: datetime
    first_required_cadence: datetime
    epoch_fingerprint: str
    selection_fingerprint: str


@dataclass(frozen=True)
class RiskBudgetEpochBoundary:
    status: str
    as_of: datetime | None
    epoch: AuthorityEpoch | None = None
    drawdown_observation_fingerprint: str | None = None


@dataclass(frozen=True)
class ActivationResult:
    status: str
    attempt_id: uuid.UUID
    epoch: AuthorityEpoch | None = None
    failure_reason: str | None = None


def schema_available_cursor(cur: Any) -> bool:
    cur.execute("SELECT to_regclass('public.joint_authority_epoch_v1')")
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def _epoch_from_row(row: Any) -> AuthorityEpoch:
    return AuthorityEpoch(
        authority_epoch_id=int(row[0]), deployment_id=str(row[1]),
        baseline_id=int(row[2]), baseline_fingerprint=str(row[3]),
        replay_cutover_id=int(row[4]), replay_cutover_at=row[5],
        replay_cutover_fingerprint=str(row[6]), drawdown_activation_id=int(row[7]),
        drawdown_generation=int(row[8]), drawdown_generation_fingerprint=str(row[9]),
        drawdown_activation_boundary=row[10], first_required_cadence=row[11],
        epoch_fingerprint=str(row[12]), selection_fingerprint=str(row[13]),
    )


def load_active_epoch_cursor(cur: Any, *, deployment_id: str) -> AuthorityEpoch | None:
    deployment = str(deployment_id).lower()
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("JOINT_AUTHORITY_DEPLOYMENT_INVALID")
    if not schema_available_cursor(cur):
        return None
    cur.execute(
        """SELECT e.authority_epoch_id,e.deployment_id,e.baseline_id,
                  e.baseline_fingerprint,e.replay_cutover_id,r.cutover_at,
                  e.replay_cutover_fingerprint,e.drawdown_activation_id,
                  e.drawdown_generation,e.drawdown_generation_fingerprint,
                  e.drawdown_activation_boundary,e.first_required_cadence,
                  e.epoch_fingerprint,s.selection_fingerprint
           FROM joint_authority_epoch_selection_v1 s
           JOIN joint_authority_epoch_v1 e USING (authority_epoch_id)
           JOIN paper_portfolio_replay_cutover_v1 r USING (replay_cutover_id)
           LEFT JOIN joint_authority_epoch_selection_v1 successor
             ON successor.previous_selection_id=s.selection_id
           WHERE s.deployment_id=%s AND successor.selection_id IS NULL""",
        (deployment,),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise JointAuthorityError("AMBIGUOUS_ACTIVE_AUTHORITY_EPOCH")
    epoch = _epoch_from_row(rows[0])
    validate_ordering(
        baseline_at=epoch.replay_cutover_at, replay_at=epoch.replay_cutover_at,
        drawdown_at=epoch.drawdown_activation_boundary,
        first_cadence_at=epoch.first_required_cadence,
    )
    return epoch


def resolve_risk_budget_boundary_cursor(
    cur: Any, *, deployment_id: str, scheduler_time: datetime,
) -> RiskBudgetEpochBoundary:
    """Resolve PAPER STATE_EVALUATION from upstream epoch, never wall clock."""
    scheduler = _require_time(scheduler_time)
    epoch = load_active_epoch_cursor(cur, deployment_id=deployment_id)
    if epoch is None:
        return RiskBudgetEpochBoundary("EXPECTED_WAITING_FOR_AUTHORITY_EPOCH", None)
    if scheduler < epoch.first_required_cadence:
        return RiskBudgetEpochBoundary(
            "EXPECTED_WAITING_FOR_FIRST_DRAWDOWN_CADENCE", None, epoch
        )
    cur.execute(
        """SELECT observation_bucket_at,evidence_fingerprint
           FROM paper_managed_equity_observation_v1
           WHERE activation_id=%s AND activation_generation=%s
             AND observation_trigger='CADENCE_15M'
             AND history_evidence_status='CANONICAL'
             AND observation_bucket_at>=%s AND observation_bucket_at<=%s
             AND observed_at<=observation_bucket_at
           ORDER BY observation_bucket_at DESC,observation_id DESC LIMIT 1""",
        (
            epoch.drawdown_activation_id, epoch.drawdown_generation,
            epoch.first_required_cadence, scheduler,
        ),
    )
    row = cur.fetchone()
    if row is None:
        return RiskBudgetEpochBoundary(
            "EXPECTED_WAITING_FOR_FIRST_DRAWDOWN_CADENCE", None, epoch
        )
    return RiskBudgetEpochBoundary("CANONICAL", row[0], epoch, str(row[1]))


def bind_risk_budget_event_cursor(
    cur: Any, *, event_id: uuid.UUID, epoch: AuthorityEpoch,
    evaluation_as_of: datetime, risk_budget_source_fingerprint: str,
) -> str:
    evaluation = _require_time(evaluation_as_of)
    validate_ordering(
        baseline_at=epoch.replay_cutover_at, replay_at=epoch.replay_cutover_at,
        drawdown_at=epoch.drawdown_activation_boundary,
        first_cadence_at=epoch.first_required_cadence, q0_at=evaluation,
    )
    payload = {
        "contract_version": CONTRACT_VERSION, "event_id": event_id,
        "authority_epoch_id": epoch.authority_epoch_id,
        "evaluation_as_of": evaluation,
        "baseline_fingerprint": epoch.baseline_fingerprint,
        "replay_cutover_fingerprint": epoch.replay_cutover_fingerprint,
        "drawdown_generation_fingerprint": epoch.drawdown_generation_fingerprint,
        "risk_budget_source_fingerprint": risk_budget_source_fingerprint,
        "calibration_replay_eligible": True,
    }
    binding_fp = fingerprint(payload)
    cur.execute(
        """INSERT INTO risk_budget_authority_epoch_binding_v1(
             event_id,authority_epoch_id,evaluation_as_of,
             calibration_replay_eligible,baseline_fingerprint,
             replay_cutover_fingerprint,drawdown_generation_fingerprint,
             risk_budget_source_fingerprint,binding_fingerprint)
           VALUES (%s,%s,%s,TRUE,%s,%s,%s,%s,%s)
           ON CONFLICT (event_id) DO NOTHING RETURNING binding_fingerprint""",
        (
            str(event_id), epoch.authority_epoch_id, evaluation,
            epoch.baseline_fingerprint, epoch.replay_cutover_fingerprint,
            epoch.drawdown_generation_fingerprint,
            risk_budget_source_fingerprint, binding_fp,
        ),
    )
    inserted = cur.fetchone()
    if inserted:
        return "INSERTED"
    cur.execute(
        "SELECT binding_fingerprint FROM risk_budget_authority_epoch_binding_v1 "
        "WHERE event_id=%s", (str(event_id),),
    )
    existing = cur.fetchone()
    if existing and str(existing[0]) == binding_fp:
        return "IDEMPOTENT"
    raise JointAuthorityError("RISK_BUDGET_EPOCH_BINDING_CONFLICT")


def activation_attempt_identity(
    *, deployment_id: str, requested_boundary: datetime, prepared_at: datetime,
    source_fingerprints: Mapping[str, str], status: str,
    failure_reason: str | None = None,
) -> tuple[uuid.UUID, str]:
    payload = {
        "contract_version": CONTRACT_VERSION,
        "deployment_id": deployment_id,
        "requested_activation_boundary": _require_time(requested_boundary),
        "prepared_at": _require_time(prepared_at), "status": status,
        "source_fingerprints": source_fingerprints,
        "failure_reason": failure_reason,
    }
    attempt_fp = fingerprint(payload)
    return uuid.uuid5(_ATTEMPT_NAMESPACE, attempt_fp), attempt_fp


def record_activation_attempt_cursor(
    cur: Any, *, deployment_id: str, requested_boundary: datetime,
    prepared_at: datetime, source_fingerprints: Mapping[str, str], status: str,
    producer_revision: str, failure_reason: str | None = None,
    previous_failed_attempt_id: uuid.UUID | None = None,
) -> uuid.UUID:
    if status not in {"PREPARED", "FAILED", "ACTIVATED"}:
        raise ValueError("JOINT_AUTHORITY_ATTEMPT_STATUS_INVALID")
    if not is_canonical_boundary(requested_boundary):
        raise ValueError("JOINT_AUTHORITY_CANONICAL_BOUNDARY_REQUIRED")
    if len(producer_revision) != 40 or any(
        character not in "0123456789abcdef" for character in producer_revision
    ):
        raise ValueError("JOINT_AUTHORITY_REVISION_REQUIRED")
    if not source_fingerprints or any(
        not _is_fingerprint(value) for value in source_fingerprints.values()
    ):
        raise ValueError("JOINT_AUTHORITY_SOURCE_FINGERPRINT_REQUIRED")
    attempt_id, attempt_fp = activation_attempt_identity(
        deployment_id=deployment_id, requested_boundary=requested_boundary,
        prepared_at=prepared_at, source_fingerprints=source_fingerprints,
        status=status, failure_reason=failure_reason,
    )
    cur.execute(
        """INSERT INTO joint_authority_activation_attempt_v1(
             attempt_id,deployment_id,authority_identity,previous_failed_attempt_id,
             attempt_status,requested_activation_boundary,prepared_at,
             failure_reason,activated_at,source_fingerprints,producer_revision,
             attempt_fingerprint,contract_version)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (attempt_id) DO NOTHING""",
        (
            str(attempt_id), deployment_id,
            f"{CONTRACT_VERSION}:{deployment_id}",
            None if previous_failed_attempt_id is None else str(previous_failed_attempt_id),
            status, requested_boundary, prepared_at, failure_reason,
            requested_boundary if status == "ACTIVATED" else None,
            Json(dict(source_fingerprints)), producer_revision, attempt_fp,
            CONTRACT_VERSION,
        ),
    )
    return attempt_id


def calibration_eligible(*, epoch: AuthorityEpoch, evaluation_as_of: datetime) -> bool:
    return _require_time(evaluation_as_of) >= epoch.first_required_cadence


def activate_drawdown_epoch_cursor(
    cur: Any, *, deployment_id: str, requested_boundary: datetime,
    supersession_reason: str, expected_previous_history_status: str,
    approval_evidence: Mapping[str, Any], producer_identity: str,
    git_revision: str, deployment_identity: str,
    contract_versions: Mapping[str, str],
    contract_fingerprints: Mapping[str, str],
    portfolio_state_reader: Callable[..., Any],
    prepared_at: datetime | None = None,
    previous_failed_attempt_id: uuid.UUID | None = None,
) -> ActivationResult:
    """Atomically append generation, seed, selections and epoch.

    A savepoint removes every activation artifact on any incomplete source.  The
    immutable FAILED attempt is then appended outside that savepoint.  The
    caller owns the outer transaction and must commit the returned result.
    """
    from common.paper_drawdown_history import (
        PRODUCER_IDENTITY as DRAWDOWN_PRODUCER,
        capture_observation_candidate,
        create_activation_generation_cursor,
        persist_observation_candidate,
    )
    from common.paper_equity_baseline_v2 import fetch_paper_equity_baseline_v2
    from common.paper_portfolio_replay_cutover import load_replay_cutover_cursor

    deployment = str(deployment_id).lower()
    requested = _require_time(requested_boundary)
    prepared = _require_time(prepared_at or datetime.now(timezone.utc))
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("JOINT_AUTHORITY_DEPLOYMENT_INVALID")
    if supersession_reason not in APPROVED_SUPERSESSION_REASONS:
        raise ValueError("JOINT_AUTHORITY_SUPERSESSION_REASON_NOT_APPROVED")
    if not is_canonical_boundary(requested):
        raise ValueError("JOINT_AUTHORITY_CANONICAL_BOUNDARY_REQUIRED")
    baseline = fetch_paper_equity_baseline_v2(cur, deployment_id=deployment)
    replay = load_replay_cutover_cursor(cur, deployment_id=deployment)
    if baseline is None or baseline.evidence_status != "COMPLETE" or replay is None:
        raise JointAuthorityError("JOINT_AUTHORITY_UPSTREAM_INCOMPLETE")
    if baseline.baseline_timestamp > replay.cutover_at:
        raise JointAuthorityError("JOINT_AUTHORITY_BASELINE_AFTER_REPLAY")
    if (
        not contract_versions or not contract_fingerprints
        or set(contract_versions) != set(contract_fingerprints)
        or any(not str(value).strip() for value in contract_versions.values())
        or any(not _is_fingerprint(value) for value in contract_fingerprints.values())
    ):
        raise JointAuthorityError("JOINT_AUTHORITY_CONTRACT_FINGERPRINT_INVALID")
    normal_boundary = drawdown_activation_boundary(replay.cutover_at)
    recovery_at = prepared if prepared > normal_boundary else None
    expected_boundary = drawdown_activation_boundary(
        replay.cutover_at, operational_recovery_at=recovery_at,
    )
    if requested < replay.cutover_at or requested != expected_boundary:
        raise JointAuthorityError("JOINT_AUTHORITY_ACTIVATION_BOUNDARY_INVALID")
    sources = {
        "baseline": baseline.activation_fingerprint,
        "replay_cutover": replay.cutover_fingerprint,
        **{f"contract:{key}": value for key, value in contract_fingerprints.items()},
    }
    cur.execute("SAVEPOINT joint_authority_activation_v1")
    try:
        activation = create_activation_generation_cursor(
            cur, deployment_id=deployment, cutover_at=requested,
            selection_reason=supersession_reason,
            approval_evidence=approval_evidence,
            expected_previous_status=expected_previous_history_status,
            producer_identity=producer_identity, git_revision=git_revision,
        )
        state = portfolio_state_reader(
            cur, environment="PAPER", deployment_id=deployment,
            as_of=requested, runtime_revision=git_revision,
        )
        captured = capture_observation_candidate(
            state=state, baseline=baseline, activation=activation,
            observed_at=requested, observation_trigger="BASELINE_ACTIVATION",
            trigger_reference=f"JOINT_AUTHORITY_EPOCH:{requested.isoformat()}",
            producer_identity=DRAWDOWN_PRODUCER, git_revision=git_revision,
        )
        if captured.status != "CANONICAL" or captured.candidate is None:
            raise JointAuthorityError(f"JOINT_AUTHORITY_SEED_{captured.status}")
        persisted = persist_observation_candidate(cur, captured.candidate)
        if persisted.status != "CANONICAL":
            raise JointAuthorityError(f"JOINT_AUTHORITY_SEED_{persisted.status}")
        attempt_id = record_activation_attempt_cursor(
            cur, deployment_id=deployment, requested_boundary=requested,
            prepared_at=prepared, source_fingerprints=sources,
            status="ACTIVATED", producer_revision=git_revision,
            previous_failed_attempt_id=previous_failed_attempt_id,
        )
        epoch_payload = {
            "contract_version": CONTRACT_VERSION,
            "deployment_id": deployment, "baseline_id": baseline.baseline_id,
            "baseline_fingerprint": baseline.activation_fingerprint,
            "replay_cutover_id": replay.cutover_id,
            "replay_cutover_fingerprint": replay.cutover_fingerprint,
            "drawdown_activation_id": activation.activation_id,
            "drawdown_generation": activation.generation,
            "drawdown_generation_fingerprint": activation.activation_evidence_fingerprint,
            "drawdown_activation_boundary": requested,
            "first_required_cadence": requested + CADENCE,
            "activation_attempt_id": attempt_id,
            "git_revision": git_revision, "contract_versions": contract_versions,
            "contract_fingerprints": contract_fingerprints,
            "deployment_identity": deployment_identity,
        }
        epoch_fp = fingerprint(epoch_payload)
        cur.execute(
            """INSERT INTO joint_authority_epoch_v1(
                 deployment_id,baseline_id,baseline_fingerprint,replay_cutover_id,
                 replay_cutover_fingerprint,drawdown_activation_id,
                 drawdown_generation,drawdown_generation_fingerprint,
                 drawdown_activation_boundary,first_required_cadence,
                 activation_attempt_id,git_revision,contract_versions,
                 contract_fingerprints,deployment_identity,epoch_fingerprint)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               RETURNING authority_epoch_id""",
            (
                deployment, baseline.baseline_id, baseline.activation_fingerprint,
                replay.cutover_id, replay.cutover_fingerprint,
                activation.activation_id, activation.generation,
                activation.activation_evidence_fingerprint, requested,
                requested + CADENCE, str(attempt_id), git_revision,
                Json(dict(contract_versions)), Json(dict(contract_fingerprints)),
                deployment_identity, epoch_fp,
            ),
        )
        epoch_id = int(cur.fetchone()[0])
        cur.execute(
            """SELECT s.selection_id,s.authority_epoch_id
               FROM joint_authority_epoch_selection_v1 s
               LEFT JOIN joint_authority_epoch_selection_v1 successor
                 ON successor.previous_selection_id=s.selection_id
               WHERE s.deployment_id=%s AND successor.selection_id IS NULL""",
            (deployment,),
        )
        previous = cur.fetchall()
        if len(previous) > 1:
            raise JointAuthorityError("AMBIGUOUS_ACTIVE_AUTHORITY_EPOCH")
        prior = previous[0] if previous else None
        epoch_selection_reason = (
            supersession_reason if prior else "INITIAL_JOINT_AUTHORITY_EPOCH"
        )
        selection_payload = {
            "contract_version": CONTRACT_VERSION,
            "authority_epoch_id": epoch_id,
            "previous_selection_id": None if prior is None else int(prior[0]),
            "previous_authority_epoch_id": None if prior is None else int(prior[1]),
            "selected_at": requested, "selection_reason": epoch_selection_reason,
            "epoch_fingerprint": epoch_fp,
        }
        selection_fp = fingerprint(selection_payload)
        cur.execute(
            """INSERT INTO joint_authority_epoch_selection_v1(
                 authority_epoch_id,deployment_id,previous_selection_id,
                 previous_authority_epoch_id,selected_at,selection_reason,
                 selection_fingerprint,git_revision)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                epoch_id, deployment, None if prior is None else int(prior[0]),
                None if prior is None else int(prior[1]), requested,
                epoch_selection_reason, selection_fp, git_revision,
            ),
        )
        cur.execute("RELEASE SAVEPOINT joint_authority_activation_v1")
        epoch = AuthorityEpoch(
            epoch_id, deployment, baseline.baseline_id,
            baseline.activation_fingerprint, replay.cutover_id, replay.cutover_at,
            replay.cutover_fingerprint, activation.activation_id,
            activation.generation, activation.activation_evidence_fingerprint,
            requested, requested + CADENCE, epoch_fp, selection_fp,
        )
        return ActivationResult("ACTIVATED", attempt_id, epoch)
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT joint_authority_activation_v1")
        cur.execute("RELEASE SAVEPOINT joint_authority_activation_v1")
        reason = str(exc) or type(exc).__name__
        attempt_id = record_activation_attempt_cursor(
            cur, deployment_id=deployment, requested_boundary=requested,
            prepared_at=prepared, source_fingerprints=sources, status="FAILED",
            failure_reason=reason, producer_revision=git_revision,
            previous_failed_attempt_id=previous_failed_attempt_id,
        )
        return ActivationResult("FAILED", attempt_id, failure_reason=reason)
