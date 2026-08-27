"""Canonical READ_ONLY/SHADOW Risk Budget Authority V1.

This module has no execution imports and cannot block, resize, submit, or cancel
an order.  It evaluates and persists replayable advisory evidence only.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping


_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts/risk_budget_authority_v1_contract.json"
)
_CONTRACT = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
CONTRACT_VERSION = str(_CONTRACT["contract_version"])
POLICY_VERSION = str(_CONTRACT["policy"]["semantic_version"])
POLICY_STATES = frozenset(_CONTRACT["policy"]["states"])
AUTHORITY_STATUSES = frozenset(_CONTRACT["statuses"])
ADVISORY_RESULTS = frozenset(_CONTRACT["advisory_results"])
REQUIRED_SOURCE_FINGERPRINTS = frozenset(_CONTRACT["source_fingerprint_keys"])
PAPER_CONTROLLED_INFLUENCE_READY = False
ZERO = Decimal("0")
OPEN_RISK_CANONICAL_STATUSES = frozenset({"CANONICAL", "CANONICAL_EMPTY"})
_EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "waltrade:risk-budget:v1")
_DEPLOYMENT_MODES = {
    "local-paper": "PAPER",
    "vps-paper": "PAPER",
    "local-live": "LIVE",
    "vps-live": "LIVE",
}


class RiskBudgetEvidenceError(ValueError):
    pass


class RiskBudgetIdempotencyConflict(RuntimeError):
    pass


def _decimal(value: object, field: str) -> Decimal:
    if value is None or isinstance(value, float):
        raise RiskBudgetEvidenceError(f"{field}_DECIMAL_REQUIRED")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RiskBudgetEvidenceError(f"{field}_DECIMAL_REQUIRED") from exc
    if not result.is_finite():
        raise RiskBudgetEvidenceError(f"{field}_DECIMAL_REQUIRED")
    return result


def _canonical_value(value: Any) -> Any:
    if isinstance(value, float):
        raise RiskBudgetEvidenceError("BINARY_FLOAT_FORBIDDEN")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise RiskBudgetEvidenceError("TIMEZONE_AWARE_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        total_microseconds = (
            (value.days * 86_400 + value.seconds) * 1_000_000
            + value.microseconds
        )
        return {"duration_microseconds": total_microseconds}
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    raise RiskBudgetEvidenceError(
        f"CANONICAL_SERIALIZATION_UNSUPPORTED_TYPE:{type(value).__name__}"
    )


def canonical_json(value: Any) -> str:
    return json.dumps(
        _canonical_value(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


POLICY_FINGERPRINT = fingerprint(_CONTRACT["policy"])


def missing_numeric_policy_evidence() -> "NumericPolicyEvidence":
    """Return the approved semantic policy identity without invented numbers."""
    return NumericPolicyEvidence(
        policy_version=POLICY_VERSION,
        policy_fingerprint=POLICY_FINGERPRINT,
        status="MISSING_POLICY",
        policy_state=None,
        total_risk_capacity=None,
    )


def is_canonical_open_risk_status(status: object) -> bool:
    """Return whether Open Risk has one of the exact canonical V1 statuses."""
    return str(status) in OPEN_RISK_CANONICAL_STATUSES


def _is_fingerprint(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _validate_scope(
    environment: str, deployment_id: str, account_identity_fingerprint: str,
) -> tuple[str, str, str]:
    mode = str(environment or "").upper()
    deployment = str(deployment_id or "").lower()
    account = str(account_identity_fingerprint or "").lower()
    if _DEPLOYMENT_MODES.get(deployment) != mode:
        raise RiskBudgetEvidenceError("ENVIRONMENT_DEPLOYMENT_MISMATCH")
    if not _is_fingerprint(account):
        raise RiskBudgetEvidenceError("ACCOUNT_IDENTITY_FINGERPRINT_REQUIRED")
    return mode, deployment, account


@dataclass(frozen=True)
class RiskBudgetInputs:
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    as_of: datetime
    total_capital: Decimal | None
    total_capital_status: str
    open_risk: Decimal | None
    open_risk_status: str
    pre_entry_committed_risk: Decimal | None
    pre_entry_risk_status: str
    current_drawdown_abs: Decimal | None
    current_drawdown_pct: Decimal | None
    max_drawdown_abs: Decimal | None
    max_drawdown_pct: Decimal | None
    recovery_status: str | None
    drawdown_history_status: str
    source_fingerprints: Mapping[str, str]
    identity_status: str = "CANONICAL"
    source_fingerprint_status: str = "CANONICAL"
    freshness_status: str = "CANONICAL"


@dataclass(frozen=True)
class NumericPolicyEvidence:
    policy_version: str
    policy_fingerprint: str
    status: str = "MISSING_POLICY"
    policy_state: str | None = None
    total_risk_capacity: Decimal | None = None


@dataclass(frozen=True)
class RiskBudgetSnapshot:
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    as_of: datetime
    policy_version: str
    policy_fingerprint: str
    authority_status: str
    policy_state: str | None
    total_capital: Decimal | None
    open_risk: Decimal | None
    pre_entry_committed_risk: Decimal | None
    used_risk: Decimal | None
    current_drawdown_abs: Decimal | None
    current_drawdown_pct: Decimal | None
    max_drawdown_abs: Decimal | None
    max_drawdown_pct: Decimal | None
    recovery_status: str | None
    drawdown_history_status: str
    total_risk_capacity: Decimal | None
    available_risk_capacity: Decimal | None
    reason_codes: tuple[str, ...]
    source_fingerprints: Mapping[str, str]
    paper_controlled_influence_ready: bool = False


@dataclass(frozen=True)
class AdvisoryDecision:
    result: str
    authority_status: str
    reason_codes: tuple[str, ...]
    candidate_pre_entry_risk: Decimal
    candidate_evidence_fingerprint: str
    admissible_risk_capacity: Decimal | None


@dataclass(frozen=True)
class PersistResult:
    status: str
    event_id: uuid.UUID
    event_fingerprint: str


def _validated_optional_nonnegative(value: object, field: str) -> Decimal | None:
    if value is None:
        return None
    result = _decimal(value, field)
    if result < ZERO:
        raise RiskBudgetEvidenceError(f"{field}_NEGATIVE")
    return result


def _required_truth_status(inputs: RiskBudgetInputs) -> str:
    if inputs.identity_status != "CANONICAL":
        return "ACCOUNT_IDENTITY_MISMATCH"
    if (
        inputs.source_fingerprint_status != "CANONICAL"
        or not REQUIRED_SOURCE_FINGERPRINTS.issubset(inputs.source_fingerprints)
        or any(not _is_fingerprint(value) for value in inputs.source_fingerprints.values())
    ):
        return "SOURCE_FINGERPRINT_MISMATCH"
    if inputs.freshness_status != "CANONICAL":
        return "STALE_AUTHORITY"
    if inputs.total_capital_status != "CANONICAL" or inputs.total_capital is None:
        return "INCOMPLETE_PORTFOLIO_STATE"
    if (
        inputs.drawdown_history_status != "CANONICAL"
        or inputs.current_drawdown_abs is None
        or inputs.current_drawdown_pct is None
        or inputs.max_drawdown_abs is None
        or inputs.max_drawdown_pct is None
        or not inputs.recovery_status
    ):
        return "INCOMPLETE_DRAWDOWN_HISTORY"
    if (
        not is_canonical_open_risk_status(inputs.open_risk_status)
        or inputs.open_risk is None
    ):
        return "INCOMPLETE_OPEN_RISK"
    if inputs.pre_entry_risk_status != "CANONICAL" or inputs.pre_entry_committed_risk is None:
        return "INCOMPLETE_PRE_ENTRY_RISK"
    return "CANONICAL"


def evaluate_state(
    inputs: RiskBudgetInputs, policy: NumericPolicyEvidence,
) -> RiskBudgetSnapshot:
    """Evaluate truth and optional numeric policy without inventing capacity."""
    mode, deployment, account = _validate_scope(
        inputs.environment, inputs.deployment_id,
        inputs.account_identity_fingerprint,
    )
    if inputs.as_of.tzinfo is None:
        raise RiskBudgetEvidenceError("AS_OF_TIMEZONE_REQUIRED")
    if not str(policy.policy_version or "").strip() or not _is_fingerprint(policy.policy_fingerprint):
        raise RiskBudgetEvidenceError("POLICY_IDENTITY_REQUIRED")

    total = _validated_optional_nonnegative(inputs.total_capital, "TOTAL_CAPITAL")
    open_risk = _validated_optional_nonnegative(inputs.open_risk, "OPEN_RISK")
    committed = _validated_optional_nonnegative(
        inputs.pre_entry_committed_risk, "PRE_ENTRY_COMMITTED_RISK"
    )
    used = None if open_risk is None or committed is None else open_risk + committed
    current_drawdown_abs = (
        None if inputs.current_drawdown_abs is None
        else _decimal(inputs.current_drawdown_abs, "CURRENT_DRAWDOWN_ABS")
    )
    current_drawdown_pct = (
        None if inputs.current_drawdown_pct is None
        else _decimal(inputs.current_drawdown_pct, "CURRENT_DRAWDOWN_PCT")
    )
    max_drawdown_abs = (
        None if inputs.max_drawdown_abs is None
        else _decimal(inputs.max_drawdown_abs, "MAX_DRAWDOWN_ABS")
    )
    max_drawdown_pct = (
        None if inputs.max_drawdown_pct is None
        else _decimal(inputs.max_drawdown_pct, "MAX_DRAWDOWN_PCT")
    )
    truth_status = _required_truth_status(inputs)
    reasons: list[str] = [] if truth_status == "CANONICAL" else [truth_status]
    state = None
    capacity = None
    available = None
    status = truth_status

    if truth_status == "CANONICAL":
        if policy.status != "CANONICAL":
            status = "MISSING_POLICY"
            reasons.append("MISSING_POLICY")
        elif policy.policy_state not in POLICY_STATES or policy.total_risk_capacity is None:
            status = "MISSING_POLICY"
            reasons.append("MISSING_POLICY")
        else:
            capacity = _validated_optional_nonnegative(
                policy.total_risk_capacity, "TOTAL_RISK_CAPACITY"
            )
            state = policy.policy_state
            assert capacity is not None and used is not None
            available = max(ZERO, capacity - used)
            if available == ZERO:
                status = "RISK_CAPACITY_EXHAUSTED"
                reasons.append("RISK_CAPACITY_EXHAUSTED")
            else:
                status = "CANONICAL"

    return RiskBudgetSnapshot(
        environment=mode, deployment_id=deployment,
        account_identity_fingerprint=account,
        as_of=inputs.as_of.astimezone(timezone.utc),
        policy_version=str(policy.policy_version),
        policy_fingerprint=str(policy.policy_fingerprint),
        authority_status=status, policy_state=state,
        total_capital=total, open_risk=open_risk,
        pre_entry_committed_risk=committed, used_risk=used,
        current_drawdown_abs=current_drawdown_abs,
        current_drawdown_pct=current_drawdown_pct,
        max_drawdown_abs=max_drawdown_abs,
        max_drawdown_pct=max_drawdown_pct,
        recovery_status=inputs.recovery_status,
        drawdown_history_status=inputs.drawdown_history_status,
        total_risk_capacity=capacity, available_risk_capacity=available,
        reason_codes=tuple(reasons),
        source_fingerprints=dict(inputs.source_fingerprints),
        paper_controlled_influence_ready=PAPER_CONTROLLED_INFLUENCE_READY,
    )


def evaluate_pre_entry_gate(
    snapshot: RiskBudgetSnapshot, *, candidate_pre_entry_risk: Decimal,
    candidate_evidence_fingerprint: str, environment: str,
    deployment_id: str, account_identity_fingerprint: str,
) -> AdvisoryDecision:
    """Return a hypothetical decision; this function has no execution effect."""
    candidate = _decimal(candidate_pre_entry_risk, "CANDIDATE_PRE_ENTRY_RISK")
    if candidate <= ZERO:
        raise RiskBudgetEvidenceError("CANDIDATE_PRE_ENTRY_RISK_INVALID")
    if not _is_fingerprint(candidate_evidence_fingerprint):
        return AdvisoryDecision(
            "BLOCK_NEW_RISK", "SOURCE_FINGERPRINT_MISMATCH",
            ("SOURCE_FINGERPRINT_MISMATCH",), candidate,
            str(candidate_evidence_fingerprint), None,
        )
    try:
        scope = _validate_scope(environment, deployment_id, account_identity_fingerprint)
    except RiskBudgetEvidenceError:
        scope = (str(environment).upper(), str(deployment_id).lower(), str(account_identity_fingerprint))
    if scope != (
        snapshot.environment, snapshot.deployment_id,
        snapshot.account_identity_fingerprint,
    ):
        return AdvisoryDecision(
            "BLOCK_NEW_RISK", "ACCOUNT_IDENTITY_MISMATCH",
            ("ACCOUNT_IDENTITY_MISMATCH",), candidate,
            candidate_evidence_fingerprint, None,
        )
    if snapshot.authority_status != "CANONICAL":
        return AdvisoryDecision(
            "BLOCK_NEW_RISK", snapshot.authority_status,
            snapshot.reason_codes or (snapshot.authority_status,), candidate,
            candidate_evidence_fingerprint, snapshot.available_risk_capacity,
        )
    if snapshot.policy_state in {"NO_NEW_RISK", "PAUSED"}:
        return AdvisoryDecision(
            "BLOCK_NEW_RISK", "CANONICAL",
            (f"POLICY_STATE_{snapshot.policy_state}",), candidate,
            candidate_evidence_fingerprint, snapshot.available_risk_capacity,
        )
    available = snapshot.available_risk_capacity
    if available is None or available == ZERO:
        return AdvisoryDecision(
            "BLOCK_NEW_RISK", "RISK_CAPACITY_EXHAUSTED",
            ("RISK_CAPACITY_EXHAUSTED",), candidate,
            candidate_evidence_fingerprint, available,
        )
    if candidate <= available:
        return AdvisoryDecision(
            "ALLOW", "CANONICAL", ("CANDIDATE_FITS_AVAILABLE_RISK_CAPACITY",),
            candidate, candidate_evidence_fingerprint, available,
        )
    return AdvisoryDecision(
        "REDUCE", "CANONICAL", ("CANDIDATE_EXCEEDS_AVAILABLE_RISK_CAPACITY",),
        candidate, candidate_evidence_fingerprint, available,
    )


def account_scope_lock_identity(
    environment: str, deployment_id: str, account_identity_fingerprint: str,
) -> str:
    mode, deployment, account = _validate_scope(
        environment, deployment_id, account_identity_fingerprint,
    )
    return f"{CONTRACT_VERSION}:{mode}:{deployment}:{account}"


def evaluate_account_scoped_shadow_gate_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str,
    input_loader: Callable[[Any], RiskBudgetInputs],
    policy: NumericPolicyEvidence, candidate_pre_entry_risk: Decimal,
    candidate_evidence_fingerprint: str,
) -> tuple[RiskBudgetSnapshot, AdvisoryDecision]:
    """Lock the account, then re-read both active-risk owners and advise.

    ``input_loader`` must materialize fresh Portfolio State Open Risk and the
    canonical committed Pre-Entry Risk aggregate using this same cursor.
    Nothing in this API writes a commitment or influences execution.
    """
    lock_identity = account_scope_lock_identity(
        environment, deployment_id, account_identity_fingerprint,
    )
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (lock_identity,))
    fresh_inputs = input_loader(cur)
    snapshot = evaluate_state(fresh_inputs, policy)
    decision = evaluate_pre_entry_gate(
        snapshot, candidate_pre_entry_risk=candidate_pre_entry_risk,
        candidate_evidence_fingerprint=candidate_evidence_fingerprint,
        environment=environment, deployment_id=deployment_id,
        account_identity_fingerprint=account_identity_fingerprint,
    )
    return snapshot, decision


def evaluate_and_persist_account_scoped_shadow_gate_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str,
    input_loader: Callable[[Any], RiskBudgetInputs],
    policy: NumericPolicyEvidence, candidate_pre_entry_risk: Decimal,
    candidate_evidence_fingerprint: str, decision_identity: str,
    producer_identity: str, git_revision: str,
) -> tuple[RiskBudgetSnapshot, AdvisoryDecision, PersistResult]:
    """Atomically re-read, evaluate, and persist one shadow advisory.

    The transaction remains caller-owned.  The function never creates a real
    commitment and has no execution side effect.
    """
    snapshot, decision = evaluate_account_scoped_shadow_gate_cursor(
        cur, environment=environment, deployment_id=deployment_id,
        account_identity_fingerprint=account_identity_fingerprint,
        input_loader=input_loader, policy=policy,
        candidate_pre_entry_risk=candidate_pre_entry_risk,
        candidate_evidence_fingerprint=candidate_evidence_fingerprint,
    )
    persisted = persist_event_cursor(
        cur, snapshot, event_type="PRE_ENTRY_GATE_DECISION",
        event_identity=decision_identity, decision=decision,
        producer_identity=producer_identity, git_revision=git_revision,
    )
    return snapshot, decision, persisted


def _event_payload(
    snapshot: RiskBudgetSnapshot, *, event_type: str, event_identity: str,
    producer_identity: str, git_revision: str,
    decision: AdvisoryDecision | None,
) -> dict[str, Any]:
    if event_type not in {"STATE_EVALUATION", "PRE_ENTRY_GATE_DECISION"}:
        raise RiskBudgetEvidenceError("EVENT_TYPE_INVALID")
    if not str(event_identity or "").strip():
        raise RiskBudgetEvidenceError("EVENT_IDENTITY_REQUIRED")
    if not str(producer_identity or "").strip():
        raise RiskBudgetEvidenceError("PRODUCER_IDENTITY_REQUIRED")
    revision = str(git_revision or "").lower()
    if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
        raise RiskBudgetEvidenceError("GIT_REVISION_REQUIRED")
    if (event_type == "STATE_EVALUATION") != (decision is None):
        raise RiskBudgetEvidenceError("EVENT_DECISION_SHAPE_INVALID")
    payload = {
        "event_type": event_type,
        "event_identity": event_identity,
        "snapshot": asdict(snapshot),
        "decision": None if decision is None else asdict(decision),
        "producer_identity": producer_identity,
        "git_revision": revision,
        "contract_version": CONTRACT_VERSION,
        "shadow_only": True,
        "paper_controlled_influence_ready": False,
    }
    return payload


def _semantic_event_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return immutable event meaning without writer provenance.

    Producer and Git revisions explain who materialized an event, but they do
    not change the Risk Budget observation itself.  They remain stored in the
    append-only evidence payload and dedicated columns while replay equality
    is evaluated only over semantic fields.
    """
    return {
        str(key): value for key, value in payload.items()
        if key not in {"producer_identity", "git_revision"}
    }


_INSERT_COLUMNS = """
event_id,event_type,event_identity,environment,deployment_id,
account_identity_fingerprint,event_at,policy_version,policy_fingerprint,
authority_status,policy_state,total_capital,open_risk,
pre_entry_committed_risk,used_risk,current_drawdown_abs,
current_drawdown_pct,max_drawdown_abs,max_drawdown_pct,recovery_status,
drawdown_history_status,total_risk_capacity,available_risk_capacity,
candidate_pre_entry_risk,candidate_evidence_fingerprint,advisory_result,
reason_codes,source_fingerprints,evidence,producer_identity,git_revision,
contract_version,event_fingerprint,shadow_only,
paper_controlled_influence_ready
""".replace("\n", "")


def persist_event_cursor(
    cur: Any, snapshot: RiskBudgetSnapshot, *, event_type: str,
    event_identity: str, producer_identity: str, git_revision: str,
    decision: AdvisoryDecision | None = None,
) -> PersistResult:
    """Append an immutable event or accept a byte-identical replay."""
    payload = _event_payload(
        snapshot, event_type=event_type, event_identity=event_identity,
        producer_identity=producer_identity, git_revision=git_revision,
        decision=decision,
    )
    event_fp = fingerprint(payload)
    natural = (
        f"{snapshot.environment}:{snapshot.deployment_id}:"
        f"{snapshot.account_identity_fingerprint}:{event_type}:{event_identity}"
    )
    event_id = uuid.uuid5(_EVENT_NAMESPACE, natural)
    reasons = snapshot.reason_codes if decision is None else decision.reason_codes
    event_authority_status = (
        snapshot.authority_status if decision is None else decision.authority_status
    )
    values = (
        str(event_id), event_type, event_identity, snapshot.environment,
        snapshot.deployment_id, snapshot.account_identity_fingerprint,
        snapshot.as_of, snapshot.policy_version, snapshot.policy_fingerprint,
        event_authority_status, snapshot.policy_state,
        snapshot.total_capital, snapshot.open_risk,
        snapshot.pre_entry_committed_risk, snapshot.used_risk,
        snapshot.current_drawdown_abs, snapshot.current_drawdown_pct,
        snapshot.max_drawdown_abs, snapshot.max_drawdown_pct,
        snapshot.recovery_status, snapshot.drawdown_history_status,
        snapshot.total_risk_capacity, snapshot.available_risk_capacity,
        None if decision is None else decision.candidate_pre_entry_risk,
        None if decision is None else decision.candidate_evidence_fingerprint,
        None if decision is None else decision.result,
        json.dumps(list(reasons)),
        canonical_json(snapshot.source_fingerprints), canonical_json(payload),
        producer_identity, str(git_revision).lower(), CONTRACT_VERSION,
        event_fp, True, False,
    )
    placeholders = ",".join(["%s"] * len(values))
    cur.execute(
        f"INSERT INTO risk_budget_event_v1({_INSERT_COLUMNS}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING RETURNING event_id",
        values,
    )
    inserted = cur.fetchone()
    if inserted:
        return PersistResult("INSERTED", event_id, event_fp)
    cur.execute(
        "SELECT event_id,event_fingerprint,evidence FROM risk_budget_event_v1 "
        "WHERE environment=%s AND deployment_id=%s "
        "AND account_identity_fingerprint=%s AND event_type=%s "
        "AND event_identity=%s",
        (
            snapshot.environment, snapshot.deployment_id,
            snapshot.account_identity_fingerprint, event_type, event_identity,
        ),
    )
    existing = cur.fetchone()
    if existing and uuid.UUID(str(existing[0])) == event_id:
        existing_payload = dict(existing[2])
        if fingerprint(_semantic_event_payload(existing_payload)) == fingerprint(
            _semantic_event_payload(payload)
        ):
            return PersistResult("IDEMPOTENT", event_id, str(existing[1]))
    raise RiskBudgetIdempotencyConflict("RISK_BUDGET_EVENT_IDEMPOTENCY_CONFLICT")


def paper_controlled_influence_readiness() -> tuple[bool, str]:
    return (
        False,
        "APPROVED_NUMERIC_POLICY_AND_HISTORY_SUFFICIENCY_CALIBRATION_REQUIRED",
    )
