"""Canonical Capital Reservation Authority V1.

The authority stores only the missing capital-commitment lifecycle.  Orders,
fills and positions remain owned by their existing ledgers and are referenced
by identity.  Financial arithmetic is Decimal-only and current state is a
deterministic projection of append-only cumulative events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping
import uuid

from psycopg2.extras import Json


CONTRACT_VERSION = "CAPITAL_RESERVATION_AUTHORITY_V1"
PURPOSE = "ENTRY"
ZERO = Decimal("0")
RESERVATION_NAMESPACE = uuid.UUID("8f154af1-c3d5-4ea8-89d4-7d2730e9f231")
EVENT_NAMESPACE = uuid.UUID("7a6c48dc-a0cb-48ed-809c-d1cb50b91726")

ACTIVE_STATES = frozenset({
    "ACCEPTED_COMMITMENT",
    "INTERNAL_RESERVED",
    "SUBMITTED",
    "EXCHANGE_ACK",
    "EXCHANGE_LOCKED",
    "PARTIALLY_DEPLOYED",
})
TERMINAL_STATES = frozenset({
    "DEPLOYED", "RELEASED", "CANCELLED", "EXPIRED", "REJECTED",
})
REFLECTION_STATES = frozenset({
    "INTERNAL_UNREFLECTED", "EXCHANGE_REFLECTED", "PAPER_SIMULATED",
})
RECONCILIATION_STATUSES = frozenset({
    "CANONICAL", "PENDING_EXCHANGE_REFLECTION", "RECONCILIATION_FAILED",
})


def _decimal(value: object, *, field: str) -> Decimal:
    if value is None or isinstance(value, float):
        raise ValueError(f"CAPITAL_RESERVATION_INVALID_DECIMAL:{field}")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"CAPITAL_RESERVATION_INVALID_DECIMAL:{field}")
    return result


def _canonical_json(payload: Mapping[str, Any]) -> str:
    def normalize(value: Any) -> Any:
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("CAPITAL_RESERVATION_TIMESTAMP_REQUIRED")
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, Mapping):
            return {str(key): normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if isinstance(value, float):
            raise ValueError("CAPITAL_RESERVATION_FLOAT_FORBIDDEN")
        return value

    return json.dumps(
        normalize(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def paper_account_identity_fingerprint(deployment_id: str) -> str:
    deployment = str(deployment_id).strip().lower()
    if deployment not in {"local-paper", "vps-paper"}:
        raise ValueError("CAPITAL_RESERVATION_PAPER_DEPLOYMENT_INVALID")
    return hashlib.sha256(
        f"WALTRADE:PAPER_SIMULATOR:{deployment}".encode("utf-8")
    ).hexdigest()


def logical_commitment_key(
    *, environment: str, deployment_id: str, source_identity: str,
) -> str:
    mode = str(environment).strip().upper()
    deployment = str(deployment_id).strip().lower()
    source = str(source_identity).strip()
    if mode not in {"PAPER", "LIVE"} or not source:
        raise ValueError("CAPITAL_RESERVATION_COMMITMENT_IDENTITY_INVALID")
    return f"{mode}:{deployment}:{source}"


def deterministic_reservation_id(commitment_key: str) -> uuid.UUID:
    return uuid.uuid5(RESERVATION_NAMESPACE, str(commitment_key))


@dataclass(frozen=True)
class CapitalReservationEvent:
    event_id: uuid.UUID
    reservation_id: uuid.UUID
    event_sequence: int
    logical_commitment_key: str
    source_event_identity: str
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    symbol: str
    strategy: str
    interval: str
    decision_identity: str | None
    intent_identity: str | None
    order_identity: str | None
    position_id: int | None
    requested_notional: Decimal
    remaining_reserved_notional: Decimal
    deployed_notional: Decimal
    released_notional: Decimal
    state: str
    reflection_state: str
    reconciliation_status: str
    reconciliation_reason: str | None
    release_reason: str | None
    effective_at: datetime
    source_authority: str
    provenance: Mapping[str, Any]
    policy_fingerprint: str | None
    event_fingerprint: str
    contract_version: str = CONTRACT_VERSION

    def semantic_payload(self) -> dict[str, Any]:
        return {
            key: value for key, value in self.__dict__.items()
            if key not in {"event_id", "event_fingerprint"}
        }


@dataclass(frozen=True)
class CapitalReservationEvidence:
    reserved_capital: Decimal | None
    internal_unreflected_reserved: Decimal | None
    exchange_reflected_reserved: Decimal | None
    active_reservation_count: int
    status: str
    reconciliation_status: str
    latest_event_at: datetime | None
    account_identity_fingerprint: str | None
    incomplete_reasons: tuple[str, ...]


def _make_event(
    *, reservation_id: uuid.UUID, event_sequence: int,
    logical_key: str, source_event_identity: str,
    environment: str, deployment_id: str,
    account_identity_fingerprint: str, symbol: str, strategy: str,
    interval: str, decision_identity: str | None,
    intent_identity: str | None, order_identity: str | None,
    position_id: int | None, requested_notional: Decimal,
    remaining_reserved_notional: Decimal, deployed_notional: Decimal,
    released_notional: Decimal, state: str, reflection_state: str,
    reconciliation_status: str, reconciliation_reason: str | None,
    release_reason: str | None, effective_at: datetime,
    source_authority: str, provenance: Mapping[str, Any],
    policy_fingerprint: str | None,
) -> CapitalReservationEvent:
    if effective_at.tzinfo is None:
        raise ValueError("CAPITAL_RESERVATION_TIMESTAMP_REQUIRED")
    requested = _decimal(requested_notional, field="requested_notional")
    remaining = _decimal(
        remaining_reserved_notional, field="remaining_reserved_notional"
    )
    deployed = _decimal(deployed_notional, field="deployed_notional")
    released = _decimal(released_notional, field="released_notional")
    if min(requested, remaining, deployed, released) < ZERO:
        raise ValueError("CAPITAL_RESERVATION_NEGATIVE_AMOUNT")
    if requested != remaining + deployed + released:
        raise ValueError("CAPITAL_RESERVATION_ACCOUNTING_EQUATION_FAILED")
    state_value = str(state).upper()
    reflection = str(reflection_state).upper()
    reconciliation = str(reconciliation_status).upper()
    if state_value not in ACTIVE_STATES | TERMINAL_STATES:
        raise ValueError("CAPITAL_RESERVATION_STATE_INVALID")
    if reflection not in REFLECTION_STATES:
        raise ValueError("CAPITAL_RESERVATION_REFLECTION_INVALID")
    if reconciliation not in RECONCILIATION_STATUSES:
        raise ValueError("CAPITAL_RESERVATION_RECONCILIATION_INVALID")
    payload = {
        "reservation_id": reservation_id,
        "event_sequence": int(event_sequence),
        "logical_commitment_key": str(logical_key),
        "source_event_identity": str(source_event_identity),
        "environment": str(environment).upper(),
        "deployment_id": str(deployment_id).lower(),
        "account_identity_fingerprint": str(account_identity_fingerprint),
        "purpose": PURPOSE,
        "symbol": str(symbol).upper(),
        "strategy": str(strategy).upper(),
        "interval": str(interval).lower(),
        "decision_identity": decision_identity,
        "intent_identity": intent_identity,
        "order_identity": order_identity,
        "position_id": position_id,
        "requested_notional": requested,
        "remaining_reserved_notional": remaining,
        "deployed_notional": deployed,
        "released_notional": released,
        "state": state_value,
        "reflection_state": reflection,
        "reconciliation_status": reconciliation,
        "reconciliation_reason": reconciliation_reason,
        "release_reason": release_reason,
        "effective_at": effective_at,
        "source_authority": str(source_authority),
        "provenance": dict(provenance),
        "policy_fingerprint": policy_fingerprint,
        "contract_version": CONTRACT_VERSION,
    }
    fingerprint = _fingerprint(payload)
    event_id = uuid.uuid5(
        EVENT_NAMESPACE,
        f"{reservation_id}:{source_event_identity}:{fingerprint}",
    )
    return CapitalReservationEvent(
        event_id=event_id,
        event_fingerprint=fingerprint,
        **{key: value for key, value in payload.items()
           if key not in {"purpose"}},
    )


def accepted_commitment_event(
    *, environment: str, deployment_id: str,
    account_identity_fingerprint: str, source_identity: str,
    symbol: str, strategy: str, interval: str,
    requested_notional: Decimal | str | int, effective_at: datetime,
    source_authority: str, provenance: Mapping[str, Any],
    decision_identity: str | None = None,
    intent_identity: str | None = None,
    order_identity: str | None = None,
    policy_fingerprint: str | None = None,
) -> CapitalReservationEvent:
    requested = _decimal(requested_notional, field="requested_notional")
    if requested <= ZERO:
        raise ValueError("CAPITAL_RESERVATION_REQUESTED_NOTIONAL_REQUIRED")
    key = logical_commitment_key(
        environment=environment, deployment_id=deployment_id,
        source_identity=source_identity,
    )
    reflection = (
        "PAPER_SIMULATED" if str(environment).upper() == "PAPER"
        else "INTERNAL_UNREFLECTED"
    )
    return _make_event(
        reservation_id=deterministic_reservation_id(key), event_sequence=1,
        logical_key=key, source_event_identity=f"ACCEPTED:{source_identity}",
        environment=environment, deployment_id=deployment_id,
        account_identity_fingerprint=account_identity_fingerprint,
        symbol=symbol, strategy=strategy, interval=interval,
        decision_identity=decision_identity, intent_identity=intent_identity,
        order_identity=order_identity, position_id=None,
        requested_notional=requested, remaining_reserved_notional=requested,
        deployed_notional=ZERO, released_notional=ZERO,
        state="ACCEPTED_COMMITMENT", reflection_state=reflection,
        reconciliation_status="CANONICAL", reconciliation_reason=None,
        release_reason=None, effective_at=effective_at,
        source_authority=source_authority, provenance=provenance,
        policy_fingerprint=policy_fingerprint,
    )


_EVENT_COLUMNS = """
event_id,reservation_id,event_sequence,logical_commitment_key,
source_event_identity,environment,deployment_id,account_identity_fingerprint,
purpose,symbol,strategy,interval,decision_identity,intent_identity,
order_identity,position_id,requested_notional,remaining_reserved_notional,
deployed_notional,released_notional,state,reflection_state,
reconciliation_status,reconciliation_reason,release_reason,effective_at,
source_authority,provenance,policy_fingerprint,event_fingerprint,
contract_version
"""


def reservation_schema_available_cursor(cur: Any) -> bool:
    cur.execute(
        "SELECT to_regclass('public.capital_reservation_event_v1'),"
        "to_regclass('public.v_capital_reservation_current_v1')"
    )
    row = cur.fetchone()
    return bool(row and row[0] is not None and row[1] is not None)


def append_event_cursor(cur: Any, event: CapitalReservationEvent) -> str:
    cur.execute(
        "SELECT event_fingerprint FROM capital_reservation_event_v1 "
        "WHERE event_id=%s", (str(event.event_id),),
    )
    existing = cur.fetchone()
    if existing is not None:
        if str(existing[0]) == event.event_fingerprint:
            return "IDEMPOTENT"
        raise ValueError("CAPITAL_RESERVATION_EVENT_ID_CONFLICT")
    values = (
        str(event.event_id), str(event.reservation_id), event.event_sequence,
        event.logical_commitment_key, event.source_event_identity,
        event.environment, event.deployment_id,
        event.account_identity_fingerprint, PURPOSE, event.symbol,
        event.strategy, event.interval, event.decision_identity,
        event.intent_identity, event.order_identity, event.position_id,
        event.requested_notional, event.remaining_reserved_notional,
        event.deployed_notional, event.released_notional, event.state,
        event.reflection_state, event.reconciliation_status,
        event.reconciliation_reason, event.release_reason, event.effective_at,
        event.source_authority, Json(dict(event.provenance)),
        event.policy_fingerprint, event.event_fingerprint,
        event.contract_version,
    )
    cur.execute(
        f"INSERT INTO capital_reservation_event_v1 ({_EVENT_COLUMNS}) "
        "VALUES (" + ",".join(["%s"] * len(values)) + ") "
        "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
        values,
    )
    if cur.fetchone() is not None:
        return "INSERTED"
    cur.execute(
        "SELECT event_fingerprint FROM capital_reservation_event_v1 "
        "WHERE event_id=%s", (str(event.event_id),),
    )
    row = cur.fetchone()
    if row and str(row[0]) == event.event_fingerprint:
        return "IDEMPOTENT"
    raise ValueError("CAPITAL_RESERVATION_EVENT_ID_CONFLICT")


def _current_event_cursor(cur: Any, reservation_id: uuid.UUID) -> tuple[Any, ...] | None:
    cur.execute(
        """
        SELECT reservation_id,event_sequence,logical_commitment_key,
               environment,deployment_id,account_identity_fingerprint,
               symbol,strategy,interval,decision_identity,intent_identity,
               order_identity,position_id,requested_notional,
               remaining_reserved_notional,deployed_notional,released_notional,
               state,reflection_state,reconciliation_status,
               reconciliation_reason,policy_fingerprint
        FROM capital_reservation_event_v1
        WHERE reservation_id=%s
        ORDER BY event_sequence DESC LIMIT 1 FOR UPDATE
        """,
        (str(reservation_id),),
    )
    return cur.fetchone()


def transition_reservation_cursor(
    cur: Any, *, reservation_id: uuid.UUID,
    source_event_identity: str, state: str, effective_at: datetime,
    source_authority: str, provenance: Mapping[str, Any],
    deployed_notional_delta: Decimal | str | int = ZERO,
    release_remaining: bool = False,
    reflection_state: str | None = None,
    reconciliation_status: str | None = None,
    reconciliation_reason: str | None = None,
    release_reason: str | None = None,
    order_identity: str | None = None,
    position_id: int | None = None,
) -> tuple[str, CapitalReservationEvent]:
    cur.execute(
        "SELECT event_fingerprint FROM capital_reservation_event_v1 "
        "WHERE reservation_id=%s AND source_event_identity=%s",
        (str(reservation_id), str(source_event_identity)),
    )
    existing = cur.fetchone()
    if existing is not None:
        cur.execute(
            f"SELECT {_EVENT_COLUMNS} FROM capital_reservation_event_v1 "
            "WHERE reservation_id=%s AND source_event_identity=%s",
            (str(reservation_id), str(source_event_identity)),
        )
        return "IDEMPOTENT", _event_from_row(cur.fetchone())

    row = _current_event_cursor(cur, reservation_id)
    if row is None:
        raise ValueError("CAPITAL_RESERVATION_NOT_FOUND")
    (
        current_id, sequence, key, environment, deployment, account_identity,
        symbol, strategy, interval, decision_identity, intent_identity,
        current_order, current_position, requested, remaining, deployed,
        released, current_state, current_reflection, current_reconciliation,
        current_reconciliation_reason, policy_fingerprint,
    ) = row
    if str(current_state) in TERMINAL_STATES:
        raise ValueError("CAPITAL_RESERVATION_TERMINAL_REACTIVATION_FORBIDDEN")
    delta = _decimal(deployed_notional_delta, field="deployed_notional_delta")
    if delta < ZERO or delta > Decimal(str(remaining)):
        raise ValueError("CAPITAL_RESERVATION_DEPLOYMENT_DELTA_INVALID")
    next_remaining = Decimal(str(remaining)) - delta
    next_deployed = Decimal(str(deployed)) + delta
    next_released = Decimal(str(released))
    next_state = str(state).upper()
    if release_remaining:
        next_released += next_remaining
        next_remaining = ZERO
    event = _make_event(
        reservation_id=current_id, event_sequence=int(sequence) + 1,
        logical_key=str(key), source_event_identity=str(source_event_identity),
        environment=str(environment), deployment_id=str(deployment),
        account_identity_fingerprint=str(account_identity), symbol=str(symbol),
        strategy=str(strategy), interval=str(interval),
        decision_identity=decision_identity, intent_identity=intent_identity,
        order_identity=order_identity or current_order,
        position_id=position_id if position_id is not None else current_position,
        requested_notional=Decimal(str(requested)),
        remaining_reserved_notional=next_remaining,
        deployed_notional=next_deployed, released_notional=next_released,
        state=next_state,
        reflection_state=reflection_state or str(current_reflection),
        reconciliation_status=(
            reconciliation_status or str(current_reconciliation)
        ),
        reconciliation_reason=(
            reconciliation_reason
            if reconciliation_reason is not None
            else current_reconciliation_reason
        ),
        release_reason=release_reason, effective_at=effective_at,
        source_authority=source_authority, provenance=provenance,
        policy_fingerprint=policy_fingerprint,
    )
    return append_event_cursor(cur, event), event


def _event_from_row(row: tuple[Any, ...]) -> CapitalReservationEvent:
    values = list(row)
    # The SELECT includes purpose, which is a table constraint rather than a
    # dataclass field.
    purpose_index = 8
    if str(values[purpose_index]) != PURPOSE:
        raise ValueError("CAPITAL_RESERVATION_PURPOSE_INVALID")
    del values[purpose_index]
    return CapitalReservationEvent(*values)


def load_capital_reservation_evidence(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str | None,
    as_of: datetime | None = None,
) -> CapitalReservationEvidence:
    if not account_identity_fingerprint:
        return CapitalReservationEvidence(
            None, None, None, 0, "INCOMPLETE", "INCOMPLETE", None, None,
            ("CAPITAL_RESERVATION_ACCOUNT_IDENTITY_UNAVAILABLE",),
        )
    if not reservation_schema_available_cursor(cur):
        return CapitalReservationEvidence(
            None, None, None, 0, "INCOMPLETE", "INCOMPLETE", None,
            str(account_identity_fingerprint),
            ("CAPITAL_RESERVATION_SCHEMA_UNAVAILABLE",),
        )
    source = "v_capital_reservation_current_v1"
    params: tuple[Any, ...] = (
        str(environment).upper(), str(deployment_id).lower(),
        str(account_identity_fingerprint),
    )
    if as_of is not None:
        if as_of.tzinfo is None:
            raise ValueError("CAPITAL_RESERVATION_AS_OF_MUST_BE_TIMEZONE_AWARE")
        source = """(
            SELECT DISTINCT ON (reservation_id) *
            FROM capital_reservation_event_v1
            WHERE effective_at<=%s
            ORDER BY reservation_id,event_sequence DESC
        ) reservation_as_of"""
        params = (as_of, *params)
    cur.execute(
        f"""
        SELECT state,reflection_state,reconciliation_status,
               remaining_reserved_notional,effective_at
        FROM {source}
        WHERE environment=%s AND deployment_id=%s
          AND account_identity_fingerprint=%s AND purpose='ENTRY'
        """,
        params,
    )
    rows = list(cur.fetchall())
    active = [row for row in rows if str(row[0]) in ACTIVE_STATES]
    latest = max((row[4] for row in rows), default=None)
    if any(str(row[2]) == "RECONCILIATION_FAILED" for row in active):
        return CapitalReservationEvidence(
            None, None, None, len(active), "RECONCILIATION_FAILED",
            "RECONCILIATION_FAILED", latest,
            str(account_identity_fingerprint),
            ("CAPITAL_RESERVATION_RECONCILIATION_FAILED",),
        )
    reserved = sum((Decimal(str(row[3])) for row in active), ZERO)
    internal = sum((
        Decimal(str(row[3])) for row in active
        if str(row[1]) == "INTERNAL_UNREFLECTED"
    ), ZERO)
    reflected = sum((
        Decimal(str(row[3])) for row in active
        if str(row[1]) == "EXCHANGE_REFLECTED"
    ), ZERO)
    return CapitalReservationEvidence(
        reserved, internal, reflected, len(active), "CANONICAL",
        "CANONICAL", latest, str(account_identity_fingerprint), (),
    )


def accept_live_entry_intent_cursor(
    cur: Any, *, intent: Any, account_identity_fingerprint: str,
    requested_notional: Decimal | str | int, effective_at: datetime,
) -> tuple[str, uuid.UUID | None]:
    """Persist the accepted LIVE commitment before any exchange network call."""
    if not reservation_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE", None
    intent_id = str(intent.intent_id)
    event = accepted_commitment_event(
        environment="LIVE", deployment_id=str(intent.deployment_id.value),
        account_identity_fingerprint=account_identity_fingerprint,
        source_identity=f"LIVE_ENTRY_INTENT:{intent_id}",
        symbol=str(intent.symbol), strategy=str(intent.strategy),
        interval=str(intent.interval), requested_notional=requested_notional,
        effective_at=effective_at, source_authority="LIVE_ENTRY_INTENT_V1",
        provenance={
            "intent_id": intent_id,
            "client_order_id": str(intent.client_order_id),
            "intent_content_fingerprint": str(intent.content_fingerprint),
        },
        decision_identity=str(intent.decision_id), intent_identity=intent_id,
        order_identity=str(intent.client_order_id),
        policy_fingerprint=str(intent.content_fingerprint),
    )
    return append_event_cursor(cur, event), event.reservation_id


def prepare_live_submission_cursor(
    cur: Any, *, reservation_id: uuid.UUID, intent_identity: str,
    effective_at: datetime,
) -> None:
    for state, status in (
        ("INTERNAL_RESERVED", "CANONICAL"),
        ("SUBMITTED", "PENDING_EXCHANGE_REFLECTION"),
    ):
        transition_reservation_cursor(
            cur, reservation_id=reservation_id,
            source_event_identity=f"{state}:{intent_identity}", state=state,
            effective_at=effective_at,
            source_authority="LIVE_ENTRY_SUBMISSION_V1",
            provenance={"intent_id": str(intent_identity)},
            reflection_state="INTERNAL_UNREFLECTED",
            reconciliation_status=status,
        )


def reconcile_live_submission_cursor(
    cur: Any, *, reservation_id: uuid.UUID, source_event_identity: str,
    accepted: bool, effective_at: datetime, order_identity: str | None,
    reason: str | None = None,
) -> str:
    if accepted:
        status, _ = transition_reservation_cursor(
            cur, reservation_id=reservation_id,
            source_event_identity=source_event_identity,
            state="EXCHANGE_ACK", effective_at=effective_at,
            source_authority="LIVE_ENTRY_SUBMISSION_ACK_V1",
            provenance={"exchange_order_identity": order_identity},
            reflection_state="INTERNAL_UNREFLECTED",
            reconciliation_status="PENDING_EXCHANGE_REFLECTION",
            order_identity=order_identity,
        )
    else:
        rejection_reason = str(reason or "ENTRY_SUBMISSION_REJECTED")
        status, _ = transition_reservation_cursor(
            cur, reservation_id=reservation_id,
            source_event_identity=source_event_identity,
            state="REJECTED", effective_at=effective_at,
            source_authority="LIVE_ENTRY_SUBMISSION_REJECTION_V1",
            provenance={"reason": rejection_reason}, release_remaining=True,
            release_reason=rejection_reason,
            reflection_state="INTERNAL_UNREFLECTED",
            reconciliation_status="CANONICAL",
        )
    return status


def deploy_live_entry_fill_cursor(
    cur: Any, *, intent_id: uuid.UUID | str,
    fill_evidence_id: uuid.UUID | str, position_id: int,
    filled_quantity: Decimal | str | int,
    cumulative_filled_quantity: Decimal | str | int,
    requested_quantity: Decimal | str | int, effective_at: datetime,
) -> str:
    """Move the exact immutable LIVE fill slice out of reserved capital.

    Capital Reservation stores the notional accepted before submission.  A
    fill therefore deploys the same proportion of that immutable commitment
    as the fill quantity represents of the immutable requested quantity.  The
    final slice consumes the exact remaining notional, avoiding price-slippage
    guesses and Decimal rounding residue.
    """
    if not reservation_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    canonical_intent_id = str(uuid.UUID(str(intent_id)))
    canonical_fill_id = str(uuid.UUID(str(fill_evidence_id)))
    quantity = _decimal(filled_quantity, field="filled_quantity")
    cumulative = _decimal(
        cumulative_filled_quantity, field="cumulative_filled_quantity"
    )
    requested_quantity_value = _decimal(
        requested_quantity, field="requested_quantity"
    )
    if quantity == ZERO:
        return "ZERO_FILL_NOOP"
    if (
        quantity < ZERO
        or requested_quantity_value <= ZERO
        or cumulative < quantity
        or cumulative > requested_quantity_value
    ):
        raise ValueError("CAPITAL_RESERVATION_LIVE_FILL_QUANTITY_INVALID")
    cur.execute(
        "SELECT reservation_id FROM v_capital_reservation_current_v1 "
        "WHERE environment='LIVE' AND intent_identity=%s",
        (canonical_intent_id,),
    )
    reservations = list(cur.fetchall())
    if len(reservations) != 1:
        return "RESERVATION_NOT_FOUND" if not reservations else "RESERVATION_CONFLICT"
    reservation_id = uuid.UUID(str(reservations[0][0]))
    source_event_identity = f"LIVE_FILL_DEPLOYMENT:{canonical_fill_id}"
    cur.execute(
        "SELECT 1 FROM capital_reservation_event_v1 "
        "WHERE reservation_id=%s AND source_event_identity=%s",
        (str(reservation_id), source_event_identity),
    )
    if cur.fetchone() is not None:
        return "IDEMPOTENT"
    current = _current_event_cursor(cur, reservation_id)
    if current is None:
        return "RESERVATION_NOT_FOUND"
    requested_notional = Decimal(str(current[13]))
    remaining_notional = Decimal(str(current[14]))
    deployed_delta = (
        remaining_notional
        if cumulative == requested_quantity_value
        else requested_notional * quantity / requested_quantity_value
    )
    if deployed_delta > remaining_notional:
        raise ValueError("CAPITAL_RESERVATION_DEPLOYMENT_DELTA_INVALID")
    next_state = (
        "DEPLOYED" if deployed_delta == remaining_notional
        else "PARTIALLY_DEPLOYED"
    )
    status, _ = transition_reservation_cursor(
        cur, reservation_id=reservation_id,
        source_event_identity=source_event_identity,
        state=next_state, effective_at=effective_at,
        source_authority="LIVE_ENTRY_FILL_ATTRIBUTION_V1",
        provenance={
            "intent_id": canonical_intent_id,
            "fill_evidence_id": canonical_fill_id,
            "position_id": int(position_id),
            "filled_quantity": str(quantity),
            "cumulative_filled_quantity": str(cumulative),
            "requested_quantity": str(requested_quantity_value),
            "deployment_basis": "IMMUTABLE_COMMITMENT_QUANTITY_FRACTION",
        },
        deployed_notional_delta=deployed_delta,
        reflection_state="EXCHANGE_REFLECTED",
        reconciliation_status="CANONICAL",
        position_id=int(position_id),
    )
    return status


def accept_paper_simulated_order_cursor(
    cur: Any, *, simulated_order_id: int, deployment_id: str,
    symbol: str, strategy: str, interval: str,
    requested_notional: Decimal | str | int, effective_at: datetime,
    decision_identity: str | None,
) -> tuple[str, uuid.UUID | None]:
    if not reservation_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE", None
    source_identity = f"SIMULATED_ORDER:{int(simulated_order_id)}"
    event = accepted_commitment_event(
        environment="PAPER", deployment_id=deployment_id,
        account_identity_fingerprint=paper_account_identity_fingerprint(
            deployment_id
        ),
        source_identity=source_identity, symbol=symbol, strategy=strategy,
        interval=interval, requested_notional=requested_notional,
        effective_at=effective_at, source_authority="PAPER_SIMULATED_ORDER",
        provenance={"simulated_order_id": int(simulated_order_id)},
        decision_identity=decision_identity,
        order_identity=str(simulated_order_id),
    )
    return append_event_cursor(cur, event), event.reservation_id


def deploy_paper_simulated_fill_cursor(
    cur: Any, *, simulated_order_id: int, fill_id: int,
    position_id: int, deployed_notional: Decimal | str | int,
    effective_at: datetime,
) -> str:
    if not reservation_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    key = logical_commitment_key(
        environment="PAPER",
        deployment_id=_paper_deployment_for_order(cur, simulated_order_id),
        source_identity=f"SIMULATED_ORDER:{int(simulated_order_id)}",
    )
    reservation_id = deterministic_reservation_id(key)
    current = _current_event_cursor(cur, reservation_id)
    if current is None:
        return "RESERVATION_NOT_FOUND"
    remaining = Decimal(str(current[14]))
    delta = _decimal(deployed_notional, field="deployed_notional")
    next_state = "DEPLOYED" if delta == remaining else "PARTIALLY_DEPLOYED"
    status, _event = transition_reservation_cursor(
        cur, reservation_id=reservation_id,
        source_event_identity=f"SIMULATED_FILL:{int(fill_id)}",
        state=next_state, effective_at=effective_at,
        source_authority="PAPER_SIMULATED_FILL",
        provenance={
            "simulated_order_id": int(simulated_order_id),
            "fill_id": int(fill_id), "position_id": int(position_id),
        },
        deployed_notional_delta=delta,
        reflection_state="PAPER_SIMULATED",
        position_id=int(position_id),
    )
    return status


def _paper_deployment_for_order(cur: Any, simulated_order_id: int) -> str:
    cur.execute(
        "SELECT deployment_id FROM simulated_orders WHERE id=%s",
        (int(simulated_order_id),),
    )
    row = cur.fetchone()
    deployment = str(row[0] or "").strip().lower() if row else ""
    # Forward simulated orders predate explicit scope columns. Runtime identity
    # is therefore used only by the writer hook that created the reservation.
    if not deployment:
        cur.execute(
            "SELECT deployment_id FROM capital_reservation_event_v1 "
            "WHERE order_identity=%s ORDER BY event_sequence LIMIT 1",
            (str(simulated_order_id),),
        )
        identity_row = cur.fetchone()
        deployment = str(identity_row[0]) if identity_row else ""
    if deployment not in {"local-paper", "vps-paper"}:
        raise ValueError("CAPITAL_RESERVATION_PAPER_DEPLOYMENT_UNAVAILABLE")
    return deployment
