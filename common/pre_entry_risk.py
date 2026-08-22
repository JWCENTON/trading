"""Immutable Pre-Entry Risk Authority V1.

This authority owns only accepted risk which has not yet been transferred to
canonical Position Open Risk.  Capital reservations, boundary policy, market
prices and fee evidence remain owned by their existing authorities.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
import hashlib
import json
import os
from typing import Any, Mapping
import uuid

from psycopg2.extras import Json


CONTRACT_VERSION = "PRE_ENTRY_RISK_AUTHORITY_V1"
REFERENCE_PRICE_SOURCE = "candles.close/FRESH_20_MINUTES"
REFERENCE_PRICE_FRESHNESS = timedelta(minutes=20)
PRE_ENTRY_RISK_NAMESPACE = uuid.UUID("8d620c03-b667-4d08-a004-23a58c4d04cc")
EVENT_NAMESPACE = uuid.UUID("436266c6-e64c-4388-bf76-4cf9fed084e6")
ZERO = Decimal("0")
HUNDRED = Decimal("100")
NUMERIC_18 = Decimal("0.000000000000000001")
NUMERIC_12 = Decimal("0.000000000001")

EVIDENCE_STATUSES = frozenset({
    "CANONICAL",
    "MISSING_BOUNDARY_POLICY",
    "MISSING_REFERENCE_PRICE",
    "STALE_REFERENCE_PRICE",
    "MISSING_PROPOSED_QUANTITY",
    "MISSING_EXIT_COST_AUTHORITY",
    "STALE_EXIT_COST_AUTHORITY",
    "ACCOUNT_IDENTITY_MISMATCH",
    "DEPLOYMENT_MISMATCH",
    "INSTRUMENT_MISMATCH",
    "INVALID_QUANTITY",
    "INVALID_BOUNDARY",
    "EVIDENCE_INCOMPLETE",
})
LIFECYCLE_STATES = frozenset({
    "ACTIVE_COMMITTED",
    "PARTIALLY_TRANSFERRED",
    "REPLACED_BY_OPEN_RISK",
    "RELEASED",
})


class PreEntryRiskIncomplete(RuntimeError):
    """A required upstream authority is not canonical."""

    def __init__(self, status: str):
        self.status = str(status)
        super().__init__(f"PRE_ENTRY_RISK_INCOMPLETE:{self.status}")


def _decimal(value: object, field: str) -> Decimal:
    if value is None or isinstance(value, float):
        raise ValueError(f"PRE_ENTRY_RISK_INVALID_DECIMAL:{field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"PRE_ENTRY_RISK_INVALID_DECIMAL:{field}") from exc
    if not result.is_finite():
        raise ValueError(f"PRE_ENTRY_RISK_INVALID_DECIMAL:{field}")
    return result


def _quantize(value: Decimal, quantum: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 100
        return value.quantize(quantum, rounding=ROUND_HALF_EVEN)


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("PRE_ENTRY_RISK_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    if isinstance(value, float):
        raise ValueError("PRE_ENTRY_RISK_FLOAT_FORBIDDEN")
    return value


def fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def deterministic_pre_entry_risk_id(reservation_id: uuid.UUID | str) -> uuid.UUID:
    return uuid.uuid5(PRE_ENTRY_RISK_NAMESPACE, str(reservation_id))


def reference_price_row_identity(
    *, symbol: str, interval: str, candle_open_time: datetime,
) -> str:
    if candle_open_time.tzinfo is None:
        raise ValueError("PRE_ENTRY_RISK_TIMESTAMP_REQUIRED")
    return (
        f"candles:{str(symbol).upper()}:{str(interval).lower()}:"
        f"{candle_open_time.astimezone(timezone.utc).isoformat()}"
    )


def reference_price_fingerprint(
    *, symbol: str, interval: str, candle_open_time: datetime,
    reference_price: Decimal | str | int,
) -> str:
    return fingerprint({
        "source": REFERENCE_PRICE_SOURCE,
        "row_identity": reference_price_row_identity(
            symbol=symbol, interval=interval,
            candle_open_time=candle_open_time,
        ),
        "price": _decimal(reference_price, "reference_price"),
    })


def quantity_fingerprint(*, source: str, quantity: Decimal | str | int) -> str:
    return fingerprint({
        "quantity_source": str(source),
        "proposed_quantity": _decimal(quantity, "proposed_quantity"),
    })


@dataclass(frozen=True)
class PreEntryRiskCalculation:
    proposed_boundary_price: Decimal | None
    pre_entry_core_price_risk: Decimal | None
    pre_entry_exit_fee_estimate: Decimal | None
    total_pre_entry_risk: Decimal | None
    evidence_status: str


def calculate_pre_entry_risk(
    *, reference_entry_price: Decimal | str | int | None,
    boundary_distance_pct: Decimal | str | int | None,
    proposed_inventory_qty: Decimal | str | int | None,
    canonical_exit_fee_rate: Decimal | str | int | None,
    reference_price_timestamp: datetime | None,
    effective_at: datetime,
    boundary_policy_status: str = "CANONICAL",
    exit_cost_status: str = "CANONICAL",
    identity_status: str = "CANONICAL",
) -> PreEntryRiskCalculation:
    """Evaluate exact SPOT LONG risk and return NULL on incomplete evidence."""
    if effective_at.tzinfo is None:
        raise ValueError("PRE_ENTRY_RISK_TIMESTAMP_REQUIRED")
    if identity_status != "CANONICAL":
        status = (
            identity_status if identity_status in EVIDENCE_STATUSES
            else "EVIDENCE_INCOMPLETE"
        )
        return PreEntryRiskCalculation(None, None, None, None, status)
    if boundary_policy_status != "CANONICAL" or boundary_distance_pct is None:
        return PreEntryRiskCalculation(
            None, None, None, None, "MISSING_BOUNDARY_POLICY"
        )
    if reference_entry_price is None or reference_price_timestamp is None:
        return PreEntryRiskCalculation(
            None, None, None, None, "MISSING_REFERENCE_PRICE"
        )
    if reference_price_timestamp.tzinfo is None:
        return PreEntryRiskCalculation(
            None, None, None, None, "EVIDENCE_INCOMPLETE"
        )
    if reference_price_timestamp > effective_at:
        return PreEntryRiskCalculation(
            None, None, None, None, "EVIDENCE_INCOMPLETE"
        )
    if reference_price_timestamp < effective_at - REFERENCE_PRICE_FRESHNESS:
        return PreEntryRiskCalculation(
            None, None, None, None, "STALE_REFERENCE_PRICE"
        )
    if proposed_inventory_qty is None:
        return PreEntryRiskCalculation(
            None, None, None, None, "MISSING_PROPOSED_QUANTITY"
        )
    try:
        quantity = _decimal(proposed_inventory_qty, "proposed_quantity")
    except ValueError:
        return PreEntryRiskCalculation(None, None, None, None, "INVALID_QUANTITY")
    if quantity <= ZERO:
        return PreEntryRiskCalculation(None, None, None, None, "INVALID_QUANTITY")
    if exit_cost_status == "STALE_EXIT_COST_AUTHORITY":
        return PreEntryRiskCalculation(
            None, None, None, None, "STALE_EXIT_COST_AUTHORITY"
        )
    if exit_cost_status != "CANONICAL" or canonical_exit_fee_rate is None:
        return PreEntryRiskCalculation(
            None, None, None, None, "MISSING_EXIT_COST_AUTHORITY"
        )
    try:
        reference = _decimal(reference_entry_price, "reference_price")
        distance = _decimal(boundary_distance_pct, "boundary_distance_pct")
        fee_rate = _decimal(canonical_exit_fee_rate, "canonical_exit_fee_rate")
    except ValueError:
        return PreEntryRiskCalculation(None, None, None, None, "EVIDENCE_INCOMPLETE")
    if reference <= ZERO or distance <= ZERO or distance >= HUNDRED:
        return PreEntryRiskCalculation(None, None, None, None, "INVALID_BOUNDARY")
    if fee_rate < ZERO or fee_rate > Decimal("0.10"):
        return PreEntryRiskCalculation(
            None, None, None, None, "MISSING_EXIT_COST_AUTHORITY"
        )
    boundary = reference * (Decimal("1") - distance / HUNDRED)
    if boundary <= ZERO or boundary >= reference:
        return PreEntryRiskCalculation(None, None, None, None, "INVALID_BOUNDARY")
    core = abs(reference - boundary) * quantity
    exit_fee = boundary * quantity * fee_rate
    return PreEntryRiskCalculation(
        boundary, core, exit_fee, core + exit_fee, "CANONICAL"
    )


@dataclass(frozen=True)
class PreEntryRiskEvent:
    event_id: uuid.UUID
    pre_entry_risk_id: uuid.UUID
    event_sequence: int
    source_event_identity: str
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    decision_id: str
    commitment_id: str
    reservation_id: uuid.UUID
    intent_id: str | None
    order_identity: str | None
    symbol: str
    strategy: str
    interval: str
    side: str
    boundary_id: uuid.UUID
    boundary_policy_id: str
    boundary_policy_version: str
    boundary_policy_fingerprint: str
    boundary_distance_pct: Decimal
    proposed_boundary_price: Decimal
    reference_price: Decimal
    reference_price_timestamp: datetime
    reference_price_source: str
    reference_price_row_identity: str
    reference_price_fingerprint: str
    proposed_quantity: Decimal
    quantity_source: str
    quantity_evidence_fingerprint: str
    exit_cost_snapshot_or_model_id: str
    exit_cost_evidence_fingerprint: str
    canonical_exit_fee_rate: Decimal
    pre_entry_core_price_risk: Decimal
    pre_entry_exit_fee_estimate: Decimal
    total_pre_entry_risk: Decimal
    original_quantity: Decimal
    transferred_quantity: Decimal
    remaining_committed_quantity: Decimal
    released_quantity: Decimal
    evidence_status: str
    lifecycle_state: str
    open_risk_position_id: int | None
    open_risk_boundary_id: uuid.UUID | None
    open_risk_evidence_fingerprint: str | None
    runtime_revision: str
    effective_at: datetime
    source_authority: str
    provenance: Mapping[str, Any]
    evidence_fingerprint: str
    contract_version: str = CONTRACT_VERSION


def _semantic_payload(event: PreEntryRiskEvent) -> dict[str, Any]:
    return {
        key: value for key, value in event.__dict__.items()
        if key not in {"event_id", "evidence_fingerprint"}
    }


def _make_event(**values: Any) -> PreEntryRiskEvent:
    provisional = PreEntryRiskEvent(
        event_id=uuid.UUID(int=0), evidence_fingerprint="", **values
    )
    evidence_fp = fingerprint(_semantic_payload(provisional))
    event_id = uuid.uuid5(
        EVENT_NAMESPACE,
        f"{provisional.pre_entry_risk_id}:{provisional.source_event_identity}:"
        f"{evidence_fp}",
    )
    return replace(
        provisional, event_id=event_id, evidence_fingerprint=evidence_fp
    )


_EVENT_COLUMNS = """
event_id,pre_entry_risk_id,event_sequence,source_event_identity,environment,
deployment_id,account_identity_fingerprint,decision_id,commitment_id,
reservation_id,intent_id,order_identity,symbol,strategy,interval,side,
boundary_id,boundary_policy_id,boundary_policy_version,
boundary_policy_fingerprint,boundary_distance_pct,proposed_boundary_price,
reference_price,reference_price_timestamp,reference_price_source,
reference_price_row_identity,reference_price_fingerprint,proposed_quantity,
quantity_source,quantity_evidence_fingerprint,exit_cost_snapshot_or_model_id,
exit_cost_evidence_fingerprint,canonical_exit_fee_rate,
pre_entry_core_price_risk,pre_entry_exit_fee_estimate,total_pre_entry_risk,
original_quantity,transferred_quantity,remaining_committed_quantity,
released_quantity,evidence_status,lifecycle_state,open_risk_position_id,
open_risk_boundary_id,open_risk_evidence_fingerprint,runtime_revision,
effective_at,source_authority,provenance,evidence_fingerprint,contract_version
"""


def pre_entry_risk_schema_available_cursor(cur: Any) -> bool:
    cur.execute(
        "SELECT to_regclass('public.pre_entry_risk_event_v1'),"
        "to_regclass('public.v_pre_entry_risk_current_v1')"
    )
    row = cur.fetchone()
    return bool(row and row[0] is not None and row[1] is not None)


def _event_values(event: PreEntryRiskEvent) -> tuple[Any, ...]:
    return (
        str(event.event_id), str(event.pre_entry_risk_id), event.event_sequence,
        event.source_event_identity, event.environment, event.deployment_id,
        event.account_identity_fingerprint, event.decision_id,
        event.commitment_id, str(event.reservation_id), event.intent_id,
        event.order_identity, event.symbol, event.strategy, event.interval,
        event.side, str(event.boundary_id), event.boundary_policy_id,
        event.boundary_policy_version, event.boundary_policy_fingerprint,
        event.boundary_distance_pct, event.proposed_boundary_price,
        event.reference_price, event.reference_price_timestamp,
        event.reference_price_source, event.reference_price_row_identity,
        event.reference_price_fingerprint, event.proposed_quantity,
        event.quantity_source, event.quantity_evidence_fingerprint,
        event.exit_cost_snapshot_or_model_id,
        event.exit_cost_evidence_fingerprint, event.canonical_exit_fee_rate,
        event.pre_entry_core_price_risk,
        event.pre_entry_exit_fee_estimate, event.total_pre_entry_risk,
        event.original_quantity, event.transferred_quantity,
        event.remaining_committed_quantity, event.released_quantity,
        event.evidence_status, event.lifecycle_state,
        event.open_risk_position_id,
        None if event.open_risk_boundary_id is None
        else str(event.open_risk_boundary_id),
        event.open_risk_evidence_fingerprint, event.runtime_revision,
        event.effective_at, event.source_authority,
        Json(dict(event.provenance)), event.evidence_fingerprint,
        event.contract_version,
    )


def append_pre_entry_risk_event_cursor(cur: Any, event: PreEntryRiskEvent) -> str:
    cur.execute(
        "SELECT evidence_fingerprint FROM pre_entry_risk_event_v1 "
        "WHERE event_id=%s", (str(event.event_id),),
    )
    existing = cur.fetchone()
    if existing is not None:
        if str(existing[0]) == event.evidence_fingerprint:
            return "IDEMPOTENT"
        raise ValueError("PRE_ENTRY_RISK_EVENT_ID_CONFLICT")
    values = _event_values(event)
    cur.execute(
        f"INSERT INTO pre_entry_risk_event_v1 ({_EVENT_COLUMNS}) VALUES ("
        + ",".join(["%s"] * len(values))
        + ") ON CONFLICT(event_id) DO NOTHING RETURNING event_id",
        values,
    )
    if cur.fetchone() is not None:
        return "INSERTED"
    cur.execute(
        "SELECT evidence_fingerprint FROM pre_entry_risk_event_v1 "
        "WHERE event_id=%s", (str(event.event_id),),
    )
    row = cur.fetchone()
    if row and str(row[0]) == event.evidence_fingerprint:
        return "IDEMPOTENT"
    raise ValueError("PRE_ENTRY_RISK_EVENT_ID_CONFLICT")


def _event_from_row(row: tuple[Any, ...]) -> PreEntryRiskEvent:
    values = list(row)
    for index in (0, 1, 9, 16):
        values[index] = uuid.UUID(str(values[index]))
    if values[43] is not None:
        values[43] = uuid.UUID(str(values[43]))
    return PreEntryRiskEvent(*values)


def load_current_pre_entry_risk_cursor(
    cur: Any, *, reservation_id: uuid.UUID | str,
) -> PreEntryRiskEvent | None:
    if not pre_entry_risk_schema_available_cursor(cur):
        return None
    cur.execute(
        f"SELECT {_EVENT_COLUMNS} FROM v_pre_entry_risk_current_v1 "
        "WHERE reservation_id=%s", (str(reservation_id),),
    )
    row = cur.fetchone()
    return None if row is None else _event_from_row(row)


def freeze_pre_entry_risk_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str, decision_id: str,
    commitment_id: str, reservation_id: uuid.UUID,
    intent_id: str | None, order_identity: str | None,
    symbol: str, strategy: str, interval: str, boundary_id: uuid.UUID,
    boundary_policy_id: str, boundary_policy_version: str,
    boundary_policy_fingerprint: str,
    boundary_distance_pct: Decimal | str | int,
    reference_price: Decimal | str | int,
    reference_price_timestamp: datetime, reference_price_source: str,
    reference_price_row_identity_value: str,
    reference_price_fingerprint_value: str,
    proposed_quantity: Decimal | str | int, quantity_source: str,
    quantity_evidence_fingerprint_value: str,
    exit_cost_snapshot_or_model_id: str,
    exit_cost_evidence_fingerprint: str,
    canonical_exit_fee_rate: Decimal | str | int,
    runtime_revision: str, effective_at: datetime,
    source_authority: str, provenance: Mapping[str, Any],
) -> tuple[str, PreEntryRiskEvent]:
    if not pre_entry_risk_schema_available_cursor(cur):
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    normalized_reference = _quantize(
        _decimal(reference_price, "reference_price"), NUMERIC_18
    )
    normalized_distance = _quantize(
        _decimal(boundary_distance_pct, "boundary_distance_pct"), NUMERIC_12
    )
    normalized_quantity = _quantize(
        _decimal(proposed_quantity, "proposed_quantity"), NUMERIC_18
    )
    normalized_fee_rate = _quantize(
        _decimal(canonical_exit_fee_rate, "canonical_exit_fee_rate"), NUMERIC_12
    )
    calculation = calculate_pre_entry_risk(
        reference_entry_price=normalized_reference,
        boundary_distance_pct=normalized_distance,
        proposed_inventory_qty=normalized_quantity,
        canonical_exit_fee_rate=normalized_fee_rate,
        reference_price_timestamp=reference_price_timestamp,
        effective_at=effective_at,
    )
    if calculation.evidence_status != "CANONICAL":
        raise PreEntryRiskIncomplete(calculation.evidence_status)
    risk_id = deterministic_pre_entry_risk_id(reservation_id)
    existing = load_current_pre_entry_risk_cursor(
        cur, reservation_id=reservation_id,
    )
    boundary_price = _quantize(
        calculation.proposed_boundary_price, NUMERIC_18
    )
    core_risk = _quantize(
        abs(normalized_reference - boundary_price) * normalized_quantity,
        NUMERIC_18,
    )
    exit_fee_risk = _quantize(
        boundary_price * normalized_quantity * normalized_fee_rate,
        NUMERIC_18,
    )
    quantity = normalized_quantity
    event = _make_event(
        pre_entry_risk_id=risk_id, event_sequence=1,
        source_event_identity=f"FROZEN:{reservation_id}",
        environment=str(environment).upper(),
        deployment_id=str(deployment_id).lower(),
        account_identity_fingerprint=str(account_identity_fingerprint),
        decision_id=str(decision_id), commitment_id=str(commitment_id),
        reservation_id=reservation_id,
        intent_id=(None if intent_id is None else str(intent_id)),
        order_identity=(None if order_identity is None else str(order_identity)),
        symbol=str(symbol).upper(), strategy=str(strategy).upper(),
        interval=str(interval).lower(), side="LONG", boundary_id=boundary_id,
        boundary_policy_id=str(boundary_policy_id),
        boundary_policy_version=str(boundary_policy_version),
        boundary_policy_fingerprint=str(boundary_policy_fingerprint),
        boundary_distance_pct=normalized_distance,
        proposed_boundary_price=boundary_price,
        reference_price=normalized_reference,
        reference_price_timestamp=reference_price_timestamp,
        reference_price_source=str(reference_price_source),
        reference_price_row_identity=str(reference_price_row_identity_value),
        reference_price_fingerprint=str(reference_price_fingerprint_value),
        proposed_quantity=quantity, quantity_source=str(quantity_source),
        quantity_evidence_fingerprint=str(quantity_evidence_fingerprint_value),
        exit_cost_snapshot_or_model_id=str(exit_cost_snapshot_or_model_id),
        exit_cost_evidence_fingerprint=str(exit_cost_evidence_fingerprint),
        canonical_exit_fee_rate=normalized_fee_rate,
        pre_entry_core_price_risk=core_risk,
        pre_entry_exit_fee_estimate=exit_fee_risk,
        total_pre_entry_risk=core_risk + exit_fee_risk,
        original_quantity=quantity, transferred_quantity=ZERO,
        remaining_committed_quantity=quantity, released_quantity=ZERO,
        evidence_status="CANONICAL", lifecycle_state="ACTIVE_COMMITTED",
        open_risk_position_id=None, open_risk_boundary_id=None,
        open_risk_evidence_fingerprint=None,
        runtime_revision=str(runtime_revision), effective_at=effective_at,
        source_authority=str(source_authority), provenance=dict(provenance),
    )
    if existing is not None:
        if existing.evidence_fingerprint == event.evidence_fingerprint:
            return "IDEMPOTENT", existing
        raise ValueError("PRE_ENTRY_RISK_FROZEN_EVIDENCE_CONFLICT")
    return append_pre_entry_risk_event_cursor(cur, event), event


def _load_reservation_and_boundary_cursor(
    cur: Any, *, reservation_id: uuid.UUID,
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    cur.execute(
        "SELECT logical_commitment_key,environment,deployment_id,"
        "account_identity_fingerprint,decision_identity,intent_identity,"
        "order_identity,symbol,strategy,interval,state "
        "FROM v_capital_reservation_current_v1 WHERE reservation_id=%s",
        (str(reservation_id),),
    )
    reservation = cur.fetchone()
    if reservation is None:
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    cur.execute(
        "SELECT boundary_id,decision_id,intent_id,order_identity,symbol,strategy,"
        "interval,side,policy_id,policy_version,policy_fingerprint,"
        "boundary_distance_pct,state,account_identity_fingerprint,deployment_id "
        "FROM v_position_risk_boundary_current_v1 WHERE reservation_id=%s",
        (str(reservation_id),),
    )
    boundary = cur.fetchone()
    if boundary is None or str(boundary[12]) != "BOUNDARY_POLICY_ACCEPTED":
        raise PreEntryRiskIncomplete("MISSING_BOUNDARY_POLICY")
    if str(reservation[10]) != "ACCEPTED_COMMITMENT":
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    if str(reservation[2]) != str(boundary[14]):
        raise PreEntryRiskIncomplete("DEPLOYMENT_MISMATCH")
    if str(reservation[3]) != str(boundary[13]):
        raise PreEntryRiskIncomplete("ACCOUNT_IDENTITY_MISMATCH")
    if (
        str(reservation[7]).upper() != str(boundary[4]).upper()
        or str(reservation[8]).upper() != str(boundary[5]).upper()
        or str(reservation[9]).lower() != str(boundary[6]).lower()
        or str(boundary[7]) != "LONG"
    ):
        raise PreEntryRiskIncomplete("INSTRUMENT_MISMATCH")
    if (
        str(reservation[4]) != str(boundary[1])
        or str(reservation[5]) != str(boundary[2])
        or str(reservation[6]) != str(boundary[3])
    ):
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    return reservation, boundary


def freeze_paper_pre_entry_risk_cursor(
    cur: Any, *, simulated_order_id: int, deployment_id: str,
    effective_at: datetime, runtime_revision_value: str | None = None,
) -> tuple[str, uuid.UUID | None]:
    if not pre_entry_risk_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE", None
    accepted_runtime_revision = (
        runtime_revision() if runtime_revision_value is None
        else str(runtime_revision_value)
    )
    from common.capital_reservation import (
        deterministic_reservation_id, logical_commitment_key,
    )
    from common.paper_simulation_fee_config import (
        FEE_MODEL_V2, load_paper_simulation_fee_config,
    )

    key = logical_commitment_key(
        environment="PAPER", deployment_id=deployment_id,
        source_identity=f"SIMULATED_ORDER:{int(simulated_order_id)}",
    )
    reservation_id = deterministic_reservation_id(key)
    reservation, boundary = _load_reservation_and_boundary_cursor(
        cur, reservation_id=reservation_id,
    )
    cur.execute(
        "SELECT symbol,strategy,interval,price,quantity_btc,candle_open_time,"
        "decision_id FROM simulated_orders WHERE id=%s",
        (int(simulated_order_id),),
    )
    order = cur.fetchone()
    if order is None:
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    symbol, strategy, interval, order_price, quantity, candle_at, order_decision = order
    cur.execute(
        "SELECT close FROM candles WHERE symbol=%s AND interval=%s "
        "AND open_time=%s", (symbol, interval, candle_at),
    )
    candle = cur.fetchone()
    if candle is None or candle[0] is None:
        raise PreEntryRiskIncomplete("MISSING_REFERENCE_PRICE")
    reference = _decimal(candle[0], "reference_price")
    if order_price is None or _decimal(order_price, "order_price") != reference:
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    fee = load_paper_simulation_fee_config()
    if fee.model_version != FEE_MODEL_V2:
        raise PreEntryRiskIncomplete("MISSING_EXIT_COST_AUTHORITY")
    price_row_id = reference_price_row_identity(
        symbol=str(symbol), interval=str(interval), candle_open_time=candle_at,
    )
    price_fp = reference_price_fingerprint(
        symbol=str(symbol), interval=str(interval), candle_open_time=candle_at,
        reference_price=reference,
    )
    qty_fp = quantity_fingerprint(
        source="simulated_orders.quantity_btc", quantity=quantity,
    )
    fee_fp = fingerprint({
        "model_id": fee.model_version, "fee_rate": fee.rate,
        "config_source": fee.config_source,
    })
    status, event = freeze_pre_entry_risk_cursor(
        cur, environment="PAPER", deployment_id=deployment_id,
        account_identity_fingerprint=str(reservation[3]),
        decision_id=str(order_decision or reservation[4]),
        commitment_id=str(reservation[0]), reservation_id=reservation_id,
        intent_id=None, order_identity=str(simulated_order_id),
        symbol=str(symbol), strategy=str(strategy), interval=str(interval),
        boundary_id=uuid.UUID(str(boundary[0])),
        boundary_policy_id=str(boundary[8]),
        boundary_policy_version=str(boundary[9]),
        boundary_policy_fingerprint=str(boundary[10]),
        boundary_distance_pct=boundary[11], reference_price=reference,
        reference_price_timestamp=candle_at,
        reference_price_source=REFERENCE_PRICE_SOURCE,
        reference_price_row_identity_value=price_row_id,
        reference_price_fingerprint_value=price_fp,
        proposed_quantity=quantity,
        quantity_source="simulated_orders.quantity_btc",
        quantity_evidence_fingerprint_value=qty_fp,
        exit_cost_snapshot_or_model_id=fee.model_version,
        exit_cost_evidence_fingerprint=fee_fp,
        canonical_exit_fee_rate=fee.rate,
        runtime_revision=accepted_runtime_revision, effective_at=effective_at,
        source_authority="PAPER_ACCEPTED_ENTRY_COMMITMENT",
        provenance={"simulated_order_id": int(simulated_order_id)},
    )
    return status, event.pre_entry_risk_id


def freeze_live_pre_entry_risk_cursor(
    cur: Any, *, intent: Any, reservation_id: uuid.UUID,
    account_identity_fingerprint: str,
    reference_price_timestamp: datetime | None,
    effective_at: datetime,
) -> tuple[str, uuid.UUID | None]:
    if not pre_entry_risk_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE", None
    if reference_price_timestamp is None:
        raise PreEntryRiskIncomplete("MISSING_REFERENCE_PRICE")
    reservation, boundary = _load_reservation_and_boundary_cursor(
        cur, reservation_id=reservation_id,
    )
    if str(reservation[2]) != str(intent.deployment_id.value):
        raise PreEntryRiskIncomplete("DEPLOYMENT_MISMATCH")
    if str(reservation[3]) != str(account_identity_fingerprint):
        raise PreEntryRiskIncomplete("ACCOUNT_IDENTITY_MISMATCH")
    cur.execute(
        "SELECT close FROM candles WHERE symbol=%s AND interval=%s "
        "AND open_time=%s", (
            str(intent.symbol), str(intent.interval), reference_price_timestamp,
        ),
    )
    candle = cur.fetchone()
    if candle is None or candle[0] is None:
        raise PreEntryRiskIncomplete("MISSING_REFERENCE_PRICE")
    reference = _decimal(candle[0], "reference_price")
    cur.execute(
        "SELECT exit_cost_snapshot_id,canonical_fee_rate,"
        "snapshot_fingerprint,expires_at,account_identity_fingerprint,"
        "deployment_id,instrument_type,symbol "
        "FROM live_exit_cost_snapshot_v1 WHERE environment='LIVE' "
        "AND deployment_id=%s AND account_identity_fingerprint=%s "
        "AND instrument_type='SPOT' AND symbol=%s AND fee_role='TAKER' "
        "AND effective_at<=%s ORDER BY effective_at DESC,created_at DESC LIMIT 1",
        (
            str(intent.deployment_id.value), str(account_identity_fingerprint),
            str(intent.symbol).upper().replace("-", ""), effective_at,
        ),
    )
    fee = cur.fetchone()
    if fee is None:
        raise PreEntryRiskIncomplete("MISSING_EXIT_COST_AUTHORITY")
    if fee[3] <= effective_at:
        raise PreEntryRiskIncomplete("STALE_EXIT_COST_AUTHORITY")
    if str(fee[4]) != str(account_identity_fingerprint):
        raise PreEntryRiskIncomplete("ACCOUNT_IDENTITY_MISMATCH")
    if str(fee[5]) != str(intent.deployment_id.value):
        raise PreEntryRiskIncomplete("DEPLOYMENT_MISMATCH")
    if str(fee[6]) != "SPOT" or str(fee[7]) != str(intent.symbol).upper().replace("-", ""):
        raise PreEntryRiskIncomplete("INSTRUMENT_MISMATCH")
    price_row_id = reference_price_row_identity(
        symbol=str(intent.symbol), interval=str(intent.interval),
        candle_open_time=reference_price_timestamp,
    )
    status, event = freeze_pre_entry_risk_cursor(
        cur, environment="LIVE", deployment_id=str(intent.deployment_id.value),
        account_identity_fingerprint=str(account_identity_fingerprint),
        decision_id=str(intent.decision_id), commitment_id=str(reservation[0]),
        reservation_id=reservation_id, intent_id=str(intent.intent_id),
        order_identity=str(intent.client_order_id), symbol=str(intent.symbol),
        strategy=str(intent.strategy), interval=str(intent.interval),
        boundary_id=uuid.UUID(str(boundary[0])),
        boundary_policy_id=str(boundary[8]),
        boundary_policy_version=str(boundary[9]),
        boundary_policy_fingerprint=str(boundary[10]),
        boundary_distance_pct=boundary[11], reference_price=reference,
        reference_price_timestamp=reference_price_timestamp,
        reference_price_source=REFERENCE_PRICE_SOURCE,
        reference_price_row_identity_value=price_row_id,
        reference_price_fingerprint_value=reference_price_fingerprint(
            symbol=str(intent.symbol), interval=str(intent.interval),
            candle_open_time=reference_price_timestamp,
            reference_price=reference,
        ), proposed_quantity=intent.requested_qty,
        quantity_source="LIVE_ENTRY_INTENT_V1.requested_qty",
        quantity_evidence_fingerprint_value=quantity_fingerprint(
            source="LIVE_ENTRY_INTENT_V1.requested_qty",
            quantity=intent.requested_qty,
        ), exit_cost_snapshot_or_model_id=str(fee[0]),
        exit_cost_evidence_fingerprint=str(fee[2]),
        canonical_exit_fee_rate=fee[1], runtime_revision=str(intent.git_revision),
        effective_at=effective_at,
        source_authority="LIVE_ACCEPTED_ENTRY_COMMITMENT",
        provenance={
            "intent_id": str(intent.intent_id),
            "intent_fingerprint": str(intent.content_fingerprint),
        },
    )
    return status, event.pre_entry_risk_id


def transition_pre_entry_risk_cursor(
    cur: Any, *, reservation_id: uuid.UUID,
    source_event_identity: str, effective_at: datetime,
    transfer_quantity: Decimal | str | int = ZERO,
    release_remaining: bool = False,
    open_risk_status: str | None = None,
    open_risk_position_id: int | None = None,
    open_risk_boundary_id: uuid.UUID | None = None,
    open_risk_evidence_fingerprint: str | None = None,
    no_unattributed_fill_status: str | None = None,
    source_authority: str = "PRE_ENTRY_RISK_TRANSITION_V1",
    provenance: Mapping[str, Any] | None = None,
) -> tuple[str, PreEntryRiskEvent]:
    current = load_current_pre_entry_risk_cursor(
        cur, reservation_id=reservation_id,
    )
    if current is None:
        raise ValueError("PRE_ENTRY_RISK_NOT_FOUND")
    cur.execute(
        "SELECT evidence_fingerprint FROM pre_entry_risk_event_v1 "
        "WHERE pre_entry_risk_id=%s AND source_event_identity=%s",
        (str(current.pre_entry_risk_id), str(source_event_identity)),
    )
    if cur.fetchone() is not None:
        cur.execute(
            f"SELECT {_EVENT_COLUMNS} FROM pre_entry_risk_event_v1 "
            "WHERE pre_entry_risk_id=%s AND source_event_identity=%s",
            (str(current.pre_entry_risk_id), str(source_event_identity)),
        )
        return "IDEMPOTENT", _event_from_row(cur.fetchone())
    if current.lifecycle_state in {"REPLACED_BY_OPEN_RISK", "RELEASED"}:
        raise ValueError("PRE_ENTRY_RISK_TERMINAL_REACTIVATION_FORBIDDEN")
    delta = _decimal(transfer_quantity, "transfer_quantity")
    if delta < ZERO or delta > current.remaining_committed_quantity:
        raise ValueError("PRE_ENTRY_RISK_TRANSFER_QUANTITY_INVALID")
    if delta > ZERO:
        if (
            open_risk_status != "CANONICAL"
            or open_risk_position_id is None
            or open_risk_boundary_id is None
            or not open_risk_evidence_fingerprint
        ):
            raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
        cur.execute(
            "SELECT state,position_id,boundary_id FROM "
            "v_position_risk_boundary_current_v1 WHERE reservation_id=%s",
            (str(reservation_id),),
        )
        boundary = cur.fetchone()
        if (
            boundary is None
            or str(boundary[0]) not in {
                "BOUNDARY_ACTIVATED", "BOUNDARY_REVISED_ENTRY_BASIS"
            }
            or int(boundary[1]) != int(open_risk_position_id)
            or uuid.UUID(str(boundary[2])) != open_risk_boundary_id
        ):
            raise PreEntryRiskIncomplete("MISSING_BOUNDARY_POLICY")
    if release_remaining:
        if no_unattributed_fill_status != "CANONICAL_NONE":
            raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
        cur.execute(
            "SELECT state FROM v_capital_reservation_current_v1 "
            "WHERE reservation_id=%s", (str(reservation_id),),
        )
        reservation = cur.fetchone()
        if reservation is None or str(reservation[0]) not in {
            "REJECTED", "CANCELLED", "EXPIRED", "RELEASED", "DEPLOYED"
        }:
            raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    transferred = current.transferred_quantity + delta
    remaining = current.remaining_committed_quantity - delta
    released = current.released_quantity
    if release_remaining:
        released += remaining
        remaining = ZERO
    if remaining > ZERO and transferred > ZERO:
        lifecycle = "PARTIALLY_TRANSFERRED"
    elif remaining == ZERO and released > ZERO:
        lifecycle = "RELEASED"
    elif remaining == ZERO and transferred == current.original_quantity:
        lifecycle = "REPLACED_BY_OPEN_RISK"
    else:
        lifecycle = "ACTIVE_COMMITTED"
    # Current risk is always the untransferred slice of the immutable frozen
    # risk.  Scale from the original proposed quantity so repeated partial
    # transfers cannot compound rounding or shrink the risk twice.
    unit_core_risk = (
        current.pre_entry_core_price_risk
        / current.remaining_committed_quantity
    )
    unit_exit_fee_risk = (
        current.pre_entry_exit_fee_estimate
        / current.remaining_committed_quantity
    )
    event = _make_event(
        **{
            key: value for key, value in current.__dict__.items()
            if key not in {
                "event_id", "event_sequence", "source_event_identity",
                "pre_entry_core_price_risk", "pre_entry_exit_fee_estimate",
                "total_pre_entry_risk", "transferred_quantity",
                "remaining_committed_quantity", "released_quantity",
                "lifecycle_state", "open_risk_position_id",
                "open_risk_boundary_id", "open_risk_evidence_fingerprint",
                "effective_at", "source_authority", "provenance",
                "evidence_fingerprint", "contract_version",
            }
        },
        event_sequence=current.event_sequence + 1,
        source_event_identity=str(source_event_identity),
        pre_entry_core_price_risk=unit_core_risk * remaining,
        pre_entry_exit_fee_estimate=unit_exit_fee_risk * remaining,
        total_pre_entry_risk=(unit_core_risk + unit_exit_fee_risk) * remaining,
        transferred_quantity=transferred,
        remaining_committed_quantity=remaining,
        released_quantity=released, lifecycle_state=lifecycle,
        open_risk_position_id=(
            open_risk_position_id or current.open_risk_position_id
        ), open_risk_boundary_id=(
            open_risk_boundary_id or current.open_risk_boundary_id
        ), open_risk_evidence_fingerprint=(
            open_risk_evidence_fingerprint
            or current.open_risk_evidence_fingerprint
        ), effective_at=effective_at, source_authority=source_authority,
        provenance=dict(provenance or {}), contract_version=CONTRACT_VERSION,
    )
    return append_pre_entry_risk_event_cursor(cur, event), event


def release_pre_entry_risk_cursor(
    cur: Any, *, reservation_id: uuid.UUID, source_event_identity: str,
    effective_at: datetime, no_unattributed_fill_status: str,
    provenance: Mapping[str, Any] | None = None,
) -> tuple[str, PreEntryRiskEvent]:
    return transition_pre_entry_risk_cursor(
        cur, reservation_id=reservation_id,
        source_event_identity=source_event_identity,
        effective_at=effective_at, release_remaining=True,
        no_unattributed_fill_status=no_unattributed_fill_status,
        source_authority="PRE_ENTRY_RISK_TERMINAL_RELEASE_V1",
        provenance=provenance,
    )


def runtime_revision(environ: Mapping[str, str] | None = None) -> str:
    source = os.environ if environ is None else environ
    revision = str(source.get("GIT_SHA") or "").strip().lower()
    if len(revision) != 40 or any(ch not in "0123456789abcdef" for ch in revision):
        raise PreEntryRiskIncomplete("EVIDENCE_INCOMPLETE")
    return revision


@dataclass(frozen=True)
class CommittedPreEntryRiskEvidence:
    total_pre_entry_risk: Decimal | None
    active_commitment_count: int
    evidence_status: str
    contract_version: str = CONTRACT_VERSION


def load_committed_pre_entry_risk_evidence_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str,
) -> CommittedPreEntryRiskEvidence:
    """Return the one canonical aggregate future Risk Budget may consume."""
    if not pre_entry_risk_schema_available_cursor(cur):
        return CommittedPreEntryRiskEvidence(None, 0, "EVIDENCE_INCOMPLETE")
    cur.execute(
        "SELECT count(*),coalesce(sum(total_pre_entry_risk),0),"
        "count(*) FILTER (WHERE evidence_status<>'CANONICAL') "
        "FROM v_pre_entry_risk_current_v1 WHERE environment=%s "
        "AND deployment_id=%s AND account_identity_fingerprint=%s "
        "AND lifecycle_state IN ('ACTIVE_COMMITTED','PARTIALLY_TRANSFERRED')",
        (
            str(environment).upper(), str(deployment_id).lower(),
            str(account_identity_fingerprint),
        ),
    )
    count, total, incomplete = cur.fetchone()
    if int(incomplete or 0) != 0:
        return CommittedPreEntryRiskEvidence(
            None, int(count or 0), "EVIDENCE_INCOMPLETE"
        )
    return CommittedPreEntryRiskEvidence(
        _decimal(total, "total_pre_entry_risk"), int(count or 0), "CANONICAL"
    )
