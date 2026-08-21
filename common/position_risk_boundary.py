"""Position Risk Boundary Authority V1 and pure risk semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping
import uuid

from psycopg2.extras import Json

from common.capital_reservation import (
    deterministic_reservation_id,
    logical_commitment_key,
    paper_account_identity_fingerprint,
)


CONTRACT_VERSION = "POSITION_RISK_BOUNDARY_AUTHORITY_V1"
POLICY_TYPE = "ENTRY_BASIS_PERCENT_DISTANCE"
POLICY_ID = "SPOT_LONG_SOFTWARE_STOP"
POLICY_VERSION = "SPOT_LONG_SOFTWARE_STOP_V1"
BOUNDARY_TYPE = "SOFTWARE_TRIGGER"
EXECUTION_PRICE_GUARANTEE = "NO"
TAIL_RISK_STATUS = "UNBOUNDED_BY_TRIGGER"
ZERO = Decimal("0")
ONE = Decimal("1")
BOUNDARY_NAMESPACE = uuid.UUID("335682bf-c4cd-4303-89ed-832c3832e2ee")
EVENT_NAMESPACE = uuid.UUID("b0a7891e-d14b-4030-b817-b43bf26f87fe")


def _decimal(value: object, field: str) -> Decimal:
    if value is None or isinstance(value, float):
        raise ValueError(f"RISK_BOUNDARY_INVALID_DECIMAL:{field}")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"RISK_BOUNDARY_INVALID_DECIMAL:{field}")
    return result


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("RISK_BOUNDARY_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    if isinstance(value, float):
        raise ValueError("RISK_BOUNDARY_FLOAT_FORBIDDEN")
    return value


def _fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def policy_fingerprint(
    *, strategy: str, interval: str,
    boundary_distance_pct: Decimal | str | int,
) -> str:
    distance = _decimal(boundary_distance_pct, "boundary_distance_pct")
    if distance <= ZERO or distance >= Decimal("100"):
        raise ValueError("RISK_BOUNDARY_DISTANCE_INVALID")
    return _fingerprint({
        "boundary_policy_type": POLICY_TYPE,
        "boundary_distance_pct": distance,
        "boundary_type": BOUNDARY_TYPE,
        "execution_price_guarantee": EXECUTION_PRICE_GUARANTEE,
        "instrument_model": "OKX_SPOT_LONG_ONLY",
        "interval": str(interval).lower(),
        "policy_id": POLICY_ID,
        "policy_version": POLICY_VERSION,
        "strategy": str(strategy).upper(),
    })


def boundary_price_from_basis(
    entry_basis_price: Decimal | str | int,
    boundary_distance_pct: Decimal | str | int,
) -> Decimal:
    basis = _decimal(entry_basis_price, "entry_basis_price")
    distance = _decimal(boundary_distance_pct, "boundary_distance_pct")
    if basis <= ZERO or distance <= ZERO or distance >= Decimal("100"):
        raise ValueError("RISK_BOUNDARY_ACTIVATION_INPUT_INVALID")
    return basis * (ONE - distance / Decimal("100"))


@dataclass(frozen=True)
class BoundaryEvent:
    event_id: uuid.UUID
    boundary_id: uuid.UUID
    event_sequence: int
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    reservation_id: uuid.UUID
    position_id: int | None
    decision_id: str
    intent_id: str | None
    order_identity: str | None
    symbol: str
    strategy: str
    interval: str
    side: str
    boundary_policy_type: str
    boundary_distance_pct: Decimal
    entry_basis_price: Decimal | None
    entry_basis_authority: str | None
    boundary_price: Decimal | None
    boundary_type: str
    execution_price_guarantee: str
    policy_id: str
    policy_version: str
    policy_fingerprint: str
    state: str
    effective_at: datetime
    source_authority: str
    provenance: Mapping[str, Any]
    event_fingerprint: str
    contract_version: str = CONTRACT_VERSION


@dataclass(frozen=True)
class RiskBoundaryProjection:
    boundary_id: uuid.UUID
    position_id: int | None
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    side: str
    state: str
    boundary_distance_pct: Decimal
    entry_basis_price: Decimal | None
    entry_basis_authority: str | None
    boundary_price: Decimal | None
    boundary_type: str
    execution_price_guarantee: str
    policy_fingerprint: str
    effective_at: datetime
    exit_fee_rate: Decimal | None = None
    exit_fee_model: str | None = None


@dataclass(frozen=True)
class PositionRiskEvidence:
    position_id: int
    core_price_risk: Decimal | None
    exit_fee_estimate: Decimal | None
    open_risk_to_trigger: Decimal | None
    status: str
    boundary_price: Decimal | None
    boundary_type: str | None
    execution_price_guarantee: str | None
    tail_risk_status: str
    spread_status: str = "UNKNOWN"
    slippage_status: str = "UNKNOWN"


def evaluate_position_risk(
    *, position_id: int, side: str, remaining_inventory_qty: Decimal | None,
    mark_price: Decimal | None, mark_status: str,
    projection: RiskBoundaryProjection | None,
    require_exit_cost: bool = True,
) -> PositionRiskEvidence:
    base = dict(
        position_id=int(position_id), core_price_risk=None,
        exit_fee_estimate=None, open_risk_to_trigger=None,
        boundary_price=(projection.boundary_price if projection else None),
        boundary_type=(projection.boundary_type if projection else None),
        execution_price_guarantee=(
            projection.execution_price_guarantee if projection else None
        ), tail_risk_status=TAIL_RISK_STATUS,
    )
    if projection is None or projection.state not in {
        "BOUNDARY_ACTIVATED", "BOUNDARY_REVISED_ENTRY_BASIS"
    }:
        return PositionRiskEvidence(status="MISSING_BOUNDARY", **base)
    if projection.position_id != int(position_id):
        return PositionRiskEvidence(status="MISSING_POSITION_LINKAGE", **base)
    if str(side).upper() not in {"LONG", "BUY"} or projection.side != "LONG":
        return PositionRiskEvidence(status="BOUNDARY_INVALID", **base)
    if mark_status == "PRICE_UNAVAILABLE":
        return PositionRiskEvidence(status="MISSING_MARK", **base)
    if mark_status == "PRICE_STALE":
        return PositionRiskEvidence(status="STALE_MARK", **base)
    if mark_status != "CANONICAL" or remaining_inventory_qty is None:
        return PositionRiskEvidence(status="INCOMPLETE", **base)
    if projection.entry_basis_price is None:
        return PositionRiskEvidence(status="MISSING_ENTRY_BASIS", **base)
    qty = _decimal(remaining_inventory_qty, "remaining_inventory_qty")
    mark = _decimal(mark_price, "mark_price")
    boundary = _decimal(projection.boundary_price, "boundary_price")
    basis = _decimal(projection.entry_basis_price, "entry_basis_price")
    if qty < ZERO or boundary <= ZERO or boundary >= basis:
        return PositionRiskEvidence(status="BOUNDARY_INVALID", **base)
    if qty == ZERO:
        return PositionRiskEvidence(
            status="CANONICAL", core_price_risk=ZERO,
            exit_fee_estimate=ZERO, open_risk_to_trigger=ZERO,
            **{key: value for key, value in base.items()
               if key not in {"core_price_risk", "exit_fee_estimate", "open_risk_to_trigger"}},
        )
    if mark <= boundary:
        return PositionRiskEvidence(
            status="BOUNDARY_BREACHED_UNRESOLVED", **base
        )
    core = (mark - boundary) * qty
    if require_exit_cost and projection.exit_fee_rate is None:
        return PositionRiskEvidence(
            status="MISSING_COST_AUTHORITY", core_price_risk=core,
            **{key: value for key, value in base.items() if key != "core_price_risk"},
        )
    fee = (
        ZERO if projection.exit_fee_rate is None
        else boundary * qty * _decimal(projection.exit_fee_rate, "exit_fee_rate")
    )
    return PositionRiskEvidence(
        status="CANONICAL", core_price_risk=core,
        exit_fee_estimate=fee, open_risk_to_trigger=core + fee,
        **{key: value for key, value in base.items()
           if key not in {"core_price_risk", "exit_fee_estimate", "open_risk_to_trigger"}},
    )


def boundary_schema_available_cursor(cur: Any) -> bool:
    cur.execute(
        "SELECT to_regclass('public.position_risk_boundary_event_v1'),"
        "to_regclass('public.v_position_risk_boundary_current_v1')"
    )
    row = cur.fetchone()
    return bool(row and row[0] is not None and row[1] is not None)


def _make_event(**values: Any) -> BoundaryEvent:
    semantic = dict(values)
    fingerprint = _fingerprint({**semantic, "contract_version": CONTRACT_VERSION})
    event_id = uuid.uuid5(
        EVENT_NAMESPACE,
        f"{semantic['boundary_id']}:{semantic['event_sequence']}:{fingerprint}",
    )
    return BoundaryEvent(
        event_id=event_id, event_fingerprint=fingerprint,
        contract_version=CONTRACT_VERSION, **semantic,
    )


_COLUMNS = """
event_id,boundary_id,event_sequence,environment,deployment_id,
account_identity_fingerprint,reservation_id,position_id,decision_id,intent_id,
order_identity,symbol,strategy,interval,side,boundary_policy_type,
boundary_distance_pct,entry_basis_price,entry_basis_authority,boundary_price,
boundary_type,execution_price_guarantee,policy_id,policy_version,
policy_fingerprint,state,effective_at,source_authority,provenance,
event_fingerprint,contract_version
"""


def append_boundary_event_cursor(cur: Any, event: BoundaryEvent) -> str:
    cur.execute(
        "SELECT event_fingerprint FROM position_risk_boundary_event_v1 "
        "WHERE event_id=%s", (str(event.event_id),),
    )
    row = cur.fetchone()
    if row:
        if str(row[0]) == event.event_fingerprint:
            return "IDEMPOTENT"
        raise ValueError("RISK_BOUNDARY_EVENT_ID_CONFLICT")
    values = (
        str(event.event_id), str(event.boundary_id), event.event_sequence,
        event.environment, event.deployment_id,
        event.account_identity_fingerprint, str(event.reservation_id),
        event.position_id, event.decision_id, event.intent_id,
        event.order_identity, event.symbol, event.strategy, event.interval,
        event.side, event.boundary_policy_type,
        event.boundary_distance_pct, event.entry_basis_price,
        event.entry_basis_authority, event.boundary_price,
        event.boundary_type, event.execution_price_guarantee,
        event.policy_id, event.policy_version, event.policy_fingerprint,
        event.state, event.effective_at, event.source_authority,
        Json(dict(event.provenance)), event.event_fingerprint,
        event.contract_version,
    )
    cur.execute(
        f"INSERT INTO position_risk_boundary_event_v1 ({_COLUMNS}) VALUES ("
        + ",".join(["%s"] * len(values))
        + ") ON CONFLICT(event_id) DO NOTHING RETURNING event_id",
        values,
    )
    return "INSERTED" if cur.fetchone() else "IDEMPOTENT"


def _effective_stop_distance_cursor(
    cur: Any, *, symbol: str, strategy: str, interval: str,
) -> Decimal:
    cur.execute(
        "SELECT param_value FROM strategy_params WHERE symbol=%s "
        "AND upper(strategy)=upper(%s) AND interval=%s "
        "AND param_name='STOP_LOSS_PCT'",
        (str(symbol), str(strategy), str(interval)),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("RISK_BOUNDARY_EFFECTIVE_STOP_POLICY_UNAVAILABLE")
    return _decimal(row[0], "boundary_distance_pct")


def accept_boundary_policy_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str, reservation_id: uuid.UUID,
    decision_id: str, intent_id: str | None, order_identity: str | None,
    symbol: str, strategy: str, interval: str, effective_at: datetime,
    source_authority: str, provenance: Mapping[str, Any],
    boundary_distance_pct: Decimal | None = None,
) -> tuple[str, uuid.UUID | None]:
    if not boundary_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE", None
    boundary_id = uuid.uuid5(BOUNDARY_NAMESPACE, str(reservation_id))
    cur.execute(
        f"SELECT {_COLUMNS} FROM v_position_risk_boundary_current_v1 "
        "WHERE boundary_id=%s",
        (str(boundary_id),),
    )
    existing_row = cur.fetchone()
    if existing_row is not None:
        existing = _event_from_row(existing_row)
        expected_identity = (
            str(environment).upper(), str(deployment_id).lower(),
            str(account_identity_fingerprint), str(reservation_id),
            str(decision_id), (str(intent_id) if intent_id else None),
            (str(order_identity) if order_identity else None),
            str(symbol).upper(), str(strategy).upper(), str(interval).lower(),
        )
        actual_identity = (
            existing.environment, existing.deployment_id,
            existing.account_identity_fingerprint, str(existing.reservation_id),
            existing.decision_id, existing.intent_id, existing.order_identity,
            existing.symbol, existing.strategy, existing.interval,
        )
        if actual_identity != expected_identity:
            raise ValueError("RISK_BOUNDARY_ACCEPTED_POLICY_CONFLICT")
        if (
            boundary_distance_pct is not None
            and existing.boundary_distance_pct
            != _decimal(boundary_distance_pct, "boundary_distance_pct")
        ):
            raise ValueError("RISK_BOUNDARY_ACCEPTED_POLICY_CONFLICT")
        return "IDEMPOTENT", boundary_id
    distance = (
        _effective_stop_distance_cursor(
            cur, symbol=symbol, strategy=strategy, interval=interval,
        ) if boundary_distance_pct is None else
        _decimal(boundary_distance_pct, "boundary_distance_pct")
    )
    fingerprint = policy_fingerprint(
        strategy=strategy, interval=interval,
        boundary_distance_pct=distance,
    )
    event = _make_event(
        boundary_id=boundary_id, event_sequence=1,
        environment=str(environment).upper(),
        deployment_id=str(deployment_id).lower(),
        account_identity_fingerprint=str(account_identity_fingerprint),
        reservation_id=reservation_id, position_id=None,
        decision_id=str(decision_id), intent_id=(str(intent_id) if intent_id else None),
        order_identity=(str(order_identity) if order_identity else None),
        symbol=str(symbol).upper(), strategy=str(strategy).upper(),
        interval=str(interval).lower(), side="LONG",
        boundary_policy_type=POLICY_TYPE,
        boundary_distance_pct=distance, entry_basis_price=None,
        entry_basis_authority=None, boundary_price=None,
        boundary_type=BOUNDARY_TYPE,
        execution_price_guarantee=EXECUTION_PRICE_GUARANTEE,
        policy_id=POLICY_ID, policy_version=POLICY_VERSION,
        policy_fingerprint=fingerprint, state="BOUNDARY_POLICY_ACCEPTED",
        effective_at=effective_at, source_authority=source_authority,
        provenance=dict(provenance),
    )
    return append_boundary_event_cursor(cur, event), boundary_id


def accept_paper_boundary_cursor(
    cur: Any, *, simulated_order_id: int, deployment_id: str,
    decision_id: str, symbol: str, strategy: str, interval: str,
    effective_at: datetime,
) -> str:
    key = logical_commitment_key(
        environment="PAPER", deployment_id=deployment_id,
        source_identity=f"SIMULATED_ORDER:{int(simulated_order_id)}",
    )
    status, _ = accept_boundary_policy_cursor(
        cur, environment="PAPER", deployment_id=deployment_id,
        account_identity_fingerprint=paper_account_identity_fingerprint(deployment_id),
        reservation_id=deterministic_reservation_id(key),
        decision_id=decision_id, intent_id=None,
        order_identity=str(simulated_order_id), symbol=symbol,
        strategy=strategy, interval=interval, effective_at=effective_at,
        source_authority="PAPER_ACCEPTED_ENTRY_COMMITMENT",
        provenance={"simulated_order_id": int(simulated_order_id)},
    )
    return status


def activate_boundary_for_position_cursor(
    cur: Any, *, position_id: int, environment: str,
    deployment_id: str, effective_at: datetime,
    source_authority: str,
) -> str:
    if not boundary_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    cur.execute(
        "SELECT simulated_order_id FROM simulated_execution_fills_v1 "
        "WHERE position_id=%s AND order_purpose='ENTRY' "
        "ORDER BY execution_at,id LIMIT 1", (int(position_id),),
    )
    order = cur.fetchone()
    if order is None:
        return "MISSING_POSITION_LINKAGE"
    cur.execute(
        f"SELECT {_COLUMNS} FROM v_position_risk_boundary_current_v1 "
        "WHERE order_identity=%s AND environment=%s AND deployment_id=%s",
        (str(order[0]), str(environment).upper(), str(deployment_id).lower()),
    )
    row = cur.fetchone()
    if row is None:
        return "MISSING_BOUNDARY"
    previous = _event_from_row(row)
    cur.execute(
        "SELECT sum(fill_qty*fill_price)/nullif(sum(fill_qty),0) "
        "FROM simulated_execution_fills_v1 WHERE position_id=%s "
        "AND order_purpose='ENTRY'", (int(position_id),),
    )
    basis_row = cur.fetchone()
    if basis_row is None or basis_row[0] is None:
        return "MISSING_ENTRY_BASIS"
    basis = _decimal(basis_row[0], "entry_basis_price")
    if previous.entry_basis_price == basis and previous.position_id == int(position_id):
        return "IDEMPOTENT"
    state = (
        "BOUNDARY_ACTIVATED" if previous.entry_basis_price is None
        else "BOUNDARY_REVISED_ENTRY_BASIS"
    )
    event = _make_event(
        boundary_id=previous.boundary_id,
        event_sequence=previous.event_sequence + 1,
        environment=previous.environment, deployment_id=previous.deployment_id,
        account_identity_fingerprint=previous.account_identity_fingerprint,
        reservation_id=previous.reservation_id, position_id=int(position_id),
        decision_id=previous.decision_id, intent_id=previous.intent_id,
        order_identity=previous.order_identity, symbol=previous.symbol,
        strategy=previous.strategy, interval=previous.interval, side="LONG",
        boundary_policy_type=previous.boundary_policy_type,
        boundary_distance_pct=previous.boundary_distance_pct,
        entry_basis_price=basis,
        entry_basis_authority="CANONICAL_WEIGHTED_ENTRY_FILL_EVIDENCE",
        boundary_price=boundary_price_from_basis(
            basis, previous.boundary_distance_pct,
        ), boundary_type=BOUNDARY_TYPE,
        execution_price_guarantee=EXECUTION_PRICE_GUARANTEE,
        policy_id=previous.policy_id, policy_version=previous.policy_version,
        policy_fingerprint=previous.policy_fingerprint, state=state,
        effective_at=effective_at, source_authority=source_authority,
        provenance={
            "position_id": int(position_id),
            "old_entry_basis_price": (
                None if previous.entry_basis_price is None
                else str(previous.entry_basis_price)
            ),
            "new_entry_basis_price": str(basis),
            "order_identity": previous.order_identity,
        },
    )
    return append_boundary_event_cursor(cur, event)


def activate_live_boundary_cursor(
    cur: Any, *, intent_id: str, position_id: int,
    canonical_entry_basis: Decimal | str | int, effective_at: datetime,
) -> str:
    if not boundary_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    cur.execute(
        f"SELECT {_COLUMNS} FROM v_position_risk_boundary_current_v1 "
        "WHERE intent_id=%s", (str(intent_id),),
    )
    row = cur.fetchone()
    if row is None:
        return "MISSING_BOUNDARY"
    previous = _event_from_row(row)
    basis = _decimal(canonical_entry_basis, "entry_basis_price")
    if previous.entry_basis_price == basis and previous.position_id == int(position_id):
        return "IDEMPOTENT"
    state = (
        "BOUNDARY_ACTIVATED" if previous.entry_basis_price is None
        else "BOUNDARY_REVISED_ENTRY_BASIS"
    )
    event = _make_event(
        boundary_id=previous.boundary_id,
        event_sequence=previous.event_sequence + 1,
        environment=previous.environment, deployment_id=previous.deployment_id,
        account_identity_fingerprint=previous.account_identity_fingerprint,
        reservation_id=previous.reservation_id, position_id=int(position_id),
        decision_id=previous.decision_id, intent_id=previous.intent_id,
        order_identity=previous.order_identity, symbol=previous.symbol,
        strategy=previous.strategy, interval=previous.interval, side="LONG",
        boundary_policy_type=previous.boundary_policy_type,
        boundary_distance_pct=previous.boundary_distance_pct,
        entry_basis_price=basis,
        entry_basis_authority="LIVE_ENTRY_POSITION_PROJECTION_V1_WEIGHTED_FILL",
        boundary_price=boundary_price_from_basis(
            basis, previous.boundary_distance_pct,
        ), boundary_type=BOUNDARY_TYPE,
        execution_price_guarantee=EXECUTION_PRICE_GUARANTEE,
        policy_id=previous.policy_id, policy_version=previous.policy_version,
        policy_fingerprint=previous.policy_fingerprint, state=state,
        effective_at=effective_at,
        source_authority="LIVE_ENTRY_POSITION_PROJECTION_V1",
        provenance={
            "position_id": int(position_id),
            "old_entry_basis_price": (
                None if previous.entry_basis_price is None
                else str(previous.entry_basis_price)
            ),
            "new_entry_basis_price": str(basis),
            "intent_id": previous.intent_id,
        },
    )
    return append_boundary_event_cursor(cur, event)


def _event_from_row(row: tuple[Any, ...]) -> BoundaryEvent:
    values = list(row)
    values[0] = uuid.UUID(str(values[0]))
    values[1] = uuid.UUID(str(values[1]))
    values[6] = uuid.UUID(str(values[6]))
    return BoundaryEvent(*values)


def load_boundary_projections_cursor(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str | None,
) -> tuple[dict[int, RiskBoundaryProjection], str]:
    if not account_identity_fingerprint or not boundary_schema_available_cursor(cur):
        return {}, "MISSING_BOUNDARY"
    cur.execute(
        "SELECT boundary_id,position_id,environment,deployment_id,"
        "account_identity_fingerprint,side,state,boundary_distance_pct,"
        "entry_basis_price,entry_basis_authority,boundary_price,boundary_type,"
        "execution_price_guarantee,policy_fingerprint,effective_at "
        "FROM v_position_risk_boundary_current_v1 WHERE environment=%s "
        "AND deployment_id=%s AND account_identity_fingerprint=%s "
        "AND position_id IS NOT NULL",
        (str(environment).upper(), str(deployment_id).lower(),
         str(account_identity_fingerprint)),
    )
    result: dict[int, RiskBoundaryProjection] = {}
    for row in cur.fetchall():
        projection = RiskBoundaryProjection(
            uuid.UUID(str(row[0])), int(row[1]), str(row[2]), str(row[3]),
            str(row[4]), str(row[5]), str(row[6]), Decimal(str(row[7])),
            None if row[8] is None else Decimal(str(row[8])), row[9],
            None if row[10] is None else Decimal(str(row[10])),
            str(row[11]), str(row[12]), str(row[13]), row[14],
        )
        result[int(row[1])] = projection
    return result, "CANONICAL"


def load_frozen_boundary_price(
    connection_factory: Any, *, position_id: int,
) -> Decimal | None:
    connection = connection_factory()
    try:
        with connection.cursor() as cur:
            if not boundary_schema_available_cursor(cur):
                return None
            cur.execute(
                "SELECT boundary_price FROM v_position_risk_boundary_current_v1 "
                "WHERE position_id=%s AND state IN "
                "('BOUNDARY_ACTIVATED','BOUNDARY_REVISED_ENTRY_BASIS')",
                (int(position_id),),
            )
            row = cur.fetchone()
            return None if not row or row[0] is None else Decimal(str(row[0]))
    finally:
        connection.close()
