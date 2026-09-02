from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import asdict, dataclass
from contextlib import nullcontext
from decimal import Decimal
import hashlib
import json
import logging
import os
import uuid

import psycopg2
from psycopg2.extras import Json

from common.contract_adoption import (
    contract_adoption_compatible,
    log_runtime_revision_provenance_diagnostic,
    require_runtime_git_revision,
)
from common.financial_truth_identity import IDENTITY_VERSION
from common.financial_truth_repository import ExecutionEvidenceContext
from common.financial_truth_writer import FinancialTruthReconciler
from common.entry_opportunity_evidence import (
    capture_entry_opportunity_snapshot_fail_open_cursor,
    link_entry_opportunity_order_fail_open_cursor,
    link_entry_opportunity_position_fail_open_cursor,
)
from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    InstrumentExecutionLimits,
    project_inventory_from_execution_evidence,
)
from common.paper_simulation_fee_config import (
    FEE_MODEL_V2,
    PaperSimulationFeeConfig,
    load_paper_simulation_fee_config,
)
from common.simulated_order_namespace import (
    ADMINISTRATIVE_ORDER_CLASS,
    FORWARD_ORDER_CLASS,
    detect_simulated_order_namespace,
)
from common.capital_reservation import (
    accept_paper_simulated_order_cursor,
    deploy_paper_simulated_fill_cursor,
)
from common.position_risk_boundary import (
    RiskBoundaryProjection,
    accept_paper_boundary_cursor,
    activate_boundary_for_position_cursor,
    evaluate_position_risk,
)
from common.pre_entry_risk import (
    freeze_paper_pre_entry_risk_cursor,
    load_current_pre_entry_risk_cursor,
    pre_entry_risk_schema_available_cursor,
    transition_pre_entry_risk_cursor,
)


SIMULATED_IDENTITY_VERSION = "SIMULATED_ACCOUNT_IDENTITY_V1"
INSTRUMENT_METADATA_VERSION = "EXECUTION_INSTRUMENT_SNAPSHOT_V1"
PAPER_OPEN_RISK_MARK_FRESHNESS = timedelta(minutes=20)


@dataclass(frozen=True)
class SimulatedOrderWriteBlocked:
    status: str
    existing_order_id: int | None = None

    def __bool__(self) -> bool:
        return False


@dataclass(frozen=True)
class PaperEntryAtomicResult:
    persisted: bool
    status: str
    simulated_order_id: int | None = None
    position_id: int | None = None

    def __bool__(self) -> bool:
        return self.persisted


@dataclass(frozen=True)
class PaperRealizableNetEvidence:
    """Causal PAPER full-close economics from frozen execution evidence."""

    status: str
    position_id: int
    symbol: str
    interval: str
    strategy: str
    observed_at: datetime
    mark_price: Decimal
    source_candle_id: str
    entry_fill_ids: tuple[int, ...] = ()
    fee_contract_fingerprint: str | None = None
    exit_fee_rate: Decimal | None = None
    quantity: Decimal | None = None
    hypothetical_exit_notional: Decimal | None = None
    hypothetical_exit_fee: Decimal | None = None
    realizable_net_after_all_costs: Decimal | None = None
    market_data_complete: bool = False
    ordering_evidence_available: str = "CAUSAL_RUNTIME_EVALUATION_SEQUENCE"
    position_entry_time: datetime | None = None
    peak_mark_price: Decimal | None = None

    @property
    def authoritative(self) -> bool:
        return self.status == "AUTHORITATIVE"


def load_paper_realizable_net_evidence(
    connection_factory,
    *,
    trading_mode: str,
    position_id: int,
    symbol: str,
    interval: str,
    strategy: str,
    current_price: Decimal,
    observed_at: datetime,
    source_candle_id: str,
    connection=None,
) -> PaperRealizableNetEvidence:
    """Read one point-in-time hypothetical full close; never changes trading state."""
    mark = Decimal(str(current_price))

    def incomplete(status: str) -> PaperRealizableNetEvidence:
        return PaperRealizableNetEvidence(
            status=status,
            position_id=int(position_id), symbol=str(symbol), interval=str(interval),
            strategy=str(strategy).upper(), observed_at=observed_at,
            mark_price=mark, source_candle_id=str(source_candle_id),
        )

    if str(trading_mode).upper() != "PAPER":
        return incomplete("NOT_APPLICABLE_NON_PAPER")
    if not mark.is_finite() or mark <= 0:
        return incomplete("INCOMPLETE:MARK_PRICE")

    owns_connection = connection is None
    conn = connection if connection is not None else connection_factory()
    try:
        if owns_connection:
            conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.status,p.side,p.remaining_inventory_qty,
                       p.inventory_evidence_status,p.entry_time,p.exit_order_id,
                       e.fee_rate_exit_assumption,e.fee_model_version
                FROM positions p
                LEFT JOIN entry_opportunity_evidence_v1 e
                  ON e.snapshot_id=p.entry_opportunity_snapshot_id
                WHERE p.id=%s AND p.symbol=%s AND p.strategy=%s
                  AND p.interval=%s
                """,
                (int(position_id), str(symbol), str(strategy).upper(), str(interval)),
            )
            position = cur.fetchone()
            if (
                position is None or str(position[0]).upper() != "OPEN"
                or str(position[1]).upper() != "LONG"
            ):
                return incomplete("INCOMPLETE:POSITION")
            qty, inventory_status, entry_time, exit_order_id, fee_rate, fee_model = position[2:]
            if str(inventory_status).upper() != "COMPLETE" or qty is None:
                return incomplete("INCOMPLETE:INVENTORY")
            if fee_rate is None or str(fee_model) != FEE_MODEL_V2:
                return incomplete("INCOMPLETE:COST_AUTHORITY")
            if exit_order_id is not None:
                return incomplete("INCOMPLETE:EXISTING_EXIT_COMMITTED")
            qty_d = Decimal(str(qty))
            rate_d = Decimal(str(fee_rate))
            if qty_d <= 0 or rate_d < 0:
                return incomplete("INCOMPLETE:COST_AUTHORITY")
            cur.execute(
                """
                SELECT array_agg(id ORDER BY id),COALESCE(sum(fill_qty),0),
                       COALESCE(sum(fill_notional),0),
                       COALESCE(sum(authoritative_fee_usdc),0),
                       count(DISTINCT simulation_fee_rate),
                       count(*) FILTER (WHERE fee_model_version<>%s)
                FROM simulated_execution_fills_v1
                WHERE position_id=%s AND order_purpose='ENTRY'
                """,
                (FEE_MODEL_V2, int(position_id)),
            )
            fill_ids, entry_qty, entry_notional, entry_fees, rates, bad_models = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM simulated_execution_fills_v1 "
                "WHERE position_id=%s AND order_purpose='EXIT'",
                (int(position_id),),
            )
            prior_exits = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT max(close) FROM candles
                WHERE symbol=%s AND interval=%s
                  AND open_time>=%s AND open_time<=%s
                """,
                (str(symbol), str(interval), entry_time, observed_at),
            )
            peak_price = cur.fetchone()[0]
        if owns_connection:
            conn.rollback()
    finally:
        if owns_connection:
            conn.close()

    entry_qty_d = Decimal(str(entry_qty))
    if (
        not fill_ids or entry_qty_d != qty_d or int(rates) != 1
        or int(bad_models) != 0 or prior_exits != 0
    ):
        return incomplete("INCOMPLETE:EXECUTION_SCOPE")
    entry_notional_d = Decimal(str(entry_notional))
    entry_fees_d = Decimal(str(entry_fees))
    exit_notional = qty_d * mark
    exit_fee = exit_notional * rate_d
    realizable = exit_notional - entry_notional_d - entry_fees_d - exit_fee
    fingerprint_payload = {
        "fee_model_version": FEE_MODEL_V2,
        "exit_fee_rate": format(rate_d, "f"),
        "entry_fill_ids": [int(value) for value in fill_ids],
        "entry_fees": format(entry_fees_d, "f"),
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return PaperRealizableNetEvidence(
        status="AUTHORITATIVE", position_id=int(position_id), symbol=str(symbol),
        interval=str(interval), strategy=str(strategy).upper(), observed_at=observed_at,
        mark_price=mark, source_candle_id=str(source_candle_id),
        entry_fill_ids=tuple(int(value) for value in fill_ids),
        fee_contract_fingerprint=fingerprint, exit_fee_rate=rate_d,
        quantity=qty_d, hypothetical_exit_notional=exit_notional,
        hypothetical_exit_fee=exit_fee,
        realizable_net_after_all_costs=realizable,
        market_data_complete=True,
        position_entry_time=entry_time,
        peak_mark_price=(Decimal(str(peak_price)) if peak_price is not None else None),
    )


class _PaperEntryAtomicBlocked(RuntimeError):
    def __init__(self, result: PaperEntryAtomicResult):
        super().__init__(result.status)
        self.result = result


def simulated_order_write_status(value) -> str:
    if isinstance(value, SimulatedOrderWriteBlocked):
        return value.status
    if not value:
        return "DB_GUARD_DUPLICATE"
    return "INSERTED"


@dataclass(frozen=True)
class PaperExitPreflightResult:
    """Read-only decision for a PAPER exit before execution evidence exists."""

    allowed: bool
    reason_code: str
    position_id: int | None
    position_status: str | None
    position_adoption_id: int | None
    position_generation: int | None
    active_adoption_id: int | None
    active_generation: int | None
    legacy_compatibility: bool
    active_adoption_git_revision: str | None = None
    runtime_git_revision: str | None = None
    runtime_revision_matches_adoption_provenance: bool | None = None
    detail: str | None = None

    def event_fields(self) -> dict:
        return asdict(self)


def _paper_exit_result(
    *,
    allowed: bool,
    reason_code: str,
    position=None,
    active=None,
    legacy_compatibility: bool = False,
    runtime_git_revision: str | None = None,
    runtime_revision_matches_adoption_provenance: bool | None = None,
    detail: str | None = None,
) -> PaperExitPreflightResult:
    position = position or (None, None, None, None, None)
    active = active or (None, None, None, None)
    return PaperExitPreflightResult(
        allowed=bool(allowed),
        reason_code=str(reason_code),
        position_id=int(position[0]) if position[0] is not None else None,
        position_status=str(position[1]) if position[1] is not None else None,
        position_adoption_id=(
            int(position[2]) if position[2] is not None else None
        ),
        position_generation=(
            int(position[3]) if position[3] is not None else None
        ),
        active_adoption_id=int(active[0]) if active[0] is not None else None,
        active_generation=int(active[1]) if active[1] is not None else None,
        legacy_compatibility=bool(legacy_compatibility),
        active_adoption_git_revision=(
            str(active[3]) if active[3] is not None else None
        ),
        runtime_git_revision=runtime_git_revision,
        runtime_revision_matches_adoption_provenance=(
            runtime_revision_matches_adoption_provenance
        ),
        detail=detail,
    )


def paper_exit_preflight_cursor(
    cur,
    *,
    deployment_id: str,
    symbol: str,
    strategy: str,
    interval: str,
    position_id: int | None = None,
) -> PaperExitPreflightResult:
    """Classify one PAPER position without creating durable state."""
    revision = require_runtime_git_revision()
    scope = f"paper-exit|{deployment_id}|{symbol}|{strategy}|{interval}"
    # The transaction-scoped lock serializes patched workers for one strategy slot.
    # It disappears on rollback and does not mutate application data.
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (scope,))
    cur.fetchone()

    if position_id is None:
        cur.execute(
            """
            SELECT id,status,inventory_contract_adoption_id,
                   inventory_contract_generation,entry_time
            FROM positions
            WHERE symbol=%s AND strategy=%s AND interval=%s
            ORDER BY CASE WHEN status='OPEN' THEN 0 ELSE 1 END,
                     entry_time DESC NULLS LAST,id DESC
            LIMIT 1
            """,
            (str(symbol), str(strategy), str(interval)),
        )
    else:
        cur.execute(
            """
            SELECT id,status,inventory_contract_adoption_id,
                   inventory_contract_generation,entry_time
            FROM positions
            WHERE id=%s AND symbol=%s AND strategy=%s AND interval=%s
            """,
            (int(position_id), str(symbol), str(strategy), str(interval)),
        )
    position = cur.fetchone()
    if position is None:
        return _paper_exit_result(
            allowed=False, reason_code="POSITION_NOT_FOUND"
        )
    if str(position[1]).upper() != "OPEN":
        return _paper_exit_result(
            allowed=False,
            reason_code="POSITION_ALREADY_CLOSED",
            position=position,
        )

    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,git_revision
        FROM runtime_contract_adoption_v2
        WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
          AND environment='paper'
          AND deployment_id=%s
          AND status='ACTIVE'
        """,
        (str(deployment_id),),
    )
    active_row = cur.fetchone()
    active = None
    if active_row is not None and contract_adoption_compatible(
        contract_name=active_row[1],
        environment=active_row[2],
        deployment_id=active_row[3],
        status=active_row[5],
        generation=active_row[4],
        expected_environment="paper",
        expected_deployment_id=str(deployment_id),
    ):
        active = (
            active_row[0], active_row[4], active_row[6], active_row[7]
        )
    if (
        active is None
        or active[0] is None
        or active[1] is None
        or active[2] is None
    ):
        return _paper_exit_result(
            allowed=False,
            reason_code="INVENTORY_CONTRACT_INCOMPLETE",
            position=position,
            active=active,
            runtime_git_revision=revision,
        )

    revision_matches = log_runtime_revision_provenance_diagnostic(
        adoption_id=int(active[0]),
        generation=int(active[1]),
        adoption_git_revision=str(active[3]),
        runtime_git_revision=revision,
    )

    cur.execute(
        "SELECT is_existing_projected_c2_2_compatible(%s,'paper')",
        (int(position[0]),),
    )
    compatibility_row = cur.fetchone()
    compatible = bool(compatibility_row and compatibility_row[0])
    adoption_id, generation, entry_time = position[2], position[3], position[4]
    active_adoption_id, active_generation, adopted_at = active[:3]

    def fee_contract_denial() -> PaperExitPreflightResult | None:
        try:
            _fee_contract_for_fill(
                cur, purpose="EXIT", position_id=int(position[0]),
            )
        except RuntimeError as exc:
            reason = str(exc)
            if reason not in {
                "PAPER_EXIT_ENTRY_FEE_CONTRACT_MISSING",
                "LEGACY_ENTRY_FEE_CONTRACT_UNRESOLVED",
                "PAPER_ENTRY_FEE_CONTRACT_CONFLICT",
            }:
                raise
            return _paper_exit_result(
                allowed=False,
                reason_code=reason,
                position=position,
                active=active,
                legacy_compatibility=compatible,
                runtime_git_revision=revision,
                runtime_revision_matches_adoption_provenance=revision_matches,
                detail="PAPER_EXIT_REQUIRES_FROZEN_ENTRY_FEE_CONTRACT",
            )
        return None

    if (
        adoption_id == active_adoption_id
        and generation == active_generation
    ):
        denial = fee_contract_denial()
        if denial is not None:
            return denial
        return _paper_exit_result(
            allowed=True, reason_code="ACTIVE_GENERATION_MATCH",
            position=position, active=active,
            runtime_git_revision=revision,
            runtime_revision_matches_adoption_provenance=revision_matches,
        )
    if compatible:
        denial = fee_contract_denial()
        if denial is not None:
            return denial
        return _paper_exit_result(
            allowed=True, reason_code="LEGACY_COMPATIBLE",
            position=position, active=active, legacy_compatibility=True,
            runtime_git_revision=revision,
            runtime_revision_matches_adoption_provenance=revision_matches,
        )
    if adoption_id is None and generation is not None:
        reason = "MISSING_ADOPTION_ID"
    elif adoption_id is not None and generation is None:
        reason = "MISSING_GENERATION"
    elif adoption_id is None and generation is None:
        if entry_time is None:
            reason = "LEGACY_NOT_COMPATIBLE"
        elif entry_time < adopted_at:
            reason = "ENTRY_BEFORE_ACTIVE_ADOPTION"
        else:
            # Preserve the existing guard's forward-position compatibility rule.
            denial = fee_contract_denial()
            if denial is not None:
                return denial
            return _paper_exit_result(
                allowed=True, reason_code="FORWARD_ENTRY_COMPATIBLE",
                position=position, active=active,
                runtime_git_revision=revision,
                runtime_revision_matches_adoption_provenance=revision_matches,
            )
    elif (
        adoption_id != active_adoption_id
        or generation != active_generation
    ):
        reason = "GENERATION_MISMATCH"
    else:
        reason = "MUTATION_NOT_ALLOWED_OTHER"
    return _paper_exit_result(
        allowed=False, reason_code=reason, position=position, active=active,
        runtime_git_revision=revision,
        runtime_revision_matches_adoption_provenance=revision_matches,
    )


class PaperExitPreflightGuard:
    """Hold the slot lock until the existing PAPER execution path completes."""

    def __init__(self, connection_factory, **kwargs):
        self._connection_factory = connection_factory
        self._kwargs = kwargs
        self._conn = None
        self._cur = None
        self.result = _paper_exit_result(
            allowed=False, reason_code="INVENTORY_CONTRACT_INCOMPLETE"
        )

    def __enter__(self) -> PaperExitPreflightResult:
        try:
            self._conn = self._connection_factory()
            self._cur = self._conn.cursor()
            self.result = paper_exit_preflight_cursor(
                self._cur, **self._kwargs
            )
        except Exception as exc:
            self.result = _paper_exit_result(
                allowed=False,
                reason_code="INVENTORY_CONTRACT_INCOMPLETE",
                detail=f"{type(exc).__name__}:{exc}",
            )
        return self.result

    def __exit__(self, _exc_type, _exc, _tb):
        if self._conn is not None:
            try:
                self._conn.rollback()
            finally:
                if self._cur is not None:
                    self._cur.close()
                self._conn.close()
        return False


def paper_exit_preflight_guard(connection_factory, **kwargs):
    return PaperExitPreflightGuard(connection_factory, **kwargs)


def execute_paper_exit_after_preflight(
    connection_factory,
    *,
    deployment_id: str,
    symbol: str,
    strategy: str,
    interval: str,
    exit_trigger: str,
    decision: str,
    price: float,
    candle_open_time,
    emit_event,
    action,
):
    """Run an existing PAPER exit path only while its preflight lease is held."""
    with paper_exit_preflight_guard(
        connection_factory,
        deployment_id=deployment_id,
        symbol=symbol,
        strategy=strategy,
        interval=interval,
    ) as result:
        if result.allowed:
            return action(result)
        info = {
            **result.event_fields(),
            "symbol": str(symbol),
            "interval": str(interval),
            "strategy": str(strategy),
            "exit_trigger": str(exit_trigger),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        emit_event(
            event_type="PAPER_EXIT_PREFLIGHT_BLOCKED",
            decision=decision,
            reason=result.reason_code,
            price=float(price),
            candle_open_time=candle_open_time,
            info=info,
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "order_accepted": False,
            "live_ok": False,
            "paper_executed": False,
            "blocked_reason": "PAPER_EXIT_PREFLIGHT_BLOCKED",
            "preflight_reason_code": result.reason_code,
            "position_id": result.position_id,
            "client_order_id": None,
            "resp": {"paper_exit_preflight": info},
        }


def paper_position_mutation_allowed_cursor(
    cur, *, position_id: int, deployment_id: str
) -> bool:
    try:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM positions p
              JOIN runtime_contract_adoption_v2 adoption
                ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
               AND adoption.environment='paper'
               AND adoption.deployment_id=%s
               AND adoption.status='ACTIVE'
              WHERE p.id=%s
                AND (
                  (
                    p.inventory_contract_adoption_id=adoption.adoption_id
                    AND p.inventory_contract_generation=adoption.generation
                  )
                  OR (
                    is_existing_projected_c2_2_compatible(p.id,'paper')
                  )
                  OR (
                    p.inventory_contract_adoption_id IS NULL
                    AND p.inventory_contract_generation IS NULL
                    AND p.entry_time>=adoption.adopted_at
                  )
                )
            )
            """,
            (
                str(deployment_id),
                int(position_id),
            ),
        )
        return bool(cur.fetchone()[0])
    except AssertionError:
        # Existing characterization cursors do not model the additive schema.
        return True


def _hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _assets(symbol: str) -> tuple[str, str]:
    symbol = str(symbol).upper()
    for quote in ("USDC", "USDT", "USD", "EUR"):
        if symbol.endswith(quote):
            return symbol[:-len(quote)], quote
    raise ValueError("unsupported quote asset")


def handoff_paper_fill_pre_entry_risk_cursor(
    cur,
    *,
    fill_id: int | None,
    environment: str,
    deployment_id: str,
) -> str:
    """Transfer only a canonical PAPER entry fill into canonical Open Risk."""
    if fill_id is None:
        return "ZERO_FILL_NOOP"
    if str(environment).upper() != "PAPER":
        return "NOT_APPLICABLE"
    if not pre_entry_risk_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"

    cur.execute(
        "SELECT simulated_order_id,position_id,fill_qty,fill_price,execution_at,"
        "environment,deployment_id,simulation_model_version,simulation_fee_rate,"
        "fee_model_version FROM simulated_execution_fills_v1 "
        "WHERE id=%s AND order_purpose='ENTRY'",
        (int(fill_id),),
    )
    fill = cur.fetchone()
    if fill is None:
        return "ENTRY_FILL_UNAVAILABLE"
    (
        simulated_order_id, position_id, fill_qty, fill_price, execution_at,
        fill_environment, fill_deployment_id, simulation_model_version,
        simulation_fee_rate, fee_model_version,
    ) = fill
    quantity = Decimal(str(fill_qty))
    if quantity == 0:
        return "ZERO_FILL_NOOP"
    if quantity < 0:
        return "INVALID_FILL_QUANTITY"
    if (
        str(fill_environment).upper() != "PAPER"
        or str(fill_deployment_id).lower() != str(deployment_id).lower()
    ):
        return "FILL_SCOPE_MISMATCH"

    cur.execute(
        "SELECT reservation_id,boundary_id,position_id,state,environment,"
        "deployment_id,account_identity_fingerprint,side,boundary_distance_pct,"
        "entry_basis_price,entry_basis_authority,boundary_price,boundary_type,"
        "execution_price_guarantee,policy_fingerprint,effective_at,event_fingerprint "
        "FROM v_position_risk_boundary_current_v1 WHERE order_identity=%s "
        "AND environment='PAPER' AND deployment_id=%s",
        (str(simulated_order_id), str(deployment_id).lower()),
    )
    boundary = cur.fetchone()
    if boundary is None:
        return "OPEN_RISK_INCOMPLETE:MISSING_BOUNDARY"
    (
        reservation_id, boundary_id, boundary_position_id, boundary_state,
        boundary_environment, boundary_deployment_id,
        account_identity_fingerprint, boundary_side, boundary_distance_pct,
        entry_basis_price, entry_basis_authority, boundary_price, boundary_type,
        execution_price_guarantee, policy_fingerprint, boundary_effective_at,
        boundary_event_fingerprint,
    ) = boundary
    current = load_current_pre_entry_risk_cursor(
        cur, reservation_id=uuid.UUID(str(reservation_id)),
    )
    if current is None:
        return "PRE_ENTRY_RISK_UNAVAILABLE"
    source_event_identity = f"PAPER_FILL_HANDOFF:{int(fill_id)}"
    cur.execute(
        "SELECT 1 FROM pre_entry_risk_event_v1 WHERE pre_entry_risk_id=%s "
        "AND source_event_identity=%s",
        (str(current.pre_entry_risk_id), source_event_identity),
    )
    if cur.fetchone() is not None:
        return "IDEMPOTENT"
    if (
        str(boundary_state) not in {
            "BOUNDARY_ACTIVATED", "BOUNDARY_REVISED_ENTRY_BASIS"
        }
        or boundary_position_id is None
        or int(boundary_position_id) != int(position_id)
    ):
        return "OPEN_RISK_INCOMPLETE:MISSING_BOUNDARY"
    cur.execute(
        "SELECT state,position_id FROM v_capital_reservation_current_v1 "
        "WHERE reservation_id=%s",
        (str(reservation_id),),
    )
    reservation = cur.fetchone()
    if (
        reservation is None
        or str(reservation[0]) not in {"PARTIALLY_DEPLOYED", "DEPLOYED"}
        or reservation[1] is None
        or int(reservation[1]) != int(position_id)
    ):
        return "OPEN_RISK_INCOMPLETE:MISSING_DEPLOYED_RESERVATION"
    if (
        str(simulation_model_version) != FEE_MODEL_V2
        or str(fee_model_version) != FEE_MODEL_V2
        or str(current.exit_cost_snapshot_or_model_id) != FEE_MODEL_V2
        or simulation_fee_rate is None
        or Decimal(str(simulation_fee_rate)) != current.canonical_exit_fee_rate
    ):
        return "OPEN_RISK_INCOMPLETE:MISSING_COST_AUTHORITY"

    cur.execute(
        "SELECT p.status,p.side,p.remaining_inventory_qty,"
        "p.inventory_evidence_status,p.symbol,p.interval,"
        "p.entry_opportunity_snapshot_id,e.fee_rate_exit_assumption,"
        "e.fee_model_version FROM positions p "
        "LEFT JOIN entry_opportunity_evidence_v1 e "
        "ON e.snapshot_id=p.entry_opportunity_snapshot_id WHERE p.id=%s",
        (int(position_id),),
    )
    position = cur.fetchone()
    if position is None:
        return "OPEN_RISK_INCOMPLETE:INVENTORY_DATA_QUALITY_ERROR"
    (
        position_status, position_side, remaining_inventory_qty,
        inventory_evidence_status, symbol, interval,
        entry_opportunity_snapshot_id, exit_fee_rate, exit_fee_model,
    ) = position
    if (
        str(position_status) != "OPEN"
        or str(inventory_evidence_status) != "COMPLETE"
        or remaining_inventory_qty is None
        or Decimal(str(remaining_inventory_qty)) < quantity
    ):
        return "OPEN_RISK_INCOMPLETE:INVENTORY_DATA_QUALITY_ERROR"
    if (
        entry_opportunity_snapshot_id is None
        or exit_fee_rate is None
        or str(exit_fee_model) != FEE_MODEL_V2
        or Decimal(str(exit_fee_rate)) != current.canonical_exit_fee_rate
    ):
        return "OPEN_RISK_INCOMPLETE:MISSING_COST_AUTHORITY"

    cur.execute(
        "SELECT close,open_time FROM candles WHERE symbol=%s AND interval=%s "
        "AND open_time<=%s ORDER BY open_time DESC LIMIT 1",
        (str(symbol), str(interval), execution_at),
    )
    mark = cur.fetchone()
    if mark is None or mark[0] is None or mark[1] is None:
        mark_price = None
        mark_timestamp = None
        mark_status = "PRICE_UNAVAILABLE"
    else:
        mark_price = Decimal(str(mark[0]))
        mark_timestamp = mark[1]
        mark_status = (
            "PRICE_STALE"
            if mark_timestamp < execution_at - PAPER_OPEN_RISK_MARK_FRESHNESS
            else "CANONICAL"
        )
    projection = RiskBoundaryProjection(
        boundary_id=uuid.UUID(str(boundary_id)),
        position_id=int(boundary_position_id),
        environment=str(boundary_environment),
        deployment_id=str(boundary_deployment_id),
        account_identity_fingerprint=str(account_identity_fingerprint),
        side=str(boundary_side), state=str(boundary_state),
        boundary_distance_pct=Decimal(str(boundary_distance_pct)),
        entry_basis_price=Decimal(str(entry_basis_price)),
        entry_basis_authority=str(entry_basis_authority),
        boundary_price=Decimal(str(boundary_price)),
        boundary_type=str(boundary_type),
        execution_price_guarantee=str(execution_price_guarantee),
        policy_fingerprint=str(policy_fingerprint),
        effective_at=boundary_effective_at,
        exit_fee_rate=current.canonical_exit_fee_rate,
        exit_fee_model=str(current.exit_cost_snapshot_or_model_id),
        exit_fee_status="CANONICAL",
    )
    open_risk = evaluate_position_risk(
        position_id=int(position_id), side=str(position_side),
        remaining_inventory_qty=Decimal(str(remaining_inventory_qty)),
        mark_price=mark_price, mark_status=mark_status,
        projection=projection, require_exit_cost=True,
    )
    if open_risk.status != "CANONICAL":
        return f"OPEN_RISK_INCOMPLETE:{open_risk.status}"
    open_risk_fingerprint = _hash({
        "authority": "POSITION_RISK_BOUNDARY_AUTHORITY_V1_PLUS_"
        "PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
        "boundary_event_fingerprint": str(boundary_event_fingerprint),
        "boundary_id": str(boundary_id),
        "fill_id": int(fill_id),
        "fill_price": Decimal(str(fill_price)),
        "filled_quantity": quantity,
        "mark_price": mark_price,
        "mark_timestamp": mark_timestamp,
        "open_risk_to_trigger": open_risk.open_risk_to_trigger,
        "position_id": int(position_id),
        "remaining_inventory_qty": Decimal(str(remaining_inventory_qty)),
    })
    status, _ = transition_pre_entry_risk_cursor(
        cur, reservation_id=uuid.UUID(str(reservation_id)),
        source_event_identity=source_event_identity,
        effective_at=execution_at, transfer_quantity=quantity,
        open_risk_status="CANONICAL", open_risk_position_id=int(position_id),
        open_risk_boundary_id=uuid.UUID(str(boundary_id)),
        open_risk_evidence_fingerprint=open_risk_fingerprint,
        source_authority="PAPER_CANONICAL_OPEN_RISK_HANDOFF",
        provenance={
            "fill_id": int(fill_id),
            "simulated_order_id": int(simulated_order_id),
            "position_id": int(position_id),
            "filled_quantity": str(quantity),
            "mark_timestamp": (
                mark_timestamp.astimezone(timezone.utc).isoformat()
                if mark_timestamp is not None else None
            ),
        },
    )
    return status


def lock_simulated_exit_slot_cursor(
    cur, *, symbol: str, interval: str, strategy: str
) -> None:
    """Serialize canonical PAPER exit intent/fill activity for one slot.

    The lock is transaction-scoped.  It is shared by normal forward PAPER
    writers and the bounded legacy-retirement writer, avoiding a broad table
    lock while making an exact-slot snapshot stable during re-plan and CAS.
    """
    identity = (
        "PAPER_SIMULATED_EXIT_SLOT_V1:"
        f"{str(symbol).upper()}:{str(interval)}:{str(strategy)}"
    )
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (identity,),
    )


def create_simulated_order_cursor(
    cur,
    *,
    symbol: str,
    interval: str,
    strategy: str,
    side: str,
    price: Decimal,
    quantity: Decimal,
    reason: str,
    candle_open_time,
    is_exit: bool,
    rsi_14: Decimal | None = None,
    ema_21: Decimal | None = None,
    order_class: str = FORWARD_ORDER_CLASS,
    position_id: int | None = None,
    environment: str | None = None,
    deployment_id: str | None = None,
    market_regime: str | None = None,
    regime_source_provenance: dict | None = None,
    failure_injector=None,
) -> int | SimulatedOrderWriteBlocked:
    """Canonical transaction-bound simulated-order writer.

    The caller owns the transaction.  Expected unique conflicts are contained
    by a savepoint and classified by reading the exact namespace identity.
    """
    order_class = str(order_class).upper()
    if order_class not in {FORWARD_ORDER_CLASS, ADMINISTRATIVE_ORDER_CLASS}:
        raise ValueError(f"unsupported simulated order class: {order_class}")
    if order_class == FORWARD_ORDER_CLASS:
        if any(value is not None for value in (position_id, environment, deployment_id)):
            raise ValueError("FORWARD_ORDER_IDENTITY_MUST_BE_NULL")
    else:
        if (
            position_id is None
            or not str(environment or "").strip()
            or not str(deployment_id or "").strip()
            or not is_exit
            or str(side).upper() != "SELL"
            or str(reason) != ADMINISTRATIVE_ORDER_CLASS
        ):
            raise ValueError("ADMINISTRATIVE_ORDER_IDENTITY_INVALID")

    namespace = detect_simulated_order_namespace(cur.connection)
    if not (namespace.is_legacy or namespace.is_namespace_v1):
        raise RuntimeError(
            "SIMULATED_ORDER_NAMESPACE_SCHEMA_INVALID:"
            + ",".join(namespace.issues)
        )
    if order_class == ADMINISTRATIVE_ORDER_CLASS and not namespace.is_namespace_v1:
        raise RuntimeError("SIMULATED_ORDER_NAMESPACE_MIGRATION_REQUIRED")

    forward_decision_id = None
    forward_contract_version = None
    if (
        order_class == FORWARD_ORDER_CLASS
        and not is_exit
        and regime_source_provenance is not None
    ):
        provenance = dict(regime_source_provenance)
        if (
            not str(market_regime or "").strip()
            or provenance.get("regime_attribution_version")
                != "CANONICAL_REGIME_ATTRIBUTION_V1"
            or provenance.get("regime_source") != "market_regime"
            or str(provenance.get("regime_source_symbol", "")).upper()
                != str(symbol).upper()
            or str(provenance.get("regime_source_interval", "")).lower()
                != str(interval).lower()
            or provenance.get("regime_source_ts") is None
        ):
            return SimulatedOrderWriteBlocked(
                "CANONICAL_REGIME_ATTRIBUTION_REQUIRED"
            )
        source_ts = datetime.fromisoformat(
            str(provenance["regime_source_ts"]).replace("Z", "+00:00")
        )
        if source_ts > candle_open_time:
            return SimulatedOrderWriteBlocked(
                "CANONICAL_REGIME_ATTRIBUTION_REQUIRED"
            )
        cur.execute(
            """
            SELECT public.register_forward_entry_decision_v1(
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
            )
            """,
            (
                str(symbol), str(interval), str(strategy), str(side).upper(),
                Decimal(str(price)), Decimal(str(quantity)), str(reason),
                candle_open_time, str(market_regime), Json(provenance),
            ),
        )
        registered = cur.fetchone()
        if registered is None or registered[0] is None:
            raise RuntimeError("FORWARD_DECISION_REGISTRY_REQUIRED")
        forward_decision_id = registered[0]
        forward_contract_version = "CANONICAL_REGIME_ATTRIBUTION_V1"
        if failure_injector is not None:
            failure_injector("AFTER_DECISION_BEFORE_ORDER")

    if is_exit:
        lock_simulated_exit_slot_cursor(
            cur, symbol=symbol, interval=interval, strategy=strategy,
        )
    params = (
        str(symbol), str(interval), str(strategy), str(side).upper(),
        Decimal(str(price)), Decimal(str(quantity)), str(reason),
        None if rsi_14 is None else Decimal(str(rsi_14)),
        None if ema_21 is None else Decimal(str(ema_21)),
        candle_open_time, bool(is_exit),
    )
    if namespace.is_namespace_v1:
        insert_sql = """
            INSERT INTO simulated_orders (
              symbol,interval,strategy,side,price,quantity_btc,reason,
              rsi_14,ema_21,candle_open_time,is_exit,
              order_class,position_id,environment,deployment_id,
              decision_id,decision_contract_version
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """
        insert_params = params + (
            order_class,
            None if position_id is None else int(position_id),
            None if environment is None else str(environment),
            None if deployment_id is None else str(deployment_id),
            forward_decision_id,
            forward_contract_version,
        )
    else:
        insert_sql = """
            INSERT INTO simulated_orders (
              symbol,interval,strategy,side,price,quantity_btc,reason,
              rsi_14,ema_21,candle_open_time,is_exit
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """
        insert_params = params

    savepoint = "simulated_order_namespace_v1_insert"
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        cur.execute(insert_sql, insert_params)
        inserted = cur.fetchone()
    except psycopg2.errors.UniqueViolation as exc:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        if namespace.is_legacy:
            expected_constraints = {
                "sim_orders_uniq_candle_exit",
                "ux_sim_orders_one_per_candle",
                "ux_sim_orders_one_per_candle_isexit",
            }
        elif order_class == FORWARD_ORDER_CLASS:
            expected_constraints = {"ux_sim_orders_forward_one_per_candle"}
        else:
            expected_constraints = {"ux_sim_orders_admin_position"}
        if getattr(exc.diag, "constraint_name", None) not in expected_constraints:
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        if order_class == FORWARD_ORDER_CLASS:
            class_filter = (
                " AND order_class='FORWARD'" if namespace.is_namespace_v1 else ""
            )
            cur.execute(
                """
                SELECT id,side,price,quantity_btc,reason,rsi_14,ema_21,is_exit
                FROM simulated_orders
                WHERE symbol=%s AND interval=%s AND strategy=%s
                  AND candle_open_time=%s
                """ + class_filter + " ORDER BY id LIMIT 2",
                (str(symbol), str(interval), str(strategy), candle_open_time),
            )
            existing = cur.fetchall()
            conflict_status = "PAPER_ORDER_SLOT_ALREADY_OCCUPIED"
            identical_status = "IDEMPOTENT_EXISTING_FORWARD_ORDER"
        else:
            cur.execute(
                """
                SELECT id,side,price,quantity_btc,reason,rsi_14,ema_21,is_exit
                FROM simulated_orders
                WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE'
                  AND environment=%s AND deployment_id=%s AND position_id=%s
                ORDER BY id LIMIT 2
                """,
                (str(environment), str(deployment_id), int(position_id)),
            )
            existing = cur.fetchall()
            conflict_status = "ADMINISTRATIVE_ORDER_IDENTITY_CONFLICT"
            identical_status = "IDEMPOTENT_EXISTING_ADMINISTRATIVE_ORDER"
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        if len(existing) != 1:
            raise
        row = existing[0]
        expected_identity = (
            str(side).upper(), Decimal(str(price)), Decimal(str(quantity)),
            str(reason),
            None if rsi_14 is None else Decimal(str(rsi_14)),
            None if ema_21 is None else Decimal(str(ema_21)),
            bool(is_exit),
        )
        actual_identity = (
            str(row[1]).upper(), Decimal(str(row[2])), Decimal(str(row[3])),
            str(row[4]),
            None if row[5] is None else Decimal(str(row[5])),
            None if row[6] is None else Decimal(str(row[6])),
            bool(row[7]),
        )
        return SimulatedOrderWriteBlocked(
            identical_status if actual_identity == expected_identity else conflict_status,
            int(row[0]),
        )
    else:
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        if inserted is None:
            raise RuntimeError("SIMULATED_ORDER_INSERT_RETURNING_MISSING")
        inserted_order_id = int(inserted[0])
        if failure_injector is not None:
            failure_injector("AFTER_ORDER_BEFORE_COMMITMENT")
        runtime_mode = str(os.getenv("TRADING_MODE", "PAPER")).strip().upper()
        if (
            order_class == FORWARD_ORDER_CLASS
            and not is_exit
            and runtime_mode == "PAPER"
        ):
            commitment_effective_at = datetime.now(timezone.utc)
            paper_deployment = str(
                os.getenv("DEPLOYMENT_ID")
                or os.getenv("WALTRADE_DEPLOYMENT_ID")
                or "local-paper"
            ).strip().lower()
            _reservation_status, reservation_id = accept_paper_simulated_order_cursor(
                cur, simulated_order_id=inserted_order_id,
                deployment_id=paper_deployment, symbol=str(symbol),
                strategy=str(strategy), interval=str(interval),
                requested_notional=(
                    Decimal(str(price)) * Decimal(str(quantity))
                ),
                effective_at=commitment_effective_at,
                decision_identity=(
                    None if forward_decision_id is None
                    else str(forward_decision_id)
                ),
            )
            if reservation_id is not None:
                accept_paper_boundary_cursor(
                    cur, simulated_order_id=inserted_order_id,
                    deployment_id=paper_deployment,
                    decision_id=(
                        str(forward_decision_id) if forward_decision_id is not None
                        else f"SIMULATED_ORDER:{inserted_order_id}"
                    ), symbol=str(symbol), strategy=str(strategy),
                    interval=str(interval),
                    effective_at=commitment_effective_at,
                )
                _risk_status, pre_entry_risk_id = freeze_paper_pre_entry_risk_cursor(
                    cur, simulated_order_id=inserted_order_id,
                    deployment_id=paper_deployment,
                    effective_at=commitment_effective_at,
                )
                if pre_entry_risk_id is not None:
                    from common.risk_budget_runtime import (
                        record_paper_pre_entry_shadow_gate_fail_open_cursor,
                    )
                    shadow = record_paper_pre_entry_shadow_gate_fail_open_cursor(
                        cur, pre_entry_risk_id=pre_entry_risk_id,
                        deployment_id=paper_deployment,
                        as_of=commitment_effective_at,
                        git_revision=os.getenv("GIT_SHA", ""),
                    )
                    logging.info(
                        "risk_budget_shadow_gate status=%s risk_status=%s "
                        "pre_entry_risk_id=%s execution_effect=NONE",
                        shadow.status, _risk_status, pre_entry_risk_id,
                    )
            if failure_injector is not None:
                failure_injector("AFTER_COMMITMENT_BEFORE_FILL")
        if forward_decision_id is not None:
            try:
                snapshot_id = capture_entry_opportunity_snapshot_fail_open_cursor(
                    cur,
                    decision_id=forward_decision_id,
                    simulated_order_id=inserted_order_id,
                    planned_entry_notional=(
                        Decimal(str(price)) * Decimal(str(quantity))
                    ),
                    fee_config=load_paper_simulation_fee_config(),
                )
                if snapshot_id is not None:
                    link_entry_opportunity_order_fail_open_cursor(
                        cur,
                        decision_id=forward_decision_id,
                        simulated_order_id=inserted_order_id,
                    )
            except Exception:
                # Evidence is observational. It must never change the order
                # result returned by the existing writer.
                logging.exception("entry_opportunity_evidence_fail_open")
        return inserted_order_id


def create_simulated_execution_fill_cursor(
    cur,
    *,
    simulated_order_id: int,
    position_id: int,
    order_purpose: str,
    side: str,
    symbol: str,
    quantity: Decimal,
    price: Decimal,
    account_identity_id: int | None,
    instrument_snapshot_id: int | None,
    environment: str,
    deployment_id: str,
    execution_at,
    interval: str | None = None,
    strategy: str | None = None,
    account_identity_fingerprint: str | None = None,
    instrument_metadata_fingerprint: str | None = None,
) -> int | None:
    """Canonical transaction-bound PAPER fill writer and fee policy."""
    purpose = str(order_purpose).upper()
    if purpose not in {"ENTRY", "EXIT"}:
        raise ValueError("INVALID_SIMULATED_ORDER_PURPOSE")
    if purpose == "EXIT":
        if interval is None or strategy is None:
            raise ValueError("EXIT_SLOT_IDENTITY_REQUIRED")
        lock_simulated_exit_slot_cursor(
            cur, symbol=symbol, interval=interval, strategy=strategy,
        )
    quantity = Decimal(str(quantity))
    price = Decimal(str(price))
    if quantity <= 0 or price < 0:
        raise ValueError("INVALID_SIMULATED_EXECUTION_VALUE")
    _base_asset, quote_asset = _assets(symbol)
    notional = quantity * price
    fee_config = _fee_contract_for_fill(
        cur, purpose=purpose, position_id=int(position_id),
    )
    fee_usdc = notional * fee_config.rate
    source_payload = {
        "simulated_order_id": int(simulated_order_id),
        "position_id": int(position_id),
        "purpose": purpose,
        "quantity": str(quantity),
        "price": str(price),
        "fee_usdc": str(fee_usdc),
        "identity": account_identity_fingerprint,
        "instrument": instrument_metadata_fingerprint,
        "environment": str(environment),
        "deployment_id": str(deployment_id),
        "simulation_fee_rate": str(fee_config.rate),
        "fee_model_version": fee_config.model_version,
        "fee_config_source": fee_config.config_source,
    }
    cur.execute(
        """
        INSERT INTO simulated_execution_fills_v1 (
          simulated_order_id,position_id,order_purpose,side,symbol,
          fill_qty,fill_price,fill_notional,fee_qty,fee_asset,
          authoritative_fee_usdc,account_identity_id,instrument_snapshot_id,
          environment,deployment_id,simulation_model_version,
          simulation_fee_rate,fee_model_version,fee_config_source,
          execution_at,source_fingerprint
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (simulated_order_id,fill_index) DO NOTHING
        RETURNING id
        """,
        (
            int(simulated_order_id), int(position_id), purpose,
            str(side).upper(), str(symbol), quantity, price, notional,
            fee_usdc, quote_asset, fee_usdc, account_identity_id,
            instrument_snapshot_id, str(environment), str(deployment_id),
            fee_config.model_version, fee_config.rate,
            fee_config.model_version, fee_config.config_source,
            execution_at, _hash(source_payload),
        ),
    )
    row = cur.fetchone()
    fill_id = int(row[0]) if row is not None else None
    return fill_id


def _fee_contract_for_fill(
    cur,
    *,
    purpose: str,
    position_id: int,
) -> PaperSimulationFeeConfig:
    configured = load_paper_simulation_fee_config()
    if purpose == "ENTRY":
        return configured

    cur.execute(
        """
        SELECT simulation_model_version,simulation_fee_rate,
               fee_model_version,fee_config_source,fill_notional,
               authoritative_fee_usdc
        FROM simulated_execution_fills_v1
        WHERE position_id=%s AND order_purpose='ENTRY'
        ORDER BY execution_at,id
        """,
        (int(position_id),),
    )
    entries = list(cur.fetchall())
    if not entries:
        raise RuntimeError("PAPER_EXIT_ENTRY_FEE_CONTRACT_MISSING")

    contracts: set[tuple[Decimal, str, str]] = set()
    for (
        simulation_model_version,
        stored_rate,
        fee_model_version,
        fee_config_source,
        fill_notional,
        authoritative_fee_usdc,
    ) in entries:
        if stored_rate is None:
            notional = Decimal(str(fill_notional))
            if notional <= 0 or authoritative_fee_usdc is None:
                raise RuntimeError("LEGACY_ENTRY_FEE_CONTRACT_UNRESOLVED")
            rate = Decimal(str(authoritative_fee_usdc)) / notional
            model = str(simulation_model_version)
            source = "FROZEN_LEGACY_ENTRY_EVIDENCE"
        else:
            rate = Decimal(str(stored_rate))
            model = str(fee_model_version or simulation_model_version)
            source = str(fee_config_source or "FROZEN_ENTRY_EVIDENCE")
        contracts.add((rate, model, source))

    if len(contracts) != 1:
        raise RuntimeError("PAPER_ENTRY_FEE_CONTRACT_CONFLICT")
    rate, model, source = next(iter(contracts))
    return PaperSimulationFeeConfig(
        rate=rate,
        model_version=model,
        config_source=source,
    )


def _instrument_values(client, symbol: str, *, allow_remote: bool = True):
    try:
        from common.sizing import _FILTERS_CACHE
        exchange = os.getenv("EXCHANGE", "BINANCE").strip().upper()
        cached = _FILTERS_CACHE.get(f"{exchange}:{symbol}")
        if cached is not None:
            step = Decimal(str(cached.step))
            return (
                step,
                Decimal(str(cached.min_qty)),
                Decimal(str(cached.min_notional)),
                abs(step.normalize().as_tuple().exponent) if step else None,
                None,
            )
        if not allow_remote:
            return None
        info = client.get_symbol_info(symbol)
        filters = {item.get("filterType"): item for item in info.get("filters", [])}
        lot = filters.get("LOT_SIZE") or {}
        notional = filters.get("MIN_NOTIONAL") or {}
        step = Decimal(str(lot.get("stepSize")))
        min_qty = Decimal(str(lot.get("minQty") or 0))
        min_notional = Decimal(str(notional.get("minNotional") or 0))
        raw = info.get("raw") or {}
        quantity_precision = (
            abs(step.normalize().as_tuple().exponent) if step else None
        )
        price_tick = raw.get("tickSz")
        price_precision = (
            abs(Decimal(str(price_tick)).normalize().as_tuple().exponent)
            if price_tick else None
        )
        return step, min_qty, min_notional, quantity_precision, price_precision
    except Exception:
        return None


def record_simulated_fill_evidence(
    connection_factory,
    *,
    client,
    simulated_order_id: int,
    position_id: int | None,
    environment: str,
    deployment_id: str,
    exit_reason: str | None = None,
    require_terminal_close: bool = False,
    connection=None,
    position_market_regime: str | None = None,
    position_entry_time=None,
    require_atomic_entry: bool = False,
    failure_injector=None,
) -> bool | PaperEntryAtomicResult:
    """Atomically persist one PAPER fill and its inventory lifecycle mutation."""
    if str(environment).lower() != "paper":
        return False
    if require_terminal_close and exit_reason is None:
        raise ValueError("TERMINAL_CLOSE_EXIT_REASON_REQUIRED")
    owns_connection = connection is None
    conn = connection if connection is not None else connection_factory()
    try:
        with (conn if owns_connection else nullcontext(conn)):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol,side,price,quantity_btc,is_exit,created_at,
                           "interval",strategy,reason
                    FROM simulated_orders WHERE id=%s
                    """,
                    (int(simulated_order_id),),
                )
                order = cur.fetchone()
                if order is None:
                    return False
                (
                    symbol, side, price, qty, is_exit, execution_at,
                    interval, strategy, order_reason,
                ) = order
                if position_id is None:
                    if is_exit or not require_atomic_entry:
                        raise ValueError("ATOMIC_ENTRY_POSITION_REQUIRED")
                    slot = (
                        "PAPER_FORWARD_ENTRY_ATOMIC_V1:"
                        f"{str(symbol).upper()}:{interval}:{strategy}"
                    )
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (slot,),
                    )
                    if failure_injector is not None:
                        failure_injector("AFTER_ORDER_BEFORE_POSITION")
                    cur.execute(
                        """
                        SELECT p.id,f.id,
                               f.simulation_fee_rate,f.fee_model_version,
                               f.authoritative_fee_usdc,
                               o.entry_opportunity_snapshot_id,
                               p.entry_opportunity_snapshot_id,
                               f.entry_opportunity_snapshot_id
                        FROM positions p
                        LEFT JOIN simulated_execution_fills_v1 f
                          ON f.position_id=p.id
                         AND f.simulated_order_id=%s
                         AND f.order_purpose='ENTRY'
                        JOIN simulated_orders o ON o.id=%s
                        WHERE p.symbol=%s AND p.strategy=%s AND p.interval=%s
                          AND p.status='OPEN'
                        ORDER BY p.entry_time DESC,p.id DESC
                        LIMIT 1
                        FOR UPDATE OF p
                        """,
                        (
                            int(simulated_order_id), int(simulated_order_id),
                            str(symbol), str(strategy), str(interval),
                        ),
                    )
                    open_row = cur.fetchone()
                    if open_row is not None:
                        if open_row[1] is not None:
                            canonical_existing = bool(
                                open_row[2] is not None
                                and open_row[3] is not None
                                and open_row[4] is not None
                                and open_row[5] is not None
                                and open_row[6] == open_row[5]
                                and open_row[7] == open_row[5]
                            )
                            if not canonical_existing:
                                return PaperEntryAtomicResult(
                                    False, "PAPER_EXISTING_ENTRY_INCOMPLETE",
                                    int(simulated_order_id), int(open_row[0]),
                                )
                            return PaperEntryAtomicResult(
                                True, "IDEMPOTENT", int(simulated_order_id),
                                int(open_row[0]),
                            )
                        return PaperEntryAtomicResult(
                            False, "PAPER_POSITION_ALREADY_OPEN",
                            int(simulated_order_id), int(open_row[0]),
                        )
                    cur.execute(
                        """
                        INSERT INTO positions(
                          symbol,strategy,interval,status,side,qty,entry_price,
                          entry_time,entry_client_order_id,market_regime
                        ) VALUES (
                          %s,%s,%s,'OPEN',%s,%s,%s,COALESCE(%s,now()),NULL,%s
                        ) RETURNING id
                        """,
                        (
                            str(symbol), str(strategy), str(interval),
                            "LONG" if str(side).upper() == "BUY" else "SHORT",
                            Decimal(str(qty)), Decimal(str(price)),
                            position_entry_time, position_market_regime,
                        ),
                    )
                    created = cur.fetchone()
                    if created is None:
                        raise RuntimeError("PAPER_ATOMIC_POSITION_INSERT_FAILED")
                    position_id = int(created[0])
                    if failure_injector is not None:
                        failure_injector("AFTER_POSITION_PREPARED")
                cur.execute(
                    """
                    SELECT
                      COALESCE(
                        p.inventory_contract_adoption_id,
                        adoption.adoption_id
                      ),
                      COALESCE(
                        p.inventory_contract_generation,
                        adoption.generation
                      ),
                      adoption.git_revision,
                      CASE
                        WHEN p.inventory_contract_adoption_id=adoption.adoption_id
                         AND p.inventory_contract_generation=adoption.generation
                          THEN 'FORWARD_C2_2'
                        WHEN is_existing_projected_c2_2_compatible(
                          p.id,'paper'
                        ) THEN 'EXISTING_PROJECTED_C2_2'
                        WHEN p.inventory_contract_adoption_id IS NOT NULL
                          THEN 'ADOPTION_GENERATION_MISMATCH'
                        WHEN p.inventory_contract_adoption_id IS NULL
                         AND p.inventory_contract_generation IS NULL
                         AND p.entry_time>=adoption.adopted_at
                          THEN 'FORWARD_C2_2'
                        ELSE 'LEGACY_UNPROJECTED'
                      END
                    FROM positions p
                    JOIN runtime_contract_adoption_v2 adoption
                      ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
                     AND adoption.environment='paper'
                     AND adoption.deployment_id=%s
                     AND adoption.status='ACTIVE'
                    WHERE p.id=%s
                    FOR UPDATE OF p
                    """,
                    (
                        str(deployment_id),
                        int(position_id),
                    ),
                )
                generation_gate = cur.fetchone()
                if generation_gate is None:
                    if require_atomic_entry:
                        raise RuntimeError("PAPER_ATOMIC_GENERATION_GATE_MISSING")
                    return False
                (
                    adoption_id,
                    contract_generation,
                    adoption_git_revision,
                    classification,
                ) = generation_gate
                if classification not in (
                    "FORWARD_C2_2", "EXISTING_PROJECTED_C2_2"
                ):
                    if require_atomic_entry:
                        raise RuntimeError(
                            "PAPER_ATOMIC_GENERATION_GATE_REJECTED:"
                            + str(classification)
                        )
                    return False
                log_runtime_revision_provenance_diagnostic(
                    adoption_id=int(adoption_id),
                    generation=int(contract_generation),
                    adoption_git_revision=str(adoption_git_revision),
                    runtime_git_revision=require_runtime_git_revision(),
                )
                cur.execute(
                    """
                    UPDATE positions
                    SET inventory_contract_adoption_id=%s,
                        inventory_contract_generation=%s
                    WHERE id=%s
                      AND inventory_contract_adoption_id IS NULL
                      AND inventory_contract_generation IS NULL
                    """,
                    (
                        int(adoption_id), int(contract_generation),
                        int(position_id),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO financial_truth_simulated_account_v1 (
                      deployment_id,simulated_account_uid,identity_version
                    ) VALUES (%s,%s,%s)
                    ON CONFLICT (deployment_id) DO UPDATE
                      SET deployment_id=EXCLUDED.deployment_id
                    RETURNING simulated_account_uid
                    """,
                    (
                        str(deployment_id), str(uuid.uuid4()),
                        SIMULATED_IDENTITY_VERSION,
                    ),
                )
                simulated_uid = str(cur.fetchone()[0])
                identity_payload = {
                    "exchange": "SIMULATOR", "uid": simulated_uid,
                    "main_uid": simulated_uid, "scope": "SIMULATED",
                    "source": "SIMULATED_ACCOUNT_LEDGER",
                    "version": SIMULATED_IDENTITY_VERSION,
                }
                identity_fingerprint = _hash(identity_payload)
                cur.execute(
                    """
                    INSERT INTO financial_truth_account_identity_v1 (
                      source_authority,exchange,account_uid,main_account_uid,
                      account_scope,identity_source,identity_version,
                      identity_fingerprint,captured_at
                    ) VALUES (
                      'SIMULATED_EXECUTION','SIMULATOR',%s,%s,'SIMULATED',
                      'SIMULATED_ACCOUNT_LEDGER',%s,%s,clock_timestamp()
                    )
                    ON CONFLICT (identity_fingerprint) DO UPDATE
                      SET identity_fingerprint=EXCLUDED.identity_fingerprint
                    RETURNING id
                    """,
                    (
                        simulated_uid, simulated_uid,
                        SIMULATED_IDENTITY_VERSION, identity_fingerprint,
                    ),
                )
                identity_id = cur.fetchone()[0]
                metadata = _instrument_values(client, symbol, allow_remote=False)
                instrument_id = None
                metadata_fingerprint = None
                base_asset, quote_asset = _assets(symbol)
                if metadata is not None:
                    step, min_qty, min_notional, qty_precision, price_precision = metadata
                    metadata_payload = {
                        "source_authority": "SIMULATED_EXECUTION",
                        "exchange": os.getenv("EXCHANGE", "OKX").upper(),
                        "symbol": symbol, "base_asset": base_asset,
                        "quote_asset": quote_asset, "step_size": str(step),
                        "min_qty": str(min_qty), "min_notional": str(min_notional),
                        "quantity_precision": qty_precision,
                        "price_precision": price_precision,
                        "source": "EXCHANGE_PUBLIC_AT_EXECUTION",
                        "version": INSTRUMENT_METADATA_VERSION,
                    }
                    metadata_fingerprint = _hash(metadata_payload)
                    cur.execute(
                        """
                        INSERT INTO financial_truth_instrument_snapshot_v1 (
                          source_authority,exchange,symbol,base_asset,quote_asset,
                          step_size,min_qty,quantity_precision,price_precision,
                          min_notional,metadata_source,metadata_version,
                          metadata_fingerprint,captured_at
                        ) VALUES (
                          'SIMULATED_EXECUTION',%s,%s,%s,%s,%s,%s,%s,%s,%s,
                          'EXCHANGE_PUBLIC_AT_EXECUTION',%s,%s,clock_timestamp()
                        )
                        ON CONFLICT (metadata_fingerprint) DO UPDATE
                          SET metadata_fingerprint=EXCLUDED.metadata_fingerprint
                        RETURNING id
                        """,
                        (
                            os.getenv("EXCHANGE", "OKX").upper(), symbol,
                            base_asset, quote_asset, step, min_qty, qty_precision,
                            price_precision, min_notional,
                            INSTRUMENT_METADATA_VERSION, metadata_fingerprint,
                        ),
                    )
                    instrument_id = cur.fetchone()[0]
                quantity = Decimal(str(qty))
                fill_price = Decimal(str(price))
                fill_id = create_simulated_execution_fill_cursor(
                    cur,
                    simulated_order_id=int(simulated_order_id),
                    position_id=int(position_id),
                    order_purpose="EXIT" if is_exit else "ENTRY",
                    side=str(side), symbol=str(symbol), quantity=quantity,
                    price=fill_price, account_identity_id=int(identity_id),
                    instrument_snapshot_id=(
                        int(instrument_id) if instrument_id is not None else None
                    ),
                    environment=str(environment), deployment_id=str(deployment_id),
                    execution_at=execution_at,
                    interval=str(interval), strategy=str(strategy),
                    account_identity_fingerprint=identity_fingerprint,
                    instrument_metadata_fingerprint=metadata_fingerprint,
                )
                if fill_id is None:
                    if require_atomic_entry:
                        raise RuntimeError("PAPER_ATOMIC_ENTRY_FILL_NOT_INSERTED")
                    return False
                fill_inserted = True
                if require_atomic_entry and failure_injector is not None:
                    failure_injector("AFTER_ENTRY_FILL")

                cur.execute(
                    """
                    SELECT order_purpose,fill_qty,fee_qty,fee_asset,
                           fill_price,execution_at,simulated_order_id
                    FROM simulated_execution_fills_v1
                    WHERE position_id=%s
                    ORDER BY execution_at,id
                    """,
                    (int(position_id),),
                )
                fill_rows = cur.fetchall()
                entry_fills = []
                exit_fills = []
                latest_exit_price = None
                latest_exit_time = None
                latest_exit_order_id = None
                for (
                    purpose, fill_qty, fee_qty, fee_asset, evidence_price,
                    evidence_time, evidence_order_id,
                ) in fill_rows:
                    item = {
                        "executed_qty": fill_qty,
                        "commission_amount": fee_qty,
                        "commission_asset": fee_asset,
                    }
                    if str(purpose).upper() == "ENTRY":
                        entry_fills.append(item)
                    else:
                        exit_fills.append(item)
                        latest_exit_price = Decimal(str(evidence_price))
                        latest_exit_time = evidence_time
                        latest_exit_order_id = str(evidence_order_id)

                inventory = project_inventory_from_execution_evidence(
                    symbol=str(symbol),
                    entry_fills=entry_fills,
                    exit_fills=exit_fills,
                    quote_asset=quote_asset,
                )
                cur.execute(
                    """
                    SELECT qty,COALESCE(cumulative_exit_executed_qty,0)
                    FROM positions WHERE id=%s FOR UPDATE
                    """,
                    (int(position_id),),
                )
                position = cur.fetchone()
                if position is None:
                    raise RuntimeError("simulated fill position missing")
                previous_qty, previous_high_water = position
                if metadata is None:
                    limits = InstrumentExecutionLimits(
                        None, None, None, None, False
                    )
                else:
                    step, min_qty, min_notional, _, _ = metadata
                    limits = InstrumentExecutionLimits(
                        Decimal(str(step)), Decimal(str(min_qty)),
                        Decimal(str(min_notional)), fill_price, True,
                    )
                mutation = apply_inventory_lifecycle_mutation(
                    cur,
                    position_id=int(position_id),
                    order_id=(
                        latest_exit_order_id
                        or f"simulated-entry-{int(simulated_order_id)}"
                    ),
                    inventory=inventory,
                    limits=limits,
                    previous_remaining_qty=Decimal(str(previous_qty)),
                    previous_exit_high_water=Decimal(str(previous_high_water)),
                    has_exit_evidence=bool(exit_fills),
                    exit_price=latest_exit_price,
                    exit_time=latest_exit_time,
                    execution_source="PAPER_SIMULATED",
                    exit_reason=(
                        str(exit_reason or order_reason)
                        if is_exit else None
                    ),
                )
                if not is_exit:
                    deployment_status = deploy_paper_simulated_fill_cursor(
                        cur, simulated_order_id=int(simulated_order_id),
                        fill_id=fill_id, position_id=int(position_id),
                        deployed_notional=quantity * fill_price,
                        effective_at=execution_at,
                    )
                    boundary_status = activate_boundary_for_position_cursor(
                        cur, position_id=int(position_id),
                        environment=str(environment),
                        deployment_id=str(deployment_id),
                        effective_at=execution_at,
                        source_authority="PAPER_CANONICAL_SIMULATED_ENTRY_FILL",
                    )
                    try:
                        link_entry_opportunity_position_fail_open_cursor(
                            cur,
                            simulated_order_id=int(simulated_order_id),
                            position_id=int(position_id),
                            fill_id=fill_id,
                        )
                    except Exception:
                        logging.exception(
                            "entry_opportunity_position_link_fail_open"
                        )
                    if require_atomic_entry and failure_injector is not None:
                        failure_injector("AFTER_POSITION_LINKAGE")
                    handoff_status = handoff_paper_fill_pre_entry_risk_cursor(
                        cur, fill_id=fill_id, environment=str(environment),
                        deployment_id=str(deployment_id),
                    )
                    if require_atomic_entry:
                        if deployment_status not in {"INSERTED", "IDEMPOTENT"}:
                            raise RuntimeError(
                                "PAPER_ATOMIC_RESERVATION_DEPLOY_FAILED:"
                                + str(deployment_status)
                            )
                        if boundary_status not in {
                            "INSERTED", "IDEMPOTENT", "BOUNDARY_ACTIVATED",
                            "BOUNDARY_REVISED_ENTRY_BASIS",
                        }:
                            raise RuntimeError(
                                "PAPER_ATOMIC_BOUNDARY_ACTIVATION_FAILED:"
                                + str(boundary_status)
                            )
                        if handoff_status not in {"INSERTED", "IDEMPOTENT"}:
                            raise RuntimeError(
                                "PAPER_ATOMIC_RISK_HANDOFF_FAILED:"
                                + str(handoff_status)
                            )
                        cur.execute(
                            """
                            SELECT o.entry_opportunity_snapshot_id,
                                   p.entry_opportunity_snapshot_id,
                                   f.entry_opportunity_snapshot_id
                            FROM simulated_orders o
                            JOIN positions p ON p.id=%s
                            JOIN simulated_execution_fills_v1 f ON f.id=%s
                            WHERE o.id=%s
                            """,
                            (int(position_id), int(fill_id), int(simulated_order_id)),
                        )
                        linkage = cur.fetchone()
                        if (
                            linkage is None
                            or linkage[0] is None
                            or linkage[1] != linkage[0]
                            or linkage[2] != linkage[0]
                        ):
                            raise RuntimeError(
                                "PAPER_ATOMIC_OPPORTUNITY_LINKAGE_INCOMPLETE"
                            )
                    if handoff_status not in {"INSERTED", "IDEMPOTENT"}:
                        logging.warning(
                            "paper pre-entry risk handoff status=%s fill_id=%s "
                            "position_id=%s",
                            handoff_status, fill_id, position_id,
                        )
                if (
                    require_terminal_close
                    and (not is_exit or mutation.position_status != "CLOSED")
                ):
                    raise RuntimeError(
                        "SIMULATED_EXIT_TERMINAL_CLOSE_NOT_COMMITTED"
                    )
                if require_terminal_close:
                    FinancialTruthReconciler(
                        connection_factory
                    ).reconcile_in_transaction(
                        int(position_id),
                        connection=conn,
                        cursor=cur,
                        evidence_context=ExecutionEvidenceContext(
                            environment=str(environment),
                            exchange=None,
                            deployment_id=str(deployment_id),
                        ),
                        invocation_identity=(
                            "PAPER_SIMULATED_EXIT:"
                            + str(int(simulated_order_id))
                        ),
                    )
                if require_atomic_entry:
                    return PaperEntryAtomicResult(
                        bool(fill_inserted), "INSERTED",
                        int(simulated_order_id), int(position_id),
                    )
                return fill_inserted
    finally:
        if owns_connection:
            conn.close()


def record_forward_paper_entry_atomic(
    connection_factory,
    *,
    client,
    symbol: str,
    interval: str,
    strategy: str,
    side: str,
    price: Decimal,
    quantity: Decimal,
    reason: str,
    candle_open_time,
    deployment_id: str,
    market_regime: str | None,
    regime_source_provenance: dict | None,
    rsi_14: Decimal | None = None,
    ema_21: Decimal | None = None,
    position_entry_time=None,
    failure_injector=None,
) -> PaperEntryAtomicResult:
    """Commit a complete forward PAPER entry, or commit none of it.

    The decision, order, accepted commitments, position, fill, fee contract,
    inventory, opportunity linkage, boundary, and risk handoff all use one
    caller-owned PostgreSQL transaction.  The canonical order identity makes a
    retry return the one already-complete entry instead of duplicating it.
    """
    conn = connection_factory()
    try:
        try:
            with conn:
                with conn.cursor() as cur:
                    written = create_simulated_order_cursor(
                        cur,
                        symbol=str(symbol), interval=str(interval),
                        strategy=str(strategy), side=str(side),
                        price=Decimal(str(price)),
                        quantity=Decimal(str(quantity)), reason=str(reason),
                        candle_open_time=candle_open_time, is_exit=False,
                        rsi_14=(
                            None if rsi_14 is None else Decimal(str(rsi_14))
                        ),
                        ema_21=(
                            None if ema_21 is None else Decimal(str(ema_21))
                        ),
                        market_regime=market_regime,
                        regime_source_provenance=(
                            None if regime_source_provenance is None
                            else dict(regime_source_provenance)
                        ),
                        failure_injector=failure_injector,
                    )
                if isinstance(written, SimulatedOrderWriteBlocked):
                    if (
                        written.status != "IDEMPOTENT_EXISTING_FORWARD_ORDER"
                        or written.existing_order_id is None
                    ):
                        raise _PaperEntryAtomicBlocked(PaperEntryAtomicResult(
                            False, written.status, written.existing_order_id, None,
                        ))
                    simulated_order_id = int(written.existing_order_id)
                else:
                    simulated_order_id = int(written)

                result = record_simulated_fill_evidence(
                    connection_factory,
                    client=client,
                    simulated_order_id=simulated_order_id,
                    position_id=None,
                    environment="paper",
                    deployment_id=str(deployment_id),
                    connection=conn,
                    position_market_regime=market_regime,
                    position_entry_time=position_entry_time,
                    require_atomic_entry=True,
                    failure_injector=failure_injector,
                )
                if not isinstance(result, PaperEntryAtomicResult) or not result:
                    blocked = (
                        result if isinstance(result, PaperEntryAtomicResult)
                        else PaperEntryAtomicResult(
                            False, "PAPER_ATOMIC_ENTRY_EVIDENCE_FAILED",
                            simulated_order_id, None,
                        )
                    )
                    raise _PaperEntryAtomicBlocked(blocked)
                if failure_injector is not None:
                    failure_injector("BEFORE_ENTRY_COMMIT")
                return result
        except _PaperEntryAtomicBlocked as exc:
            return exc.result
    finally:
        conn.close()
