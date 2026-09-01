from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
import json
import logging
from typing import Any

from common.db import get_db_conn
from common.simulated_execution_evidence import (
    PaperRealizableNetEvidence,
    load_paper_realizable_net_evidence,
)


CONTRACT_VERSION = "ECONOMIC_FLOOR_AFTER_COST_COVER_V1_SHADOW"
ARM_EVENT = "ECONOMIC_FLOOR_SHADOW_ARMED"
OBSERVATION_EVENT = "ECONOMIC_FLOOR_SHADOW_OBSERVED"
FINAL_EVENT = "ECONOMIC_FLOOR_SHADOW_FINAL"
ZERO = Decimal("0")


@dataclass(frozen=True)
class ShadowState:
    armed: bool
    first_armed_at: datetime | None = None
    first_armed_realizable_net: Decimal | None = None
    peak_realizable_net_after_arming: Decimal | None = None
    returned_to_zero_or_negative: bool = False


@dataclass(frozen=True)
class ShadowDecision:
    status: str
    event_type: str | None
    decision: str
    state: ShadowState


def evaluate_shadow_transition(
    state: ShadowState,
    *,
    evidence: PaperRealizableNetEvidence,
) -> ShadowDecision:
    """Pure shadow reducer. Its result is telemetry and never exit authority."""
    if not evidence.authoritative or evidence.realizable_net_after_all_costs is None:
        return ShadowDecision("NO_ARM_MISSING_AUTHORITY", None, "NO_SHADOW_DECISION", state)
    current = evidence.realizable_net_after_all_costs
    if not state.armed:
        if current < ZERO:
            return ShadowDecision("NOT_COST_COVERED", None, "NOT_ARMED", state)
        armed = ShadowState(
            armed=True,
            first_armed_at=evidence.observed_at,
            first_armed_realizable_net=current,
            peak_realizable_net_after_arming=current,
            returned_to_zero_or_negative=False,
        )
        return ShadowDecision("ARMED_NOW", ARM_EVENT, "ARMED_NO_EXIT", armed)

    peak = max(state.peak_realizable_net_after_arming or current, current)
    returned = state.returned_to_zero_or_negative or current <= ZERO
    updated = ShadowState(
        armed=True,
        first_armed_at=state.first_armed_at,
        first_armed_realizable_net=state.first_armed_realizable_net,
        peak_realizable_net_after_arming=peak,
        returned_to_zero_or_negative=returned,
    )
    return ShadowDecision(
        "OBSERVED", OBSERVATION_EVENT,
        "WOULD_EXIT_AT_ECONOMIC_FLOOR" if current <= ZERO else "HOLD_UPSIDE_OPEN",
        updated,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _payload(
    evidence: PaperRealizableNetEvidence,
    transition: ShadowDecision,
    *,
    existing_exit_decision: str,
    existing_exit_reason: str,
) -> dict[str, Any]:
    state = transition.state
    return {
        "contract_version": CONTRACT_VERSION,
        "active_exit_influence": "OFF",
        "position_id": evidence.position_id,
        "symbol": evidence.symbol,
        "interval": evidence.interval,
        "strategy": evidence.strategy,
        "entry_fill_ids": list(evidence.entry_fill_ids),
        "fee_contract_fingerprint": evidence.fee_contract_fingerprint,
        "exit_fee_rate": evidence.exit_fee_rate,
        "source_mark_timestamp": evidence.observed_at,
        "source_candle_id_or_equivalent_canonical_id": evidence.source_candle_id,
        "mark_price": evidence.mark_price,
        "hypothetical_exit_notional": evidence.hypothetical_exit_notional,
        "hypothetical_exit_fee": evidence.hypothetical_exit_fee,
        "realizable_net_after_all_costs": evidence.realizable_net_after_all_costs,
        "economic_floor_armed": state.armed,
        "first_armed_at": state.first_armed_at,
        "first_armed_realizable_net": state.first_armed_realizable_net,
        "peak_realizable_net_after_arming": state.peak_realizable_net_after_arming,
        "current_realizable_net": evidence.realizable_net_after_all_costs,
        "returned_to_zero_or_negative_after_arming": state.returned_to_zero_or_negative,
        "existing_exit_decision": existing_exit_decision,
        "existing_exit_reason": existing_exit_reason,
        "shadow_economic_floor_decision": transition.decision,
        "market_data_complete": evidence.market_data_complete,
        "ordering_evidence_available": evidence.ordering_evidence_available,
    }


def _state_from_arm_and_latest(arm: dict[str, Any], latest: dict[str, Any] | None) -> ShadowState:
    source = latest or arm
    return ShadowState(
        armed=True,
        first_armed_at=datetime.fromisoformat(str(arm["first_armed_at"])),
        first_armed_realizable_net=Decimal(str(arm["first_armed_realizable_net"])),
        peak_realizable_net_after_arming=Decimal(str(source["peak_realizable_net_after_arming"])),
        returned_to_zero_or_negative=bool(source.get("returned_to_zero_or_negative_after_arming")),
    )


def observe_economic_floor_shadow(
    *,
    trading_mode: str,
    position_id: int,
    symbol: str,
    interval: str,
    strategy: str,
    current_price: Decimal,
    observed_at: datetime,
    source_candle_id: str,
    existing_exit_decision: str = "CONTINUE_EXISTING_EXIT_EVALUATION",
    existing_exit_reason: str = "NO_EXIT_SELECTED_YET",
    connection_factory=get_db_conn,
) -> ShadowDecision:
    """Append causal PAPER shadow evidence, failing open to existing trading."""
    if str(trading_mode).upper() != "PAPER":
        return ShadowDecision("NOT_APPLICABLE_NON_PAPER", None, "NO_SHADOW_DECISION", ShadowState(False))
    try:
        evidence = load_paper_realizable_net_evidence(
            connection_factory, trading_mode=trading_mode, position_id=position_id,
            symbol=symbol, interval=interval, strategy=strategy,
            current_price=current_price, observed_at=observed_at,
            source_candle_id=source_candle_id,
        )
        if not evidence.authoritative:
            return evaluate_shadow_transition(ShadowState(False), evidence=evidence)
        conn = connection_factory()
        try:
            with conn.cursor() as cur:
                lock_key = f"{CONTRACT_VERSION}|{int(position_id)}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (lock_key,))
                cur.fetchone()
                cur.execute(
                    """
                    SELECT info FROM strategy_events
                    WHERE event_type=%s AND info->>'position_id'=%s
                    ORDER BY id LIMIT 1
                    """,
                    (ARM_EVENT, str(int(position_id))),
                )
                arm_row = cur.fetchone()
                cur.execute(
                    """
                    SELECT info FROM strategy_events
                    WHERE event_type=%s AND info->>'position_id'=%s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (OBSERVATION_EVENT, str(int(position_id))),
                )
                latest_row = cur.fetchone()
                if arm_row and str(dict(arm_row[0]).get(
                    "source_candle_id_or_equivalent_canonical_id"
                )) == str(source_candle_id):
                    state = _state_from_arm_and_latest(
                        dict(arm_row[0]), dict(latest_row[0]) if latest_row else None,
                    )
                    conn.rollback()
                    return ShadowDecision(
                        "IDEMPOTENT_RETRY", None, "ARMED_NO_EXIT", state,
                    )
                state = (
                    _state_from_arm_and_latest(dict(arm_row[0]), dict(latest_row[0]) if latest_row else None)
                    if arm_row else ShadowState(False)
                )
                transition = evaluate_shadow_transition(state, evidence=evidence)
                if transition.event_type is None:
                    conn.rollback()
                    return transition
                payload = _payload(
                    evidence, transition,
                    existing_exit_decision=existing_exit_decision,
                    existing_exit_reason=existing_exit_reason,
                )
                if transition.event_type == OBSERVATION_EVENT:
                    cur.execute(
                        """
                        SELECT 1 FROM strategy_events
                        WHERE event_type=%s AND info->>'position_id'=%s
                          AND info->>'source_candle_id_or_equivalent_canonical_id'=%s
                        LIMIT 1
                        """,
                        (OBSERVATION_EVENT, str(int(position_id)), str(source_candle_id)),
                    )
                    if cur.fetchone():
                        conn.rollback()
                        return ShadowDecision("IDEMPOTENT_RETRY", None, transition.decision, transition.state)
                cur.execute(
                    """
                    INSERT INTO strategy_events
                    (symbol,interval,strategy,event_type,decision,reason,price,candle_open_time,info)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        symbol, interval, strategy.upper(), transition.event_type,
                        "HOLD", transition.status, float(current_price), observed_at,
                        json.dumps(payload, default=_json_default),
                    ),
                )
            conn.commit()
            return transition
        finally:
            conn.close()
    except Exception:
        logging.exception("economic floor shadow observation failed; existing exits unchanged")
        return ShadowDecision("SHADOW_EVIDENCE_FAILURE", None, "NO_SHADOW_DECISION", ShadowState(False))


def reconcile_economic_floor_shadow_closures(
    *, trading_mode: str, connection_factory=get_db_conn,
) -> int:
    """Link armed positions to immutable COMPLETE FT after their normal close."""
    if str(trading_mode).upper() != "PAPER":
        return 0
    conn = None
    try:
        conn = connection_factory()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT (arm.info->>'position_id')::bigint,
                       arm.symbol,arm.interval,arm.strategy,
                       p.exit_reason,ft.financial_truth_status,
                       ft.authoritative_net_pnl
                FROM strategy_events arm
                JOIN positions p ON p.id=(arm.info->>'position_id')::bigint
                JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
                WHERE arm.event_type=%s
                  AND p.status<>'OPEN'
                  AND ft.financial_truth_status='COMPLETE'
                  AND NOT EXISTS (
                    SELECT 1 FROM strategy_events done
                    WHERE done.event_type=%s
                      AND done.info->>'position_id'=arm.info->>'position_id'
                  )
                """,
                (ARM_EVENT, FINAL_EVENT),
            )
            rows = cur.fetchall()
            for position_id, symbol, interval, strategy, exit_reason, ft_status, final_net in rows:
                lock_key = f"{CONTRACT_VERSION}|final|{int(position_id)}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (lock_key,))
                cur.fetchone()
                cur.execute(
                    "SELECT 1 FROM strategy_events WHERE event_type=%s "
                    "AND info->>'position_id'=%s LIMIT 1",
                    (FINAL_EVENT, str(int(position_id))),
                )
                if cur.fetchone():
                    continue
                payload = {
                    "contract_version": CONTRACT_VERSION,
                    "active_exit_influence": "OFF",
                    "position_id": int(position_id),
                    "final_position_close_id": int(position_id),
                    "existing_exit_decision": "EXIT_EXECUTED_BY_EXISTING_AUTHORITY",
                    "existing_exit_reason": exit_reason,
                    "final_financial_truth_status": ft_status,
                    "final_net_pnl_after_fees": final_net,
                }
                cur.execute(
                    """
                    INSERT INTO strategy_events
                    (symbol,interval,strategy,event_type,decision,reason,info)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (symbol, interval, strategy, FINAL_EVENT, "OBSERVED", exit_reason,
                     json.dumps(payload, default=_json_default)),
                )
        conn.commit()
        return len(rows)
    except Exception:
        if conn is not None:
            conn.rollback()
        logging.exception("economic floor shadow FT reconciliation failed")
        return 0
    finally:
        if conn is not None:
            conn.close()
