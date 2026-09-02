from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import logging
import os
from typing import Any

from common.db import get_db_conn
from common.simulated_execution_evidence import (
    PaperRealizableNetEvidence,
    load_paper_realizable_net_evidence,
    lock_simulated_exit_slot_cursor,
)


CONTRACT_VERSION = "ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION"
MODE_ENV = "ECONOMIC_FLOOR_V2_MODE"
ACTIVE_VERSION_ENV = "ACTIVE_ECONOMIC_FLOOR_VERSION"
COMMON_CADENCE_SECONDS = 60
MAX_CANONICAL_MARK_AGE = timedelta(seconds=120)

V2_ARM_EVENT = "ECONOMIC_FLOOR_V2_ARMED"
V2_OBSERVATION_EVENT = "ECONOMIC_FLOOR_V2_OBSERVED"
V2_ACTIVE_EXIT_INTENT_EVENT = "ECONOMIC_FLOOR_V2_ACTIVE_EXIT_INTENT"
V2_FINAL_EVENT = "ECONOMIC_FLOOR_V2_FINAL"
V2_ACTIVE_EXIT_REASON = "ECONOMIC_FLOOR_V2_CADENCE_INDEPENDENT_PROTECTION"
V2_EVENTS = (
    V2_ARM_EVENT,
    V2_OBSERVATION_EVENT,
    V2_ACTIVE_EXIT_INTENT_EVENT,
)
ZERO = Decimal("0")


@dataclass(frozen=True)
class CanonicalOneMinuteMark:
    status: str
    symbol: str
    evaluated_at: datetime
    candle_id: int | None = None
    close_time: datetime | None = None
    price: Decimal | None = None
    source_id: str | None = None

    @property
    def authoritative(self) -> bool:
        return self.status == "AUTHORITATIVE"


@dataclass(frozen=True)
class V2State:
    armed: bool
    first_armed_at: datetime | None = None
    first_armed_realizable_net: Decimal | None = None
    first_arm_source_id: str | None = None
    peak_realizable_net_after_arming: Decimal | None = None


@dataclass(frozen=True)
class V2Decision:
    status: str
    position_id: int
    state: V2State
    event_type: str | None = None
    exit_requested: bool = False
    exit_reason: str | None = None
    mark_price: Decimal | None = None
    observed_at: datetime | None = None
    source_id: str | None = None
    realizable_net: Decimal | None = None


def economic_floor_v2_active(
    trading_mode: str, environ: dict[str, str] | None = None,
) -> bool:
    values = os.environ if environ is None else environ
    return (
        str(trading_mode).upper() == "PAPER"
        and str(values.get(ACTIVE_VERSION_ENV, "")).strip().upper() == "V2"
        and str(values.get(MODE_ENV, "OFF")).strip().upper() == "TREATMENT"
    )


def classify_canonical_one_minute_mark(
    *,
    symbol: str,
    evaluated_at: datetime,
    row,
    max_age: timedelta = MAX_CANONICAL_MARK_AGE,
) -> CanonicalOneMinuteMark:
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)
    if row is None:
        return CanonicalOneMinuteMark("MISSING_FINALIZED_1M", symbol, evaluated_at)
    candle_id, close, close_time = row
    if close_time is None or close is None or candle_id is None:
        return CanonicalOneMinuteMark("INCOMPLETE_1M_SOURCE", symbol, evaluated_at)
    if close_time.tzinfo is None or close_time.utcoffset() is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    price = Decimal(str(close))
    if close_time > evaluated_at:
        return CanonicalOneMinuteMark(
            "UNCLOSED_1M_SOURCE", symbol, evaluated_at,
            candle_id=int(candle_id), close_time=close_time, price=price,
        )
    if evaluated_at - close_time > max_age:
        return CanonicalOneMinuteMark(
            "STALE_1M_SOURCE", symbol, evaluated_at,
            candle_id=int(candle_id), close_time=close_time, price=price,
        )
    if not price.is_finite() or price <= 0:
        return CanonicalOneMinuteMark(
            "INVALID_1M_PRICE", symbol, evaluated_at,
            candle_id=int(candle_id), close_time=close_time, price=price,
        )
    source_id = f"candles:{int(candle_id)}:close:{close_time.isoformat()}"
    return CanonicalOneMinuteMark(
        "AUTHORITATIVE", symbol, evaluated_at,
        candle_id=int(candle_id), close_time=close_time,
        price=price, source_id=source_id,
    )


def load_latest_finalized_canonical_one_minute_mark(
    cur, *, symbol: str, evaluated_at: datetime,
) -> CanonicalOneMinuteMark:
    cur.execute(
        """
        SELECT id,close,close_time
        FROM candles
        WHERE symbol=%s AND interval='1m' AND close_time<=%s
        ORDER BY close_time DESC,id DESC
        LIMIT 1
        """,
        (str(symbol), evaluated_at),
    )
    return classify_canonical_one_minute_mark(
        symbol=str(symbol), evaluated_at=evaluated_at, row=cur.fetchone(),
    )


def evaluate_v2_transition(
    state: V2State,
    *,
    evidence: PaperRealizableNetEvidence,
    source_already_evaluated: bool = False,
    existing_exit_committed: bool = False,
    intent_exists: bool = False,
) -> V2Decision:
    base = {
        "position_id": int(evidence.position_id),
        "state": state,
        "mark_price": evidence.mark_price,
        "observed_at": evidence.observed_at,
        "source_id": evidence.source_candle_id,
        "realizable_net": evidence.realizable_net_after_all_costs,
    }
    if source_already_evaluated:
        return V2Decision("IDEMPOTENT_SOURCE_ALREADY_EVALUATED", **base)
    if existing_exit_committed or evidence.status == "INCOMPLETE:EXISTING_EXIT_COMMITTED":
        return V2Decision("EXISTING_EXIT_PRECEDENCE", **base)
    if not evidence.authoritative or evidence.realizable_net_after_all_costs is None:
        return V2Decision("FAIL_CLOSED_MISSING_AUTHORITY", **base)

    current = evidence.realizable_net_after_all_costs
    if not state.armed:
        if current < ZERO:
            return V2Decision(
                "NOT_COST_COVERED", event_type=V2_OBSERVATION_EVENT, **base,
            )
        armed = V2State(
            armed=True,
            first_armed_at=evidence.observed_at,
            first_armed_realizable_net=current,
            first_arm_source_id=evidence.source_candle_id,
            peak_realizable_net_after_arming=current,
        )
        return V2Decision(
            "ARMED_NOW_NO_SAME_SOURCE_EXIT", position_id=int(evidence.position_id),
            state=armed, event_type=V2_ARM_EVENT, mark_price=evidence.mark_price,
            observed_at=evidence.observed_at, source_id=evidence.source_candle_id,
            realizable_net=current,
        )

    peak = max(state.peak_realizable_net_after_arming or current, current)
    updated = V2State(
        armed=True,
        first_armed_at=state.first_armed_at,
        first_armed_realizable_net=state.first_armed_realizable_net,
        first_arm_source_id=state.first_arm_source_id,
        peak_realizable_net_after_arming=peak,
    )
    updated_base = {**base, "state": updated}
    if str(state.first_arm_source_id) == str(evidence.source_candle_id):
        return V2Decision("FIRST_ARM_SOURCE_NO_EXIT", **updated_base)
    if current > ZERO:
        return V2Decision(
            "ARMED_UPSIDE_OPEN", event_type=V2_OBSERVATION_EVENT, **updated_base,
        )
    if intent_exists:
        return V2Decision("IDEMPOTENT_EXIT_INTENT_EXISTS", **updated_base)
    return V2Decision(
        "V2_EXIT_CLAIMED", event_type=V2_ACTIVE_EXIT_INTENT_EVENT,
        exit_requested=True, exit_reason=V2_ACTIVE_EXIT_REASON, **updated_base,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _state_from_payload(payload: dict[str, Any]) -> V2State:
    first_armed_at = payload.get("first_armed_at")
    return V2State(
        armed=bool(first_armed_at),
        first_armed_at=(
            datetime.fromisoformat(str(first_armed_at)) if first_armed_at else None
        ),
        first_armed_realizable_net=(
            Decimal(str(payload["realizable_net_at_arm"]))
            if payload.get("realizable_net_at_arm") is not None else None
        ),
        first_arm_source_id=payload.get("first_arm_source_id"),
        peak_realizable_net_after_arming=(
            Decimal(str(payload["peak_realizable_net_after_arming"]))
            if payload.get("peak_realizable_net_after_arming") is not None else None
        ),
    )


def _payload(
    *, evidence: PaperRealizableNetEvidence, decision: V2Decision,
) -> dict[str, Any]:
    state = decision.state
    return {
        "contract_version": CONTRACT_VERSION,
        "economic_floor_mode": "V2_TREATMENT",
        "active_exit_influence": "ON_LOCAL_PAPER_ONLY",
        "position_id": int(evidence.position_id),
        "symbol": str(evidence.symbol),
        "originating_interval": str(evidence.interval),
        "strategy": str(evidence.strategy).upper(),
        "common_cadence_seconds": COMMON_CADENCE_SECONDS,
        "canonical_mark_interval": "1m",
        "source_mark_timestamp": evidence.observed_at,
        "source_candle_id_or_equivalent_canonical_id": evidence.source_candle_id,
        "entry_fill_ids": list(evidence.entry_fill_ids),
        "fee_contract_fingerprint": evidence.fee_contract_fingerprint,
        "exit_fee_rate": evidence.exit_fee_rate,
        "mark_price": evidence.mark_price,
        "hypothetical_exit_notional": evidence.hypothetical_exit_notional,
        "hypothetical_exit_fee": evidence.hypothetical_exit_fee,
        "realizable_net_after_all_costs": evidence.realizable_net_after_all_costs,
        "first_armed_at": state.first_armed_at,
        "realizable_net_at_arm": state.first_armed_realizable_net,
        "first_arm_source_id": state.first_arm_source_id,
        "peak_realizable_net_after_arming": state.peak_realizable_net_after_arming,
        "realizable_net_at_floor_exit": (
            evidence.realizable_net_after_all_costs if decision.exit_requested else None
        ),
        "economic_floor_exit_at": (
            evidence.observed_at if decision.exit_requested else None
        ),
        "existing_exit_decision_at_same_evaluation": "NONE_COMMITTED",
        "market_data_complete": evidence.market_data_complete,
        "ordering_evidence_available": evidence.ordering_evidence_available,
        "status": decision.status,
    }


def evaluate_economic_floor_v2_owner_cycle(
    *,
    trading_mode: str,
    position_id: int,
    symbol: str,
    interval: str,
    strategy: str,
    evaluated_at: datetime | None = None,
    connection_factory=get_db_conn,
    environ: dict[str, str] | None = None,
) -> V2Decision:
    empty = V2State(False)
    if not economic_floor_v2_active(trading_mode, environ):
        return V2Decision("INACTIVE", int(position_id), empty)
    evaluated_at = evaluated_at or datetime.now(timezone.utc)
    conn = None
    try:
        conn = connection_factory()
        with conn.cursor() as cur:
            lock_key = f"{CONTRACT_VERSION}|position|{int(position_id)}"
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (lock_key,),
            )
            cur.fetchone()
            lock_simulated_exit_slot_cursor(
                cur, symbol=symbol, interval=interval, strategy=strategy,
            )
            mark = load_latest_finalized_canonical_one_minute_mark(
                cur, symbol=symbol, evaluated_at=evaluated_at,
            )
            if not mark.authoritative:
                conn.rollback()
                return V2Decision(mark.status, int(position_id), empty)

            cur.execute(
                """
                SELECT 1 FROM strategy_events
                WHERE event_type=ANY(%s)
                  AND info->>'position_id'=%s
                  AND info->>'source_candle_id_or_equivalent_canonical_id'=%s
                LIMIT 1
                """,
                (list(V2_EVENTS), str(int(position_id)), str(mark.source_id)),
            )
            source_seen = cur.fetchone() is not None
            cur.execute(
                """
                SELECT info FROM strategy_events
                WHERE event_type=%s AND info->>'position_id'=%s
                ORDER BY id LIMIT 1
                """,
                (V2_ARM_EVENT, str(int(position_id))),
            )
            arm_row = cur.fetchone()
            cur.execute(
                """
                SELECT info FROM strategy_events
                WHERE event_type=ANY(%s) AND info->>'position_id'=%s
                ORDER BY id DESC LIMIT 1
                """,
                (
                    [V2_ARM_EVENT, V2_OBSERVATION_EVENT,
                     V2_ACTIVE_EXIT_INTENT_EVENT],
                    str(int(position_id)),
                ),
            )
            latest_row = cur.fetchone()
            cur.execute(
                """
                SELECT 1 FROM strategy_events
                WHERE event_type=%s AND info->>'position_id'=%s LIMIT 1
                """,
                (V2_ACTIVE_EXIT_INTENT_EVENT, str(int(position_id))),
            )
            intent_exists = cur.fetchone() is not None

            state = (
                _state_from_payload(dict(latest_row[0] if latest_row else arm_row[0]))
                if arm_row else empty
            )
            evidence = load_paper_realizable_net_evidence(
                connection_factory,
                trading_mode=trading_mode,
                position_id=int(position_id), symbol=symbol, interval=interval,
                strategy=strategy, current_price=mark.price,
                observed_at=mark.close_time, source_candle_id=str(mark.source_id),
                connection=conn,
            )
            decision = evaluate_v2_transition(
                state, evidence=evidence,
                source_already_evaluated=source_seen,
                existing_exit_committed=(
                    evidence.status == "INCOMPLETE:EXISTING_EXIT_COMMITTED"
                ),
                intent_exists=intent_exists,
            )
            if decision.event_type is None:
                conn.rollback()
                return decision

            payload = _payload(evidence=evidence, decision=decision)
            cur.execute(
                """
                INSERT INTO strategy_events
                (symbol,interval,strategy,event_type,decision,reason,price,
                 candle_open_time,info)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    symbol, interval, strategy.upper(), decision.event_type,
                    "EXIT" if decision.exit_requested else "HOLD",
                    decision.exit_reason or decision.status,
                    float(mark.price), mark.close_time,
                    json.dumps(payload, default=_json_default),
                ),
            )
        conn.commit()
        return decision
    except Exception:
        if conn is not None:
            conn.rollback()
        logging.exception("economic floor V2 owner-cycle gate failed closed")
        return V2Decision("V2_GATE_FAILURE_FAIL_CLOSED", int(position_id), empty)
    finally:
        if conn is not None:
            conn.close()


def reconcile_economic_floor_v2_closures(
    *, trading_mode: str, connection_factory=get_db_conn,
    environ: dict[str, str] | None = None,
) -> int:
    if not economic_floor_v2_active(trading_mode, environ):
        return 0
    conn = None
    try:
        conn = connection_factory()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT (arm.info->>'position_id')::bigint,
                       arm.symbol,arm.interval,arm.strategy,p.exit_reason,
                       ft.financial_truth_status,ft.authoritative_net_pnl
                FROM strategy_events arm
                JOIN positions p ON p.id=(arm.info->>'position_id')::bigint
                JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
                WHERE arm.event_type=%s AND p.status<>'OPEN'
                  AND ft.financial_truth_status='COMPLETE'
                  AND NOT EXISTS (
                    SELECT 1 FROM strategy_events done
                    WHERE done.event_type=%s
                      AND done.info->>'position_id'=arm.info->>'position_id'
                  )
                """,
                (V2_ARM_EVENT, V2_FINAL_EVENT),
            )
            rows = cur.fetchall()
            for position_id, symbol, interval, strategy, exit_reason, ft_status, final_net in rows:
                lock_key = f"{CONTRACT_VERSION}|final|{int(position_id)}"
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (lock_key,),
                )
                cur.fetchone()
                cur.execute(
                    "SELECT 1 FROM strategy_events WHERE event_type=%s "
                    "AND info->>'position_id'=%s LIMIT 1",
                    (V2_FINAL_EVENT, str(int(position_id))),
                )
                if cur.fetchone():
                    continue
                cur.execute(
                    """
                    SELECT info FROM strategy_events
                    WHERE event_type=%s AND info->>'position_id'=%s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (V2_ACTIVE_EXIT_INTENT_EVENT, str(int(position_id))),
                )
                intent_row = cur.fetchone()
                intent = dict(intent_row[0]) if intent_row else {}
                payload = {
                    "contract_version": CONTRACT_VERSION,
                    "position_id": int(position_id),
                    "first_armed_at": intent.get("first_armed_at"),
                    "realizable_net_at_arm": intent.get("realizable_net_at_arm"),
                    "peak_realizable_net_after_arming": intent.get(
                        "peak_realizable_net_after_arming"
                    ),
                    "realizable_net_at_floor_exit": intent.get(
                        "realizable_net_at_floor_exit"
                    ),
                    "economic_floor_exit_at": intent.get("economic_floor_exit_at"),
                    "existing_exit_decision_at_same_evaluation": intent.get(
                        "existing_exit_decision_at_same_evaluation"
                    ),
                    "actual_exit_reason": exit_reason,
                    "final_financial_truth_status": ft_status,
                    "final_net_pnl_after_fees": final_net,
                }
                cur.execute(
                    """
                    INSERT INTO strategy_events
                    (symbol,interval,strategy,event_type,decision,reason,info)
                    VALUES (%s,%s,%s,%s,'OBSERVED',%s,%s)
                    """,
                    (
                        symbol, interval, strategy, V2_FINAL_EVENT, exit_reason,
                        json.dumps(payload, default=_json_default),
                    ),
                )
        conn.commit()
        return len(rows)
    except Exception:
        if conn is not None:
            conn.rollback()
        logging.exception("economic floor V2 FT reconciliation failed")
        return 0
    finally:
        if conn is not None:
            conn.close()
