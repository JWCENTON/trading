from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
from typing import Any


CONTRACT_VERSION = "ECONOMIC_FLOOR_BOUNDARY_REFINEMENT_V1_FORWARD_EVIDENCE"
EVIDENCE_EVENT = "ECONOMIC_FLOOR_BOUNDARY_V1_EVIDENCE"
FINAL_EVENT = "ECONOMIC_FLOOR_BOUNDARY_V1_FINAL"
ZERO = Decimal("0")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def build_evidence_payload(
    *, evidence, mark, armed_at: datetime, previous: dict[str, Any] | None,
    existing_exit_decision: str, existing_exit_reason: str,
    existing_exit_committed: bool, recent_realized_volatility: Decimal | None,
    regime: str | None, regime_at: datetime | None,
) -> dict[str, Any]:
    """Build telemetry only; this contract has no exit-decision field."""
    current = evidence.realizable_net_after_all_costs
    notional = evidence.hypothetical_exit_notional
    if current is None or notional is None or notional <= ZERO:
        raise ValueError("authoritative realizable net and notional required")

    previous_net = _decimal(previous.get("realizable_net")) if previous else None
    previous_at = (
        datetime.fromisoformat(str(previous["source_1m_close_time"]))
        if previous and previous.get("source_1m_close_time") else None
    )
    change = current - previous_net if previous_net is not None else None
    elapsed_minutes = (
        Decimal(str((mark.close_time - previous_at).total_seconds())) / Decimal("60")
        if previous_at is not None and mark.close_time > previous_at else None
    )
    slope = change / elapsed_minutes if change is not None and elapsed_minutes else None
    prior_peak = _decimal(previous.get("peak_realizable_net")) if previous else None
    peak = max(current, prior_peak) if prior_peak is not None else current
    distance = peak - current
    prior_count = int(previous.get("distinct_evaluations_since_arm", 0)) if previous else 0

    candle_range = (
        mark.high - mark.low if mark.high is not None and mark.low is not None else None
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "evidence_authority": "FORWARD_CAUSAL_DISCOVERY_ONLY",
        "active_boundary_influence": "OFF",
        "position_id": int(evidence.position_id),
        "symbol": str(evidence.symbol),
        "originating_interval": str(evidence.interval),
        "strategy": str(evidence.strategy).upper(),
        "source_1m_candle_id": mark.candle_id,
        "source_1m_close_time": mark.close_time,
        "source_1m_identity": mark.source_id,
        "position_notional": notional,
        "realizable_net": current,
        "realizable_net_pct_of_notional": current / notional * Decimal("100"),
        "previous_realizable_net": previous_net,
        "realizable_net_change": change,
        "realizable_net_slope_per_minute": slope,
        "peak_realizable_net": peak,
        "distance_from_peak_usdc": distance,
        "distance_from_peak_pct": distance / notional * Decimal("100"),
        "one_minute_candle_range": candle_range,
        "one_minute_candle_range_pct": (
            candle_range / mark.price * Decimal("100")
            if candle_range is not None and mark.price > ZERO else None
        ),
        "atr_pct": mark.atr_pct,
        "recent_realized_volatility": recent_realized_volatility,
        "recent_realized_volatility_window": (
            "5_FINALIZED_1M_RETURNS" if recent_realized_volatility is not None else None
        ),
        "regime": regime,
        "regime_observed_at": regime_at,
        "armed_at": armed_at,
        "seconds_since_arm": Decimal(str((mark.close_time - armed_at).total_seconds())),
        "distinct_evaluations_since_arm": prior_count + 1,
        "existing_exit_decision": existing_exit_decision,
        "existing_exit_reason": existing_exit_reason,
        "existing_exit_committed": bool(existing_exit_committed),
        "entry_fill_ids": list(evidence.entry_fill_ids),
        "fee_contract_fingerprint": evidence.fee_contract_fingerprint,
        "exit_fee_rate": evidence.exit_fee_rate,
        "market_data_complete": evidence.market_data_complete,
        "ordering_evidence_available": evidence.ordering_evidence_available,
    }


def persist_boundary_evidence_cursor(
    cur, *, evidence, mark, armed_at: datetime, is_forward_arm: bool,
    existing_exit_decision: str, existing_exit_reason: str,
    existing_exit_committed: bool,
) -> str:
    """Append one idempotent causal observation without acquiring authority."""
    cur.execute(
        "SELECT info FROM strategy_events WHERE event_type=%s "
        "AND info->>'position_id'=%s ORDER BY id DESC LIMIT 1",
        (EVIDENCE_EVENT, str(int(evidence.position_id))),
    )
    latest_row = cur.fetchone()
    if latest_row is None and not is_forward_arm:
        return "PREEXISTING_ARM_EXCLUDED"
    latest = dict(latest_row[0]) if latest_row else None
    if latest and str(latest.get("source_1m_identity")) == str(mark.source_id):
        return "IDEMPOTENT_SOURCE_ALREADY_RECORDED"

    cur.execute(
        """
        SELECT close FROM candles
        WHERE symbol=%s AND interval='1m' AND close_time<=%s
        ORDER BY close_time DESC,id DESC LIMIT 6
        """,
        (str(evidence.symbol), mark.close_time),
    )
    closes = [Decimal(str(row[0])) for row in reversed(cur.fetchall())]
    returns = [closes[index] / closes[index - 1] - Decimal("1") for index in range(1, len(closes))]
    recent_vol = None
    if len(returns) == 5:
        mean = sum(returns, ZERO) / Decimal(len(returns))
        variance = sum(((value - mean) ** 2 for value in returns), ZERO) / Decimal(len(returns) - 1)
        recent_vol = variance.sqrt() * Decimal("100")

    cur.execute(
        """
        SELECT regime,ts FROM market_regime
        WHERE symbol=%s AND interval='1m' AND ts<=%s
        ORDER BY ts DESC LIMIT 1
        """,
        (str(evidence.symbol), mark.close_time),
    )
    regime_row = cur.fetchone()
    payload = build_evidence_payload(
        evidence=evidence, mark=mark, armed_at=armed_at, previous=latest,
        existing_exit_decision=existing_exit_decision,
        existing_exit_reason=existing_exit_reason,
        existing_exit_committed=existing_exit_committed,
        recent_realized_volatility=recent_vol,
        regime=(str(regime_row[0]) if regime_row else None),
        regime_at=(regime_row[1] if regime_row else None),
    )
    cur.execute(
        """
        INSERT INTO strategy_events
        (symbol,interval,strategy,event_type,decision,reason,price,
         candle_open_time,info)
        VALUES (%s,%s,%s,%s,'OBSERVE','FORWARD_EVIDENCE_ONLY',%s,%s,%s)
        """,
        (
            evidence.symbol, evidence.interval, evidence.strategy, EVIDENCE_EVENT,
            float(mark.price), mark.close_time,
            json.dumps(payload, default=_json_default),
        ),
    )
    return "RECORDED"


def reconcile_boundary_finals_cursor(cur) -> int:
    cur.execute(
        """
        SELECT DISTINCT (e.info->>'position_id')::bigint,e.symbol,e.interval,
               e.strategy,p.exit_reason,ft.financial_truth_status,
               ft.authoritative_net_pnl
        FROM strategy_events e
        JOIN positions p ON p.id=(e.info->>'position_id')::bigint
        JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
        WHERE e.event_type=%s AND p.status<>'OPEN'
          AND ft.financial_truth_status='COMPLETE'
          AND NOT EXISTS (
            SELECT 1 FROM strategy_events done WHERE done.event_type=%s
              AND done.info->>'position_id'=e.info->>'position_id'
          )
        """,
        (EVIDENCE_EVENT, FINAL_EVENT),
    )
    rows = cur.fetchall()
    for position_id, symbol, interval, strategy, exit_reason, ft_status, final_net in rows:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"{CONTRACT_VERSION}|final|{int(position_id)}",),
        )
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
            "active_boundary_influence": "OFF",
            "position_id": int(position_id),
            "final_financial_truth_status": ft_status,
            "final_net_after_fees": final_net,
            "actual_exit_reason": exit_reason,
        }
        cur.execute(
            """
            INSERT INTO strategy_events
            (symbol,interval,strategy,event_type,decision,reason,info)
            VALUES (%s,%s,%s,%s,'OBSERVED',%s,%s)
            """,
            (symbol, interval, strategy, FINAL_EVENT, exit_reason,
             json.dumps(payload, default=_json_default)),
        )
    return len(rows)
