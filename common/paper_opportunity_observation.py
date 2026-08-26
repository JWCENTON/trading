"""Compact PAPER-only opportunity observations derived from FinalDecision.

The causal FinalDecision outbox remains the transport and source decision
contract.  This module adds a bounded, immutable projection used for future
path quality labels; it never submits orders or mutates inventory.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import os
from typing import Any, Mapping

from common.decision_contract import DecisionReason, DecisionType, FinalDecision
from common.paper_simulation_fee_config import (
    FEE_MODEL_V2,
    load_paper_simulation_fee_config,
)


CONTRACT_VERSION = "FULL_PAPER_OPPORTUNITY_OBSERVATION_V1"
FEATURE_FLAG = "FULL_PAPER_OPPORTUNITY_OBSERVATION_V1_ENABLED"
SUPPORTED_STRATEGIES = frozenset({"RSI", "TREND", "SUPERTREND", "BBRANGE"})
SUPPORTED_INTERVALS = frozenset({"1m", "5m"})
SUPPORTED_SYMBOLS = frozenset({"BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"})
SUPPORTED_DEPLOYMENTS = frozenset({"local-paper", "vps-paper"})
SUPPORTED_OUTCOME_HORIZONS = (15, 30, 60, 240)


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _details(decision: FinalDecision) -> dict[str, Any]:
    return {str(key): value for key, value in decision.details.items()}


def _blocked_reason(decision: FinalDecision, details: Mapping[str, Any]) -> str | None:
    return _text(details.get("blocked_reason") or decision.reason_text)


def observation_type(decision: FinalDecision) -> str:
    """Map the existing decision taxonomy to the observation taxonomy."""
    details = _details(decision)
    blocked = (_blocked_reason(decision, details) or "").upper()
    if "CONTAIN" in blocked:
        return "CONTAINMENT_BLOCK"
    if decision.reason_code is DecisionReason.NO_NEW_CANDLE:
        return "NO_NEW_CANDLE"
    if decision.reason_code is DecisionReason.INDICATORS_NOT_READY:
        return "INDICATOR_NOT_READY"
    if decision.reason_code in {
        DecisionReason.CANDLE_MISSING_CLOSE,
        DecisionReason.CANDLE_MISSING_FIELDS,
        DecisionReason.NOT_ENOUGH_CANDLES,
        DecisionReason.NO_ROW,
    }:
        return "DATA_NOT_READY"
    if decision.reason_code is DecisionReason.POSITION_HOLD or decision.action == "HOLD":
        return "ALREADY_OPEN_BLOCK"
    if decision.reason_code is DecisionReason.POLICY_BLOCK:
        return "POLICY_BLOCK"
    if decision.decision_type is DecisionType.SIGNAL_REJECTED:
        return "SIGNAL_REJECTED"
    if decision.decision_type in {
        DecisionType.ENTRY_BLOCKED,
        DecisionType.ENTRY_SUPPRESSED,
        DecisionType.ACTION_SUPPRESSED,
    }:
        return "GATE_BLOCKED"
    if decision.decision_type in {
        DecisionType.PAPER_SIMULATION,
        DecisionType.TRADE_EXECUTED,
    }:
        return "EXECUTED"
    if decision.reason_code is DecisionReason.NO_SIGNAL:
        return "NO_SIGNAL"
    if decision.decision_type is DecisionType.SYSTEM_NOT_EVALUATED:
        return "DATA_NOT_READY"
    return "GATE_BLOCKED"


def _stable_hash(values: Mapping[str, Any]) -> str:
    def default(value: Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        raise TypeError(type(value).__name__)

    encoded = json.dumps(
        dict(values), default=default, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PaperOpportunityEnvelope:
    observation_key: str
    observation_type: str
    candle_open_time: datetime
    evaluation_started_at: datetime
    decision_type: str
    decision_subtype: str
    reason_text: str | None
    raw_signal_state: str
    base_decision: str
    final_decision: str
    data_readiness: str
    indicator_readiness: str
    gate_state: str
    gate_reason: str | None
    already_open_state: str
    containment_state: str
    outcome_eligible: bool
    opportunity_direction: str
    reference_price: Decimal | None
    runtime_enabled: bool | None
    live_orders_enabled: bool | None
    treatment_name: str | None
    treatment_status: str
    treatment_base_decision: str | None
    treatment_decision: str | None
    treatment_reason: str | None
    fee_rate_entry: Decimal
    fee_rate_exit: Decimal
    full_cost_hurdle_pct: Decimal
    fee_model_version: str
    fee_config_source: str
    source_revision: str
    engine_name: str
    engine_version: str | None
    position_id: int | None
    strategy_event_id: int | None
    simulated_order_id: int | None
    contract_version: str = CONTRACT_VERSION


def build_paper_opportunity_envelope(
    decision: FinalDecision,
    *,
    environ: Mapping[str, str] | None = None,
) -> PaperOpportunityEnvelope | None:
    """Build immutable observation-time evidence, or ``None`` outside PAPER."""
    values = os.environ if environ is None else environ
    ctx = decision.evaluation
    strategy = str(ctx.strategy).upper()
    symbol = str(ctx.symbol).upper()
    interval = str(ctx.interval)
    if not _enabled(values.get(FEATURE_FLAG, "0")):
        return None
    if (
        not ctx.paper_mode
        or ctx.environment != "trading_paper"
        or ctx.deployment_id not in SUPPORTED_DEPLOYMENTS
        or strategy not in SUPPORTED_STRATEGIES
        or symbol not in SUPPORTED_SYMBOLS
        or interval not in SUPPORTED_INTERVALS
    ):
        return None

    fee = load_paper_simulation_fee_config(values)
    if fee.model_version != FEE_MODEL_V2:
        # Opportunity viability must never silently use legacy costs.
        return None
    hurdle = (Decimal("2") * fee.rate / (Decimal("1") - fee.rate)) * Decimal("100")
    details = _details(decision)
    kind = observation_type(decision)
    blocked = _blocked_reason(decision, details)
    treatment_name = _text(details.get("treatment_name"))
    treatment_active = bool(
        strategy == "BBRANGE"
        and _enabled(values.get("BBRANGE_PAPER_TREATMENT_V1_ENABLED", "0"))
    )
    treatment_status = (
        "ACTIVE" if treatment_active else "NOT_APPLICABLE"
        if strategy != "BBRANGE" else "INACTIVE"
    )
    if treatment_name and treatment_status == "NOT_APPLICABLE":
        treatment_status = "OBSERVED"

    if decision.side == "SELL":
        direction = "SHORT"
    else:
        # V1's supported PAPER strategies are explicitly spot-long entry
        # strategies. This is a versioned contract, not outcome inference.
        direction = "LONG"

    outcome_eligible = bool(
        kind not in {"NO_NEW_CANDLE", "DATA_NOT_READY", "INDICATOR_NOT_READY"}
        and decision.action != "EXIT"
        and decision.reference_price is not None
    )
    identity = {
        "deployment_id": ctx.deployment_id,
        "environment": ctx.environment,
        "strategy": strategy,
        "symbol": symbol,
        "interval": interval,
        "candle_open_time": ctx.candle_open_time,
        "observation_type": kind,
        "decision_reason": decision.reason_code.value,
        "reason_text": decision.reason_text,
    }
    return PaperOpportunityEnvelope(
        observation_key=_stable_hash(identity),
        observation_type=kind,
        candle_open_time=ctx.candle_open_time,
        evaluation_started_at=ctx.evaluation_started_at,
        decision_type=decision.decision_type.value,
        decision_subtype=decision.decision_subtype.value,
        reason_text=decision.reason_text,
        raw_signal_state=("PRESENT" if decision.signal_detected else "ABSENT"),
        base_decision=_text(details.get("base_decision")) or (
            "BUY" if decision.signal_detected else "NO_TRADE"
        ),
        final_decision=_text(details.get("treatment_decision")) or (
            decision.action or decision.decision_type.value
        ),
        data_readiness=("NOT_READY" if kind == "DATA_NOT_READY" else "READY"),
        indicator_readiness=(
            "NOT_READY" if kind == "INDICATOR_NOT_READY" else "READY"
        ),
        gate_state=("BLOCKED" if kind in {
            "SIGNAL_REJECTED", "GATE_BLOCKED", "ALREADY_OPEN_BLOCK",
            "CONTAINMENT_BLOCK", "POLICY_BLOCK",
        } else "PASS"),
        gate_reason=blocked or decision.reason_code.value,
        already_open_state=("OPEN" if kind == "ALREADY_OPEN_BLOCK" else "CLEAR"),
        containment_state=("ACTIVE" if kind == "CONTAINMENT_BLOCK" else "CLEAR"),
        outcome_eligible=outcome_eligible,
        opportunity_direction=direction,
        reference_price=decision.reference_price,
        runtime_enabled=ctx.runtime_enabled,
        live_orders_enabled=ctx.live_orders_enabled,
        treatment_name=treatment_name or (
            "BBRANGE_PAPER_TREATMENT_V1" if treatment_active else None
        ),
        treatment_status=treatment_status,
        treatment_base_decision=_text(details.get("base_decision")),
        treatment_decision=_text(details.get("treatment_decision")),
        treatment_reason=_text(details.get("treatment_reason")),
        fee_rate_entry=fee.rate,
        fee_rate_exit=fee.rate,
        full_cost_hurdle_pct=hurdle,
        fee_model_version=fee.model_version,
        fee_config_source=fee.config_source,
        source_revision=_text(values.get("GIT_SHA") or values.get("REVISION")) or "UNKNOWN",
        engine_name=ctx.engine_name,
        engine_version=ctx.engine_version,
        position_id=decision.position_id,
        strategy_event_id=decision.strategy_event_id,
        simulated_order_id=decision.simulated_order_id,
    )


def persist_paper_opportunity_observation_cursor(
    cur: Any,
    payload: Mapping[str, Any],
) -> bool:
    """Project one enriched causal payload into the compact canonical table."""
    item = payload.get("paper_opportunity")
    if not isinstance(item, Mapping):
        return False
    params = (
        item["observation_key"], payload["event_id"], payload["decision_key"],
        payload["environment"], payload["deployment_id"], payload["strategy"],
        payload["symbol"], payload["interval"], payload["decision_created_at"],
        item["candle_open_time"], item["evaluation_started_at"],
        item["observation_type"], item["decision_type"], item["decision_subtype"],
        payload["decision_reason"], item.get("reason_text"),
        item["raw_signal_state"], item["base_decision"], item["final_decision"],
        item["data_readiness"], item["indicator_readiness"], item["gate_state"],
        item.get("gate_reason"), item["already_open_state"],
        item["containment_state"], item["outcome_eligible"],
        item["opportunity_direction"], item.get("reference_price"),
        item.get("runtime_enabled"), item.get("live_orders_enabled"),
        item.get("treatment_name"), item["treatment_status"],
        item.get("treatment_base_decision"), item.get("treatment_decision"),
        item.get("treatment_reason"), item["fee_rate_entry"],
        item["fee_rate_exit"], item["full_cost_hurdle_pct"],
        item["fee_model_version"], item["fee_config_source"],
        item["source_revision"], item["engine_name"], item.get("engine_version"),
        item.get("position_id"), item.get("strategy_event_id"),
        item.get("simulated_order_id"), item["contract_version"],
    )
    cur.execute(
        """
        INSERT INTO paper_opportunity_observation_v1(
          observation_key,causal_event_id,decision_key,environment,deployment_id,
          strategy,symbol,interval,observed_at,candle_open_time,
          evaluation_started_at,observation_type,decision_type,decision_subtype,
          decision_reason,reason_text,raw_signal_state,base_decision,final_decision,
          data_readiness,indicator_readiness,gate_state,gate_reason,
          already_open_state,containment_state,outcome_eligible,
          opportunity_direction,reference_price,runtime_enabled,
          live_orders_enabled,treatment_name,treatment_status,
          treatment_base_decision,treatment_decision,treatment_reason,
          fee_rate_entry,fee_rate_exit,full_cost_hurdle_pct,fee_model_version,
          fee_config_source,source_revision,engine_name,engine_version,position_id,
          strategy_event_id,simulated_order_id,contract_version,
          entry_trace_event_id,entry_opportunity_snapshot_id,
          realtime_availability_status,mme_availability_status,mme_direction,
          mme_sequence_stage,mme_source_refreshed_at,orc_availability_status,
          orc_run_id,observation_payload_hash
        )
        SELECT
          p.*,trace.id,snapshot.snapshot_id,
          CASE WHEN snapshot.snapshot_id IS NOT NULL
                 THEN snapshot.realtime_availability_status
               WHEN trace.id IS NOT NULL THEN 'AVAILABLE'
               ELSE 'MISSING_AT_OBSERVATION' END,
          CASE WHEN snapshot.snapshot_id IS NOT NULL
                 THEN snapshot.mme_availability_status
               WHEN mme.refreshed_at IS NOT NULL THEN 'AVAILABLE'
               ELSE 'MISSING_AT_OBSERVATION' END,
          COALESCE(snapshot.mme_context#>>'{snapshot,direction}',mme.direction),
          COALESCE(snapshot.mme_context#>>'{snapshot,payload,sequence_stage}',
                   mme.sequence_stage),
          COALESCE((snapshot.mme_context#>>'{snapshot,refreshed_at}')::timestamptz,
                   mme.refreshed_at),
          CASE WHEN snapshot.snapshot_id IS NOT NULL
                 THEN snapshot.orc_availability_status
               WHEN orc.run_id IS NOT NULL THEN 'AVAILABLE'
               ELSE 'MISSING_AT_OBSERVATION' END,
          orc.run_id,
          encode(digest(concat_ws('|',p.observation_key,p.causal_event_id::text,
            p.decision_key,p.observation_type,p.decision_reason,
            coalesce(p.reference_price::text,''),p.full_cost_hurdle_pct::text,
            p.contract_version),'sha256'),'hex')
        FROM (VALUES (
          %s,%s::uuid,%s,%s,%s,%s,%s,%s,%s::timestamptz,%s::timestamptz,
          %s::timestamptz,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s::numeric,%s::boolean,%s::boolean,%s,%s,%s,%s,%s,%s::numeric,
          %s::numeric,%s::numeric,%s,%s,%s,%s,%s,%s::bigint,%s::bigint,
          %s::bigint,%s
        )) AS p(
          observation_key,causal_event_id,decision_key,environment,deployment_id,
          strategy,symbol,interval,observed_at,candle_open_time,
          evaluation_started_at,observation_type,decision_type,decision_subtype,
          decision_reason,reason_text,raw_signal_state,base_decision,final_decision,
          data_readiness,indicator_readiness,gate_state,gate_reason,
          already_open_state,containment_state,outcome_eligible,
          opportunity_direction,reference_price,runtime_enabled,
          live_orders_enabled,treatment_name,treatment_status,
          treatment_base_decision,treatment_decision,treatment_reason,
          fee_rate_entry,fee_rate_exit,full_cost_hurdle_pct,fee_model_version,
          fee_config_source,source_revision,engine_name,engine_version,position_id,
          strategy_event_id,simulated_order_id,contract_version
        )
        LEFT JOIN LATERAL (
          SELECT t.id FROM entry_trace_events t
          WHERE t.strategy=p.strategy AND t.symbol=p.symbol AND t.interval=p.interval
            AND t.candle_open_time=p.candle_open_time
            AND t.created_at<=p.observed_at
          ORDER BY t.created_at DESC,t.id DESC LIMIT 1
        ) trace ON true
        LEFT JOIN LATERAL (
          SELECT e.* FROM entry_opportunity_evidence_v1 e
          WHERE e.decision_key=p.decision_key
          ORDER BY e.captured_at DESC LIMIT 1
        ) snapshot ON true
        LEFT JOIN LATERAL (
          SELECT m.direction,m.sequence_stage,m.refreshed_at
          FROM market_memory_sequence m
          WHERE m.symbol=p.symbol AND m.interval=p.interval
            AND m.refreshed_at<=p.observed_at
          ORDER BY m.refreshed_at DESC LIMIT 1
        ) mme ON true
        LEFT JOIN LATERAL (
          SELECT o.run_id FROM orc_apply_slot_decisions_v1 o
          WHERE o.deployment_id=p.deployment_id AND o.environment=p.environment
            AND o.strategy=p.strategy AND o.symbol=p.symbol AND o.interval=p.interval
            AND o.recorded_at<=p.observed_at
          ORDER BY o.recorded_at DESC LIMIT 1
        ) orc ON true
        ON CONFLICT (observation_key) DO NOTHING
        RETURNING observation_key
        """,
        params,
    )
    inserted = cur.fetchone()
    if inserted is not None:
        return True
    cur.execute(
        "SELECT causal_event_id FROM paper_opportunity_observation_v1 "
        "WHERE observation_key=%s",
        (item["observation_key"],),
    )
    return cur.fetchone() is not None
