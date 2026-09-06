from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Optional, Tuple

from common.db import get_db_conn


REGIME_GATE_CONTRACT_VERSION = "REGIME_GATE_MARKET_REGIME_SSOT_V1"
REGIME_POLICY_VERSION = "REGIME_POLICY_20260906_V1"
REGIME_POLICY_FINGERPRINT = "585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e"
CANONICAL_REGIMES = ("RANGE_HIGHVOL", "RANGE_LOWVOL", "SHOCK", "TREND_DOWN", "TREND_UP")


@dataclass(frozen=True)
class RegimeSourceRecord:
    symbol: str
    interval: str
    source_ts: datetime
    regime: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class RegimeGateDecision:
    allow: bool
    why: str
    regime: Optional[str]
    mode: Optional[str]
    would_block: Optional[bool]
    meta: dict


def canonical_strategy(strategy: str) -> str:
    value = str(strategy or "").strip().upper()
    return "SUPERTREND" if value == "SUPER_TREND" else value


def regime_freshness_limit_seconds(interval: str) -> int:
    normalized = str(interval or "").lower()
    if normalized == "1m":
        key, default = "REGIME_LAG_MAX_1M_SECONDS", "420"
    elif normalized == "5m":
        key, default = "REGIME_LAG_MAX_5M_SECONDS", "1200"
    else:
        key, default = "REGIME_MAX_AGE_SECONDS", "900"
    try:
        return max(1, int(os.environ.get(key, default)))
    except (TypeError, ValueError):
        return int(default)


def regime_record_is_fresh(
    record: RegimeSourceRecord,
    *,
    decision_candle_timestamp: datetime,
    evaluated_at: datetime,
) -> tuple[bool, dict]:
    source_lag = (decision_candle_timestamp - record.source_ts).total_seconds()
    pipeline_lag = (evaluated_at - record.created_at).total_seconds()
    limit = regime_freshness_limit_seconds(record.interval)
    meta = {
        "source_lag_seconds": source_lag,
        "pipeline_lag_seconds": pipeline_lag,
        "freshness_limit_seconds": limit,
    }
    return (
        source_lag >= 0
        and source_lag <= limit
        and pipeline_lag >= 0
        and pipeline_lag <= limit,
        meta,
    )


def get_current_regime_record(
    symbol: str,
    interval: str,
    *,
    decision_candle_timestamp: datetime,
) -> Optional[RegimeSourceRecord]:
    """Load the causal market_regime row at or before the decision source."""
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol,interval,ts,regime,created_at
            FROM market_regime
            WHERE symbol=%s AND interval=%s AND ts<=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (str(symbol).upper(), str(interval).lower(), decision_candle_timestamp),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return RegimeSourceRecord(
            symbol=str(row[0]).upper(), interval=str(row[1]).lower(), source_ts=row[2],
            regime=None if row[3] is None else str(row[3]).upper(), created_at=row[4],
        )
    finally:
        cur.close()
        conn.close()


def get_policy(strategy: str, regime: str) -> Optional[Tuple[bool, Optional[str]]]:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT allow_entry, note FROM regime_policy WHERE strategy=%s AND regime=%s",
            (canonical_strategy(strategy), str(regime).upper()),
        )
        row = cur.fetchone()
        return None if not row else (bool(row[0]), row[1] if row[1] is not None else None)
    finally:
        cur.close()
        conn.close()


def regime_source_record_from_evaluation(evaluation) -> Optional[RegimeSourceRecord]:
    """Recover the already-frozen causal source used by the entry decision."""
    context = evaluation.context
    source_ts = context.get("regime_source_ts")
    created_at = context.get("regime_source_created_at")
    if source_ts is None or created_at is None:
        return None
    return RegimeSourceRecord(
        symbol=str(context.get("regime_source_symbol") or evaluation.symbol).upper(),
        interval=str(context.get("regime_source_interval") or evaluation.interval).lower(),
        source_ts=source_ts,
        regime=None if evaluation.market_regime is None else str(evaluation.market_regime).upper(),
        created_at=created_at,
    )


def resolve_effective_regime_mode(
    bot_control_mode: str, configured_mode: Optional[str]
) -> tuple[str, bool]:
    """Per-slot control is authority; explicit runtime config must agree."""
    control = str(bot_control_mode or "").strip().upper()
    configured = str(configured_mode or control).strip().upper()
    valid = {"DRY_RUN", "ENFORCE"}
    if control not in valid or configured not in valid or control != configured:
        return "ENFORCE", False
    return control, True


def _blocked(*, why: str, regime: Optional[str], mode: str, strategy: str,
             decision: str, meta: dict) -> RegimeGateDecision:
    return RegimeGateDecision(False, why, regime, mode, True,
                              {"strategy": strategy, "decision": decision, **meta})


def decide_regime_gate(
    *, symbol: str, interval: str, strategy: str, decision: str,
    regime_enabled: bool, regime_mode: str,
    configured_regime_mode: Optional[str] = None,
    decision_candle_timestamp: Optional[datetime] = None,
    evaluated_at: Optional[datetime] = None,
    source_record: Optional[RegimeSourceRecord] = None,
    require_source_record: bool = False,
) -> RegimeGateDecision:
    """Canonical entry gate backed only by the causal market_regime SSOT."""
    canonical = canonical_strategy(strategy)
    effective_mode, config_valid = resolve_effective_regime_mode(regime_mode, configured_regime_mode)
    base_meta = {
        "contract_version": REGIME_GATE_CONTRACT_VERSION,
        "regime_source": "market_regime",
        "policy_version": REGIME_POLICY_VERSION,
        "policy_fingerprint": REGIME_POLICY_FINGERPRINT,
        "configured_mode": configured_regime_mode,
        "bot_control_mode": regime_mode,
    }
    if not config_valid:
        return _blocked(why="REGIME_CONFIG_CONFLICT", regime=None, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=base_meta)
    if not regime_enabled:
        return RegimeGateDecision(True, "REGIME_DISABLED", None, effective_mode, False,
                                  {"strategy": canonical, "decision": decision, **base_meta})

    evaluated_at = evaluated_at or datetime.now(timezone.utc)
    decision_candle_timestamp = decision_candle_timestamp or evaluated_at
    record = source_record
    if require_source_record and record is None:
        return _blocked(why="NO_REGIME_STATE", regime=None, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=base_meta)
    if record is None:
        try:
            record = get_current_regime_record(
                symbol, interval, decision_candle_timestamp=decision_candle_timestamp
            )
        except Exception as exc:
            return _blocked(why="REGIME_SOURCE_ERROR", regime=None, mode=effective_mode,
                            strategy=canonical, decision=decision,
                            meta={**base_meta, "source_error": type(exc).__name__})
    if record is None:
        return _blocked(why="NO_REGIME_STATE", regime=None, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=base_meta)
    source_meta = {
        **base_meta,
        "regime_source_symbol": record.symbol,
        "regime_source_interval": record.interval,
        "regime_source_ts": record.source_ts.isoformat(),
        "regime_source_created_at": record.created_at.isoformat(),
    }
    if record.regime not in CANONICAL_REGIMES:
        return _blocked(why="UNKNOWN_REGIME", regime=record.regime, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=source_meta)
    fresh, freshness_meta = regime_record_is_fresh(
        record, decision_candle_timestamp=decision_candle_timestamp, evaluated_at=evaluated_at
    )
    source_meta.update(freshness_meta)
    if not fresh:
        return _blocked(why="STALE_REGIME_STATE", regime=record.regime, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=source_meta)
    try:
        policy = get_policy(canonical, record.regime)
    except Exception as exc:
        return _blocked(why="POLICY_SOURCE_ERROR", regime=record.regime, mode=effective_mode,
                        strategy=canonical, decision=decision,
                        meta={**source_meta, "policy_error": type(exc).__name__})
    if policy is None:
        return _blocked(why="MISSING_POLICY", regime=record.regime, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=source_meta)
    allow_entry, note = policy
    policy_meta = {**source_meta, "policy_note": note}
    if allow_entry:
        return RegimeGateDecision(True, "POLICY_ALLOW", record.regime, effective_mode, False,
                                  {"strategy": canonical, "decision": decision, **policy_meta})
    if effective_mode == "ENFORCE":
        return _blocked(why="POLICY_BLOCK", regime=record.regime, mode=effective_mode,
                        strategy=canonical, decision=decision, meta=policy_meta)
    return RegimeGateDecision(True, "POLICY_WOULD_BLOCK", record.regime, effective_mode, True,
                              {"strategy": canonical, "decision": decision, **policy_meta})


def emit_regime_gate_event(*, symbol: str, interval: str, strategy: str,
                           decision: str, d: RegimeGateDecision) -> int:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO regime_gate_events(symbol, interval, strategy, decision, allow, regime, mode, would_block, why, meta)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) RETURNING id
            """,
            (symbol, interval, canonical_strategy(strategy), decision, d.allow, d.regime,
             d.mode, d.would_block, d.why, json.dumps(d.meta or {})),
        )
        event_id = int(cur.fetchone()[0])
        conn.commit()
        return event_id
    finally:
        cur.close()
        conn.close()


def attach_regime_gate_event(evaluation, *, gate_event_id: int | None,
                             decision: RegimeGateDecision):
    """Carry exact gate, source and policy identity into decision/position flow."""
    if gate_event_id is None:
        return evaluation
    context = dict(evaluation.context)
    context.update({
        "regime_gate_event_id": int(gate_event_id),
        "regime_gate_regime": decision.regime,
        "regime_gate_mode": decision.mode,
        "regime_gate_would_block": decision.would_block,
        "regime_gate_why": decision.why,
        "regime_gate_contract_version": decision.meta.get("contract_version"),
        "regime_gate_policy_version": decision.meta.get("policy_version"),
        "regime_gate_policy_fingerprint": decision.meta.get("policy_fingerprint"),
        "regime_gate_source_ts": decision.meta.get("regime_source_ts"),
        "regime_gate_source_created_at": decision.meta.get("regime_source_created_at"),
    })
    return replace(evaluation, context=context)
