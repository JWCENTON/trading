"""Immutable, shadow-only entry opportunity evidence.

This module is deliberately fail-open for trading and fail-closed for evidence:
missing layers are explicit NULL/status values and no current state is used later
to repair an already captured snapshot.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import logging
import os
import uuid
from typing import Any, Callable

from psycopg2.extras import Json, register_uuid

from common.paper_simulation_fee_config import PaperSimulationFeeConfig
from common.realtime_engine import compute_realtime_snapshot


SCHEMA_VERSION = "ENTRY_OPPORTUNITY_EVIDENCE_V1"
_UUID_NAMESPACE = uuid.UUID("d925d1a4-4bf5-4f72-baf8-98a61d8c51fe")
_LOGGER = logging.getLogger(__name__)
_PAPER_RUNTIME_PROVENANCE = {
    "local-paper": ("trading_paper", "LOCAL"),
    "vps-paper": ("trading_paper", "VPS"),
}

# Keep UUID handling explicit for standalone writers/tests as well as runtime
# connections created through common.db.
register_uuid()


def canonical_runtime_paper_provenance(
    environ: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Map explicit runtime identity to the canonical registry provenance."""
    source = os.environ if environ is None else environ
    trading_mode = str(source.get("TRADING_MODE") or "").strip().upper()
    environment = str(source.get("ENVIRONMENT") or "").strip().lower()
    deployment = str(
        source.get("DEPLOYMENT_ID")
        or source.get("WALTRADE_DEPLOYMENT_ID")
        or ""
    ).strip().lower()
    if trading_mode != "PAPER" or environment not in {"paper", "trading_paper"}:
        raise RuntimeError("ENTRY_OPPORTUNITY_RUNTIME_ENVIRONMENT_NOT_PAPER")
    provenance = _PAPER_RUNTIME_PROVENANCE.get(deployment)
    if provenance is None:
        raise RuntimeError("ENTRY_OPPORTUNITY_RUNTIME_DEPLOYMENT_NOT_ALLOWED")
    return provenance


def validate_registry_runtime_provenance(
    registry_environment: str,
    registry_deployment_id: str,
    *,
    runtime_provenance_provider: Callable[[], tuple[str, str]] = (
        canonical_runtime_paper_provenance
    ),
) -> None:
    expected = runtime_provenance_provider()
    actual = (str(registry_environment), str(registry_deployment_id))
    if actual != expected:
        raise RuntimeError("ENTRY_OPPORTUNITY_RUNTIME_REGISTRY_IDENTITY_MISMATCH")


def cost_assumptions(
    planned_entry_notional: Decimal,
    entry_rate: Decimal,
    exit_rate: Decimal,
) -> dict[str, Decimal]:
    """Pure fee contract projection; it is evidence, never an entry gate."""
    notional = Decimal(str(planned_entry_notional))
    rate_entry = Decimal(str(entry_rate))
    rate_exit = Decimal(str(exit_rate))
    if notional < 0 or rate_entry < 0 or rate_exit < 0:
        raise ValueError("INVALID_ENTRY_OPPORTUNITY_COST_INPUT")
    if rate_entry > Decimal("0.10") or rate_exit >= Decimal("1"):
        raise ValueError("INVALID_ENTRY_OPPORTUNITY_FEE_RATE")
    return {
        "expected_round_trip_fee_usdc": notional * (rate_entry + rate_exit),
        "expected_round_trip_fee_pct": (rate_entry + rate_exit) * Decimal("100"),
        "break_even_move_pct": (
            ((Decimal("1") + rate_entry) / (Decimal("1") - rate_exit))
            - Decimal("1")
        ) * Decimal("100"),
    }


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _payload_hash(payload: dict[str, Any]) -> str:
    rendered = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=_json_default,
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _json(value: Any) -> Json:
    return Json(
        value,
        dumps=lambda item: json.dumps(item, default=_json_default, allow_nan=False),
    )


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _relation_exists(cur, relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{relation}",))
    row = cur.fetchone()
    return bool(row and row[0])


def _snapshot_id(decision_id: Any) -> uuid.UUID:
    return uuid.uuid5(_UUID_NAMESPACE, str(decision_id))


def _audit(
    cur,
    *,
    decision_id,
    snapshot_id,
    environment: str | None,
    deployment_id: str | None,
    event_type: str,
    reason: str | None = None,
    error_class: str | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO entry_opportunity_evidence_audit_v1(
          decision_id,snapshot_id,environment,deployment_id,event_type,
          status_reason,error_class
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            decision_id, snapshot_id, environment, deployment_id, event_type,
            reason, error_class,
        ),
    )


def _load_realtime(
    symbol: str,
    interval: str,
    candle_open_time,
) -> dict[str, Any]:
    return compute_realtime_snapshot(
        symbol=symbol,
        interval=interval,
        candle_open_time=candle_open_time,
    )


def capture_entry_opportunity_snapshot_cursor(
    cur,
    *,
    decision_id,
    simulated_order_id: int,
    planned_entry_notional: Decimal,
    fee_config: PaperSimulationFeeConfig,
    realtime_provider: Callable[[str, str, Any], dict[str, Any]] = _load_realtime,
    runtime_provenance_provider: Callable[[], tuple[str, str]] = (
        canonical_runtime_paper_provenance
    ),
    captured_at: datetime | None = None,
) -> uuid.UUID:
    """Capture one immutable snapshot inside the caller-owned transaction."""
    captured = captured_at or datetime.now(timezone.utc)
    snapshot_id = _snapshot_id(decision_id)

    cur.execute(
        """
        SELECT decision_id,legacy_decision_key,decision_timestamp,environment,
               deployment_id,strategy,symbol,interval,market_regime,
               decision_action,decision_reason,decision_payload
          FROM decision_registry_v1
         WHERE decision_id=%s
         FOR SHARE
        """,
        (decision_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("ENTRY_OPPORTUNITY_DECISION_ID_NOT_FOUND")
    (
        _decision_id, decision_key, decision_created_at, environment,
        deployment_id, strategy, symbol, interval, market_regime,
        decision_action, decision_reason, decision_payload,
    ) = row
    decision_payload = dict(decision_payload or {})
    if environment != "trading_paper":
        raise RuntimeError("ENTRY_OPPORTUNITY_PAPER_FEE_CONTRACT_NOT_APPLICABLE")
    validate_registry_runtime_provenance(
        environment,
        deployment_id,
        runtime_provenance_provider=runtime_provenance_provider,
    )

    cur.execute(
        """
        SELECT decision,reason,price,candle_open_time,created_at,info
          FROM strategy_events
         WHERE symbol=%s AND interval=%s AND strategy=%s
           AND event_type='SIGNAL'
           AND candle_open_time=%s
           AND created_at<=%s
         ORDER BY created_at DESC,id DESC
         LIMIT 1
        """,
        (symbol, interval, strategy, decision_created_at, captured),
    )
    signal_row = cur.fetchone()
    if signal_row is None:
        signal_action = decision_action
        signal_reason = decision_reason
        strategy_status = "MISSING_AT_ENTRY:NO_SIGNAL_EVENT"
        strategy_features: dict[str, Any] = {}
    else:
        (
            signal_action, _signal_reason_code, signal_price,
            signal_candle_time, signal_created_at, signal_info,
        ) = signal_row
        signal_reason = decision_reason
        strategy_status = "AVAILABLE"
        strategy_features = {
            "price": signal_price,
            "candle_open_time": signal_candle_time,
            "signal_created_at": signal_created_at,
            "features": dict(signal_info or {}),
        }

    realtime_context = None
    realtime_status = "MISSING_AT_ENTRY:REALTIME_CAPTURE_FAILED"
    try:
        realtime = dict(realtime_provider(symbol, interval, decision_created_at) or {})
        realtime_context = {
            "snapshot_observed_at": captured,
            "source_candle_open_time": decision_created_at,
            "age_seconds": Decimal(str((captured-decision_created_at).total_seconds())),
            "realtime_score": realtime.get("realtime_score"),
            "realtime_status": realtime.get("realtime_status"),
            "atr_component": realtime.get("atr_component"),
            "ema_component": realtime.get("ema_component"),
            "momentum_component": realtime.get("momentum_component"),
            "volume_component": realtime.get("volume_component"),
            "breakout_component": realtime.get("breakout_component"),
            "raw": realtime,
        }
        realtime_status = "AVAILABLE" if realtime.get("ok") else (
            "MISSING_AT_ENTRY:" + str(realtime.get("reason") or "REALTIME_NO_DATA")
        )
    except Exception as exc:  # telemetry layer remains explicit and fail-open
        realtime_status = f"MISSING_AT_ENTRY:{type(exc).__name__}"

    cur.execute(
        """
        SELECT to_jsonb(sequence_row)
          FROM market_memory_sequence sequence_row
         WHERE symbol=%s AND interval=%s
           AND refreshed_at<=%s
           AND (expires_at IS NULL OR expires_at>=%s)
         ORDER BY orc_readiness_score DESC NULLS LAST,refreshed_at DESC
         LIMIT 1
        """,
        (symbol, interval, captured, captured),
    )
    mme_row = cur.fetchone()
    mme_raw = mme_row[0] if mme_row else None
    mme_context = None
    if mme_raw is not None:
        mme_timestamp = _as_datetime(mme_raw.get("refreshed_at"))
        mme_age = None
        if mme_timestamp is not None:
            mme_age = Decimal(str((captured-mme_timestamp).total_seconds()))
        mme_context = {
            "snapshot": mme_raw,
            "snapshot_timestamp": mme_timestamp,
            "age_seconds": mme_age,
        }
    mme_status = "AVAILABLE" if mme_context is not None else (
        "MISSING_AT_ENTRY:NO_ACTIVE_MME_SEQUENCE"
    )

    cur.execute(
        """
        SELECT jsonb_agg(to_jsonb(slot_row) ORDER BY window_minutes)
          FROM slot_brain_snapshot slot_row
         WHERE symbol=%s AND interval=%s AND strategy=%s
           AND calculated_at<=%s
        """,
        (symbol, interval, strategy, captured),
    )
    slot_row = cur.fetchone()
    slot_raw = slot_row[0] if slot_row and slot_row[0] else None
    slot_context = None
    if slot_raw is not None:
        timestamps = [
            _as_datetime(item.get("calculated_at")) for item in slot_raw
            if isinstance(item, dict)
        ]
        timestamps = [timestamp for timestamp in timestamps if timestamp is not None]
        latest_slot_timestamp = max(timestamps) if timestamps else None
        slot_context = {
            "snapshots": slot_raw,
            "latest_snapshot_timestamp": latest_slot_timestamp,
            "age_seconds": (
                Decimal(str((captured-latest_slot_timestamp).total_seconds()))
                if latest_slot_timestamp is not None else None
            ),
            "contract_version": "SLOT_BRAIN_SNAPSHOT_V1_1",
        }
    slot_status = "AVAILABLE" if slot_context is not None else (
        "MISSING_AT_ENTRY:NO_SLOT_BRAIN_SNAPSHOT"
    )

    cur.execute(
        """
        SELECT jsonb_build_object(
          'captured_at',%s::timestamptz,
          'contract_version','ORC_V5_V63_CURRENT_STATE_CAPTURE',
          'bot_control',(
            SELECT to_jsonb(control_row) FROM bot_control control_row
             WHERE symbol=%s AND interval=%s AND strategy=%s
          ),
          'candidate_v5c',(
            SELECT to_jsonb(candidate_row) FROM v_orc_candidates_v5c candidate_row
             WHERE symbol=%s AND interval=%s AND strategy=%s
          ),
          'explain_v63',(
            SELECT to_jsonb(explain_row) FROM v_orc_v63_explain explain_row
             WHERE symbol=%s AND interval=%s AND strategy=%s
          ),
          'picked_v5',EXISTS(
            SELECT 1 FROM v_orc_picks_v5 picked_row
             WHERE symbol=%s AND interval=%s AND strategy=%s
          )
        )
        """,
        (
            captured,
            symbol, interval, strategy,
            symbol, interval, strategy,
            symbol, interval, strategy,
            symbol, interval, strategy,
        ),
    )
    orc_row = cur.fetchone()
    orc_context = orc_row[0] if orc_row else None
    control = (orc_context or {}).get("bot_control")
    orc_status = "AVAILABLE" if control is not None else (
        "MISSING_AT_ENTRY:NO_ORC_CONTROL_STATE"
    )
    explain = (orc_context or {}).get("explain_v63") or {}
    compatibility_status = explain.get("compatibility_status")
    compatibility_score = explain.get("compatibility_score")

    cur.execute(
        """
        SELECT to_jsonb(hysteresis_row)
          FROM v_market_regime_hysteresis_latest hysteresis_row
         WHERE symbol=%s AND interval=%s
         LIMIT 1
        """,
        (symbol, interval),
    )
    hysteresis_row = cur.fetchone()
    hysteresis_context = hysteresis_row[0] if hysteresis_row else None
    hysteresis_regime = (
        hysteresis_context.get("hysteresis_regime")
        if hysteresis_context is not None else None
    )

    regime_source = dict(decision_payload.get("regime_source") or {})
    market_context = {
        "regime_source": regime_source,
        "regime_attribution_version": decision_payload.get(
            "regime_attribution_version"
        ),
        "hysteresis": hysteresis_context,
        "hysteresis_status": (
            "AVAILABLE_SHADOW_CONTEXT" if hysteresis_context is not None
            else "MISSING_AT_ENTRY:NO_HYSTERESIS_STATE"
        ),
        "compatibility_status_reason": (
            "AVAILABLE_IN_FROZEN_ORC_CONTEXT"
            if compatibility_status is not None
            else "MISSING_AT_ENTRY:NOT_IN_FINAL_DECISION_STATE"
        ),
    }
    regime_confidence = regime_source.get("regime_source_confidence")
    economics = cost_assumptions(
        Decimal(str(planned_entry_notional)), fee_config.rate, fee_config.rate,
    )
    payload = {
        "identity": {
            "snapshot_id": str(snapshot_id),
            "decision_id": str(decision_id),
            "decision_key": decision_key,
            "decision_created_at": decision_created_at,
            "environment": environment,
            "deployment_id": deployment_id,
            "source_revision": decision_payload.get("source_revision"),
            "strategy": strategy,
            "symbol": symbol,
            "interval": interval,
        },
        "market": market_context,
        "strategy": strategy_features,
        "realtime": realtime_context,
        "mme": mme_context,
        "slot_brain": slot_context,
        "orc": orc_context,
        "economics": {
            "planned_entry_notional": planned_entry_notional,
            "fee_rate_entry_assumption": fee_config.rate,
            "fee_rate_exit_assumption": fee_config.rate,
            **economics,
            "fee_model_version": fee_config.model_version,
            "fee_config_source": fee_config.config_source,
        },
        "expected_move_pct": None,
        "expected_move_model_version": None,
    }
    digest = _payload_hash(payload)

    cur.execute(
        """
        INSERT INTO entry_opportunity_evidence_v1(
          snapshot_id,decision_id,decision_key,decision_created_at,environment,
          deployment_id,source_revision,strategy,symbol,interval,
          market_regime,regime_confidence,hysteresis_regime,
          compatibility_status,compatibility_score,market_availability_status,
          market_context,signal_action,signal_reason,
          strategy_availability_status,strategy_features,
          realtime_availability_status,realtime_context,
          mme_availability_status,mme_context,
          slot_brain_availability_status,slot_brain_context,
          orc_availability_status,orc_context,
          planned_entry_notional,fee_rate_entry_assumption,
          fee_rate_exit_assumption,expected_round_trip_fee_usdc,
          expected_round_trip_fee_pct,break_even_move_pct,
          fee_model_version,fee_config_source,spread_pct,
          execution_quality_status,execution_quality_context,
          expected_move_pct,expected_move_model_version,
          evidence_payload_hash,schema_version,captured_at
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s::jsonb,
          %s,%s::jsonb,%s,%s::jsonb,%s,%s::jsonb,%s,%s::jsonb,
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
          NULL,NULL,%s,%s,%s
        )
        ON CONFLICT(decision_id) DO NOTHING
        RETURNING snapshot_id
        """,
        (
            snapshot_id, decision_id, decision_key, decision_created_at,
            environment, deployment_id, decision_payload.get("source_revision"),
            strategy, symbol, interval, market_regime, regime_confidence,
            hysteresis_regime, compatibility_status, compatibility_score,
            "AVAILABLE" if market_regime else "MISSING_AT_ENTRY:NO_REGIME",
            _json(market_context), signal_action, signal_reason, strategy_status,
            _json(strategy_features), realtime_status,
            None if realtime_context is None else _json(realtime_context),
            mme_status, None if mme_context is None else _json(mme_context),
            slot_status, None if slot_context is None else _json(slot_context),
            orc_status, None if orc_context is None else _json(orc_context),
            Decimal(str(planned_entry_notional)), fee_config.rate,
            fee_config.rate, economics["expected_round_trip_fee_usdc"],
            economics["expected_round_trip_fee_pct"],
            economics["break_even_move_pct"], fee_config.model_version,
            fee_config.config_source, None,
            "MISSING_AT_ENTRY:NO_EXECUTION_QUALITY_MODEL", None,
            digest, SCHEMA_VERSION, captured,
        ),
    )
    inserted = cur.fetchone()
    event_type = "CAPTURED" if inserted else "IDEMPOTENT_EXISTING"
    effective_snapshot_id = inserted[0] if inserted else snapshot_id
    cur.execute(
        """
        UPDATE decision_registry_v1
           SET entry_opportunity_snapshot_id=COALESCE(entry_opportunity_snapshot_id,%s),
               entry_opportunity_evidence_status='CAPTURED',
               entry_opportunity_evidence_reason=%s
         WHERE decision_id=%s
           AND (entry_opportunity_snapshot_id IS NULL
                OR entry_opportunity_snapshot_id=%s)
        """,
        (effective_snapshot_id, event_type, decision_id, effective_snapshot_id),
    )
    _audit(
        cur, decision_id=decision_id, snapshot_id=effective_snapshot_id,
        environment=environment, deployment_id=deployment_id,
        event_type=event_type,
    )
    return effective_snapshot_id


def capture_entry_opportunity_snapshot_fail_open_cursor(cur, **kwargs):
    """Never changes the result of the surrounding order writer."""
    if not _relation_exists(cur, "entry_opportunity_evidence_v1"):
        return None
    decision_id = kwargs.get("decision_id")
    cur.execute("SAVEPOINT entry_opportunity_capture_v1")
    try:
        result = capture_entry_opportunity_snapshot_cursor(cur, **kwargs)
        cur.execute("RELEASE SAVEPOINT entry_opportunity_capture_v1")
        return result
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT entry_opportunity_capture_v1")
        cur.execute("RELEASE SAVEPOINT entry_opportunity_capture_v1")
        _LOGGER.exception("entry_opportunity_evidence_capture_failed")
        try:
            cur.execute("SAVEPOINT entry_opportunity_missing_v1")
            cur.execute(
                """
                UPDATE decision_registry_v1
                   SET entry_opportunity_evidence_status='ENTRY_OPPORTUNITY_EVIDENCE_MISSING',
                       entry_opportunity_evidence_reason=%s
                 WHERE decision_id=%s
                   AND entry_opportunity_snapshot_id IS NULL
                """,
                (type(exc).__name__, decision_id),
            )
            cur.execute(
                """
                SELECT environment,deployment_id
                  FROM decision_registry_v1 WHERE decision_id=%s
                """,
                (decision_id,),
            )
            identity = cur.fetchone() or (None, None)
            _audit(
                cur, decision_id=decision_id, snapshot_id=None,
                environment=identity[0], deployment_id=identity[1],
                event_type="ENTRY_OPPORTUNITY_EVIDENCE_MISSING",
                reason=str(exc), error_class=type(exc).__name__,
            )
            cur.execute("RELEASE SAVEPOINT entry_opportunity_missing_v1")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT entry_opportunity_missing_v1")
            cur.execute("RELEASE SAVEPOINT entry_opportunity_missing_v1")
            _LOGGER.exception("entry_opportunity_evidence_missing_audit_failed")
        return None


def link_entry_opportunity_order_fail_open_cursor(
    cur, *, decision_id, simulated_order_id: int,
) -> None:
    if not _relation_exists(cur, "entry_opportunity_evidence_v1"):
        return
    cur.execute("SAVEPOINT entry_opportunity_order_link_v1")
    try:
        cur.execute(
            """
            UPDATE simulated_orders order_row
               SET entry_opportunity_snapshot_id=registry.entry_opportunity_snapshot_id
              FROM decision_registry_v1 registry
             WHERE registry.decision_id=%s
               AND order_row.id=%s
               AND order_row.decision_id=registry.decision_id
               AND order_row.entry_opportunity_snapshot_id IS NULL
               AND registry.entry_opportunity_snapshot_id IS NOT NULL
            """,
            (decision_id, int(simulated_order_id)),
        )
        cur.execute("RELEASE SAVEPOINT entry_opportunity_order_link_v1")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT entry_opportunity_order_link_v1")
        cur.execute("RELEASE SAVEPOINT entry_opportunity_order_link_v1")
        _LOGGER.exception("entry_opportunity_order_link_failed")


def link_entry_opportunity_position_fail_open_cursor(
    cur, *, simulated_order_id: int, position_id: int, fill_id: int | None,
) -> None:
    if not _relation_exists(cur, "entry_opportunity_evidence_v1"):
        return
    cur.execute("SAVEPOINT entry_opportunity_position_link_v1")
    try:
        cur.execute(
            """
            SELECT COALESCE(order_row.entry_opportunity_snapshot_id,
                            registry.entry_opportunity_snapshot_id)
              FROM simulated_orders order_row
              LEFT JOIN decision_registry_v1 registry
                ON registry.decision_id=order_row.decision_id
             WHERE order_row.id=%s
            """,
            (int(simulated_order_id),),
        )
        row = cur.fetchone()
        snapshot_id = row[0] if row else None
        if snapshot_id is None:
            cur.execute("RELEASE SAVEPOINT entry_opportunity_position_link_v1")
            return
        cur.execute(
            """
            UPDATE positions SET entry_opportunity_snapshot_id=%s
             WHERE id=%s AND entry_opportunity_snapshot_id IS NULL
            """,
            (snapshot_id, int(position_id)),
        )
        if fill_id is not None:
            cur.execute(
                """
                UPDATE simulated_execution_fills_v1
                   SET entry_opportunity_snapshot_id=%s
                 WHERE id=%s AND entry_opportunity_snapshot_id IS NULL
                """,
                (snapshot_id, int(fill_id)),
            )
        for table_name in ("decision_replay_v1", "learning_feature_warehouse_v1"):
            cur.execute(
                f"""
                UPDATE {table_name}
                   SET entry_opportunity_snapshot_id=%s
                 WHERE position_id=%s
                   AND entry_opportunity_snapshot_id IS NULL
                """,
                (snapshot_id, int(position_id)),
            )
        cur.execute("RELEASE SAVEPOINT entry_opportunity_position_link_v1")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT entry_opportunity_position_link_v1")
        cur.execute("RELEASE SAVEPOINT entry_opportunity_position_link_v1")
        _LOGGER.exception("entry_opportunity_position_link_failed")
