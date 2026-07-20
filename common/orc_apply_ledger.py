"""Immutable, deterministic evidence helpers for authoritative ORC apply."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from psycopg2.extras import Json


SCHEMA_VERSION = "ORC_APPLY_LEDGER_V1_2"
EXECUTION_MODE_APPLY = "APPLY"
EXECUTION_MODE_OBSERVE_ONLY = "OBSERVE_ONLY"
VALID_DEPLOYMENTS = frozenset(
    {"local-live", "local-paper", "vps-live", "vps-paper"}
)
ENVIRONMENT_BY_DEPLOYMENT = {
    "local-live": "trading_live",
    "local-paper": "trading_paper",
    "vps-live": "trading_live",
    "vps-paper": "trading_paper",
}


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        default=_json_value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def canonical_slot_key(symbol: str, interval: str, strategy: str) -> str:
    return f"{symbol.upper()}|{interval}|{strategy.upper()}"


def deterministic_picks_hash(slots: Iterable[dict[str, Any]]) -> str:
    identities = sorted(
        (
            canonical_slot_key(row["symbol"], row["interval"], row["strategy"]),
            row.get("pick_source") or "ORC_V6_3",
        )
        for row in slots
        if row.get("want_on")
    )
    return stable_hash({"included_slots": identities}) if identities else ""


def transition_type(previous: bool, desired: bool) -> str:
    if previous and desired:
        return "RETAINED_ON"
    if previous and not desired:
        return "DISABLED"
    if not previous and desired:
        return "ENABLED"
    return "RETAINED_OFF"


class OrcObserveOnlyGuardError(RuntimeError):
    """PAPER observation was requested without unequivocal execution isolation."""

    error_classification = "ORC_OBSERVE_ONLY_GUARD_FAILED"


def parse_required_execution_guard(name: str, value: str | None) -> bool:
    normalized = str(value).strip().lower() if value is not None else ""
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise OrcObserveOnlyGuardError(f"{name} must be explicitly 0 or 1")


def resolve_execution_mode(
    identity: "WriterIdentity",
    trading_mode: str,
    *,
    observe_only_enabled: bool,
    live_orders_enabled: bool,
    execution_enabled: bool,
) -> str | None:
    mode = str(trading_mode).upper()
    if identity.deployment_id.endswith("-live"):
        if mode != "LIVE":
            raise ValueError("DEPLOYMENT_ID does not match TRADING_MODE")
        return EXECUTION_MODE_APPLY
    if mode != "PAPER":
        raise ValueError("DEPLOYMENT_ID does not match TRADING_MODE")
    if not observe_only_enabled:
        return None
    if live_orders_enabled or execution_enabled:
        raise OrcObserveOnlyGuardError(
            "PAPER OBSERVE_ONLY requires LIVE_ORDERS_ENABLED=0 and "
            "OKX_EXECUTION_ENABLED=0"
        )
    return EXECUTION_MODE_OBSERVE_ONLY


@dataclass(frozen=True)
class WriterIdentity:
    deployment_id: str
    environment: str
    service: str
    instance: str
    version: str | None
    git_sha: str | None

    @classmethod
    def from_env(cls, trading_mode: str) -> "WriterIdentity":
        deployment_id = str(os.getenv("DEPLOYMENT_ID") or "").strip().lower()
        if deployment_id not in VALID_DEPLOYMENTS:
            raise ValueError(
                "DEPLOYMENT_ID must explicitly identify one of: "
                + ", ".join(sorted(VALID_DEPLOYMENTS))
            )
        expected_mode = "LIVE" if deployment_id.endswith("-live") else "PAPER"
        if str(trading_mode).upper() != expected_mode:
            raise ValueError("DEPLOYMENT_ID does not match TRADING_MODE")
        return cls(
            deployment_id=deployment_id,
            environment=ENVIRONMENT_BY_DEPLOYMENT[deployment_id],
            service="automation-runner",
            instance=str(os.getenv("ORC_WRITER_INSTANCE") or socket.gethostname()),
            version=os.getenv("ORC_WRITER_VERSION") or None,
            git_sha=os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA") or None,
        )


def rows_as_dicts(cur) -> list[dict[str, Any]]:
    columns = [item[0] for item in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def make_slot_decision(
    control: dict[str, Any],
    source: dict[str, Any] | None,
    *,
    want_on: bool,
    pick_source: str | None,
    on_reason: str,
    off_reason: str,
    execution_mode: str = EXECUTION_MODE_APPLY,
) -> dict[str, Any]:
    source = deepcopy(source or {})
    previous = bool(control.get("live_orders_enabled"))
    desired_transition = transition_type(previous, want_on)
    observe_only = execution_mode == EXECUTION_MODE_OBSERVE_ONLY
    transition = (
        {
            "ENABLED": "WOULD_ENABLE",
            "DISABLED": "WOULD_DISABLE",
            "RETAINED_ON": "WOULD_RETAIN_ON",
            "RETAINED_OFF": "WOULD_RETAIN_OFF",
        }[desired_transition]
        if observe_only
        else desired_transition
    )
    decision_effect = (
        transition
        if observe_only
        else {
            "ENABLED": "APPLIED_ENABLE",
            "DISABLED": "APPLIED_DISABLE",
            "RETAINED_ON": "RETAINED_ON",
            "RETAINED_OFF": "RETAINED_OFF",
        }[desired_transition]
    )
    reason = on_reason if want_on else off_reason
    if want_on and pick_source == "ORC_EXPLORE_V1":
        reason = "ORC_EXPLORE_V1: controlled exploration (entries ON, ENFORCE)"
    snapshot = {
        "control_before": control,
        "source": source,
        "decision": {
            "want_on": want_on,
            "pick_source": pick_source,
            "reason": reason,
            "transition_type": transition,
            "decision_effect": decision_effect,
            "execution_mode": execution_mode,
        },
    }
    mutation_required = any(
        (
            bool(control.get("live_orders_enabled")) != want_on,
            (control.get("control_mode") or "AUTO") != "AUTO",
            (control.get("control_source") or "ORC") != "ORC",
            control.get("manual_override_reason") is not None,
            control.get("manual_override_updated_at") is not None,
            control.get("regime_mode") != ("ENFORCE" if want_on else "DRY_RUN"),
            control.get("regime_enabled") is not True,
            control.get("reason") != reason,
        )
    )
    touched = mutation_required if not observe_only else False
    resulting = previous if observe_only else want_on
    return {
        "symbol": control["symbol"],
        "interval": control["interval"],
        "strategy": control["strategy"],
        "slot_key": canonical_slot_key(
            control["symbol"], control["interval"], control["strategy"]
        ),
        "previous_live": previous,
        "want_on": want_on,
        "resulting_live": resulting,
        "transition_type": transition,
        "decision_effect": decision_effect,
        "touched": touched,
        "mutation_required": mutation_required,
        "state_changed": (previous != want_on) and not observe_only,
        "control_mode": control.get("control_mode") or "AUTO",
        "control_source": control.get("control_source") or "ORC",
        "reason": reason,
        "pick_source": pick_source,
        "source": source,
        "snapshot": snapshot,
        "snapshot_hash": stable_hash(snapshot),
    }


class LedgerSlotCountMismatch(RuntimeError):
    """Fail-closed cardinality violation for the immutable apply ledger."""

    error_classification = "LEDGER_SLOT_COUNT_MISMATCH"


def validate_slot_counts(
    source_candidate_count: int,
    evaluated_count: int,
    prepared_slot_count: int,
    inserted_slot_count: int | None = None,
) -> int:
    source_excluded_count = source_candidate_count - evaluated_count
    mismatches = []
    if source_excluded_count < 0:
        mismatches.append("source_candidate_count < evaluated_count")
    if evaluated_count != prepared_slot_count:
        mismatches.append("evaluated_count != prepared_slot_count")
    if inserted_slot_count is not None and inserted_slot_count != evaluated_count:
        mismatches.append("inserted_slot_count != evaluated_count")
    if mismatches:
        raise LedgerSlotCountMismatch(
            "; ".join(mismatches)
            + f" (source={source_candidate_count}, evaluated={evaluated_count}, "
            + f"prepared={prepared_slot_count}, inserted={inserted_slot_count})"
        )
    return source_excluded_count


def insert_slot_decision(cur, run_id: uuid.UUID, identity: WriterIdentity, row: dict[str, Any]) -> int:
    source = row["source"]
    cur.execute(
        """
        INSERT INTO orc_apply_slot_decisions_v1 (
          run_id,deployment_id,environment,symbol,interval,strategy,slot_key,
          previous_live_orders_enabled,want_on,resulting_live_orders_enabled,
          transition_type,touched,control_mode,control_source,decision_reason,
          pick_source,v63_eligible,v63_picked,v63_reason,v63_score,v63_rank,
          trades_3d,net_pnl_3d,profit_factor_3d,gate_fresh,
          hysteresis_regime,hysteresis_confidence,hysteresis_reason,
          hysteresis_holding_previous,v7_ready,v7_readiness_reason,v7_reason,
          v7_rank,runs_15m,buy_decisions_15m,signals_15m,hard_blocks_15m,
          mme_avoid,mme_remaining_score,mme_exhaustion_risk,mme_status,
          mme_hint,mme_readiness_score,mme_sequence_type,mme_sequence_stage,
          mme_sequence_quality,mme_late_entry_risk,mme_context_status,
          context_v2_ready_now,included_in_pick_set,source_refreshed_at,
          slot_snapshot_hash,snapshot_json,writer_service,writer_instance,
          schema_version,decision_effect
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        (
            str(run_id), identity.deployment_id, identity.environment,
            row["symbol"], row["interval"], row["strategy"], row["slot_key"],
            row["previous_live"], row["want_on"], row["resulting_live"],
            row["transition_type"], row["touched"], row["control_mode"],
            row["control_source"], row["reason"], row["pick_source"],
            source.get("eligible_v63"), source.get("picked_v63_now"),
            source.get("v63_reason"), source.get("v63_score"),
            source.get("final_rn") or source.get("context_v2_rn"),
            source.get("n_trades_3d"), source.get("net_sum_3d"),
            source.get("profit_factor_3d"), source.get("gate_fresh"),
            source.get("current_hysteresis_regime"),
            source.get("current_hysteresis_confidence"),
            source.get("hysteresis_reason"),
            source.get("hysteresis_holding_previous"), source.get("orc_v7_ready"),
            source.get("readiness_reason"), source.get("v7_reason"),
            source.get("v7_rn"), source.get("runs_15m"),
            source.get("buy_decisions_15m"), source.get("signals_15m"),
            source.get("hard_blocks_15m"), source.get("mme_orc_avoid"),
            source.get("mme_remaining_score"), source.get("mme_exhaustion_risk"),
            source.get("mme_orc_status"), source.get("mme_orc_hint"),
            source.get("mme_orc_readiness_score"), source.get("mme_sequence_type"),
            source.get("mme_sequence_stage"), source.get("mme_sequence_quality"),
            source.get("mme_late_entry_risk"), source.get("mme_context_status"),
            source.get("context_v2_ready_now"), row["want_on"],
            source.get("refreshed_at"), row["snapshot_hash"],
            Json(row["snapshot"], dumps=canonical_json), identity.service,
            identity.instance, SCHEMA_VERSION, row["decision_effect"],
        ),
    )
    return cur.rowcount


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
