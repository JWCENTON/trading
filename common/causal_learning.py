from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


CAUSAL_SCHEMA_VERSION = "CAUSAL_LEARNING_TELEMETRY_V1"
NO_RECOMMENDATION = "NO_ACTIVE_RECOMMENDATION"
LEGACY_NOT_ATTRIBUTABLE = "LEGACY_NOT_ATTRIBUTABLE"


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def payload_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def slot_key(
    *,
    environment: str,
    strategy: str,
    symbol: str,
    interval: str,
    regime: str | None = None,
) -> str:
    parts = [environment, strategy, symbol, interval, regime or "*"]
    return "|".join(str(part).strip().upper() for part in parts)


def recommendation_id(
    *,
    environment: str,
    strategy: str,
    symbol: str,
    interval: str,
    regime: str | None,
    recommendation_type: str,
    recommendation_action: str,
    evidence_cutoff_at: datetime,
    policy_version: str,
) -> str:
    if evidence_cutoff_at.tzinfo is None:
        raise ValueError("evidence_cutoff_at must be timezone-aware")
    identity = {
        "schema": CAUSAL_SCHEMA_VERSION,
        "slot_key": slot_key(
            environment=environment,
            strategy=strategy,
            symbol=symbol,
            interval=interval,
            regime=regime,
        ),
        "recommendation_type": recommendation_type,
        "recommendation_action": recommendation_action,
        "evidence_cutoff_at": evidence_cutoff_at.astimezone(timezone.utc).isoformat(),
        "policy_version": policy_version,
    }
    return payload_hash(identity)


@dataclass(frozen=True)
class DecisionAttribution:
    recommendation_id: str | None = None
    recommendation_version: str | None = None
    activation_id: str | None = None
    experiment_id: str | None = None
    experiment_arm: str = "BASELINE"
    baseline_policy_version: str | None = None
    candidate_policy_version: str | None = None
    promotion_event_id: int | None = None
    consumed_promotion_hash: str | None = None
    consumed_promotion_version: str | None = None
    causal_linkage_status: str = NO_RECOMMENDATION
