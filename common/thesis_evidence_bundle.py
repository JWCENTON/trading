"""Immutable, evidence-only observations for future thesis research.

The producer deliberately has no trading imports or sinks.  It freezes raw
closed-candle structure, the current MME sequence projection, and tactical
opportunity identities in one repeatable-read transaction.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import re
from typing import Any, Callable, Iterable, Mapping

from psycopg2.extras import Json, register_uuid

from common.db import get_db_conn


CONTRACT_VERSION = "THESIS_EVIDENCE_BUNDLE_V1"
PIPELINE_CONTRACT_VERSION = "THESIS_EVIDENCE_PIPELINE_RUN_V1"
STRUCTURAL_CONTRACT_VERSION = "THESIS_STRUCTURAL_OBSERVATION_V1"
MME_OBSERVATION_CONTRACT_VERSION = "THESIS_MME_SEQUENCE_OBSERVATION_V1"
MME_TRANSITION_CONTRACT_VERSION = "THESIS_MME_TRANSITION_OBSERVATION_V1"
TACTICAL_SET_CONTRACT_VERSION = "THESIS_TACTICAL_OPPORTUNITY_SET_V1"
CUTOVER_CONTRACT_VERSION = "THESIS_EVIDENCE_BUNDLE_V1_CUTOVER"
CUTOFF_CONTRACT_VERSION = "LAST_CLOSED_5M_BOUNDARY_V1"
STRUCTURAL_SOURCE_VERSION = "CLOSED_CANDLE_WINDOW_SOURCE_V1"
MME_SOURCE_VERSION = "MARKET_MEMORY_SEQUENCE_V16"
TACTICAL_SOURCE_VERSION = "TACTICAL_SIGNAL_SET_SOURCE_V1"
CADENCE_MINUTES = 5
HORIZON_MINUTES = {"6h": 360, "24h": 1440, "3d": 4320}
HEX_40_OR_64 = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")

register_uuid()


def _decimal_string(value: Decimal) -> str:
    if value == 0:
        return "0"
    rendered = format(value.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _timestamp_string(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("THESIS_EVIDENCE_TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_value(value: Any) -> Any:
    """Return the JSON-safe canonical value used by every V1 fingerprint."""
    if isinstance(value, datetime):
        return _timestamp_string(value)
    if isinstance(value, Decimal):
        return _decimal_string(value)
    if isinstance(value, Mapping):
        return {
            str(key): canonical_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _json(value: Any) -> Json:
    return Json(value, dumps=canonical_json)


def canonical_evidence_cutoff(evaluated_at: datetime) -> datetime:
    """Floor an aware UTC instant to the deterministic closed 5m boundary."""
    if evaluated_at.tzinfo is None:
        raise ValueError("THESIS_EVIDENCE_EVALUATED_AT_MUST_BE_TIMEZONE_AWARE")
    value = evaluated_at.astimezone(timezone.utc)
    minute = value.minute - (value.minute % CADENCE_MINUTES)
    return value.replace(minute=minute, second=0, microsecond=0)


def runtime_provenance(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str, str]:
    source = os.environ if environ is None else environ
    mode = str(source.get("TRADING_MODE") or "").strip().upper()
    runtime_deployment = str(
        source.get("DEPLOYMENT_ID")
        or source.get("WALTRADE_DEPLOYMENT_ID")
        or ""
    ).strip().lower()
    expected = {
        ("PAPER", "local-paper"): ("trading_paper", "LOCAL"),
        ("PAPER", "vps-paper"): ("trading_paper", "VPS"),
        ("LIVE", "local-live"): ("trading_live", "LOCAL"),
        ("LIVE", "vps-live"): ("trading_live", "VPS"),
    }.get((mode, runtime_deployment))
    if expected is None:
        raise RuntimeError("THESIS_EVIDENCE_RUNTIME_PROVENANCE_INVALID")
    git_revision = str(source.get("GIT_SHA") or "").strip().lower()
    if HEX_40_OR_64.fullmatch(git_revision) is None:
        raise RuntimeError("THESIS_EVIDENCE_GIT_REVISION_INVALID")
    return expected[0], expected[1], git_revision


def source_version_manifest() -> dict[str, Any]:
    return {
        "bundle": {
            "producer_name": "thesis_evidence_bundle",
            "producer_contract_version": CONTRACT_VERSION,
            "producer_semantic_version": CONTRACT_VERSION,
            "source_identity": "common.thesis_evidence_bundle",
        },
        "cutoff": {
            "producer_name": "canonical_evidence_cutoff",
            "producer_contract_version": CUTOFF_CONTRACT_VERSION,
            "producer_semantic_version": CUTOFF_CONTRACT_VERSION,
            "source_identity": "closed_5m_boundary",
        },
        "structural": {
            "producer_name": "structural_observation",
            "producer_contract_version": STRUCTURAL_CONTRACT_VERSION,
            "producer_semantic_version": STRUCTURAL_SOURCE_VERSION,
            "source_identity": "public.candles",
            "upstream_semantic_version": "UNVERSIONED_LEGACY_SOURCE",
        },
        "mme": {
            "producer_name": "mme_sequence_observation",
            "producer_contract_version": MME_OBSERVATION_CONTRACT_VERSION,
            "producer_semantic_version": MME_SOURCE_VERSION,
            "source_identity": "public.market_memory_sequence",
            "coherence_contract": "REPEATABLE_READ_CURRENT_PROJECTION_V1",
        },
        "tactical": {
            "producer_name": "tactical_opportunity_set",
            "producer_contract_version": TACTICAL_SET_CONTRACT_VERSION,
            "producer_semantic_version": TACTICAL_SOURCE_VERSION,
            "source_identity": "public.strategy_events+public.decision_registry_v1",
            "strategy_events_upstream_version": "UNVERSIONED_LEGACY_SOURCE",
        },
    }


def deterministic_pipeline_run_id(
    environment: str,
    deployment_id: str,
    evidence_cutoff: datetime,
    manifest: Mapping[str, Any] | None = None,
) -> str:
    return fingerprint({
        "contract_version": PIPELINE_CONTRACT_VERSION,
        "environment": environment,
        "deployment_id": deployment_id,
        "evidence_cutoff": evidence_cutoff,
        "source_version_manifest": manifest or source_version_manifest(),
    })


def evidence_status(missing_sources: Iterable[str]) -> str:
    return "INCOMPLETE" if tuple(missing_sources) else "COMPLETE"


def _semantic_id(kind: str, payload: Mapping[str, Any]) -> str:
    return fingerprint({"identity_kind": kind, **payload})


def _as_decimal(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _structural_observation(
    cur,
    *,
    pipeline_run_id: str,
    environment: str,
    deployment_id: str,
    git_revision: str,
    symbol: str,
    horizon: str,
    evidence_cutoff: datetime,
) -> dict[str, Any]:
    minutes = HORIZON_MINUTES[horizon]
    window_start = evidence_cutoff - timedelta(minutes=minutes)
    cur.execute(
        """
        SELECT open_time,close_time,open,high,low,close,volume
          FROM public.candles
         WHERE symbol=%s AND interval='5m'
           AND open_time >= %s AND open_time < %s
           AND close_time IS NOT NULL AND close_time <= %s
         ORDER BY open_time
        """,
        (symbol, window_start, evidence_cutoff, evidence_cutoff),
    )
    rows = cur.fetchall()
    expected = minutes // CADENCE_MINUTES
    timestamps = [row[0] for row in rows]
    expected_timestamps = [
        window_start + timedelta(minutes=CADENCE_MINUTES * index)
        for index in range(expected)
    ]
    complete = (
        len(rows) == expected
        and timestamps == expected_timestamps
        and all(row[1] is not None and row[1] <= evidence_cutoff for row in rows)
    )
    first_close = _as_decimal(rows[0][5]) if rows else None
    last_close = _as_decimal(rows[-1][5]) if rows else None
    high_price = max((_as_decimal(row[3]) for row in rows), default=None)
    low_price = min((_as_decimal(row[4]) for row in rows), default=None)
    directional_return = None
    range_pct = None
    drawdown = None
    range_position = None
    if first_close not in (None, Decimal("0")) and last_close is not None:
        directional_return = (last_close - first_close) / first_close * Decimal("100")
        if high_price is not None and low_price is not None:
            range_pct = (high_price - low_price) / first_close * Decimal("100")
    if high_price not in (None, Decimal("0")) and last_close is not None:
        drawdown = (high_price - last_close) / high_price * Decimal("100")
    if (
        high_price is not None and low_price is not None and last_close is not None
        and high_price != low_price
    ):
        range_position = (last_close - low_price) / (high_price - low_price)
    candle_source = [
        {
            "open_time": row[0], "close_time": row[1], "open": _as_decimal(row[2]),
            "high": _as_decimal(row[3]), "low": _as_decimal(row[4]),
            "close": _as_decimal(row[5]), "volume": _as_decimal(row[6]),
        }
        for row in rows
    ]
    source_fingerprint = fingerprint(candle_source)
    values = {
        "contract_version": STRUCTURAL_CONTRACT_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "environment": environment,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "horizon": horizon,
        "evidence_cutoff": evidence_cutoff,
        "window_start": window_start,
        "window_end": evidence_cutoff,
        "first_candle_ts": rows[0][0] if rows else None,
        "last_candle_ts": rows[-1][0] if rows else None,
        "first_close": first_close,
        "last_close": last_close,
        "high_price": high_price,
        "low_price": low_price,
        "directional_return_pct": directional_return,
        "range_pct": range_pct,
        "drawdown_from_high_pct": drawdown,
        "close_position_in_range": range_position,
        "candle_count": len(rows),
        "expected_candle_count": expected,
        "coverage_status": "COMPLETE" if complete else "INCOMPLETE",
        "source_max_ts": max((row[1] for row in rows), default=None),
        "source_version": STRUCTURAL_SOURCE_VERSION,
        "source_fingerprint": source_fingerprint,
        "git_revision": git_revision,
    }
    values["observation_id"] = _semantic_id(
        "STRUCTURAL_OBSERVATION", {
            "pipeline_run_id": pipeline_run_id, "symbol": symbol, "horizon": horizon,
        },
    )
    values["observation_fingerprint"] = fingerprint(values)
    return values


MME_COLUMNS = (
    "sequence_key", "sequence_type", "sequence_stage", "direction",
    "sequence_quality", "continuation_score", "reversal_score",
    "late_entry_risk", "orc_readiness_score", "orc_hint", "reason",
    "ranking_status", "action_hint", "first_event_at", "last_event_at",
    "expires_at", "refreshed_at", "payload",
)


def _mme_observation(
    cur,
    *,
    pipeline_run_id: str,
    environment: str,
    deployment_id: str,
    git_revision: str,
    symbol: str,
    interval: str,
    evidence_cutoff: datetime,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT sequence_key,sequence_type,sequence_stage,direction,
               sequence_quality,continuation_score,reversal_score,
               late_entry_risk,orc_readiness_score,orc_hint,reason,
               ranking_status,action_hint,first_event_at,last_event_at,
               expires_at,refreshed_at,payload
          FROM public.market_memory_sequence
         WHERE symbol=%s AND interval=%s
         ORDER BY refreshed_at DESC
         LIMIT 1
        """,
        (symbol, interval),
    )
    row = cur.fetchone()
    source = dict(zip(MME_COLUMNS, row)) if row is not None else {}
    if not source or (
        source.get("expires_at") is not None
        and source["expires_at"] < evidence_cutoff
    ):
        availability = "ABSENT"
        source = {}
    elif any(
        source.get(field) is not None and source[field] > evidence_cutoff
        for field in ("first_event_at", "last_event_at", "refreshed_at")
    ):
        availability = "FUTURE_SOURCE"
    else:
        availability = "AVAILABLE"
    semantic_source = {
        "symbol": symbol,
        "interval": interval,
        "availability_state": availability,
        **{key: source.get(key) for key in MME_COLUMNS},
    }
    source_fingerprint = fingerprint(semantic_source)
    values = {
        "contract_version": MME_OBSERVATION_CONTRACT_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "environment": environment,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "interval": interval,
        "evidence_cutoff": evidence_cutoff,
        "availability_state": availability,
        **{key: source.get(key) for key in MME_COLUMNS[:-1]},
        "source_payload": source.get("payload"),
        "source_version": MME_SOURCE_VERSION,
        "source_fingerprint": source_fingerprint,
        "git_revision": git_revision,
    }
    values["source_refreshed_at"] = values.pop("refreshed_at", None)
    values["observation_id"] = _semantic_id(
        "MME_SEQUENCE_OBSERVATION", {
            "pipeline_run_id": pipeline_run_id,
            "symbol": symbol,
            "interval": interval,
        },
    )
    values["observation_fingerprint"] = fingerprint(values)
    return values


MME_TRANSITION_FIELDS = (
    "availability_state", "sequence_key", "sequence_type", "sequence_stage", "direction",
    "sequence_quality", "continuation_score", "reversal_score",
    "late_entry_risk", "orc_readiness_score", "orc_hint", "ranking_status",
    "action_hint",
)


def _prior_mme(cur, values: Mapping[str, Any]) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT observation_id,availability_state,sequence_key,sequence_type,sequence_stage,
               direction,sequence_quality,continuation_score,reversal_score,
               late_entry_risk,orc_readiness_score,orc_hint,ranking_status,
               action_hint,source_fingerprint
          FROM public.thesis_mme_sequence_observation_v1
         WHERE environment=%s AND deployment_id=%s AND symbol=%s AND interval=%s
           AND evidence_cutoff < %s
         ORDER BY evidence_cutoff DESC,created_at DESC
         LIMIT 1
        """,
        (
            values["environment"], values["deployment_id"], values["symbol"],
            values["interval"], values["evidence_cutoff"],
        ),
    )
    row = cur.fetchone()
    if row is None:
        return None
    names = ("observation_id", *MME_TRANSITION_FIELDS, "source_fingerprint")
    return dict(zip(names, row))


def _mme_transition(
    prior: Mapping[str, Any] | None,
    current: Mapping[str, Any],
) -> dict[str, Any] | None:
    changed = [
        field for field in MME_TRANSITION_FIELDS
        if prior is None or canonical_value(prior.get(field)) != canonical_value(current.get(field))
    ]
    if prior is not None and not changed:
        return None
    availability = current["availability_state"]
    prior_availability = None if prior is None else prior["availability_state"]
    if availability == "ABSENT":
        category = "SOURCE_ABSENT"
    elif prior is None or prior_availability == "ABSENT":
        category = "SOURCE_APPEARED"
    elif "direction" in changed:
        category = "DIRECTION_CHANGED"
    elif "sequence_type" in changed:
        category = "TYPE_CHANGED"
    elif "sequence_stage" in changed:
        category = "STAGE_CHANGED"
    else:
        category = "SOURCE_CHANGED"
    values = {
        "contract_version": MME_TRANSITION_CONTRACT_VERSION,
        "pipeline_run_id": current["pipeline_run_id"],
        "environment": current["environment"],
        "deployment_id": current["deployment_id"],
        "symbol": current["symbol"],
        "interval": current["interval"],
        "evidence_cutoff": current["evidence_cutoff"],
        "previous_observation_id": None if prior is None else prior["observation_id"],
        "current_observation_id": current["observation_id"],
        "transition_category": category,
        "changed_fields": changed,
        "git_revision": current["git_revision"],
    }
    values["transition_id"] = _semantic_id(
        "MME_SEQUENCE_TRANSITION", {
            "previous_observation_id": values["previous_observation_id"],
            "current_observation_id": values["current_observation_id"],
            "transition_category": category,
        },
    )
    values["transition_fingerprint"] = fingerprint(values)
    return values


def _tactical_set(
    cur,
    *,
    pipeline_run_id: str,
    environment: str,
    deployment_id: str,
    git_revision: str,
    symbol: str,
    evidence_cutoff: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    window_start = evidence_cutoff - timedelta(minutes=CADENCE_MINUTES)
    cur.execute(
        """
        SELECT se.id,se.strategy,se.symbol,se.interval,se.created_at,
               se.candle_open_time,se.reason,se.run_id,se.info,
               decision_row.decision_id,decision_row.engine_version,
               decision_row.schema_version,decision_row.observed_at
          FROM public.strategy_events se
          LEFT JOIN LATERAL (
              SELECT d.decision_id,d.engine_version,d.schema_version,d.observed_at
                FROM public.decision_registry_v1 d
               WHERE d.environment=%s AND d.deployment_id=%s
                 AND d.decision_type='ENTRY_DECISION'
                 AND d.symbol=se.symbol AND d.interval=se.interval
                 AND d.strategy=se.strategy
                 AND d.decision_timestamp=se.candle_open_time
                 AND d.observed_at<=%s
               ORDER BY d.observed_at DESC
               LIMIT 1
          ) decision_row ON TRUE
         WHERE se.symbol=%s
           AND se.created_at>%s AND se.created_at<=%s
           AND se.event_type='SIGNAL' AND se.decision='BUY' AND se.reason='OK'
         ORDER BY se.strategy,se.interval,se.candle_open_time,se.created_at,se.id
        """,
        (
            environment, deployment_id, evidence_cutoff, symbol,
            window_start, evidence_cutoff,
        ),
    )
    members_by_identity: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        (
            _event_id, strategy, row_symbol, interval, signal_created_at,
            candle_timestamp, signal_reason, run_id, info, decision_id,
            engine_version, schema_version, decision_observed_at,
        ) = row
        source_version = (
            str(engine_version or schema_version)
            if engine_version or schema_version
            else "UNVERSIONED_LEGACY_SOURCE"
        )
        semantic_source = {
            "environment": environment,
            "deployment_id": deployment_id,
            "strategy": strategy,
            "symbol": row_symbol,
            "interval": interval,
            "direction": "LONG",
            "decision_timestamp": decision_observed_at or signal_created_at,
            "decision_candle_timestamp": candle_timestamp,
            "signal_reason": signal_reason,
            "run_id": run_id,
            "info": info or {},
            "decision_id": decision_id,
            "source_version": source_version,
        }
        opportunity_identity = _semantic_id("TACTICAL_OPPORTUNITY", semantic_source)
        members_by_identity[opportunity_identity] = {
            "opportunity_identity": opportunity_identity,
            "decision_id": decision_id,
            "strategy": strategy,
            "symbol": row_symbol,
            "interval": interval,
            "direction": "LONG",
            "decision_timestamp": decision_observed_at or signal_created_at,
            "decision_candle_timestamp": candle_timestamp,
            "signal_reason": signal_reason,
            "source_version": source_version,
            "source_fingerprint": fingerprint(semantic_source),
            "git_revision": git_revision,
        }
    members = [members_by_identity[key] for key in sorted(members_by_identity)]
    completeness = (
        "INCOMPLETE"
        if any(member["source_version"] == "UNVERSIONED_LEGACY_SOURCE" for member in members)
        else "COMPLETE"
    )
    set_fingerprint = fingerprint([
        {
            key: member[key]
            for key in (
                "opportunity_identity", "decision_id", "strategy", "symbol",
                "interval", "direction", "decision_timestamp",
                "decision_candle_timestamp", "signal_reason", "source_version",
                "source_fingerprint",
            )
        }
        for member in members
    ])
    values = {
        "contract_version": TACTICAL_SET_CONTRACT_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "environment": environment,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "evidence_cutoff": evidence_cutoff,
        "observation_window_start": window_start,
        "observation_window_end": evidence_cutoff,
        "member_count": len(members),
        "completeness_status": completeness,
        "source_version": TACTICAL_SOURCE_VERSION,
        "set_fingerprint": set_fingerprint,
        "git_revision": git_revision,
    }
    values["tactical_set_id"] = _semantic_id(
        "TACTICAL_OPPORTUNITY_SET", {
            "pipeline_run_id": pipeline_run_id, "symbol": symbol,
        },
    )
    return values, members


def _insert_immutable(
    cur,
    *,
    table: str,
    id_column: str,
    identity: str,
    fingerprint_column: str,
    expected_fingerprint: str,
    columns: tuple[str, ...],
    values: Mapping[str, Any],
    json_columns: frozenset[str] = frozenset(),
) -> bool:
    placeholders = ",".join(["%s"] * len(columns))
    rendered_values = [
        _json(values[column]) if column in json_columns else values[column]
        for column in columns
    ]
    cur.execute(
        f"INSERT INTO public.{table}({','.join(columns)}) "
        f"VALUES({placeholders}) ON CONFLICT ({id_column}) DO NOTHING",
        rendered_values,
    )
    if cur.rowcount == 1:
        return True
    cur.execute(
        f"SELECT {fingerprint_column} FROM public.{table} WHERE {id_column}=%s",
        (identity,),
    )
    row = cur.fetchone()
    if row is None or row[0] != expected_fingerprint:
        raise RuntimeError(f"THESIS_EVIDENCE_FINGERPRINT_CONFLICT:{table}:{identity}")
    return False


def _configured_universe(cur) -> dict[str, list[str]]:
    cur.execute(
        """
        SELECT symbol,interval
          FROM public.bot_control
         WHERE enabled=true AND symbol IS NOT NULL AND interval IS NOT NULL
         ORDER BY symbol,interval
        """
    )
    universe: dict[str, list[str]] = {}
    for symbol, interval in cur.fetchall():
        universe.setdefault(str(symbol), [])
        if str(interval) not in universe[str(symbol)]:
            universe[str(symbol)].append(str(interval))
    return universe


def _build_cycle(
    cur,
    *,
    environment: str,
    deployment_id: str,
    git_revision: str,
    evidence_cutoff: datetime,
) -> dict[str, Any]:
    manifest = source_version_manifest()
    for producer in manifest.values():
        producer["git_revision"] = git_revision
    pipeline_run_id = deterministic_pipeline_run_id(
        environment, deployment_id, evidence_cutoff, manifest,
    )
    universe = _configured_universe(cur)
    bundles: list[dict[str, Any]] = []
    all_missing: set[str] = set()
    for symbol, intervals in universe.items():
        structural = {
            horizon: _structural_observation(
                cur,
                pipeline_run_id=pipeline_run_id,
                environment=environment,
                deployment_id=deployment_id,
                git_revision=git_revision,
                symbol=symbol,
                horizon=horizon,
                evidence_cutoff=evidence_cutoff,
            )
            for horizon in HORIZON_MINUTES
        }
        mme = [
            _mme_observation(
                cur,
                pipeline_run_id=pipeline_run_id,
                environment=environment,
                deployment_id=deployment_id,
                git_revision=git_revision,
                symbol=symbol,
                interval=interval,
                evidence_cutoff=evidence_cutoff,
            )
            for interval in intervals
        ]
        transitions = [
            transition
            for observation in mme
            for transition in [_mme_transition(_prior_mme(cur, observation), observation)]
            if transition is not None
        ]
        tactical_set, tactical_members = _tactical_set(
            cur,
            pipeline_run_id=pipeline_run_id,
            environment=environment,
            deployment_id=deployment_id,
            git_revision=git_revision,
            symbol=symbol,
            evidence_cutoff=evidence_cutoff,
        )
        missing: list[str] = []
        for horizon, observation in structural.items():
            if observation["coverage_status"] != "COMPLETE":
                missing.append(f"STRUCTURAL_{horizon.upper()}_INCOMPLETE")
            if (
                observation["source_max_ts"] is not None
                and observation["source_max_ts"] > evidence_cutoff
            ):
                missing.append(f"STRUCTURAL_{horizon.upper()}_FUTURE_SOURCE")
        for observation in mme:
            if observation["availability_state"] == "FUTURE_SOURCE":
                missing.append(f"MME_FUTURE_SOURCE:{observation['interval']}")
        if tactical_set["completeness_status"] != "COMPLETE":
            missing.append("TACTICAL_SOURCE_VERSION_UNKNOWN")
        missing = sorted(set(missing))
        all_missing.update(f"{symbol}:{item}" for item in missing)
        source_timestamps = {
            "structural": {
                horizon: observation["source_max_ts"]
                for horizon, observation in structural.items()
            },
            "mme": [
                {
                    "interval": observation["interval"],
                    "first_event_at": observation["first_event_at"],
                    "last_event_at": observation["last_event_at"],
                    "source_refreshed_at": observation["source_refreshed_at"],
                }
                for observation in mme
            ],
            "tactical_window_end": tactical_set["observation_window_end"],
        }
        bundle_values = {
            "contract_version": CONTRACT_VERSION,
            "pipeline_run_id": pipeline_run_id,
            "environment": environment,
            "deployment_id": deployment_id,
            "symbol": symbol,
            "direction_scope": "LONG_ONLY_OBSERVATION",
            "evidence_cutoff": evidence_cutoff,
            "evidence_status": evidence_status(missing),
            "missing_sources": missing,
            "structural_6h_id": structural["6h"]["observation_id"],
            "structural_24h_id": structural["24h"]["observation_id"],
            "structural_3d_id": structural["3d"]["observation_id"],
            "mme_observation_ids": sorted(item["observation_id"] for item in mme),
            "mme_transition_ids": sorted(item["transition_id"] for item in transitions),
            "tactical_set_id": tactical_set["tactical_set_id"],
            "source_version_manifest": manifest,
            "source_timestamps": source_timestamps,
            "git_revision": git_revision,
        }
        bundle_values["bundle_id"] = _semantic_id(
            "THESIS_EVIDENCE_BUNDLE", {
                "pipeline_run_id": pipeline_run_id, "symbol": symbol,
            },
        )
        bundle_values["bundle_fingerprint"] = fingerprint({
            **bundle_values,
            "structural_fingerprints": {
                horizon: observation["observation_fingerprint"]
                for horizon, observation in structural.items()
            },
            "mme_fingerprints": [
                observation["observation_fingerprint"] for observation in mme
            ],
            "transition_fingerprints": [
                transition["transition_fingerprint"] for transition in transitions
            ],
            "tactical_set_fingerprint": tactical_set["set_fingerprint"],
        })
        bundles.append({
            "structural": structural,
            "mme": mme,
            "transitions": transitions,
            "tactical_set": tactical_set,
            "tactical_members": tactical_members,
            "bundle": bundle_values,
        })
    if not universe:
        all_missing.add("UNIVERSE_EMPTY")
    run_values = {
        "pipeline_run_id": pipeline_run_id,
        "contract_version": PIPELINE_CONTRACT_VERSION,
        "environment": environment,
        "deployment_id": deployment_id,
        "evidence_cutoff": evidence_cutoff,
        "evidence_status": evidence_status(sorted(all_missing)),
        "missing_sources": sorted(all_missing),
        "source_version_manifest": manifest,
        "git_revision": git_revision,
    }
    run_values["run_fingerprint"] = fingerprint(run_values)
    return {"run": run_values, "bundles": bundles, "manifest": manifest}


def _persist_cycle(cur, cycle: Mapping[str, Any]) -> dict[str, int]:
    counts = {
        "pipeline_runs": 0, "structural": 0, "mme_observations": 0,
        "mme_transitions": 0, "tactical_sets": 0, "tactical_members": 0,
        "bundles": 0, "cutovers": 0,
    }
    run = cycle["run"]
    run_columns = (
        "pipeline_run_id", "contract_version", "environment", "deployment_id",
        "evidence_cutoff", "evidence_status", "missing_sources",
        "source_version_manifest", "run_fingerprint", "git_revision",
    )
    counts["pipeline_runs"] += int(_insert_immutable(
        cur, table="thesis_evidence_pipeline_run_v1",
        id_column="pipeline_run_id", identity=run["pipeline_run_id"],
        fingerprint_column="run_fingerprint",
        expected_fingerprint=run["run_fingerprint"], columns=run_columns, values=run,
        json_columns=frozenset({"missing_sources", "source_version_manifest"}),
    ))
    for item in cycle["bundles"]:
        for observation in item["structural"].values():
            columns = tuple(key for key in observation if key != "created_at")
            counts["structural"] += int(_insert_immutable(
                cur, table="thesis_structural_observation_v1",
                id_column="observation_id", identity=observation["observation_id"],
                fingerprint_column="observation_fingerprint",
                expected_fingerprint=observation["observation_fingerprint"],
                columns=columns, values=observation,
            ))
        for observation in item["mme"]:
            columns = tuple(key for key in observation if key != "created_at")
            counts["mme_observations"] += int(_insert_immutable(
                cur, table="thesis_mme_sequence_observation_v1",
                id_column="observation_id", identity=observation["observation_id"],
                fingerprint_column="observation_fingerprint",
                expected_fingerprint=observation["observation_fingerprint"],
                columns=columns, values=observation,
                json_columns=frozenset({"source_payload"}),
            ))
        for transition in item["transitions"]:
            columns = tuple(key for key in transition if key != "created_at")
            counts["mme_transitions"] += int(_insert_immutable(
                cur, table="thesis_mme_transition_observation_v1",
                id_column="transition_id", identity=transition["transition_id"],
                fingerprint_column="transition_fingerprint",
                expected_fingerprint=transition["transition_fingerprint"],
                columns=columns, values=transition,
                json_columns=frozenset({"changed_fields"}),
            ))
        tactical_set = item["tactical_set"]
        columns = tuple(key for key in tactical_set if key != "created_at")
        counts["tactical_sets"] += int(_insert_immutable(
            cur, table="thesis_tactical_opportunity_set_v1",
            id_column="tactical_set_id", identity=tactical_set["tactical_set_id"],
            fingerprint_column="set_fingerprint",
            expected_fingerprint=tactical_set["set_fingerprint"],
            columns=columns, values=tactical_set,
        ))
        for member in item["tactical_members"]:
            member_values = {**member, "tactical_set_id": tactical_set["tactical_set_id"]}
            columns = tuple(member_values)
            counts["tactical_members"] += int(_insert_immutable(
                cur, table="thesis_tactical_opportunity_member_v1",
                id_column="opportunity_identity",
                identity=member["opportunity_identity"],
                fingerprint_column="source_fingerprint",
                expected_fingerprint=member["source_fingerprint"],
                columns=columns, values=member_values,
            ))
        bundle = item["bundle"]
        columns = tuple(key for key in bundle if key != "created_at")
        counts["bundles"] += int(_insert_immutable(
            cur, table="thesis_evidence_bundle_v1",
            id_column="bundle_id", identity=bundle["bundle_id"],
            fingerprint_column="bundle_fingerprint",
            expected_fingerprint=bundle["bundle_fingerprint"],
            columns=columns, values=bundle,
            json_columns=frozenset({
                "missing_sources", "mme_observation_ids", "mme_transition_ids",
                "source_version_manifest", "source_timestamps",
            }),
        ))
    cur.execute(
        """
        SELECT cutover_id FROM public.thesis_evidence_bundle_cutover_v1
         WHERE contract_version=%s AND environment=%s AND deployment_id=%s
        """,
        (CUTOVER_CONTRACT_VERSION, run["environment"], run["deployment_id"]),
    )
    if cur.fetchone() is None:
        cutover = {
            "contract_version": CUTOVER_CONTRACT_VERSION,
            "effective_timestamp": run["evidence_cutoff"],
            "environment": run["environment"],
            "deployment_id": run["deployment_id"],
            "git_revision": run["git_revision"],
            "first_eligible_pipeline_run_id": run["pipeline_run_id"],
            "first_eligible_evidence_cutoff": run["evidence_cutoff"],
            "source_version_manifest": cycle["manifest"],
            "rollout_mode": "SHADOW",
        }
        cutover["cutover_id"] = _semantic_id(
            "THESIS_EVIDENCE_CUTOVER", {
                "contract_version": CUTOVER_CONTRACT_VERSION,
                "environment": run["environment"],
                "deployment_id": run["deployment_id"],
            },
        )
        cutover["cutover_fingerprint"] = fingerprint(cutover)
        columns = tuple(cutover)
        counts["cutovers"] += int(_insert_immutable(
            cur, table="thesis_evidence_bundle_cutover_v1",
            id_column="cutover_id", identity=cutover["cutover_id"],
            fingerprint_column="cutover_fingerprint",
            expected_fingerprint=cutover["cutover_fingerprint"],
            columns=columns, values=cutover,
            json_columns=frozenset({"source_version_manifest"}),
        ))
    return counts


def capture_thesis_evidence_bundle_cycle(
    connection_factory: Callable[[], Any] = get_db_conn,
    *,
    evaluated_at: datetime | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Capture one deterministic shadow cycle; retries are verified no-ops."""
    environment, deployment_id, git_revision = runtime_provenance(environ)
    cutoff = canonical_evidence_cutoff(evaluated_at or datetime.now(timezone.utc))
    conn = connection_factory()
    try:
        conn.set_session(isolation_level="REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.thesis_evidence_bundle_v1')")
            if cur.fetchone()[0] is None:
                conn.rollback()
                return {"status": "SCHEMA_NOT_READY", "evidence_cutoff": cutoff}
            cycle = _build_cycle(
                cur,
                environment=environment,
                deployment_id=deployment_id,
                git_revision=git_revision,
                evidence_cutoff=cutoff,
            )
            counts = _persist_cycle(cur, cycle)
        conn.commit()
        return {
            "status": "CAPTURED",
            "pipeline_run_id": cycle["run"]["pipeline_run_id"],
            "evidence_cutoff": cutoff,
            "evidence_status": cycle["run"]["evidence_status"],
            "missing_sources": cycle["run"]["missing_sources"],
            "symbols": len(cycle["bundles"]),
            **counts,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
