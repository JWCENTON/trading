"""Immutable LIVE exit-cost evidence used by canonical Open Risk."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Mapping
import uuid


CONTRACT_VERSION = "LIVE_EXIT_COST_AUTHORITY_V1"
SOURCE = "OKX_API_V5_ACCOUNT_TRADE_FEE"
RAW_SIGN_SEMANTICS = "OKX_SIGNED_RATE_NEGATIVE_MEANS_COST"
CANONICAL_SIGN_SEMANTICS = "POSITIVE_DECIMAL_COST_RATE"
# Reuses the repository's existing OKX account-metadata refresh convention.
FRESHNESS = timedelta(hours=24)
SNAPSHOT_NAMESPACE = uuid.UUID("0927f771-cd03-4692-88ee-af3e8a0339db")
LINK_NAMESPACE = uuid.UUID("e1557134-cd33-445c-8b56-4dc1a595aaf7")


def _decimal(value: object, field: str) -> Decimal:
    if value in (None, "") or isinstance(value, float):
        raise ValueError(f"LIVE_EXIT_COST_INVALID_DECIMAL:{field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"LIVE_EXIT_COST_INVALID_DECIMAL:{field}") from exc
    if not result.is_finite():
        raise ValueError(f"LIVE_EXIT_COST_INVALID_DECIMAL:{field}")
    return result


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("LIVE_EXIT_COST_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    if isinstance(value, float):
        raise ValueError("LIVE_EXIT_COST_FLOAT_FORBIDDEN")
    return value


def fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class LiveExitCostSnapshot:
    exit_cost_snapshot_id: uuid.UUID
    environment: str
    deployment_id: str
    account_identity_fingerprint: str
    instrument_type: str
    symbol: str
    fee_role: str
    canonical_fee_rate: Decimal
    raw_fee_rate: Decimal
    raw_sign_semantics: str
    rule_type: str | None
    account_level: str | None
    observed_at: datetime
    effective_at: datetime
    expires_at: datetime
    source: str
    contract_version: str
    source_evidence_fingerprint: str
    snapshot_fingerprint: str


def parse_okx_trade_fee_response(
    payload: Mapping[str, Any], *, deployment_id: str,
    account_identity_fingerprint: str, symbol: str, observed_at: datetime,
) -> LiveExitCostSnapshot:
    if observed_at.tzinfo is None:
        raise ValueError("LIVE_EXIT_COST_TIMESTAMP_REQUIRED")
    normalized_deployment = str(deployment_id).lower()
    if normalized_deployment not in {"local-live", "vps-live"}:
        raise ValueError("LIVE_EXIT_COST_DEPLOYMENT_MISMATCH")
    identity = str(account_identity_fingerprint)
    if len(identity) != 64 or any(ch not in "0123456789abcdef" for ch in identity):
        raise ValueError("LIVE_EXIT_COST_ACCOUNT_IDENTITY_INVALID")
    if str(payload.get("code")) != "0":
        raise ValueError("LIVE_EXIT_COST_OKX_RESPONSE_NOT_SUCCESS")
    rows = payload.get("data")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise ValueError("LIVE_EXIT_COST_OKX_FEE_ROW_NOT_EXACT")
    row = rows[0]
    instrument_type = str(row.get("instType") or "").upper()
    if instrument_type != "SPOT":
        raise ValueError("LIVE_EXIT_COST_INSTRUMENT_MISMATCH")
    raw = _decimal(row.get("taker"), "taker")
    canonical = abs(raw)
    if canonical < Decimal("0") or canonical > Decimal("0.10"):
        raise ValueError("LIVE_EXIT_COST_RATE_OUT_OF_RANGE")
    normalized_symbol = str(symbol).upper().replace("-", "")
    response_symbol = str(row.get("instId") or "").upper().replace("-", "")
    if response_symbol and response_symbol != normalized_symbol:
        raise ValueError("LIVE_EXIT_COST_INSTRUMENT_MISMATCH")
    observed = observed_at.astimezone(timezone.utc)
    source_payload = {
        "account_level": row.get("level"),
        "delivery": row.get("delivery"),
        "exercise": row.get("exercise"),
        "inst_type": instrument_type,
        "maker": row.get("maker"),
        "rule_type": row.get("ruleType"),
        "symbol": normalized_symbol,
        "taker": raw,
        "ts": row.get("ts"),
    }
    source_fp = fingerprint(source_payload)
    semantic = {
        "environment": "LIVE",
        "deployment_id": normalized_deployment,
        "account_identity_fingerprint": identity,
        "instrument_type": instrument_type,
        "symbol": normalized_symbol,
        "fee_role": "TAKER",
        "canonical_fee_rate": canonical,
        "raw_fee_rate": raw,
        "raw_sign_semantics": RAW_SIGN_SEMANTICS,
        "rule_type": row.get("ruleType"),
        "account_level": row.get("level"),
        "observed_at": observed,
        "effective_at": observed,
        "expires_at": observed + FRESHNESS,
        "source": SOURCE,
        "contract_version": CONTRACT_VERSION,
        "source_evidence_fingerprint": source_fp,
    }
    snapshot_fp = fingerprint(semantic)
    return LiveExitCostSnapshot(
        exit_cost_snapshot_id=uuid.uuid5(SNAPSHOT_NAMESPACE, snapshot_fp),
        snapshot_fingerprint=snapshot_fp, **semantic,
    )


def capture_okx_exit_cost_snapshot_cursor(
    cur: Any, *, exchange_client: Any, deployment_id: str,
    account_identity_fingerprint: str, symbol: str, observed_at: datetime,
) -> tuple[str, LiveExitCostSnapshot]:
    payload = exchange_client.get_trade_fee(symbol=symbol, instrument_type="SPOT")
    snapshot = parse_okx_trade_fee_response(
        payload, deployment_id=deployment_id,
        account_identity_fingerprint=account_identity_fingerprint,
        symbol=symbol, observed_at=observed_at,
    )
    values = (
        str(snapshot.exit_cost_snapshot_id), snapshot.environment,
        snapshot.deployment_id, snapshot.account_identity_fingerprint,
        snapshot.instrument_type, snapshot.symbol, snapshot.fee_role,
        snapshot.canonical_fee_rate, snapshot.raw_fee_rate,
        snapshot.raw_sign_semantics, snapshot.rule_type,
        snapshot.account_level, snapshot.observed_at, snapshot.effective_at,
        snapshot.expires_at, snapshot.source, snapshot.contract_version,
        snapshot.source_evidence_fingerprint, snapshot.snapshot_fingerprint,
    )
    cur.execute(
        "INSERT INTO live_exit_cost_snapshot_v1("
        "exit_cost_snapshot_id,environment,deployment_id,"
        "account_identity_fingerprint,instrument_type,symbol,fee_role,"
        "canonical_fee_rate,raw_fee_rate,raw_sign_semantics,rule_type,"
        "account_level,observed_at,effective_at,expires_at,source,"
        "contract_version,source_evidence_fingerprint,snapshot_fingerprint) "
        "VALUES (" + ",".join(["%s"] * len(values)) + ") "
        "ON CONFLICT(exit_cost_snapshot_id) DO NOTHING RETURNING exit_cost_snapshot_id",
        values,
    )
    return ("INSERTED" if cur.fetchone() else "IDEMPOTENT"), snapshot


def exit_cost_schema_available_cursor(cur: Any) -> bool:
    cur.execute(
        "SELECT to_regclass('public.live_exit_cost_snapshot_v1'),"
        "to_regclass('public.live_position_exit_cost_link_v1')"
    )
    row = cur.fetchone()
    return bool(row and row[0] is not None and row[1] is not None)


def link_latest_exit_cost_snapshot_cursor(
    cur: Any, *, position_id: int, boundary_id: uuid.UUID,
    deployment_id: str, account_identity_fingerprint: str, symbol: str,
    effective_at: datetime,
) -> str:
    if not exit_cost_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    cur.execute(
        "SELECT exit_cost_snapshot_id FROM live_exit_cost_snapshot_v1 "
        "WHERE environment='LIVE' AND deployment_id=%s "
        "AND account_identity_fingerprint=%s AND instrument_type='SPOT' "
        "AND symbol=%s AND fee_role='TAKER' AND effective_at<=%s "
        "AND expires_at>%s ORDER BY effective_at DESC,created_at DESC LIMIT 1",
        (str(deployment_id).lower(), str(account_identity_fingerprint),
         str(symbol).upper().replace("-", ""), effective_at, effective_at),
    )
    row = cur.fetchone()
    if row is None:
        return "MISSING_FEE_EVIDENCE"
    snapshot_id = uuid.UUID(str(row[0]))
    link_fp = fingerprint({
        "position_id": int(position_id), "boundary_id": boundary_id,
        "exit_cost_snapshot_id": snapshot_id,
        "effective_at": effective_at, "contract_version": CONTRACT_VERSION,
    })
    link_id = uuid.uuid5(LINK_NAMESPACE, link_fp)
    cur.execute(
        "INSERT INTO live_position_exit_cost_link_v1("
        "link_id,position_id,boundary_id,exit_cost_snapshot_id,effective_at,"
        "link_fingerprint,contract_version) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT(position_id,boundary_id) DO NOTHING RETURNING link_id",
        (str(link_id), int(position_id), str(boundary_id), str(snapshot_id),
         effective_at, link_fp, CONTRACT_VERSION),
    )
    if cur.fetchone():
        return "INSERTED"
    cur.execute(
        "SELECT exit_cost_snapshot_id FROM live_position_exit_cost_link_v1 "
        "WHERE position_id=%s AND boundary_id=%s",
        (int(position_id), str(boundary_id)),
    )
    existing = cur.fetchone()
    return "IDEMPOTENT" if existing and uuid.UUID(str(existing[0])) == snapshot_id else "ALREADY_FROZEN"


def load_live_exit_cost_links_cursor(
    cur: Any, *, deployment_id: str, account_identity_fingerprint: str,
    as_of: datetime,
) -> dict[int, tuple[Decimal | None, str, str | None]]:
    if not exit_cost_schema_available_cursor(cur):
        return {}
    cur.execute(
        "SELECT l.position_id,s.canonical_fee_rate,s.contract_version,"
        "s.expires_at,s.account_identity_fingerprint,s.deployment_id,"
        "s.instrument_type,s.symbol,p.symbol "
        "FROM live_position_exit_cost_link_v1 l "
        "JOIN live_exit_cost_snapshot_v1 s USING(exit_cost_snapshot_id) "
        "JOIN positions p ON p.id=l.position_id "
        "WHERE s.environment='LIVE' AND s.deployment_id=%s",
        (str(deployment_id).lower(),),
    )
    result = {}
    for row in cur.fetchall():
        position_id = int(row[0])
        status = "CANONICAL"
        if str(row[4]) != str(account_identity_fingerprint):
            status = "ACCOUNT_IDENTITY_MISMATCH"
        elif str(row[5]) != str(deployment_id).lower():
            status = "DEPLOYMENT_MISMATCH"
        elif str(row[6]) != "SPOT" or str(row[7]) != str(row[8]).upper().replace("-", ""):
            status = "INSTRUMENT_MISMATCH"
        elif row[3] <= as_of:
            status = "STALE_FEE_EVIDENCE"
        result[position_id] = (
            _decimal(row[1], "canonical_fee_rate") if status == "CANONICAL" else None,
            status, str(row[2]),
        )
    return result
