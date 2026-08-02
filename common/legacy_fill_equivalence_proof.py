from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from common.exchange_fill_change_control import (
    FillApplicationClassification,
    FillMutationDecision,
    InventoryRowGeneration,
    RegisteredFillChange,
    _resolve_row_generation,
    authoritative_fill_fingerprint,
)


PROOF_VERSION = "LEGACY_FILL_EQUIVALENCE_PROOF_V1"
PROOF_TYPE = "LEGACY_CANONICAL_OKX_EQUIVALENCE"
EQUIVALENCE_STATE = "PROVEN"
REPAIR_IMPACT = "NONE"
CREATED_BY = "legacy-fill-equivalence-proof-v1"
APPLY_ENABLE_ENV = "LEGACY_FILL_EQUIVALENCE_PROOF_APPLY_ENABLED"
EXPECTED_ENVIRONMENT = "LIVE"
EXPECTED_DATABASE = "trading_live"
EXPECTED_EXCHANGE = "OKX"
EXPECTED_ORCHESTRATOR_ROLE = "PROCESS_SUPERVISOR"
LOCAL_LIVE_DEPLOYMENT = "local-live"
VPS_LIVE_DEPLOYMENT = "vps-live"
SUPPORTED_DEPLOYMENTS = frozenset({
    LOCAL_LIVE_DEPLOYMENT,
    VPS_LIVE_DEPLOYMENT,
})
EXPECTED_INGESTION_IDS_BY_DEPLOYMENT = {
    LOCAL_LIVE_DEPLOYMENT: frozenset({8, 10, 12, 14, 16, 18, 19, 20}),
    VPS_LIVE_DEPLOYMENT: frozenset({41, 47}),
}
EXPECTED_POSITION_IDS_BY_DEPLOYMENT = {
    LOCAL_LIVE_DEPLOYMENT: frozenset({3079, 3081, 3082, 3084, 3085}),
    VPS_LIVE_DEPLOYMENT: frozenset({3094, 3096}),
}
FORBIDDEN_INGESTION_IDS_BY_DEPLOYMENT = {
    LOCAL_LIVE_DEPLOYMENT: frozenset({22, 23, 24, 25}),
    VPS_LIVE_DEPLOYMENT: frozenset(),
}
FORBIDDEN_DB_ORDER_IDS_BY_DEPLOYMENT = {
    LOCAL_LIVE_DEPLOYMENT: frozenset({3758, 3760, 3762}),
    VPS_LIVE_DEPLOYMENT: frozenset(),
}
FORBIDDEN_EXCHANGE_ORDER_IDS_BY_DEPLOYMENT = {
    LOCAL_LIVE_DEPLOYMENT: frozenset({"3789163681263689728"}),
    VPS_LIVE_DEPLOYMENT: frozenset(),
}

# Backward-compatible names for the original LOCAL LIVE cohort. They remain
# intentionally local-only; deployment-aware code uses the mappings above.
EXPECTED_INGESTION_IDS = EXPECTED_INGESTION_IDS_BY_DEPLOYMENT[
    LOCAL_LIVE_DEPLOYMENT
]
EXPECTED_POSITION_IDS = EXPECTED_POSITION_IDS_BY_DEPLOYMENT[
    LOCAL_LIVE_DEPLOYMENT
]
FORBIDDEN_INGESTION_IDS = FORBIDDEN_INGESTION_IDS_BY_DEPLOYMENT[
    LOCAL_LIVE_DEPLOYMENT
]
FORBIDDEN_DB_ORDER_IDS = FORBIDDEN_DB_ORDER_IDS_BY_DEPLOYMENT[
    LOCAL_LIVE_DEPLOYMENT
]
FORBIDDEN_EXCHANGE_ORDER_IDS = FORBIDDEN_EXCHANGE_ORDER_IDS_BY_DEPLOYMENT[
    LOCAL_LIVE_DEPLOYMENT
]


def _rows(cur) -> list[dict[str, Any]]:
    names = [column[0] for column in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def _one(cur) -> dict[str, Any] | None:
    rows = _rows(cur)
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError("EXPECTED_EXACTLY_ONE_ROW")
    return rows[0]


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def decimal_text(value: Any) -> str:
    number = _decimal(value)
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    )


def canonical_fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _event_time_ms(value: Any) -> int:
    if isinstance(value, datetime):
        current = value
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return int(current.timestamp() * 1000)
    return int(str(value))


def fill_semantic_payload(
    *, source: Any, account_identity_key: Any, symbol: Any,
    trade_id: Any, order_id: Any, side: Any, quantity: Any, price: Any,
    fee_quantity: Any, fee_currency: Any, event_time_ms: Any,
    canonical_local_fill_id: Any,
) -> dict[str, Any]:
    qty = _decimal(quantity)
    px = _decimal(price)
    return {
        "source": str(source).lower(),
        "account_identity_key": str(account_identity_key),
        "symbol": str(symbol).upper(),
        "trade_id": str(trade_id),
        "order_id": str(order_id),
        "side": str(side).upper(),
        "quantity": decimal_text(qty),
        "price": decimal_text(px),
        "quote_quantity": decimal_text(qty * px),
        "fee_quantity": decimal_text(fee_quantity),
        "fee_currency": str(fee_currency).upper(),
        "event_time_ms": _event_time_ms(event_time_ms),
        "canonical_local_fill_id": int(canonical_local_fill_id),
    }


@dataclass(frozen=True)
class ManifestProof:
    ingestion_id: int
    position_id: int
    exchange_order_id: str
    exchange_trade_id: str
    canonical_local_fill_id: int
    correction_revision: int
    latest_observed_fingerprint: str


@dataclass(frozen=True)
class ProofManifest:
    environment: str
    deployment_id: str
    database: str
    proofs: tuple[ManifestProof, ...]

    @classmethod
    def load(cls, path: str | Path) -> "ProofManifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if set(raw) != {
            "proof_version", "environment", "deployment_id", "database", "proofs",
        }:
            raise RuntimeError("MANIFEST_FIELDS_INVALID")
        if raw["proof_version"] != PROOF_VERSION:
            raise RuntimeError("MANIFEST_PROOF_VERSION_INVALID")
        rows = []
        for item in raw["proofs"]:
            if set(item) != {
                "ingestion_id", "position_id", "exchange_order_id",
                "exchange_trade_id", "canonical_local_fill_id",
                "correction_revision", "latest_observed_fingerprint",
            }:
                raise RuntimeError("MANIFEST_PROOF_FIELDS_INVALID")
            row = ManifestProof(
                int(item["ingestion_id"]), int(item["position_id"]),
                str(item["exchange_order_id"]), str(item["exchange_trade_id"]),
                int(item["canonical_local_fill_id"]),
                int(item["correction_revision"]),
                str(item["latest_observed_fingerprint"]),
            )
            if not re.fullmatch(r"[0-9a-f]{64}", row.latest_observed_fingerprint):
                raise RuntimeError("MANIFEST_FINGERPRINT_INVALID")
            rows.append(row)
        manifest = cls(
            str(raw["environment"]), str(raw["deployment_id"]),
            str(raw["database"]), tuple(sorted(rows, key=lambda item: item.ingestion_id)),
        )
        if manifest.environment != EXPECTED_ENVIRONMENT:
            raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
        if manifest.database != EXPECTED_DATABASE:
            raise RuntimeError("DATABASE_IDENTITY_MISMATCH")
        validate_manifest_cohort(manifest)
        return manifest


def validate_manifest_cohort(manifest: ProofManifest) -> None:
    if manifest.environment != EXPECTED_ENVIRONMENT:
        raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
    if manifest.database != EXPECTED_DATABASE:
        raise RuntimeError("DATABASE_IDENTITY_MISMATCH")
    deployment_id = str(manifest.deployment_id)
    if deployment_id not in SUPPORTED_DEPLOYMENTS:
        raise RuntimeError("DEPLOYMENT_IDENTITY_MISMATCH")
    expected_ingestion_ids = EXPECTED_INGESTION_IDS_BY_DEPLOYMENT[
        deployment_id
    ]
    expected_position_ids = EXPECTED_POSITION_IDS_BY_DEPLOYMENT[deployment_id]
    if (
        {row.ingestion_id for row in manifest.proofs}
        != expected_ingestion_ids
        or {row.position_id for row in manifest.proofs}
        != expected_position_ids
        or len(manifest.proofs) != len(expected_ingestion_ids)
    ):
        raise RuntimeError("UNEXPECTED_PROOF_COHORT")
    if any(row.correction_revision != 2 for row in manifest.proofs):
        raise RuntimeError("CORRECTION_REVISION_INVALID")
    forbidden_ingestion_ids = FORBIDDEN_INGESTION_IDS_BY_DEPLOYMENT[
        deployment_id
    ]
    forbidden_exchange_order_ids = (
        FORBIDDEN_EXCHANGE_ORDER_IDS_BY_DEPLOYMENT[deployment_id]
    )
    if any(
        row.ingestion_id in forbidden_ingestion_ids
        or row.exchange_order_id in forbidden_exchange_order_ids
        for row in manifest.proofs
    ):
        raise RuntimeError("QUARANTINED_UNATTRIBUTABLE_INCIDENT")


@dataclass(frozen=True)
class RuntimeIdentity:
    exchange: str
    trading_mode: str
    deployment_id: str
    git_sha: str
    orchestrator_role: str


class DockerRuntimeIdentityProbe:
    bot_container = "trading-live-bot-runner-1"
    orchestrator_container = "trading-live-bot-runner-orchestrator-1"

    @staticmethod
    def _run(command: list[str]) -> str:
        result = subprocess.run(
            command, check=True, text=True, capture_output=True,
        )
        return result.stdout + result.stderr

    def read(self, *, repository: str | Path) -> RuntimeIdentity:
        raw_env = self._run([
            "docker", "inspect", "--format",
            "{{range .Config.Env}}{{println .}}{{end}}", self.bot_container,
        ])
        selected: dict[str, str] = {}
        for line in raw_env.splitlines():
            key, separator, value = line.partition("=")
            if separator and key in {"EXCHANGE", "TRADING_MODE", "DEPLOYMENT_ID"}:
                selected[key] = value
        logs = self._run([
            "docker", "logs", "--tail", "300", self.orchestrator_container,
        ])
        role = (
            EXPECTED_ORCHESTRATOR_ROLE
            if "reconciliation_role=PROCESS_SUPERVISOR" in logs
            else "UNKNOWN"
        )
        git_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=str(repository), check=True,
            text=True, capture_output=True,
        ).stdout.strip()
        return RuntimeIdentity(
            selected.get("EXCHANGE", ""), selected.get("TRADING_MODE", ""),
            selected.get("DEPLOYMENT_ID", ""), git_sha, role,
        )


class ExchangeEvidence(Protocol):
    place_order_calls: int
    cancel_order_calls: int

    def pending_spot_orders(self) -> tuple[Mapping[str, Any], ...]: ...
    def order(self, symbol: str, order_id: str) -> Mapping[str, Any]: ...
    def fills(self, symbol: str, order_id: str) -> tuple[Mapping[str, Any], ...]: ...


class OkxReadOnlyEvidenceClient:
    place_order_calls = 0
    cancel_order_calls = 0

    def __init__(self):
        from common.exchange_client import OkxMarketDataAdapter

        self.client = OkxMarketDataAdapter()

    def pending_spot_orders(self) -> tuple[Mapping[str, Any], ...]:
        result = self.client._private_request(
            "GET", "/api/v5/trade/orders-pending", params={"instType": "SPOT"},
        )
        return tuple(result.get("data") or ())

    def order(self, symbol: str, order_id: str) -> Mapping[str, Any]:
        result = self.client._private_request(
            "GET", "/api/v5/trade/order",
            params={"instId": f"{symbol[:-4]}-USDC", "ordId": str(order_id)},
        )
        rows = tuple(result.get("data") or ())
        if len(rows) != 1:
            raise RuntimeError("OKX_ORDER_IDENTITY_NOT_EXACT")
        row = rows[0]
        return {
            "order_id": str(row.get("ordId") or ""),
            "state": str(row.get("state") or "").lower(),
            "side": str(row.get("side") or "").upper(),
            "executed_qty": str(row.get("accFillSz") or "0"),
        }

    def fills(self, symbol: str, order_id: str) -> tuple[Mapping[str, Any], ...]:
        rows = self.client.get_my_trades(symbol=symbol, orderId=order_id, limit=100)
        return tuple({
            "trade_id": str(row.get("id") or ""),
            "order_id": str(row.get("orderId") or ""),
            "side": "BUY" if bool(row.get("isBuyer")) else "SELL",
            "quantity": str(row.get("qty") or "0"),
            "price": str(row.get("price") or "0"),
            "fee_quantity": str(row.get("commission") or "0"),
            "fee_currency": str(row.get("commissionAsset") or "").upper(),
            "event_time_ms": str(row.get("time") or ""),
        } for row in rows)


@dataclass(frozen=True)
class ProofCandidate:
    ingestion_id: int
    position_id: int
    correction_revision: int
    source: str
    account_identity_key: str
    symbol: str
    exchange_order_id: str
    exchange_trade_id: str
    canonical_local_fill_id: int
    entry_or_exit: str
    latest_observed_fingerprint: str
    canonical_fill_fingerprint: str
    okx_truth_fingerprint: str
    idempotency_key: str
    evidence_payload: Mapping[str, Any]

    def public_payload(self) -> Mapping[str, Any]:
        return {
            "ingestion": self.ingestion_id,
            "position": self.position_id,
            "revision": self.correction_revision,
            "latest_fingerprint": self.latest_observed_fingerprint,
            "canonical_fingerprint": self.canonical_fill_fingerprint,
            "okx_fingerprint": self.okx_truth_fingerprint,
            "equivalence": EQUIVALENCE_STATE,
            "repair_impact": REPAIR_IMPACT,
            "planned_proof_identity": self.idempotency_key,
        }


@dataclass(frozen=True)
class ProofPlan:
    candidates: tuple[ProofCandidate, ...]
    schema_status: str
    okx_retrieved_at: datetime

    def summary(self) -> Mapping[str, Any]:
        return {
            "proof_candidates": len(self.candidates),
            "equivalence_exact": len(self.candidates),
            "repair_impact_NONE": sum(
                row.evidence_payload["classification"]["repair_impact"] == "NONE"
                for row in self.candidates
            ),
            "blocked": 0,
            "unexpected": 0,
            "OKX_mutations": 0,
            "DB_mutations": 0,
        }


class LegacyFillEquivalenceProofService:
    def __init__(
        self,
        connection_factory: Callable,
        exchange: ExchangeEvidence,
        runtime_identity: RuntimeIdentity,
        manifest: ProofManifest,
        *,
        expected_git_sha: str,
        expected_database: str,
    ):
        self.connection_factory = connection_factory
        self.exchange = exchange
        self.runtime_identity = runtime_identity
        self.manifest = manifest
        self.expected_git_sha = str(expected_git_sha)
        self.expected_database = str(expected_database)
        validate_manifest_cohort(manifest)
        self.deployment_id = manifest.deployment_id
        self.expected_ingestion_ids = EXPECTED_INGESTION_IDS_BY_DEPLOYMENT[
            self.deployment_id
        ]
        self.expected_position_ids = EXPECTED_POSITION_IDS_BY_DEPLOYMENT[
            self.deployment_id
        ]
        self.forbidden_ingestion_ids = (
            FORBIDDEN_INGESTION_IDS_BY_DEPLOYMENT[self.deployment_id]
        )
        self.forbidden_db_order_ids = (
            FORBIDDEN_DB_ORDER_IDS_BY_DEPLOYMENT[self.deployment_id]
        )
        self._manifest = {row.ingestion_id: row for row in manifest.proofs}
        if not re.fullmatch(r"[0-9a-f]{40}", self.expected_git_sha):
            raise RuntimeError("EXPECTED_GIT_SHA_INVALID")

    def _runtime_gates(self) -> None:
        identity = self.runtime_identity
        if identity.exchange.upper() != EXPECTED_EXCHANGE:
            raise RuntimeError("EXCHANGE_IDENTITY_MISMATCH")
        if identity.trading_mode.upper() != EXPECTED_ENVIRONMENT:
            raise RuntimeError("TRADING_MODE_IDENTITY_MISMATCH")
        if identity.deployment_id != self.deployment_id:
            raise RuntimeError("DEPLOYMENT_IDENTITY_MISMATCH")
        if identity.git_sha != self.expected_git_sha:
            raise RuntimeError("GIT_SHA_IDENTITY_MISMATCH")
        if identity.orchestrator_role != EXPECTED_ORCHESTRATOR_ROLE:
            raise RuntimeError("ORCHESTRATOR_ROLE_IDENTITY_MISMATCH")
        if self.expected_database != EXPECTED_DATABASE:
            raise RuntimeError("EXPECTED_DATABASE_IDENTITY_MISMATCH")

    def _database_gates(self, cur) -> str:
        cur.execute("SELECT current_database()")
        if str(cur.fetchone()[0]) != self.expected_database:
            raise RuntimeError("DATABASE_IDENTITY_MISMATCH")
        cur.execute(
            "SELECT count(*),count(*) FILTER (WHERE live_orders_enabled) "
            "FROM bot_control"
        )
        total, enabled = cur.fetchone()
        if int(total) != 32 or int(enabled) != 0:
            raise RuntimeError("LIVE_ORDERS_NOT_CONTAINED")
        cur.execute(
            "SELECT to_regclass('public.legacy_fill_equivalence_proof_v1'),"
            "to_regclass('public.v_legacy_fill_equivalence_proof_status_v1')"
        )
        table, view = cur.fetchone()
        if table is None and view is None:
            return "MISSING"
        if table is None or view is None:
            raise RuntimeError("EQUIVALENCE_PROOF_SCHEMA_PARTIAL")
        return "PRESENT"

    def _assert_closed_cohort(self, cur) -> None:
        cur.execute(
            "SELECT state.ingestion_id,position.id "
            "FROM exchange_fill_ingestion_state_v2 state "
            "JOIN positions position ON position.entry_order_id=state.order_id "
            " OR position.exit_order_id=state.order_id "
            "WHERE position.id=ANY(%s) AND state.correction_revision>0 "
            "ORDER BY state.ingestion_id",
            (list(self.expected_position_ids),),
        )
        actual = {(int(row[0]), int(row[1])) for row in cur.fetchall()}
        expected = {(row.ingestion_id, row.position_id) for row in self.manifest.proofs}
        if actual != expected:
            raise RuntimeError("UNEXPECTED_PROOF_COHORT")
        cur.execute(
            "SELECT count(*) FROM exchange_fill_ingestion_state_v2 "
            "WHERE ingestion_id=ANY(%s)",
            (list(self.forbidden_ingestion_ids),),
        )
        if int(cur.fetchone()[0]) != len(self.forbidden_ingestion_ids):
            raise RuntimeError("QUARANTINED_INCIDENT_EVIDENCE_DRIFT")
        cur.execute(
            "SELECT count(*) FROM binance_orders WHERE id=ANY(%s)",
            (list(self.forbidden_db_order_ids),),
        )
        if int(cur.fetchone()[0]) != len(self.forbidden_db_order_ids):
            raise RuntimeError("QUARANTINED_INCIDENT_EVIDENCE_DRIFT")

    def _load_record(self, cur, manifest: ManifestProof, *, lock: bool) -> dict[str, Any]:
        suffix = " FOR UPDATE OF state,fill" if lock else ""
        cur.execute(
            "SELECT state.ingestion_id,state.source,state.account_identity_key,"
            "state.symbol,state.trade_id,state.order_id,state.source_fingerprint,"
            "state.applied_fingerprint,state.applied_at,state.application_status,"
            "state.correction_revision,state.authoritative_payload,state.adoption_id,"
            "state.contract_generation,state.local_fill_id,"
            "fill.id AS canonical_local_fill_id,fill.source AS fill_source,"
            "fill.order_id AS fill_order_id,fill.symbol AS fill_symbol,"
            "fill.side AS fill_side,fill.executed_qty,fill.avg_price,"
            "fill.quote_notional_usdc,fill.commission_amount,fill.commission_asset,"
            "fill.event_time,fill.trade_id AS fill_trade_id,"
            "fill.account_identity_id,position.id AS position_id,"
            "position.entry_order_id,position.exit_order_id "
            "FROM exchange_fill_ingestion_state_v2 state "
            "JOIN binance_order_fills fill ON fill.source=state.source "
            " AND fill.order_id=state.order_id "
            " AND fill.trade_id::TEXT=state.trade_id "
            "JOIN positions position ON position.entry_order_id=state.order_id "
            " OR position.exit_order_id=state.order_id "
            "WHERE state.ingestion_id=%s" + suffix,
            (manifest.ingestion_id,),
        )
        matches = _rows(cur)
        if not matches:
            raise RuntimeError("CANONICAL_FILL_MISSING")
        if len(matches) != 1:
            raise RuntimeError("CANONICAL_FILL_IDENTITY_NOT_EXACT")
        record = matches[0]
        identity = (
            int(record["position_id"]), str(record["order_id"]),
            str(record["trade_id"]), int(record["canonical_local_fill_id"]),
        )
        expected = (
            manifest.position_id, manifest.exchange_order_id,
            manifest.exchange_trade_id, manifest.canonical_local_fill_id,
        )
        if identity != expected:
            raise RuntimeError("IDENTITY_CONFLICT")
        cur.execute(
            "SELECT count(*) FROM binance_order_fills WHERE source=%s "
            "AND order_id=%s AND trade_id::TEXT=%s",
            (record["source"], record["order_id"], record["trade_id"]),
        )
        if int(cur.fetchone()[0]) != 1:
            raise RuntimeError("CANONICAL_FILL_IDENTITY_NOT_EXACT")
        if str(record["source_fingerprint"]) != manifest.latest_observed_fingerprint:
            raise RuntimeError("OBSERVED_FINGERPRINT_MANIFEST_DRIFT")
        if int(record["correction_revision"]) != 2:
            raise RuntimeError("CORRECTION_REVISION_INVALID")
        if str(record["application_status"]) != "CORRECTION_PENDING":
            raise RuntimeError("CORRECTION_STATUS_INVALID")
        if any(record[key] is not None for key in (
            "applied_fingerprint", "applied_at", "adoption_id",
            "contract_generation", "local_fill_id",
        )):
            raise RuntimeError("NATIVE_APPLICATION_PROOF_PRESENT")
        if (
            record["account_identity_id"] is not None
            and str(record["account_identity_id"])
            != str(record["account_identity_key"])
        ):
            raise RuntimeError("ACCOUNT_IDENTITY_CONFLICT")
        generation_value, _adoption_id, _contract_generation = _resolve_row_generation(
            cur,
            {
                "environment": EXPECTED_ENVIRONMENT,
                "deployment_id": self.deployment_id,
                "order_id": record["order_id"],
            },
        )
        try:
            generation = InventoryRowGeneration(str(generation_value))
        except ValueError as exc:
            raise RuntimeError("ROW_GENERATION_NOT_LEGACY_UNPROJECTED") from exc
        if generation is not InventoryRowGeneration.LEGACY_UNPROJECTED:
            raise RuntimeError("ROW_GENERATION_NOT_LEGACY_UNPROJECTED")
        change = RegisteredFillChange(
            int(record["ingestion_id"]),
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
            str(record["source_fingerprint"]), 2,
            row_generation=generation,
            application_status=FillApplicationClassification.CORRECTION_PENDING,
        )
        if change.permits_mutation:
            raise RuntimeError("CORRECTION_PERMITS_MUTATION")
        return record

    @staticmethod
    def _semantic_payload(
        *, source: Any, account_identity_key: Any, symbol: Any,
        trade_id: Any, order_id: Any, side: Any, quantity: Any, price: Any,
        fee_quantity: Any, fee_currency: Any, event_time_ms: Any,
        canonical_local_fill_id: Any,
    ) -> dict[str, Any]:
        return fill_semantic_payload(
            source=source, account_identity_key=account_identity_key,
            symbol=symbol, trade_id=trade_id, order_id=order_id, side=side,
            quantity=quantity, price=price, fee_quantity=fee_quantity,
            fee_currency=fee_currency, event_time_ms=event_time_ms,
            canonical_local_fill_id=canonical_local_fill_id,
        )

    def _candidate(
        self,
        record: Mapping[str, Any],
        manifest: ManifestProof,
        okx_order: Mapping[str, Any],
        okx_fill: Mapping[str, Any],
        retrieved_at: datetime,
    ) -> ProofCandidate:
        authoritative = dict(record["authoritative_payload"])
        if authoritative_fill_fingerprint(authoritative) != str(record["source_fingerprint"]):
            raise RuntimeError("LATEST_OBSERVED_FINGERPRINT_INVALID")
        latest = self._semantic_payload(
            source=authoritative["exchange"],
            account_identity_key=authoritative["account_identity"],
            symbol=authoritative["instrument"], trade_id=authoritative["trade_id"],
            order_id=authoritative["order_id"], side=authoritative["side"],
            quantity=authoritative["executed_qty"], price=authoritative["fill_price"],
            fee_quantity=authoritative["fee_quantity"],
            fee_currency=authoritative["fee_currency"],
            event_time_ms=authoritative["event_time_ms"],
            canonical_local_fill_id=record["canonical_local_fill_id"],
        )
        canonical = self._semantic_payload(
            source=record["fill_source"],
            account_identity_key=record["account_identity_key"],
            symbol=record["fill_symbol"], trade_id=record["fill_trade_id"],
            order_id=record["fill_order_id"], side=record["fill_side"],
            quantity=record["executed_qty"], price=record["avg_price"],
            fee_quantity=record["commission_amount"],
            fee_currency=record["commission_asset"],
            event_time_ms=record["event_time"],
            canonical_local_fill_id=record["canonical_local_fill_id"],
        )
        okx = self._semantic_payload(
            source=record["source"],
            account_identity_key=record["account_identity_key"],
            symbol=record["symbol"], trade_id=okx_fill["trade_id"],
            order_id=okx_fill["order_id"], side=okx_fill["side"],
            quantity=okx_fill["quantity"], price=okx_fill["price"],
            fee_quantity=okx_fill["fee_quantity"],
            fee_currency=okx_fill["fee_currency"],
            event_time_ms=okx_fill["event_time_ms"],
            canonical_local_fill_id=record["canonical_local_fill_id"],
        )
        if latest != canonical or canonical != okx:
            differing = sorted(
                key for key in canonical
                if latest.get(key) != canonical.get(key) or canonical.get(key) != okx.get(key)
            )
            raise RuntimeError("EQUIVALENCE_PROOF_CONFLICT:" + ",".join(differing))
        if (
            str(okx_order.get("order_id")) != str(record["order_id"])
            or str(okx_order.get("state")) != "filled"
            or str(okx_order.get("side")) != str(record["fill_side"])
            or _decimal(okx_order.get("executed_qty")) <= 0
        ):
            raise RuntimeError("OKX_ORDER_NOT_FILLED")
        canonical_fp = canonical_fingerprint(canonical)
        okx_fp = canonical_fingerprint(okx)
        identity_payload = {
            "proof_version": PROOF_VERSION,
            "environment": EXPECTED_ENVIRONMENT,
            "deployment_id": self.deployment_id,
            "source": str(record["source"]),
            "account_identity_key": str(record["account_identity_key"]),
            "symbol": str(record["symbol"]),
            "trade_id": str(record["trade_id"]),
            "correction_revision": int(record["correction_revision"]),
            "latest_observed_fingerprint": str(record["source_fingerprint"]),
            "canonical_fill_fingerprint": canonical_fp,
            "okx_truth_fingerprint": okx_fp,
        }
        idempotency_key = canonical_fingerprint(identity_payload)
        entry_or_exit = (
            "ENTRY" if str(record["entry_order_id"]) == str(record["order_id"])
            else "EXIT"
        )
        comparison = {key: latest[key] == canonical[key] == okx[key] for key in latest}
        evidence = {
            "ingestion": {
                "ingestion_id": int(record["ingestion_id"]),
                "source": str(record["source"]),
                "account_identity_key": str(record["account_identity_key"]),
                "symbol": str(record["symbol"]),
                "trade_id": str(record["trade_id"]),
                "order_id": str(record["order_id"]),
                "correction_revision": int(record["correction_revision"]),
                "application_status": str(record["application_status"]),
                "latest_observed_fingerprint": str(record["source_fingerprint"]),
                "authoritative_payload": authoritative,
            },
            "canonical_fill": {
                "canonical_local_fill_id": int(record["canonical_local_fill_id"]),
                "semantic_payload": canonical,
                "fingerprint": canonical_fp,
            },
            "okx_truth": {
                "order": {
                    "order_id": str(okx_order["order_id"]),
                    "state": str(okx_order["state"]),
                    "side": str(okx_order["side"]),
                    "executed_qty": decimal_text(okx_order["executed_qty"]),
                },
                "semantic_payload": okx,
                "fingerprint": okx_fp,
                "retrieved_at": retrieved_at.isoformat(),
            },
            "field_comparison": comparison,
            "position_linkage": {
                "position_id": int(record["position_id"]),
                "entry_or_exit": entry_or_exit,
            },
            "classification": {
                "application_state": "NOT_APPLIED",
                "equivalence_state": EQUIVALENCE_STATE,
                "fill_mutation": "NOT_REQUIRED",
                "repair_impact": REPAIR_IMPACT,
                "proof_type": PROOF_TYPE,
            },
        }
        if not all(comparison.values()):
            raise RuntimeError("EQUIVALENCE_PROOF_CONFLICT")
        return ProofCandidate(
            int(record["ingestion_id"]), int(record["position_id"]),
            int(record["correction_revision"]), str(record["source"]),
            str(record["account_identity_key"]), str(record["symbol"]),
            str(record["order_id"]), str(record["trade_id"]),
            int(record["canonical_local_fill_id"]), entry_or_exit,
            str(record["source_fingerprint"]), canonical_fp, okx_fp,
            idempotency_key, evidence,
        )

    def plan(self) -> ProofPlan:
        self._runtime_gates()
        pending = self.exchange.pending_spot_orders()
        if pending:
            raise RuntimeError("OKX_PENDING_SPOT_ORDERS")
        retrieved_at = datetime.now(timezone.utc)
        connection = self.connection_factory()
        try:
            connection.rollback()
            connection.set_session(readonly=True, autocommit=False)
            with connection.cursor() as cur:
                schema_status = self._database_gates(cur)
                self._assert_closed_cohort(cur)
                records = {
                    item.ingestion_id: self._load_record(cur, item, lock=False)
                    for item in self.manifest.proofs
                }
            order_cache: dict[str, Mapping[str, Any]] = {}
            fill_cache: dict[str, tuple[Mapping[str, Any], ...]] = {}
            candidates = []
            for manifest in self.manifest.proofs:
                record = records[manifest.ingestion_id]
                order_id = str(record["order_id"])
                symbol = str(record["symbol"])
                if order_id not in order_cache:
                    order_cache[order_id] = self.exchange.order(symbol, order_id)
                    fill_cache[order_id] = self.exchange.fills(symbol, order_id)
                matching = tuple(
                    row for row in fill_cache[order_id]
                    if str(row.get("trade_id")) == str(record["trade_id"])
                )
                if len(matching) != 1:
                    raise RuntimeError("OKX_TRADE_IDENTITY_NOT_EXACT")
                candidates.append(self._candidate(
                    record, manifest, order_cache[order_id], matching[0], retrieved_at,
                ))
            connection.rollback()
            if self.exchange.place_order_calls or self.exchange.cancel_order_calls:
                raise RuntimeError("EXCHANGE_MUTATION_DETECTED")
            if (
                {row.ingestion_id for row in candidates}
                != self.expected_ingestion_ids
            ):
                raise RuntimeError("UNEXPECTED_PROOF_COHORT")
            return ProofPlan(tuple(candidates), schema_status, retrieved_at)
        finally:
            connection.close()

    def _base_identity(self, candidate: ProofCandidate) -> tuple[Any, ...]:
        return (
            EXPECTED_ENVIRONMENT, self.deployment_id, candidate.source,
            candidate.account_identity_key, candidate.symbol,
            candidate.exchange_trade_id, candidate.correction_revision, PROOF_VERSION,
        )

    def apply(
        self,
        *,
        apply_requested: bool,
        environment: str,
        deployment_id: str,
        database: str,
        manifest_path: str | Path,
        stage_hook: Callable[[int, str], None] | None = None,
    ) -> Mapping[str, Any]:
        if not apply_requested:
            raise RuntimeError("APPLY_FLAG_REQUIRED")
        if environment != EXPECTED_ENVIRONMENT:
            raise RuntimeError("APPLY_ENVIRONMENT_GATE_FAILED")
        if deployment_id != self.deployment_id:
            raise RuntimeError("APPLY_DEPLOYMENT_GATE_FAILED")
        if database != EXPECTED_DATABASE:
            raise RuntimeError("APPLY_DATABASE_GATE_FAILED")
        if not str(manifest_path):
            raise RuntimeError("APPLY_MANIFEST_GATE_FAILED")
        if os.environ.get(APPLY_ENABLE_ENV) != "1":
            raise RuntimeError("APPLY_ENV_FLAG_DISABLED")
        planned = self.plan()
        if planned.schema_status != "PRESENT":
            raise RuntimeError("EQUIVALENCE_PROOF_SCHEMA_MISSING")
        connection = self.connection_factory()
        try:
            connection.rollback()
            connection.set_session(
                isolation_level="SERIALIZABLE", readonly=False, autocommit=False,
            )
            with connection.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout='5s'")
                cur.execute("SET LOCAL statement_timeout='90s'")
                if self._database_gates(cur) != "PRESENT":
                    raise RuntimeError("EQUIVALENCE_PROOF_SCHEMA_MISSING")
                self._assert_closed_cohort(cur)
                locked = {
                    item.ingestion_id: self._load_record(cur, item, lock=True)
                    for item in self.manifest.proofs
                }
                for candidate in planned.candidates:
                    current = locked[candidate.ingestion_id]
                    if (
                        str(current["source_fingerprint"])
                        != candidate.latest_observed_fingerprint
                        or int(current["canonical_local_fill_id"])
                        != candidate.canonical_local_fill_id
                    ):
                        raise RuntimeError("PROOF_PRECOMMIT_STATE_DRIFT")
                cur.execute(
                    "SELECT count(*) FROM legacy_fill_equivalence_proof_v1 "
                    "WHERE ingestion_id=ANY(%s)",
                    (list(self.expected_ingestion_ids),),
                )
                existing_count = int(cur.fetchone()[0])
                if existing_count not in (
                    0, len(self.expected_ingestion_ids),
                ):
                    raise RuntimeError("PARTIAL_PROOF_COHORT")
                inserted = 0
                proof_ids = []
                for candidate in planned.candidates:
                    cur.execute(
                        "SELECT proof_id,idempotency_key,latest_observed_fingerprint,"
                        "canonical_fill_fingerprint,okx_truth_fingerprint "
                        "FROM legacy_fill_equivalence_proof_v1 "
                        "WHERE environment=%s AND deployment_id=%s AND source=%s "
                        "AND account_identity_key=%s AND symbol=%s AND trade_id=%s "
                        "AND correction_revision=%s AND proof_version=%s",
                        self._base_identity(candidate),
                    )
                    existing = cur.fetchone()
                    if existing is not None:
                        if (
                            str(existing[1]) != candidate.idempotency_key
                            or str(existing[2]) != candidate.latest_observed_fingerprint
                            or str(existing[3]) != candidate.canonical_fill_fingerprint
                            or str(existing[4]) != candidate.okx_truth_fingerprint
                        ):
                            raise RuntimeError("EQUIVALENCE_PROOF_CONFLICT")
                        proof_ids.append(int(existing[0]))
                        continue
                    cur.execute(
                        "INSERT INTO legacy_fill_equivalence_proof_v1("
                        "proof_version,environment,deployment_id,source,"
                        "account_identity_key,symbol,trade_id,ingestion_id,"
                        "correction_revision,exchange_order_id,exchange_trade_id,"
                        "canonical_local_fill_id,latest_observed_fingerprint,"
                        "canonical_fill_fingerprint,okx_truth_fingerprint,proof_type,"
                        "equivalence_state,fill_mutation_required,repair_impact,"
                        "position_id,entry_or_exit,evidence_payload_json,created_by,"
                        "git_revision,idempotency_key) VALUES("
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s::jsonb,%s,%s,%s) RETURNING proof_id",
                        (
                            PROOF_VERSION, EXPECTED_ENVIRONMENT,
                            self.deployment_id,
                            candidate.source, candidate.account_identity_key,
                            candidate.symbol, candidate.exchange_trade_id,
                            candidate.ingestion_id, candidate.correction_revision,
                            candidate.exchange_order_id, candidate.exchange_trade_id,
                            candidate.canonical_local_fill_id,
                            candidate.latest_observed_fingerprint,
                            candidate.canonical_fill_fingerprint,
                            candidate.okx_truth_fingerprint, PROOF_TYPE,
                            EQUIVALENCE_STATE, False, REPAIR_IMPACT,
                            candidate.position_id, candidate.entry_or_exit,
                            canonical_json(candidate.evidence_payload), CREATED_BY,
                            self.expected_git_sha, candidate.idempotency_key,
                        ),
                    )
                    proof_ids.append(int(cur.fetchone()[0]))
                    inserted += 1
                    if stage_hook:
                        stage_hook(candidate.ingestion_id, "inserted")
                cur.execute(
                    "SELECT proof_id,proof_status "
                    "FROM v_legacy_fill_equivalence_proof_status_v1 "
                    "WHERE proof_id=ANY(%s) ORDER BY proof_id", (proof_ids,),
                )
                statuses = cur.fetchall()
                if len(statuses) != len(self.expected_ingestion_ids) or any(
                    str(row[1]) != "VALID" for row in statuses
                ):
                    raise RuntimeError("EQUIVALENCE_PROOF_POSTCONDITION_FAILED")
            connection.commit()
            if self.exchange.place_order_calls or self.exchange.cancel_order_calls:
                raise RuntimeError("EXCHANGE_MUTATION_DETECTED")
            return {
                "proofs": len(self.expected_ingestion_ids),
                "inserted": inserted,
                "idempotent_noop": len(self.expected_ingestion_ids) - inserted,
                "status": "VALID",
            }
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def render_plan(plan: ProofPlan) -> str:
    payload = {
        "proof_version": PROOF_VERSION,
        "schema_status": plan.schema_status,
        "okx_retrieved_at": plan.okx_retrieved_at.isoformat(),
        "candidates": [row.public_payload() for row in plan.candidates],
        "summary": plan.summary(),
    }
    return json.dumps(payload, sort_keys=True, indent=2) + "\n"
