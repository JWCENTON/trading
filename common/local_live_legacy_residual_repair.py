from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

from common.financial_truth_calculator import (
    FillEvidence,
    FinancialTruthCalculation,
    calculate_financial_truth,
)
from common.financial_truth_repository import CanonicalFinancialTruthWriteRepository
from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    ExitInventoryClassification,
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    InventoryEvidenceStatus,
    InventoryQuantity,
    classify_exit_inventory,
    project_inventory_from_execution_evidence,
)
from common.legacy_recovery import semantic_repair_fingerprint, value_fee
from common.legacy_recovery_repository import (
    LegacyProvenanceRepository,
    LegacyRepairAuditRepository,
)
from common.legacy_repair_quarantine import LearningOutcomeExclusionRepository
from common.legacy_fill_equivalence_proof import (
    canonical_fingerprint as equivalence_fingerprint,
    fill_semantic_payload,
)


CONTRACT_VERSION = "LOCAL_LIVE_LEGACY_RESIDUAL_REPAIR_V1"
MANIFEST_VERSION = "LOCAL_LIVE_LEGACY_RESIDUAL_REPAIR_MANIFEST_V1"
FINGERPRINT_CONTRACT_VERSION = CONTRACT_VERSION + "_SEMANTIC_FINGERPRINT_V1"
PROOF_CONTRACT_VERSION = "LEGACY_FILL_EQUIVALENCE_PROOF_V1"
PLACEHOLDER_FINGERPRINT = "0" * 64
PLANNER_VERSION = CONTRACT_VERSION + "_PLANNER"
WRITER_VERSION = CONTRACT_VERSION + "_WRITER"
APPLY_ENABLE_ENV = "LOCAL_LIVE_LEGACY_RESIDUAL_REPAIR_APPLY_ENABLED"
EXPECTED_ENVIRONMENT = "LIVE"
EXPECTED_DEPLOYMENT = "local-live"
EXPECTED_DATABASE = "trading_live"
EXPECTED_EXCHANGE = "OKX"
EXPECTED_ORCHESTRATOR_ROLE = "PROCESS_SUPERVISOR"
EXPECTED_AUTOMATION_AUTHORITY = "automation_runner"
ALLOWED_POSITION_IDS = frozenset({3079, 3080, 3081, 3082, 3083, 3084, 3085})
FORBIDDEN_DB_ORDER_ROW_IDS = frozenset({3758, 3760, 3762})
FORBIDDEN_INGESTION_IDS = frozenset({22, 23, 24, 25})
FORBIDDEN_EXCHANGE_ORDER_IDS = frozenset({"3789163681263689728"})
POSITION_UPDATE_ALLOWLIST = frozenset({
    "inventory_evidence_status",
    "gross_entry_executed_qty",
    "entry_base_fee_qty",
    "net_entry_inventory_qty",
    "cumulative_exit_executed_qty",
    "exit_inventory_reduction_qty",
    "remaining_inventory_qty",
    "qty",
    "terminal_dust_qty",
    "terminal_reason",
    "inventory_calculated_at",
    "status",
    "exit_price",
    "exit_time",
    "exit_reason",
})
PLANNED_MUTATIONS = (
    "learning_outcome_exclusion_v1:INSERT",
    "positions:UPDATE_ALLOWLIST",
    "position_lifecycle_events_c2_2:INSERT",
    "canonical_financial_truth_v1:INSERT",
    "canonical_financial_truth_audit_v1:INSERT",
    "legacy_repair_audit_v1:INSERT",
    "legacy_repair_provenance_v1:INSERT",
)
ELIGIBLE_LEARNING_VIEWS = (
    "v_learning_eligible_closed_positions_v1",
    "v_learning_eligible_exit_trace_v1",
    "v_learning_eligible_exit_trace_v2",
    "v_learning_eligible_exit_trace_v3",
    "v_learning_eligible_shadow_recommendations_v1",
    "v_learning_eligible_feature_warehouse_v1",
    "v_learning_eligible_decision_replay_v1",
    "v_learning_eligible_decision_registry_v1",
    "v_learning_eligible_decision_outcomes_v1",
)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value)}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    return value


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


def resolve_correction_trust(
    cur, ingestion_rows: Iterable[Mapping[str, Any]],
) -> tuple[str, dict[int, dict[str, Any]]]:
    rows = tuple(ingestion_rows)
    if not rows:
        return "CANONICAL_OKX_DIRECT_EVIDENCE", {}
    corrections = tuple(
        row for row in rows if int(row.get("correction_revision") or 0) > 0
    )
    if not corrections:
        return "NATIVE_APPLICATION_PROOF", {}
    equivalence_required = []
    for row in corrections:
        native_complete = (
            str(row.get("applied_fingerprint") or "")
            == str(row.get("source_fingerprint") or "")
            and row.get("applied_at") is not None
            and row.get("local_fill_id") is not None
            and row.get("adoption_id") is not None
            and row.get("contract_generation") is not None
            and str(row.get("application_status") or "") in {
                "APPLIED", "CORRECTION_APPLIED", "TRUE_DUPLICATE_APPLIED",
            }
        )
        if not native_complete:
            equivalence_required.append(row)
    if not equivalence_required:
        return "NATIVE_APPLICATION_PROOF", {}
    cur.execute(
        "SELECT to_regclass('public.v_legacy_fill_equivalence_proof_status_v1')"
    )
    if cur.fetchone()[0] is None:
        raise RuntimeError("BLOCKED_BY_MISSING_EQUIVALENCE_PROOF")
    ids = [int(row["ingestion_id"]) for row in equivalence_required]
    cur.execute(
        "SELECT ingestion_id,position_id,proof_version,proof_type,"
        "equivalence_state,proof_status,exchange_order_id,exchange_trade_id,"
        "canonical_local_fill_id,latest_observed_fingerprint,"
        "canonical_fill_fingerprint,okx_truth_fingerprint,"
        "fill_mutation_required,repair_impact,idempotency_key "
        "FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE ingestion_id=ANY(%s) ORDER BY ingestion_id", (ids,),
    )
    proofs = {int(row["ingestion_id"]): row for row in _rows(cur)}
    if set(proofs) != set(ids):
        raise RuntimeError("BLOCKED_BY_MISSING_EQUIVALENCE_PROOF")
    by_id = {int(row["ingestion_id"]): row for row in equivalence_required}
    for ingestion_id, proof in proofs.items():
        ingestion = by_id[ingestion_id]
        if str(proof["proof_status"]) != "VALID":
            raise RuntimeError("BLOCKED_BY_STALE_EQUIVALENCE_PROOF")
        if bool(proof["fill_mutation_required"]):
            raise RuntimeError("EQUIVALENCE_PROOF_REQUIRES_FILL_MUTATION")
        if str(proof["repair_impact"]) != "NONE":
            raise RuntimeError("EQUIVALENCE_PROOF_REPAIR_IMPACT_CONFLICT")
        if str(proof["latest_observed_fingerprint"]) != str(
            ingestion["source_fingerprint"]
        ):
            raise RuntimeError("BLOCKED_BY_STALE_EQUIVALENCE_PROOF")
    return "LEGACY_EQUIVALENCE_PROOF", proofs


@dataclass(frozen=True)
class ManifestPosition:
    position_id: int
    entry_order_id: str
    exit_order_id: str
    semantic_fingerprint: str


@dataclass(frozen=True)
class RepairManifest:
    environment: str
    deployment_id: str
    positions: tuple[ManifestPosition, ...]
    manifest_version: str = MANIFEST_VERSION
    generated_from_git_revision: str | None = None
    generated_at: str | None = None
    fingerprint_contract_version: str = FINGERPRINT_CONTRACT_VERSION
    proof_contract_version: str = PROOF_CONTRACT_VERSION

    @classmethod
    def load(
        cls, path: str | Path, *, allow_placeholders: bool = False,
    ) -> "RepairManifest":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        legacy_top = {
            "contract_version", "environment", "deployment_id", "positions",
        }
        closed_top = legacy_top | {
            "manifest_version", "generated_from_git_revision", "generated_at",
            "fingerprint_contract_version", "proof_contract_version",
        }
        allowed_shapes = {frozenset(closed_top)}
        if allow_placeholders:
            allowed_shapes.add(frozenset(legacy_top))
        if frozenset(payload) not in allowed_shapes:
            if (
                set(payload) == legacy_top
                and any(
                    row.get("semantic_fingerprint") == PLACEHOLDER_FINGERPRINT
                    for row in payload.get("positions", ())
                )
            ):
                raise RuntimeError("MANIFEST_FINGERPRINT_PLACEHOLDER")
            raise RuntimeError("MANIFEST_FIELDS_INVALID")
        if payload["contract_version"] != CONTRACT_VERSION:
            raise RuntimeError("MANIFEST_CONTRACT_INVALID")
        rows = []
        for raw in payload["positions"]:
            if set(raw) != {
                "position_id", "entry_order_id", "exit_order_id",
                "semantic_fingerprint",
            }:
                raise RuntimeError("MANIFEST_POSITION_FIELDS_INVALID")
            row = ManifestPosition(
                int(raw["position_id"]), str(raw["entry_order_id"]),
                str(raw["exit_order_id"]), str(raw["semantic_fingerprint"]),
            )
            if not re.fullmatch(r"[0-9a-f]{64}", row.semantic_fingerprint):
                raise RuntimeError("MANIFEST_FINGERPRINT_INVALID")
            if (
                row.semantic_fingerprint == PLACEHOLDER_FINGERPRINT
                and not allow_placeholders
            ):
                raise RuntimeError("MANIFEST_FINGERPRINT_PLACEHOLDER")
            rows.append(row)
        metadata_present = set(payload) == closed_top
        generated_at = str(payload["generated_at"]) if metadata_present else None
        if metadata_present:
            if payload["manifest_version"] != MANIFEST_VERSION:
                raise RuntimeError("MANIFEST_VERSION_INVALID")
            if payload["fingerprint_contract_version"] != FINGERPRINT_CONTRACT_VERSION:
                raise RuntimeError("FINGERPRINT_CONTRACT_VERSION_INVALID")
            if payload["proof_contract_version"] != PROOF_CONTRACT_VERSION:
                raise RuntimeError("PROOF_CONTRACT_VERSION_INVALID")
            if not re.fullmatch(
                r"[0-9a-f]{40}", str(payload["generated_from_git_revision"]),
            ):
                raise RuntimeError("MANIFEST_GENERATED_GIT_REVISION_INVALID")
            try:
                parsed_generated_at = datetime.fromisoformat(
                    generated_at.replace("Z", "+00:00"),
                )
            except ValueError as exc:
                raise RuntimeError("MANIFEST_GENERATED_AT_INVALID") from exc
            if parsed_generated_at.tzinfo is None:
                raise RuntimeError("MANIFEST_GENERATED_AT_INVALID")
        manifest = cls(
            str(payload["environment"]), str(payload["deployment_id"]),
            tuple(sorted(rows, key=lambda item: item.position_id)),
            str(payload.get("manifest_version", MANIFEST_VERSION)),
            (
                str(payload["generated_from_git_revision"])
                if metadata_present else None
            ),
            generated_at,
            str(payload.get(
                "fingerprint_contract_version", FINGERPRINT_CONTRACT_VERSION,
            )),
            str(payload.get("proof_contract_version", PROOF_CONTRACT_VERSION)),
        )
        if manifest.environment != EXPECTED_ENVIRONMENT:
            raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
        if manifest.deployment_id != EXPECTED_DEPLOYMENT:
            raise RuntimeError("DEPLOYMENT_IDENTITY_MISMATCH")
        if {item.position_id for item in manifest.positions} != ALLOWED_POSITION_IDS:
            raise RuntimeError("COHORT_IDENTITY_MISMATCH")
        if len({item.entry_order_id for item in manifest.positions}) != len(rows):
            raise RuntimeError("DUPLICATE_ENTRY_ORDER_IDENTITY")
        if len({item.exit_order_id for item in manifest.positions}) != len(rows):
            raise RuntimeError("DUPLICATE_EXIT_ORDER_IDENTITY")
        if any(
            order_id in FORBIDDEN_EXCHANGE_ORDER_IDS
            for item in rows
            for order_id in (item.entry_order_id, item.exit_order_id)
        ):
            raise RuntimeError("FORBIDDEN_INCIDENT_IDENTITY")
        return manifest


def stable_equivalence_proof_evidence(
    proofs: Mapping[int, Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Return only immutable proof identity fields; proof sequence IDs are excluded."""
    fields = (
        "ingestion_id", "position_id", "proof_version", "proof_type",
        "equivalence_state", "proof_status", "exchange_order_id",
        "exchange_trade_id", "canonical_local_fill_id",
        "latest_observed_fingerprint", "canonical_fill_fingerprint",
        "okx_truth_fingerprint", "fill_mutation_required", "repair_impact",
        "idempotency_key",
    )
    return tuple(
        {field: proof.get(field) for field in fields}
        for _ingestion_id, proof in sorted(proofs.items())
    )


@dataclass(frozen=True)
class RuntimeIdentity:
    exchange: str
    trading_mode: str
    deployment_id: str
    git_sha: str
    orchestrator_role: str


class DockerRuntimeIdentityProbe:
    """Read-only LOCAL LIVE identity probe. It never prints container env."""

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
    def account_fingerprint(self) -> str: ...
    def order(self, symbol: str, order_id: str) -> Mapping[str, Any]: ...
    def fills(self, symbol: str, order_id: str) -> tuple[Mapping[str, Any], ...]: ...
    def instrument(self, symbol: str) -> Mapping[str, Any]: ...


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

    def account_fingerprint(self) -> str:
        identity, _diagnostic = self.client.get_account_identity(refresh=True)
        return str(identity.fingerprint)

    def order(self, symbol: str, order_id: str) -> Mapping[str, Any]:
        inst_id = f"{symbol[:-4]}-USDC"
        result = self.client._private_request(
            "GET", "/api/v5/trade/order",
            params={"instId": inst_id, "ordId": str(order_id)},
        )
        rows = tuple(result.get("data") or ())
        if len(rows) != 1:
            raise RuntimeError("OKX_ORDER_IDENTITY_NOT_EXACT")
        row = rows[0]
        return {
            "order_id": str(row.get("ordId") or ""),
            "client_order_id": str(row.get("clOrdId") or ""),
            "state": str(row.get("state") or "").lower(),
            "side": str(row.get("side") or "").upper(),
            "executed_qty": str(row.get("accFillSz") or "0"),
            "avg_price": str(row.get("avgPx") or "0"),
            "updated_at_ms": str(row.get("uTime") or ""),
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
            "fee_asset": str(row.get("commissionAsset") or "").upper(),
            "event_time_ms": str(row.get("time") or ""),
        } for row in rows)

    def instrument(self, symbol: str) -> Mapping[str, Any]:
        from common.exchange_client import get_okx_spot_instrument

        row = get_okx_spot_instrument(symbol)
        min_notional = row.get("minNotional")
        selected = {
            "symbol": symbol,
            "instrument_id": str(row.get("instId") or ""),
            "base_asset": str(row.get("baseCcy") or symbol[:-4]).upper(),
            "quote_asset": str(row.get("quoteCcy") or "USDC").upper(),
            "lot_size": str(row.get("lotSz") or ""),
            "min_size": str(row.get("minSz") or ""),
            "min_notional": (
                "0" if min_notional in (None, "") else str(min_notional)
            ),
        }
        selected["metadata_fingerprint"] = semantic_repair_fingerprint(selected)
        return selected


@dataclass(frozen=True)
class PositionPlan:
    position_id: int
    slot: str
    entry_order_id: str
    exit_order_id: str
    entry_fill_ids: tuple[int, ...]
    exit_fill_ids: tuple[int, ...]
    inventory: InventoryQuantity
    classification: ExitInventoryClassification
    financial_truth: FinancialTruthCalculation
    min_size: Decimal
    lot_size: Decimal
    semantic_fingerprint: str
    correction_trust_source: str
    status: str
    planned_mutations: tuple[str, ...]
    immutable_payload: Mapping[str, Any]

    def public_payload(self) -> Mapping[str, Any]:
        return {
            "position_id": self.position_id,
            "slot": self.slot,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "entry_fill_ids": self.entry_fill_ids,
            "exit_fill_ids": self.exit_fill_ids,
            "gross_entry_qty": self.inventory.gross_entry_executed_qty,
            "entry_base_fee_qty": self.inventory.entry_base_fee_qty,
            "net_entry_inventory_qty": self.inventory.net_entry_inventory_qty,
            "exit_qty": self.inventory.cumulative_exit_executed_qty,
            "remaining_qty": self.classification.remaining_inventory_qty,
            "min_size": self.min_size,
            "terminal_classification": self.classification.status.value,
            "gross_pnl": self.financial_truth.authoritative_gross_pnl,
            "fees": self.financial_truth.authoritative_fees_usdc,
            "net_pnl": self.financial_truth.authoritative_net_pnl,
            "current_status": "OPEN",
            "expected_status": "CLOSED",
            "current_ft": "ABSENT",
            "expected_ft": "COMPLETE",
            "learning_exclusion": "PLANNED",
            "correction_trust_source": self.correction_trust_source,
            "fingerprint": self.semantic_fingerprint,
            "planned_mutations": self.planned_mutations,
        }


@dataclass(frozen=True)
class RunPlan:
    positions: tuple[PositionPlan, ...]
    already_repaired: tuple[int, ...]
    blocked: tuple[str, ...]
    panic_enabled: bool

    def summary(self) -> Mapping[str, Any]:
        return {
            "positions_planned": len(self.positions),
            "phantom_closes": sum(
                item.classification.status is ExitInventoryStatus.FULLY_EXECUTED_CLOSE
                for item in self.positions
            ),
            "terminal_dust_closes": sum(
                item.classification.status is ExitInventoryStatus.TERMINAL_DUST_CLOSE
                for item in self.positions
            ),
            "expected_db_mutations": len(self.positions) * len(PLANNED_MUTATIONS),
            "okx_mutations": 0,
            "blocked_rows": len(self.blocked),
            "already_repaired_rows": len(self.already_repaired),
        }


class BoundedResidualRepairService:
    def __init__(
        self,
        connection_factory: Callable,
        exchange: ExchangeEvidence,
        runtime_identity: RuntimeIdentity,
        manifest: RepairManifest,
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
        self._manifest_by_id = {
            item.position_id: item for item in manifest.positions
        }
        if not re.fullmatch(r"[0-9a-f]{40}", self.expected_git_sha):
            raise RuntimeError("EXPECTED_GIT_SHA_INVALID")

    def _runtime_gates(self) -> None:
        identity = self.runtime_identity
        if identity.exchange.upper() != EXPECTED_EXCHANGE:
            raise RuntimeError("EXCHANGE_IDENTITY_MISMATCH")
        if identity.trading_mode.upper() != EXPECTED_ENVIRONMENT:
            raise RuntimeError("TRADING_MODE_IDENTITY_MISMATCH")
        if identity.deployment_id != EXPECTED_DEPLOYMENT:
            raise RuntimeError("DEPLOYMENT_IDENTITY_MISMATCH")
        if identity.git_sha != self.expected_git_sha:
            raise RuntimeError("GIT_SHA_IDENTITY_MISMATCH")
        if identity.orchestrator_role != EXPECTED_ORCHESTRATOR_ROLE:
            raise RuntimeError("ORCHESTRATOR_ROLE_IDENTITY_MISMATCH")
        if self.expected_database != EXPECTED_DATABASE:
            raise RuntimeError("EXPECTED_DATABASE_IDENTITY_MISMATCH")

    @staticmethod
    def _begin_read_only(connection) -> None:
        connection.rollback()
        connection.set_session(readonly=True, autocommit=False)

    def _database_safety(self, connection) -> bool:
        with connection.cursor() as cur:
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
            cur.execute("SELECT panic_enabled FROM panic_state WHERE id=true")
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("PANIC_STATE_UNKNOWN")
            panic_enabled = bool(row[0])
            cur.execute(
                "SELECT value FROM automation_kv WHERE key='orc_v5_apply_mode'"
            )
            row = cur.fetchone()
            if row is None or str(row[0]) != EXPECTED_AUTOMATION_AUTHORITY:
                raise RuntimeError("AUTOMATION_AUTHORITY_MISMATCH")
            cur.execute(
                "SELECT status,meta FROM worker_heartbeats "
                "WHERE service_name='bot-runner-orchestrator' "
                "AND environment='LIVE'"
            )
            row = cur.fetchone()
            if row is None or str(row[0]) != "healthy":
                raise RuntimeError("ORCHESTRATOR_HEARTBEAT_UNHEALTHY")
            cur.execute("SELECT id FROM positions WHERE status='OPEN' ORDER BY id")
            open_ids = {int(row[0]) for row in cur.fetchall()}
            unexpected = open_ids - ALLOWED_POSITION_IDS
            if unexpected:
                raise RuntimeError(
                    "UNEXPECTED_OPEN_COHORT:" + ",".join(map(str, sorted(unexpected)))
                )
        return panic_enabled

    @staticmethod
    def _repair_state(cur, position_id: int, fingerprint: str) -> str:
        cur.execute("SELECT status FROM positions WHERE id=%s", (position_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("POSITION_NOT_FOUND")
        status = str(row[0])
        queries = (
            ("canonical_financial_truth_v1", "position_id=%s"),
            ("canonical_financial_truth_audit_v1", "position_id=%s"),
            ("position_lifecycle_events_c2_2", "position_id=%s"),
            ("learning_outcome_exclusion_v1", "position_id=%s"),
            (
                "legacy_repair_audit_v1",
                "incident_type='LEGACY_POSITION' AND incident_identity=%s",
            ),
        )
        counts = []
        for table, predicate in queries:
            value = str(position_id) if "incident_identity" in predicate else position_id
            cur.execute(f"SELECT count(*) FROM {table} WHERE {predicate}", (value,))
            counts.append(int(cur.fetchone()[0]))
        source_identity = (
            f"LIVE:{EXPECTED_DEPLOYMENT}:{EXPECTED_DATABASE}:position:{position_id}"
        )
        cur.execute(
            "SELECT count(*) FROM legacy_repair_provenance_v1 "
            "WHERE evidence_source='LEGACY_POSITION_REPAIR' AND source_identity=%s",
            (source_identity,),
        )
        counts.append(int(cur.fetchone()[0]))
        present = sum(value > 0 for value in counts)
        if status == "OPEN" and present == 0:
            return "PLANNED"
        if status == "CLOSED" and present == len(counts):
            cur.execute(
                "SELECT semantic_fingerprint_v2 FROM learning_outcome_exclusion_v1 "
                "WHERE position_id=%s",
                (position_id,),
            )
            exclusions = {str(row[0]) for row in cur.fetchall()}
            cur.execute(
                "SELECT source_fingerprint FROM legacy_repair_provenance_v1 "
                "WHERE evidence_source='LEGACY_POSITION_REPAIR' "
                "AND source_identity=%s",
                (source_identity,),
            )
            provenance = {str(row[0]) for row in cur.fetchall()}
            cur.execute(
                "SELECT financial_truth_status FROM canonical_financial_truth_v1 "
                "WHERE position_id=%s",
                (position_id,),
            )
            financial_truth_states = {str(row[0]) for row in cur.fetchall()}
            cur.execute(
                "SELECT payload->>'semantic_fingerprint' "
                "FROM position_lifecycle_events_c2_2 WHERE position_id=%s",
                (position_id,),
            )
            lifecycle_fingerprints = {str(row[0]) for row in cur.fetchall()}
            cur.execute(
                "SELECT semantic_fingerprint_expected,semantic_fingerprint_after "
                "FROM legacy_repair_audit_v1 "
                "WHERE incident_type='LEGACY_POSITION' AND incident_identity=%s",
                (str(position_id),),
            )
            audit_fingerprints = {
                str(value) for row in cur.fetchall() for value in row
            }
            if (
                exclusions == {fingerprint}
                and provenance == {fingerprint}
                and financial_truth_states == {"COMPLETE"}
                and lifecycle_fingerprints == {fingerprint}
                and audit_fingerprints == {fingerprint}
            ):
                return "ALREADY_REPAIRED"
            raise RuntimeError("IDEMPOTENCY_CONFLICT")
        raise RuntimeError("PARTIAL_REPAIR_STATE")

    @staticmethod
    def _learning_snapshot(cur, position_id: int) -> Mapping[str, Any]:
        snapshot: dict[str, Any] = {}
        specs = {
            "learning_feature_warehouse_v1": "evidence_status",
            "decision_replay_v1": "replay_status",
        }
        for table, status_column in specs.items():
            cur.execute(
                f"SELECT decision_key,{status_column} FROM {table} "
                "WHERE position_id=%s ORDER BY decision_key",
                (position_id,),
            )
            snapshot[table] = _rows(cur)
        for table in (
            "exit_trace_v1", "exit_trace_v2", "exit_trace_v3",
            "decision_outcomes_v1",
        ):
            cur.execute(f"SELECT count(*) FROM {table} WHERE position_id=%s", (position_id,))
            snapshot[table] = int(cur.fetchone()[0])
        if any(snapshot[table] for table in (
            "exit_trace_v1", "exit_trace_v2", "exit_trace_v3",
            "decision_outcomes_v1",
        )):
            raise RuntimeError("LEARNING_TERMINAL_OR_AMBIGUOUS_ARTIFACT")
        if any(
            row["evidence_status"] != "OPEN_OR_INCOMPLETE"
            for row in snapshot["learning_feature_warehouse_v1"]
        ):
            raise RuntimeError("LEARNING_FEATURE_STATUS_CONFLICT")
        if any(
            row["replay_status"] != "REPLAY_OPEN_OR_INCOMPLETE"
            for row in snapshot["decision_replay_v1"]
        ):
            raise RuntimeError("LEARNING_REPLAY_STATUS_CONFLICT")
        return snapshot

    def _position_plan(
        self,
        connection,
        manifest_row: ManifestPosition,
        *,
        enforce_fingerprint: bool,
        lock: bool = False,
    ) -> PositionPlan | str:
        position_id = manifest_row.position_id
        with connection.cursor() as cur:
            suffix = " FOR UPDATE" if lock else ""
            cur.execute(
                "SELECT id,symbol,strategy,interval,status,side,qty,entry_price,"
                "entry_time,exit_price,exit_time,exit_reason,entry_order_id,"
                "exit_order_id,inventory_evidence_status,"
                "gross_entry_executed_qty,entry_base_fee_qty,"
                "net_entry_inventory_qty,cumulative_exit_executed_qty,"
                "exit_inventory_reduction_qty,remaining_inventory_qty,"
                "terminal_dust_qty,terminal_reason "
                "FROM positions WHERE id=%s" + suffix,
                (position_id,),
            )
            position = _one(cur)
            if position is None:
                raise RuntimeError("POSITION_NOT_FOUND")
            state = self._repair_state(cur, position_id, manifest_row.semantic_fingerprint)
            if state == "ALREADY_REPAIRED":
                return state
            if str(position["status"]) != "OPEN":
                raise RuntimeError("POSITION_NOT_OPEN")
            if str(position["entry_order_id"]) != manifest_row.entry_order_id:
                raise RuntimeError("ENTRY_ORDER_IDENTITY_MISMATCH")
            if str(position["exit_order_id"]) != manifest_row.exit_order_id:
                raise RuntimeError("EXIT_ORDER_IDENTITY_MISMATCH")
            order_ids = [manifest_row.entry_order_id, manifest_row.exit_order_id]
            cur.execute(
                "SELECT id,created_at,symbol,side,client_order_id,order_id,status,"
                "position_id,is_exit,strategy,interval,order_purpose,requested_qty,"
                "order_accepted,exchange_source,reconciliation_status,"
                "reconciled_position_id,reconciled_at,reconciled_fill_count,"
                "reconciled_executed_qty,unreconciled_qty,last_reconciliation_action "
                "FROM binance_orders WHERE order_id=ANY(%s) ORDER BY order_id" + suffix,
                (order_ids,),
            )
            orders = _rows(cur)
            if len(orders) != 2 or {str(row["order_id"]) for row in orders} != set(order_ids):
                raise RuntimeError("ORDER_EVIDENCE_NOT_EXACT")
            if any(int(row["id"]) in FORBIDDEN_DB_ORDER_ROW_IDS for row in orders):
                raise RuntimeError("FORBIDDEN_INCIDENT_IDENTITY")
            if any(str(row["exchange_source"]).lower() != "okx" for row in orders):
                raise RuntimeError("ORDER_EXCHANGE_IDENTITY_MISMATCH")
            cur.execute(
                "SELECT id,source,order_id,symbol,side,role,executed_qty,avg_price,"
                "quote_notional_usdc,commission_amount,commission_asset,"
                "commission_usdc,event_time,trade_id "
                "FROM binance_order_fills WHERE order_id=ANY(%s) "
                "ORDER BY event_time,id" + suffix,
                (order_ids,),
            )
            fills = _rows(cur)
            entry_fills = tuple(
                row for row in fills if str(row["order_id"]) == manifest_row.entry_order_id
            )
            exit_fills = tuple(
                row for row in fills if str(row["order_id"]) == manifest_row.exit_order_id
            )
            if not entry_fills:
                raise RuntimeError("MISSING_ENTRY_FILL")
            if not exit_fills:
                raise RuntimeError("MISSING_EXIT_FILL")
            if any(
                row["commission_amount"] is None or not row["commission_asset"]
                or row["commission_usdc"] is None
                for row in fills
            ):
                raise RuntimeError("MISSING_FEE_EVIDENCE")
            cur.execute(
                "SELECT ingestion_id,order_id,trade_id,correction_revision,"
                "applied_fingerprint,application_status "
                "FROM exchange_fill_ingestion_state_v2 WHERE order_id=ANY(%s) "
                "ORDER BY ingestion_id" + suffix,
                (order_ids,),
            )
            ingestion = _rows(cur)
            if any(int(row["ingestion_id"]) in FORBIDDEN_INGESTION_IDS for row in ingestion):
                raise RuntimeError("FORBIDDEN_INCIDENT_IDENTITY")
            cur.execute(
                "SELECT ingestion_id,source,account_identity_key,symbol,order_id,"
                "trade_id,correction_revision,source_fingerprint,"
                "applied_fingerprint,applied_at,application_status,adoption_id,"
                "contract_generation,local_fill_id "
                "FROM exchange_fill_ingestion_state_v2 WHERE order_id=ANY(%s) "
                "ORDER BY ingestion_id" + suffix,
                (order_ids,),
            )
            correction_ingestion = _rows(cur)
            correction_trust_source, equivalence_proofs = resolve_correction_trust(
                cur, correction_ingestion,
            )
            learning_snapshot = self._learning_snapshot(cur, position_id)

        account_fingerprint = self.exchange.account_fingerprint()
        instrument = dict(self.exchange.instrument(str(position["symbol"])))
        lot_size = _decimal(instrument["lot_size"])
        min_size = _decimal(instrument["min_size"])
        min_notional = _decimal(instrument.get("min_notional") or "0")
        limits = InstrumentExecutionLimits(
            lot_size, min_size, min_notional, None, min_notional == 0,
        )
        okx_orders = {
            order_id: dict(self.exchange.order(str(position["symbol"]), order_id))
            for order_id in order_ids
        }
        okx_fills = {
            order_id: tuple(self.exchange.fills(str(position["symbol"]), order_id))
            for order_id in order_ids
        }
        for order_id in order_ids:
            order = okx_orders[order_id]
            if order.get("order_id") != order_id or order.get("state") != "filled":
                raise RuntimeError("OKX_ORDER_NOT_FILLED")
            db_rows = [row for row in fills if str(row["order_id"]) == order_id]
            exchange_rows = okx_fills[order_id]
            by_trade = {str(row["trade_id"]): row for row in exchange_rows}
            if len(by_trade) != len(exchange_rows) or {
                str(row["trade_id"]) for row in db_rows
            } != set(by_trade):
                raise RuntimeError("OKX_FILL_HIGH_WATER_MISMATCH")
            for db_fill in db_rows:
                exchange_fill = by_trade[str(db_fill["trade_id"])]
                calculated_notional = (
                    _decimal(db_fill["executed_qty"])
                    * _decimal(db_fill["avg_price"])
                )
                if _decimal(db_fill["quote_notional_usdc"]) != calculated_notional:
                    raise RuntimeError("DB_FILL_NOTIONAL_CONFLICT")
                comparisons = (
                    (_decimal(db_fill["executed_qty"]), _decimal(exchange_fill["quantity"])),
                    (_decimal(db_fill["avg_price"]), _decimal(exchange_fill["price"])),
                    (_decimal(db_fill["commission_amount"]), _decimal(exchange_fill["fee_quantity"])),
                    (str(db_fill["commission_asset"]).upper(), str(exchange_fill["fee_asset"]).upper()),
                    (str(db_fill["side"]).upper(), str(exchange_fill["side"]).upper()),
                    (
                        int(db_fill["event_time"].timestamp() * 1000),
                        int(str(exchange_fill["event_time_ms"])),
                    ),
                )
                if any(left != right for left, right in comparisons):
                    raise RuntimeError("OKX_FILL_EVIDENCE_CONFLICT")
                ingestion_row = next(
                    (
                        row for row in correction_ingestion
                        if str(row["order_id"]) == str(db_fill["order_id"])
                        and str(row["trade_id"]) == str(db_fill["trade_id"])
                    ),
                    None,
                )
                if ingestion_row is None:
                    if correction_trust_source == "CANONICAL_OKX_DIRECT_EVIDENCE":
                        continue
                    raise RuntimeError("INGESTION_FILL_IDENTITY_MISSING")
                proof = equivalence_proofs.get(int(ingestion_row["ingestion_id"]))
                if proof is not None:
                    canonical_semantic = fill_semantic_payload(
                        source=db_fill["source"],
                        account_identity_key=ingestion_row["account_identity_key"],
                        symbol=db_fill["symbol"], trade_id=db_fill["trade_id"],
                        order_id=db_fill["order_id"], side=db_fill["side"],
                        quantity=db_fill["executed_qty"], price=db_fill["avg_price"],
                        fee_quantity=db_fill["commission_amount"],
                        fee_currency=db_fill["commission_asset"],
                        event_time_ms=db_fill["event_time"],
                        canonical_local_fill_id=db_fill["id"],
                    )
                    fresh_okx_semantic = fill_semantic_payload(
                        source=db_fill["source"],
                        account_identity_key=ingestion_row["account_identity_key"],
                        symbol=db_fill["symbol"], trade_id=exchange_fill["trade_id"],
                        order_id=exchange_fill["order_id"], side=exchange_fill["side"],
                        quantity=exchange_fill["quantity"], price=exchange_fill["price"],
                        fee_quantity=exchange_fill["fee_quantity"],
                        fee_currency=exchange_fill["fee_asset"],
                        event_time_ms=exchange_fill["event_time_ms"],
                        canonical_local_fill_id=db_fill["id"],
                    )
                    if (
                        equivalence_fingerprint(canonical_semantic)
                        != str(proof["canonical_fill_fingerprint"])
                        or equivalence_fingerprint(fresh_okx_semantic)
                        != str(proof["okx_truth_fingerprint"])
                    ):
                        raise RuntimeError("BLOCKED_BY_STALE_EQUIVALENCE_PROOF")
            total = sum((_decimal(row["executed_qty"]) for row in db_rows), Decimal("0"))
            if _decimal(order["executed_qty"]) != total:
                raise RuntimeError("OKX_ORDER_FILL_TOTAL_MISMATCH")
            db_order = next(row for row in orders if str(row["order_id"]) == order_id)
            if _decimal(db_order["reconciled_executed_qty"] or 0) != total:
                raise RuntimeError("DB_RECONCILED_HIGH_WATER_MISMATCH")
            if _decimal(db_order["unreconciled_qty"] or 0) != 0:
                raise RuntimeError("PENDING_FILL_PRESENT")

        inventory = project_inventory_from_execution_evidence(
            symbol=str(position["symbol"]), entry_fills=entry_fills,
            exit_fills=exit_fills,
        )
        if inventory.evidence_status is not InventoryEvidenceStatus.COMPLETE:
            raise RuntimeError(
                "INVENTORY_EVIDENCE_INCOMPLETE:" + ",".join(inventory.incomplete_reasons)
            )
        classification = classify_exit_inventory(
            previous_remaining_qty=(
                position["remaining_inventory_qty"]
                if position["remaining_inventory_qty"] is not None
                else inventory.net_entry_inventory_qty
            ),
            cumulative_exit_inventory_reduction_qty=inventory.exit_inventory_reduction_qty,
            previous_cumulative_exit_inventory_reduction_qty=(
                position["exit_inventory_reduction_qty"] or 0
            ),
            inventory=inventory, limits=limits, tolerance=lot_size,
        )
        if classification.status not in {
            ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
            ExitInventoryStatus.TERMINAL_DUST_CLOSE,
        }:
            raise RuntimeError("TERMINAL_CLASSIFICATION_REQUIRED")

        base_asset = str(instrument["base_asset"]).upper()
        quote_asset = str(instrument["quote_asset"]).upper()
        canonical_fills = []
        for row in fills:
            fee = value_fee(
                quantity=_decimal(row["commission_amount"]),
                asset=str(row["commission_asset"]), base_asset=base_asset,
                quote_asset=quote_asset, fill_price=_decimal(row["avg_price"]),
            )
            if fee.valued_fee_usdc is None:
                raise RuntimeError("CANONICAL_FEE_VALUATION_INCOMPLETE")
            purpose = (
                "ENTRY" if str(row["order_id"]) == manifest_row.entry_order_id
                else "EXIT"
            )
            canonical_fills.append(FillEvidence(
                fill_id=f"exchange:{row['id']}", order_id=str(row["order_id"]),
                position_id=position_id, purpose=purpose,
                side=str(row["side"]).upper(), symbol=str(row["symbol"]),
                quantity=_decimal(row["executed_qty"]),
                price=_decimal(row["avg_price"]),
                notional=_decimal(row["quote_notional_usdc"]),
                fee_quantity=_decimal(row["commission_amount"]),
                fee_asset=str(row["commission_asset"]).upper(),
                authoritative_fee_usdc=fee.valued_fee_usdc,
                estimated_fee_usdc=None, event_time=row["event_time"],
                source_authority="EXCHANGE_EXECUTION", source_exchange="okx",
                source_environment="live", source_deployment_id=EXPECTED_DEPLOYMENT,
                account_identity_fingerprint=account_fingerprint,
                instrument_metadata_fingerprint=str(instrument["metadata_fingerprint"]),
                step_size=lot_size, base_asset=base_asset, quote_asset=quote_asset,
                source_version="LOCAL_LIVE_LEGACY_RESIDUAL_OKX_GET_V1",
            ))
        financial_truth = calculate_financial_truth(
            position_id=position_id, position_status="CLOSED",
            fills=tuple(canonical_fills), position_symbol=str(position["symbol"]),
            inventory_classification=classification,
        )
        if financial_truth.financial_truth_status != "COMPLETE":
            raise RuntimeError(
                "CANONICAL_FT_WRITER_GAP:"
                + str(financial_truth.failure_detail or financial_truth.failure_code)
            )

        stable_orders = [{
            key: row.get(key) for key in (
                "id", "symbol", "side", "client_order_id", "order_id",
                "position_id", "is_exit", "strategy", "interval",
                "order_purpose", "requested_qty", "order_accepted",
                "exchange_source", "reconciliation_status",
                "reconciled_position_id", "reconciled_fill_count",
                "reconciled_executed_qty", "unreconciled_qty",
                "last_reconciliation_action",
            )
        } for row in orders]
        stable_fills = [{
            key: row.get(key) for key in (
                "id", "source", "order_id", "symbol", "side", "role",
                "executed_qty", "avg_price", "quote_notional_usdc",
                "commission_amount", "commission_asset", "commission_usdc",
                "event_time", "trade_id",
            )
        } for row in fills]
        fingerprint_payload = {
            "contract_version": CONTRACT_VERSION,
            "environment": EXPECTED_ENVIRONMENT,
            "deployment_id": EXPECTED_DEPLOYMENT,
            "database": EXPECTED_DATABASE,
            "position_before": position,
            "orders": stable_orders,
            "fills": stable_fills,
            "okx_orders": okx_orders,
            "okx_fills": okx_fills,
            "instrument": instrument,
            "account_fingerprint": account_fingerprint,
            "ingestion_high_water": ingestion,
            "correction_trust_source": correction_trust_source,
            "equivalence_proofs": stable_equivalence_proof_evidence(
                equivalence_proofs,
            ),
            "learning_snapshot": learning_snapshot,
            "inventory": asdict(inventory),
            "classification": asdict(classification),
            "financial_truth": financial_truth.semantic_values(),
            "planned_mutations": PLANNED_MUTATIONS,
            "position_update_allowlist": sorted(POSITION_UPDATE_ALLOWLIST),
            "forbidden_incident": {
                "db_order_rows": sorted(FORBIDDEN_DB_ORDER_ROW_IDS),
                "ingestion_ids": sorted(FORBIDDEN_INGESTION_IDS),
                "exchange_orders": sorted(FORBIDDEN_EXCHANGE_ORDER_IDS),
            },
        }
        fingerprint_payload = _json_safe(fingerprint_payload)
        fingerprint = semantic_repair_fingerprint(fingerprint_payload)
        if enforce_fingerprint and fingerprint != manifest_row.semantic_fingerprint:
            raise RuntimeError(f"SEMANTIC_FINGERPRINT_DRIFT:{position_id}")
        return PositionPlan(
            position_id,
            f"{position['symbol']}:{position['strategy']}:{position['interval']}",
            manifest_row.entry_order_id, manifest_row.exit_order_id,
            tuple(int(row["id"]) for row in entry_fills),
            tuple(int(row["id"]) for row in exit_fills), inventory,
            classification, financial_truth, min_size, lot_size, fingerprint,
            correction_trust_source, "PLANNED", PLANNED_MUTATIONS,
            fingerprint_payload,
        )

    def _plan(
        self, *, enforce_fingerprints: bool, require_complete_proofs: bool,
    ) -> RunPlan:
        self._runtime_gates()
        pending = self.exchange.pending_spot_orders()
        if pending:
            raise RuntimeError("OKX_PENDING_SPOT_ORDERS")
        connection = self.connection_factory()
        try:
            self._begin_read_only(connection)
            panic = self._database_safety(connection)
            if require_complete_proofs:
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT count(*),"
                        "count(*) FILTER (WHERE proof_status='VALID'),"
                        "count(*) FILTER (WHERE proof_status<>'VALID') "
                        "FROM v_legacy_fill_equivalence_proof_status_v1"
                    )
                    total, valid, invalid = cur.fetchone()
                    if (int(total), int(valid), int(invalid)) != (8, 8, 0):
                        raise RuntimeError("CANDIDATE_PROOF_GATE_FAILED")
            plans = []
            already = []
            for manifest_row in self.manifest.positions:
                item = self._position_plan(
                    connection, manifest_row,
                    enforce_fingerprint=enforce_fingerprints,
                )
                if item == "ALREADY_REPAIRED":
                    already.append(manifest_row.position_id)
                else:
                    plans.append(item)
            connection.rollback()
            if self.exchange.place_order_calls or self.exchange.cancel_order_calls:
                raise RuntimeError("EXCHANGE_MUTATION_DETECTED")
            return RunPlan(tuple(plans), tuple(already), (), panic)
        finally:
            connection.close()

    def plan(self) -> RunPlan:
        return self._plan(
            enforce_fingerprints=True, require_complete_proofs=False,
        )

    def generate_manifest_candidate(self) -> RunPlan:
        fingerprints = {
            item.semantic_fingerprint for item in self.manifest.positions
        }
        if fingerprints != {PLACEHOLDER_FINGERPRINT}:
            raise RuntimeError("CANDIDATE_REQUIRES_ALL_PLACEHOLDERS")
        return self._plan(
            enforce_fingerprints=False, require_complete_proofs=True,
        )

    @staticmethod
    def _assert_learning_excluded(cur, position_id: int) -> None:
        for view in ELIGIBLE_LEARNING_VIEWS:
            cur.execute(f"SELECT count(*) FROM {view} WHERE position_id=%s", (position_id,))
            if int(cur.fetchone()[0]):
                raise RuntimeError("LEARNING_READER_EXCLUSION_FAILED:" + view)

    def apply(
        self,
        *,
        apply_requested: bool,
        environment: str,
        deployment_id: str,
        manifest_path: str | Path,
        stage_hook: Callable[[int, str], None] | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        if not apply_requested:
            raise RuntimeError("APPLY_FLAG_REQUIRED")
        if environment != EXPECTED_ENVIRONMENT:
            raise RuntimeError("APPLY_ENVIRONMENT_GATE_FAILED")
        if deployment_id != EXPECTED_DEPLOYMENT:
            raise RuntimeError("APPLY_DEPLOYMENT_GATE_FAILED")
        if not str(manifest_path):
            raise RuntimeError("APPLY_MANIFEST_GATE_FAILED")
        if os.environ.get(APPLY_ENABLE_ENV) != "1":
            raise RuntimeError("APPLY_ENV_FLAG_DISABLED")
        initial = self.plan()
        results = [
            {"position_id": position_id, "status": "ALREADY_REPAIRED", "writes": 0}
            for position_id in initial.already_repaired
        ]
        for planned in initial.positions:
            connection = self.connection_factory()
            try:
                connection.rollback()
                connection.set_session(
                    isolation_level="SERIALIZABLE", readonly=False,
                    autocommit=False,
                )
                with connection.cursor() as cur:
                    cur.execute("SET LOCAL lock_timeout='5s'")
                    cur.execute("SET LOCAL statement_timeout='90s'")
                    self._database_safety(connection)
                    if self.exchange.pending_spot_orders():
                        raise RuntimeError("OKX_PENDING_SPOT_ORDERS")
                    locked = self._position_plan(
                        connection, self._manifest_by_id[planned.position_id],
                        enforce_fingerprint=True, lock=True,
                    )
                    if locked == "ALREADY_REPAIRED":
                        connection.rollback()
                        results.append({
                            "position_id": planned.position_id,
                            "status": "ALREADY_REPAIRED", "writes": 0,
                        })
                        continue
                    if stage_hook:
                        stage_hook(planned.position_id, "locked")
                    exclusion_id = LearningOutcomeExclusionRepository.insert(
                        cur, environment=EXPECTED_ENVIRONMENT,
                        deployment_id=EXPECTED_DEPLOYMENT,
                        position_id=locked.position_id,
                        semantic_fingerprint_v2=locked.semantic_fingerprint,
                        git_sha=self.expected_git_sha,
                    )
                    if stage_hook:
                        stage_hook(planned.position_id, "exclusion")
                    mutation = apply_inventory_lifecycle_mutation(
                        cur, position_id=locked.position_id,
                        order_id=locked.exit_order_id,
                        inventory=locked.inventory,
                        limits=InstrumentExecutionLimits(
                            locked.lot_size, locked.min_size, Decimal("0"),
                            None, True,
                        ),
                        previous_remaining_qty=locked.inventory.net_entry_inventory_qty,
                        previous_exit_high_water=Decimal("0"),
                        has_exit_evidence=True,
                        exit_price=locked.financial_truth.authoritative_exit_notional
                        / locked.inventory.cumulative_exit_executed_qty,
                        exit_time=locked.financial_truth.last_exit_fill_at,
                        exit_reason=(
                            "TERMINAL_DUST" if locked.classification.status
                            is ExitInventoryStatus.TERMINAL_DUST_CLOSE
                            else "RECONCILED_OKX_EXIT_FILL"
                        ),
                        execution_source=CONTRACT_VERSION,
                        event_payload={
                            "semantic_fingerprint": locked.semantic_fingerprint,
                            "contract_version": CONTRACT_VERSION,
                        },
                        normalization_tolerance=locked.lot_size,
                    )
                    if mutation.position_status != "CLOSED" or not mutation.event_inserted:
                        raise RuntimeError("INVENTORY_LIFECYCLE_POSTCONDITION_FAILED")
                    if stage_hook:
                        stage_hook(planned.position_id, "position_lifecycle")
                    written = CanonicalFinancialTruthWriteRepository.write(
                        cur, locked.financial_truth,
                        invocation_type=CONTRACT_VERSION,
                        invocation_identity=(
                            f"LIVE:{EXPECTED_DEPLOYMENT}:{locked.position_id}:"
                            f"{locked.semantic_fingerprint}"
                        ),
                    )
                    if not written:
                        raise RuntimeError("FINANCIAL_TRUTH_WRITE_CONFLICT")
                    if stage_hook:
                        stage_hook(planned.position_id, "financial_truth")
                    now = datetime.now(timezone.utc)
                    invocation = (
                        f"LIVE:{EXPECTED_DEPLOYMENT}:{locked.position_id}:"
                        f"{locked.semantic_fingerprint}"
                    )
                    LegacyRepairAuditRepository.append(cur, {
                        "incident_type": "LEGACY_POSITION",
                        "incident_identity": str(locked.position_id),
                        "operation_type": "LOCAL_LIVE_RESIDUAL_REPAIR",
                        "planner_version": PLANNER_VERSION,
                        "writer_version": WRITER_VERSION,
                        "semantic_fingerprint_before": locked.semantic_fingerprint,
                        "semantic_fingerprint_expected": locked.semantic_fingerprint,
                        "semantic_fingerprint_after": locked.semantic_fingerprint,
                        "plan_status": "ELIGIBLE",
                        "execution_status": "APPLIED",
                        "invocation_identity": invocation,
                        "requested_at": now, "started_at": now,
                        "completed_at": now,
                        "actor_source": "BOUNDED_REPAIR_SERVICE",
                        "blocking_reasons": [],
                        "eligible_actions": list(PLANNED_MUTATIONS),
                        "executed_actions": list(PLANNED_MUTATIONS),
                        "expected_changes": list(PLANNED_MUTATIONS),
                        "actual_changes": list(PLANNED_MUTATIONS),
                        "post_state_invariants": [
                            "POSITION_CLOSED", "FINANCIAL_TRUTH_COMPLETE",
                            "LEARNING_EXCLUDED", "NO_EXCHANGE_MUTATION",
                        ],
                        "error_code": None, "error_detail": None,
                    })
                    source_identity = (
                        f"LIVE:{EXPECTED_DEPLOYMENT}:{EXPECTED_DATABASE}:"
                        f"position:{locked.position_id}"
                    )
                    LegacyProvenanceRepository.record(cur, {
                        "evidence_source": "LEGACY_POSITION_REPAIR",
                        "source_identity": source_identity,
                        "source_fingerprint": locked.semantic_fingerprint,
                        "instrument_identity": locked.slot.split(":", 1)[0],
                        "account_provenance": {
                            "source": "CURRENT_OKX_GET",
                            "fill_ids": list(
                                locked.entry_fill_ids + locked.exit_fill_ids
                            ),
                        },
                        "deployment_provenance": {
                            "environment": EXPECTED_ENVIRONMENT,
                            "deployment_id": EXPECTED_DEPLOYMENT,
                            "database": EXPECTED_DATABASE,
                            "git_sha": self.expected_git_sha,
                        },
                        "fee_evidence": {
                            "entry_base_fee_qty": str(
                                locked.inventory.entry_base_fee_qty
                            ),
                        },
                        "valuation_evidence": {
                            "financial_truth_status": "COMPLETE",
                            "authoritative_net_pnl": str(
                                locked.financial_truth.authoritative_net_pnl
                            ),
                        },
                        "immutable_payload": dict(locked.immutable_payload),
                        "observed_at": now,
                    })
                    if stage_hook:
                        stage_hook(planned.position_id, "audit_provenance")
                    cur.execute(
                        "SELECT status,remaining_inventory_qty FROM positions "
                        "WHERE id=%s",
                        (locked.position_id,),
                    )
                    status, remaining = cur.fetchone()
                    if status != "CLOSED" or _decimal(remaining) != (
                        locked.classification.remaining_inventory_qty
                    ):
                        raise RuntimeError("POSITION_POSTCONDITION_FAILED")
                    cur.execute(
                        "SELECT financial_truth_status FROM "
                        "canonical_financial_truth_v1 WHERE position_id=%s",
                        (locked.position_id,),
                    )
                    if cur.fetchone() != ("COMPLETE",):
                        raise RuntimeError("FINANCIAL_TRUTH_POSTCONDITION_FAILED")
                    cur.execute(
                        "SELECT exclusion_id FROM learning_outcome_exclusion_v1 "
                        "WHERE position_id=%s",
                        (locked.position_id,),
                    )
                    if cur.fetchone() != (exclusion_id,):
                        raise RuntimeError("LEARNING_EXCLUSION_POSTCONDITION_FAILED")
                    self._assert_learning_excluded(cur, locked.position_id)
                connection.commit()
                results.append({
                    "position_id": locked.position_id, "status": "APPLIED",
                    "writes": len(PLANNED_MUTATIONS),
                    "fingerprint": locked.semantic_fingerprint,
                })
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        if self.exchange.place_order_calls or self.exchange.cancel_order_calls:
            raise RuntimeError("EXCHANGE_MUTATION_DETECTED")
        return tuple(results)


def render_plan(plan: RunPlan) -> str:
    payload = {
        "contract_version": CONTRACT_VERSION,
        "positions": [item.public_payload() for item in plan.positions],
        "already_repaired": plan.already_repaired,
        "blocked": plan.blocked,
        "panic_enabled": plan.panic_enabled,
        "summary": plan.summary(),
    }
    return json.dumps(_json_safe(payload), sort_keys=True, indent=2) + "\n"


def render_manifest_candidate(
    plan: RunPlan,
    *,
    generated_from_git_revision: str,
    generated_at: datetime | None = None,
) -> str:
    generated_at = generated_at or datetime.now(timezone.utc)
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "manifest_version": MANIFEST_VERSION,
        "generated_from_git_revision": generated_from_git_revision,
        "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
        "fingerprint_contract_version": FINGERPRINT_CONTRACT_VERSION,
        "proof_contract_version": PROOF_CONTRACT_VERSION,
        "environment": EXPECTED_ENVIRONMENT,
        "deployment_id": EXPECTED_DEPLOYMENT,
        "positions": [
            {
                "position_id": item.position_id,
                "entry_order_id": item.entry_order_id,
                "exit_order_id": item.exit_order_id,
                "semantic_fingerprint": item.semantic_fingerprint,
            }
            for item in plan.positions
        ],
    }
    payload = {
        "candidate_manifest": manifest,
        "evidence": [item.public_payload() for item in plan.positions],
        "summary": plan.summary(),
    }
    return json.dumps(_json_safe(payload), sort_keys=True, indent=2) + "\n"
